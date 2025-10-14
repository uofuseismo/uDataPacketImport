#include <functional>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
//#include <grpcpp/ext/otel_plugin.h>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <spdlog/spdlog.h>
#include <concurrentqueue.h>
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "proto/dataPacketBroadcast.pb.h"
#include "getNow.hpp"

#define APPLICATION_NAME "uDataPacketGRPCProxy"
#define DEFAULT_QUEUE_SIZE 4096

namespace
{

struct ProgramOptions
{
    std::string applicationName{APPLICATION_NAME};
    std::string grpcHost{"0.0.0.0"};
    std::filesystem::path grpcFrontEndServerKey; // e.g., localhost.key
    std::filesystem::path grpcFrontEndServerCertificate; // e.g., localhost.crt
    std::filesystem::path grpcBackEndServerKey; // e.g., localhost.key
    std::filesystem::path grpcBackEndServerCertificate; // e.g., localhost.crt
    uint16_t grpcFrontEndPort{50000};
    uint16_t grpcBackEndPort{50001};
    int verbosity{3};
    int mMaximumInputQueueSize{DEFAULT_QUEUE_SIZE};
    bool grpcFrontEndEnableReflection{false};
    bool grpcBackEndEnableReflection{false};
};

std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[]);
void setVerbosityForSPDLOG(const int verbosity);
::ProgramOptions parseIniFile(const std::filesystem::path &iniFile);

class Stream
{
public:
    explicit Stream(const UDataPacketImport::StreamIdentifier &identifier)
    {
        mIdentifier = identifier;
        mName = identifier.toString();
    }
    void unsubscribeAll()
    {
        std::lock_guard<std::mutex> lock(mMutex);
        mSubscribers.clear();
    }
    mutable std::mutex mMutex;
    std::unordered_set<grpc::ServerContext *> mSubscribers;
 
    UDataPacketImport::StreamIdentifier mIdentifier;
    std::string mName;
};

class PacketPropagator
{
public:
    void inputPacket(UDataPacketImport::GRPC::Packet &&packet)
    {
        // Check for future data?
        if (mRejectFuturePackets)
        {
            auto endTimeMuS = UDataPacketImport::getEndTime(packet);
            if (endTimeMuS > ::getNow())
            {
                UDataPacketImport::StreamIdentifier
                    identifier{packet.stream_identifier()};
                throw std::invalid_argument(identifier.toString()
                                          + " has future data");
            }
        }
        auto queueSize = mQueue.size_approx();
        if (queueSize > mMaximumInputQueueSize)
        {
            for (size_t i = 0; i < queueSize; ++i)
            {
                if (mQueue.size_approx() < mMaximumInputQueueSize){break;}
                UDataPacketImport::GRPC::Packet work;
                if (!mQueue.try_dequeue(work))
                {
                    spdlog::warn("Failed to pop packet");
                }
            }
            if (!mQueue.try_enqueue(std::move(packet)))
            {
                spdlog::warn("Failed to pop packet back");
            }
        }
        mQueue.try_enqueue(std::move(packet)); 
    }
    void outputPacket() 
    {

    }
    std::function<void(UDataPacketImport::GRPC::Packet &&)>
        mPublisherCallbackFunction
    {
        std::bind(&::PacketPropagator::inputPacket, this,
                  std::placeholders::_1)
    };
//private:
    moodycamel::ConcurrentQueue<UDataPacketImport::GRPC::Packet> mQueue{DEFAULT_QUEUE_SIZE};
    size_t mMaximumInputQueueSize{DEFAULT_QUEUE_SIZE};
    bool mRejectFuturePackets{true};
};
 
}

class Proxy
{
public:
    [[nodiscard]] bool publishCallback(UDataPacketImport::GRPC::Packet &&packet)
    {
        auto approximateSize = static_cast<int> (mInputQueue.size_approx());
        if (approximateSize > mMaximumQueueSize)
        {
            spdlog::warn("Producer popping queue");
            UDataPacketImport::GRPC::Packet temporary;
            while (approximateSize > mMaximumQueueSize)
            {
                if (mInputQueue.try_dequeue(temporary))
                {
                    approximateSize
                        = static_cast<int> (mInputQueue.size_approx()); 
                }
                else
                {
                    break;
                }
            }
        }
        return mInputQueue.try_enqueue(std::move(packet));
    }
    moodycamel::ConcurrentQueue<UDataPacketImport::GRPC::Packet> mInputQueue;//{DEFAULT_QUEUE_SIZE};
    int mMaximumQueueSize{DEFAULT_QUEUE_SIZE};
};

class OutputBroadcastProxyImpl final :
    public UDataPacketImport::GRPC::BroadcastProxy::CallbackService
{
public:
    OutputBroadcastProxyImpl(const ::ProgramOptions &options)
    {
    }

    [[nodiscard]] grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet> 
        *Subscribe(grpc::CallbackServerContext *context,
                   const UDataPacketImport::GRPC::SubscriptionRequest *request)
    {
        class Writer : public grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet>
        {
        public:
            Writer(std::atomic<int> *subscriberCount,
                   std::atomic<bool> *keepRunning,
                   grpc::CallbackServerContext *context) :
                mSubscriberCount(subscriberCount),
                mKeepRunning(keepRunning),
                mContext(context)
            {
                mSubscriberCount->fetch_add(1);
            }
            /// Reacts to write completion.  If write is successful then send
            /// next packet. 
            void OnWriteDone(bool ok) override
            {
                if (!ok)
                {
                    Finish(grpc::Status(grpc::StatusCode::UNKNOWN,
                                        "Unexpected read failure"));
                }
                nextWrite();
            }
            void OnDone() override
            {
                mSubscriberCount->fetch_sub(1);
                delete this;
            }
            void OnCancel() override
            {
                spdlog::info("RPC subscribe canceled");
            }
        private:
            /// Attempt to get next packet from queue and write it
            void nextWrite()
            {
                auto now = ::getNow();
                if (mContext)
                {
                    /// 
                    bool wasCancelled = mContext->IsCancelled();
                    while (mKeepRunning->load() && !wasCancelled)
                    { 
                        wasCancelled = mContext->IsCancelled();
                        if (wasCancelled)
                        {
                            spdlog::info("Subscriber cancelled");
                            break;
                        }
                        UDataPacketImport::Packet packet;
                        // Any data?
                        bool gotPacket = false;
                        if (gotPacket)
                        {
                            auto nextPacket = packet.toProtobuf();
                            StartWrite(&nextPacket);
                            return;
                        }
                    }
                }
                else
                {
                    spdlog::critical("Subscriber context is null");
                }
                // All done either b/c the proxy or client is terminating
                Finish(grpc::Status::OK);
            }
        //private:
            std::atomic<int> *mSubscriberCount{nullptr};
            std::atomic<bool> *mKeepRunning{nullptr};
            grpc::CallbackServerContext *mContext{nullptr};
            std::chrono::microseconds mLastWrite{0};
        };
        return new Writer(&mSubscribersCount, &mKeepRunning, context);
    }
    void stop()
    {
         mKeepRunning = false;
    }
    std::atomic<int> mSubscribersCount{0};
    std::chrono::microseconds mSleepTime{10};
    std::atomic<bool> mKeepRunning{true};
};

class FrontEndBroadcastProxyImpl final :
    public UDataPacketImport::GRPC::BroadcastProxy::CallbackService
{
public:
    FrontEndBroadcastProxyImpl
    (
        const ::ProgramOptions &options,
        const std::function<void (UDataPacketImport::GRPC::Packet &&)> &publishCallback
    ) :
        mPublishCallback(publishCallback)
    {
    }
 
    [[nodiscard]] grpc::ServerReadReactor<UDataPacketImport::GRPC::Packet> *
        PublishPackets(grpc::CallbackServerContext *context,
                       UDataPacketImport::GRPC::EndPublicationResponse *response) override
    {
        // Check the context
        //auto authContext = context->auth_context();

        class Recorder : public grpc::ServerReadReactor<UDataPacketImport::GRPC::Packet>
        {
        public:
            Recorder(UDataPacketImport::GRPC::EndPublicationResponse *response,
                     const std::function<void (UDataPacketImport::GRPC::Packet &&)> &publishCallback,
                     std::atomic<int> *producerCount) :
                mPublishCallback(publishCallback),
                mProducerCount(producerCount)
            {
                mProducerCount->fetch_add(1);
                StartRead(&mPacket);
            }
            // Continue to read from client until an error occurrs
            void OnReadDone(bool ok) override
            {
                if (ok)
                {
                    mPacketsReceived = mPacketsReceived + 1;
                    try
                    {
                        mPublishCallback(std::move(mPacket));
                    }
                    catch (const std::exception &e)
                    {
                        spdlog::warn("Failed to propagate packet; failed with "
                                   + std::string {e.what()});
                    }
                }
                else
                {
                    // Read unsuccessful - set the response and finish
                    mResponse->set_packets_received(mPacketsReceived);
                    mResponse->set_packets_published(mPacketsPublished);
                    Finish(grpc::Status::OK);
                }
            }
            void OnDone() override
            {
                spdlog::info("gRPC packet publisher finished");
                mProducerCount->fetch_sub(1);
                delete this;
            }
            void OnCancel() override
            {
                spdlog::warn("gPRC packet publisher canceled");
            }
        private:
            UDataPacketImport::GRPC::EndPublicationResponse
                *mResponse{nullptr};
            UDataPacketImport::GRPC::Packet mPacket;
            std::function<void (UDataPacketImport::GRPC::Packet &&)>
                mPublishCallback;
            uint64_t mPacketsReceived{0};
            uint64_t mPacketsPublished{0};
            std::atomic<int> *mProducerCount{nullptr};
        };
        return new Recorder(response, mPublishCallback, &mProducerCount);
    }
    // Thread that processes input packets
//private:
    
    //std::shared_ptr
    //<
    //   moodycamel::ConcurrentQueue<UDataPacketImport::GRPC::Packet>
    //> mInputPacketQueue {nullptr};
    std::function<void (UDataPacketImport::GRPC::Packet &&)> mPublishCallback;
    std::atomic<int> mProducerCount{0};
    std::atomic<int> mSubscriberCount{0};
    size_t mMaximumInputQueueSize{4096};
};

void runServer(const ::ProgramOptions &options)
{
    auto frontEndServerAddress = options.grpcHost + ":"
                               + std::to_string(options.grpcFrontEndPort);

    auto backEndServerAddress = options.grpcHost + ":"
                              + std::to_string(options.grpcBackEndPort);

::PacketPropagator packetPropagator;
    ::FrontEndBroadcastProxyImpl
        frontEndServer{options,
                       packetPropagator.mPublisherCallbackFunction};

::OutputBroadcastProxyImpl backEndServer{options};

/*
    StationInformationServiceImpl service{options};
*/

    grpc::EnableDefaultHealthCheckService(true);
/*
    if (options.grpcEnableReflection)
    {
        spdlog::info("Enabling reflection");
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    } 
*/
 
    grpc::ServerBuilder builder;
/*
    if (options.grpcServerKey.empty() || options.grpcServerCertificate.empty())
    {
        spdlog::info("Initiating non-secured server");
        builder.AddListeningPort(serverAddress, grpc::InsecureServerCredentials());
    }
    else
    {
        grpc::SslServerCredentialsOptions::PemKeyCertPair keyCertPair
        {
            ::loadStringFromFile(options.grpcServerKey),
            ::loadStringFromFile(options.grpcServerCertificate) 
        };
        grpc::SslServerCredentialsOptions sslOptions; 
        sslOptions.pem_key_cert_pairs.emplace_back(keyCertPair);
        builder.AddListeningPort(serverAddress,
                                 grpc::SslServerCredentials(sslOptions));
    }
*/
    builder.RegisterService(&frontEndServer);

    std::unique_ptr<grpc::Server> grpcFrontEndServer(builder.BuildAndStart());
/*
    service.setHealthCheckService(server->GetHealthCheckService());
*/

    spdlog::info("Listening on " + frontEndServerAddress); 
    //std::jthread {grpcFrontEndServer->Wait()}; 
}

int main(int argc, char *argv[])
{
    // Get the ini file from the command line
    std::filesystem::path iniFile;
    try 
    {
        auto [iniFileName, isHelp] = ::parseCommandLineOptions(argc, argv);
        if (isHelp){return EXIT_SUCCESS;}
        iniFile = iniFileName;
    }
    catch (const std::exception &e)
    {
        spdlog::error(e.what());
        return EXIT_FAILURE;
    }

    // Read the program properties
    ::ProgramOptions programOptions;
    try 
    {   
        programOptions = ::parseIniFile(iniFile);
    }   
    catch (const std::exception &e) 
    {   
        spdlog::error(e.what());
        return EXIT_FAILURE;
    }   
    ::setVerbosityForSPDLOG(programOptions.verbosity);

    try
    {
    }
    catch (const std::exception &e)
    {
        spdlog::critical(e.what());
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

///--------------------------------------------------------------------------///
///                            Utility Functions                             ///
///--------------------------------------------------------------------------///
namespace
{

/// Read the program options from the command line
std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[])
{
    std::string iniFile;
    boost::program_options::options_description desc(R"""(
The uDataBroadcastGRPCProxy is an intermediary.  Publishers send data packets to
the frontend and subscribers consume packets from the backend.

    uDataBroadcastGRPCProxy --ini=uDataBroadcastGRPCProxy.ini

Allowed options)""");
    desc.add_options()
        ("help", "Produces this help message")
        ("ini",  boost::program_options::value<std::string> (), 
                 "The initialization file for this executable");
    boost::program_options::variables_map vm; 
    boost::program_options::store(
        boost::program_options::parse_command_line(argc, argv, desc), vm); 
    boost::program_options::notify(vm);
    if (vm.count("help"))
    {    
        std::cout << desc << std::endl;
        return {iniFile, true};
    }   
    if (vm.count("ini"))
    {    
        iniFile = vm["ini"].as<std::string>();
        if (!std::filesystem::exists(iniFile))
        {
            throw std::runtime_error("Initialization file: " + iniFile
                                   + " does not exist");
        }
    }   
    return {iniFile, false};
}

void setVerbosityForSPDLOG(const int verbosity)
{
    if (verbosity <= 1)
    {
        spdlog::set_level(spdlog::level::critical);
    }
    if (verbosity == 2){spdlog::set_level(spdlog::level::warn);}
    if (verbosity == 3){spdlog::set_level(spdlog::level::info);}
    if (verbosity >= 4){spdlog::set_level(spdlog::level::debug);}
}

::ProgramOptions parseIniFile(const std::filesystem::path &iniFile)
{
    ::ProgramOptions options;
    if (!std::filesystem::exists(iniFile)){return options;}
    // Parse the initialization file
    boost::property_tree::ptree propertyTree;
    boost::property_tree::ini_parser::read_ini(iniFile, propertyTree);

    // Application name
    options.applicationName
        = propertyTree.get<std::string> ("General.applicationName",
                                         options.applicationName);
    if (options.applicationName.empty())
    {
        options.applicationName = APPLICATION_NAME;
    }
    options.verbosity
        = propertyTree.get<int> ("General.verbosity", options.verbosity);

    options.grpcHost
        = propertyTree.get<std::string> ("gRPC.host",
                                         options.grpcHost);
    if (options.grpcHost.empty())
    {
        throw std::invalid_argument("gRPC end host must be specified");
    }

    // Front end - where the data comes in
    options.grpcFrontEndPort
        = propertyTree.get<uint16_t> ("gRPC.frontEndPort",
                                      options.grpcFrontEndPort);
    options.grpcFrontEndEnableReflection
        = propertyTree.get<bool> ("gRPC.frontEndEnableReflection",
                                  options.grpcFrontEndEnableReflection);

    std::string grpcServerKey = "";
    grpcServerKey
        = propertyTree.get<std::string> ("gRPC.frontEndServerKey",
                                         grpcServerKey);
    std::string grpcServerCertificate = "";
    grpcServerCertificate
        = propertyTree.get<std::string> ("gRPC.frontEndServerCertificate",
                                         grpcServerCertificate);
    if (!grpcServerKey.empty() && !grpcServerCertificate.empty())
    {
        if (!std::filesystem::exists(grpcServerKey))
        {
            throw std::invalid_argument("gRPC front end server key file "
                                      + grpcServerKey + " does not exist");
        }
        if (!std::filesystem::exists(grpcServerCertificate))
        {
            throw std::invalid_argument("gRPC front end server cert file "
                                      + grpcServerCertificate
                                      + " does not exist");
        }
        options.grpcFrontEndServerKey = grpcServerKey;
        options.grpcFrontEndServerCertificate = grpcServerCertificate;
    }


    // Back end - where the data is broadast
/*
    options.grpcBackEndHost
        = propertyTree.get<std::string> ("gRPC.backEndHost",
                                         options.grpcBackEndHost);
    if (options.grpcBackEndHost.empty())
    {   
        throw std::invalid_argument("gRPC back-end host must be specified");
    }   
*/
    options.grpcBackEndPort
        = propertyTree.get<uint16_t> ("gRPC.backEndPort",
                                      options.grpcBackEndPort);
    if (options.grpcBackEndPort == options.grpcFrontEndPort)
    {
        throw std::invalid_argument(
           "Proxy front end port cannot equal back end port");
    }
    options.grpcBackEndEnableReflection
        = propertyTree.get<bool> ("gRPC.backEndEnableReflection",
                                  options.grpcBackEndEnableReflection);

    grpcServerKey.clear();
    grpcServerKey
        = propertyTree.get<std::string> ("gRPC.backEndServerKey",
                                         grpcServerKey);
    grpcServerCertificate.clear();
    grpcServerCertificate
        = propertyTree.get<std::string> ("gRPC.backEndServerCertificate",
                                         grpcServerCertificate);
    if (!grpcServerKey.empty() && !grpcServerCertificate.empty())
    {   
        if (!std::filesystem::exists(grpcServerKey))
        {   
            throw std::invalid_argument("gRPC back end server key file "
                                      + grpcServerKey + " does not exist");
        }   
        if (!std::filesystem::exists(grpcServerCertificate))
        {   
            throw std::invalid_argument("gRPC back end server cert file "
                                      + grpcServerCertificate
                                      + " does not exist");
        }   
        options.grpcBackEndServerKey = grpcServerKey;
        options.grpcBackEndServerCertificate = grpcServerCertificate;
    } 

    return options;
}

}
