#include <string>
#include <cmath>
#include <atomic>
#include <mutex>
#include <functional>
#include <condition_variable>
#include <unordered_map>
#include <csignal>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/ext/otel_plugin.h>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <spdlog/spdlog.h>
#include <readerwriterqueue.h>
#include "uDataPacketImport/seedLink/client.hpp"
#include "uDataPacketImport/seedLink/clientOptions.hpp"
#include "uDataPacketImport/seedLink/streamSelector.hpp"
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.pb.h"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "src/getNow.hpp"
#include "src/loadStringFromFile.hpp"

#define APPLICATION_NAME "uSEEDLinkBroadcast"
#define DEFAULT_IMPORT_QUEUE_SIZE 4096

namespace
{
std::atomic<bool> mInterrupted{false};
/*

volatile bool terminateApplication{false};

void signalHandler(int signal)
{
    if (signal == SIGINT)
    {
        spdlog::info("SIGINT signal received; terminating");
        terminateApplication = true;
    }
    else if (signal == SIGTERM)
    {
        spdlog::info("SIGTERM signal received");
        terminateApplication = true;
    }
    else
    {
        spdlog::warn("Unhandled signal");
    }
}
*/

//std::atomic<bool> mInterrupted{false};

struct ProgramOptions
{
    UDataPacketImport::SEEDLink::ClientOptions seedLinkClientOptions;
    std::string prometheusURL{"localhost:9090"};
    std::string applicationName{APPLICATION_NAME};
    std::string grpcHost{"0.0.0.0"};
    std::string grpcAccessToken;
    std::string grpcServerKey; // e.g., localhost.key
    std::string grpcServerCertificate; // e.g., localhost.crt
    size_t importQueueSize{DEFAULT_IMPORT_QUEUE_SIZE};

/*
    std::string openTelemetrySchema{OTEL_SCHEMA};
    std::string openTelemetryVersion{OTEL_VERSION};
    std::string proxyFrontendAddress{PROXY_FRONTEND_ADDRESS};
    // Maximum time before a send operation returns with EAGAIN
    // -1 waits forever whereas 0 returns immediately.
    std::chrono::milliseconds sendTimeOut{1000}; // 1s is enough
    std::chrono::seconds oldestPacket{-1};
    std::chrono::seconds logPublishingPerformanceInterval{600}; // Every 10 minutes 
    std::chrono::milliseconds openTelemetryExportInterval{60000}; // 1 second
    std::chrono::milliseconds openTelemetryTimeOut{500};
    int sendHighWaterMark{4096};
    size_t maxPublisherQueueSize{MAX_QUEUE_SIZE};
*/
    uint16_t grpcPort{50000};
    int verbosity{3};
    bool grpcEnableReflection{false};
    //bool preventFuturePackets{true};
};

std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[]);
void setVerbosityForSPDLOG(const int verbosity);
::ProgramOptions parseIniFile(const std::filesystem::path &iniFile);

/// The subject in an observer pattern.
class AllPacketsSubject
{
public:
    AllPacketsSubject() = default;
    AllPacketsSubject(const AllPacketsSubject &) = delete;
    AllPacketsSubject& operator=(const AllPacketsSubject &) = delete;

    // Sets the latest packet
    [[nodiscard]] bool setLatestPacket(const UDataPacketImport::Packet &packet)
    {
        try
        {
            auto grpcPacket = packet.toProtobuf();
            setLatestPacket(grpcPacket);
            return true;
        }
        catch (const std::exception &e)
        {
            spdlog::error("Failed to broadcast packet because "
                        + std::string {e.what()}); 
        }
        return false;
    }
    // Sets the latest packet
    void setLatestPacket(const UDataPacketImport::GRPC::Packet &packet)
    {
        //auto now = ::getNow();
        //{
        std::lock_guard<std::mutex> lock(mMutex);
        for (auto &subscriber : mSubscribers)
        {
            if (!subscriber.second->try_enqueue(packet))
            {
                spdlog::warn("Failed to enqueue packet for "
                           + subscriber.first->peer());
            }
        }
        //mLatestPacket = std::move(packet);
        //mLastUpdate = now; 
        //mHavePacket = true;
        //}
    }
    // Convenience function for subscriber to get next packet 
    [[nodiscard]] std::optional<UDataPacketImport::GRPC::Packet>
        getNextPacket(grpc::CallbackServerContext *context) const noexcept
    {
        UDataPacketImport::GRPC::Packet packet;
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mSubscribers.find(context);
        if (idx == mSubscribers.end())
        {
            spdlog::warn(context->peer() + " not subscribed");
            return std::nullopt;
        }
        if (idx->second->try_dequeue(packet))
        {
            return std::optional<UDataPacketImport::GRPC::Packet> (std::move(packet));
        }
        }
        return std::nullopt;
    }
    void unsubscribeAll()
    {
        size_t count{0};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        count = mSubscribers.size();
        mSubscribers.clear();
        }
        if (count > 0)
        {
            spdlog::info("Purged " + std::to_string(count) + " subscribers");
        }
    }
    void unsubscribe(grpc::CallbackServerContext *context)
    {
        size_t count{0};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        count = mSubscribers.erase(context);
        }
        if (count > 0)
        {
            spdlog::info("Unsubscribed " + context->peer());
        }
        else
        {
            spdlog::info(context->peer() + " not found in map");
        }
    }
    [[nodiscard]] bool subscribe(grpc::CallbackServerContext *context)
    {
        std::lock_guard<std::mutex> lock(mMutex);
        if (mSubscribers.contains(context))
        {
            spdlog::debug(context->peer() + " already subscribed");
            return false;
        }
        auto newQueue
            = std::make_unique<
                moodycamel::ReaderWriterQueue<UDataPacketImport::GRPC::Packet>
              > (mSubscriberQueueSize);
        if (mSubscribers.insert({context, std::move(newQueue)}).second)
        {
            spdlog::info("Subscribed " + context->peer());
            return true;
        }
        throw std::runtime_error("Failed to subscribe " 
                               + context->peer());
    }
    mutable std::mutex mMutex;
    std::unordered_map<
        grpc::CallbackServerContext *, 
        std::unique_ptr<
          moodycamel::ReaderWriterQueue<UDataPacketImport::GRPC::Packet>
        >
    > mSubscribers;
    //UDataPacketImport::GRPC::Packet mLatestPacket;
    //std::chrono::microseconds mLastUpdate{0};
    size_t mSubscriberQueueSize{32};
    bool mHavePacket{false};
};

class StreamMetrics
{
public:
    StreamMetrics(const std::string &name,
                  UDataPacketImport::Packet &&packet) :
        mName(name),
        mLatestPacket(std::move(packet))
    {
        auto now = ::getNow();
        auto endTime = packet.getEndTime(); 
        if (endTime < now)
        {
            mLatency = now - endTime;
        }
        mMostRecentSample = endTime;
    }
    void operator()(UDataPacketImport::Packet &&packet)
    {
        auto endTime = packet.getEndTime();
        //mMostRecentSample = std::max(endTime, mMostRecentSample);
    }
    std::string mName;
    UDataPacketImport::Packet mLatestPacket;
    std::chrono::microseconds mLatency{0};
    std::chrono::microseconds mMostRecentSample{0};
    std::chrono::microseconds mCreationTime{::getNow()};
    
};

//class UDataPacketImport::GRPC::ServerImpl final
class ServiceImpl final :
    public UDataPacketImport::GRPC::SEEDLinkBroadcast::CallbackService
{
public:
    explicit ServiceImpl(const ::ProgramOptions &options) :
        mOptions(options)
    {
        mImportQueue = std::make_unique<moodycamel::ReaderWriterQueue<UDataPacketImport::Packet>> (mOptions.importQueueSize);
        mSEEDLinkClient
            = std::make_unique<UDataPacketImport::SEEDLink::Client>
              (mPacketBroadcastCallbackFunction,
               options.seedLinkClientOptions);
    }

    ~ServiceImpl()
    {
        spdlog::info("In serviceimpl destructor");
        stop();
    }

    void stop()
    {
        mKeepRunning = false;
        mAllPacketsSubject.unsubscribeAll();
        if (mNotificationThread.joinable()){mNotificationThread.join();}
        if (mSEEDLinkClient){mSEEDLinkClient->stop();}
    }

    void start()
    {
#ifndef NDEBUG
        assert(mSEEDLinkClient);
#endif
        mNotificationThread = std::thread(&::ServiceImpl::broadcastPackets, this);
        mSEEDLinkClient->start();
    }

    void importPackets(std::vector<UDataPacketImport::Packet> &&packets)
    {
        if (packets.empty()){return;}
        auto approximateQueueSize = mImportQueue->size_approx();
        if (approximateQueueSize >= mOptions.importQueueSize)
        {
            spdlog::warn("SEEDLink thread popping elements from queue");
            while (mImportQueue->size_approx() >=  mOptions.importQueueSize)
            {
                mImportQueue->pop();
            }
        }
        // Enqueue 
        for (auto &packet : packets)
        {
            if (!mImportQueue->try_enqueue(std::move(packet)))
            {
                spdlog::warn("Failed to add packet");
            }
        }
    }

    void broadcastPackets()
    {
        std::chrono::milliseconds timeOut{15};
        while (mKeepRunning)
        {
            UDataPacketImport::Packet packet; 
            if (mImportQueue->try_dequeue(packet))
            {
                try
                {
                    //spdlog::info("howdy");
                    if (!mAllPacketsSubject.setLatestPacket(packet))
                    {
                        spdlog::warn("Failed to publish latest packet");
                    }
                }
                catch (const std::exception &e)
                {
                    spdlog::error(e.what());
                }
            }
            else
            {
                std::this_thread::sleep_for(timeOut);
            }
        }
    }

    grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet> *
        Subscribe(grpc::CallbackServerContext *context,
                  const UDataPacketImport::GRPC::SubscribeToAllStreamsRequest *request) override
    {
        class Subscriber : public grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet>
        {
        public:
            Subscriber(const std::string &accessToken,
                       grpc::CallbackServerContext *context,
                       ::AllPacketsSubject *allPacketsSubject,
                       std::atomic<bool> *keepRunning,
                       std::atomic<int> *subscribersCount) :
                mContext(context),
                mAllPacketsSubject(allPacketsSubject),
                mKeepRunning(keepRunning),
                mSubscribersCount(subscribersCount)
            {
                mPeer = context->peer();
                if (!accessToken.empty())
                {
                    bool validated = false;
                    auto meta = context->client_metadata();
                    for (const auto &item : meta)
                    {
                        if (item.first == "x-custom-auth-token")
                        {
                            if (item.second == accessToken)
                            {
                                spdlog::info("Validated " + mPeer + "'s token");
                                validated = true;
                            }
                        }
                    }
                    if (!validated)
                    {
                        spdlog::info(mPeer + " rejected");
                        grpc::Status status{grpc::StatusCode::UNAUTHENTICATED,
                                            "Client must provide access token in x-custom-auth-token header"};
                        Finish(status);
                    }
                }
                // Subscribe
                try
                {
                    if (!mAllPacketsSubject->subscribe(context))
                    {
                        grpc::Status status{grpc::StatusCode::ALREADY_EXISTS,
                               "Client already subscribed - unsubscribe first"};
                        Finish(status);
                    }
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Subscription failed because "
                               + std::string {e.what()});
                    grpc::Status status{grpc::StatusCode::INTERNAL,
                                 "Internal error - could not subscribe client"};
                    Finish(status);
                }
                mSubscribersCount->fetch_add(1);
                nextWrite();
            }
            void OnWriteDone(bool ok) override
            {
                if (!ok)
                {
                    Finish(grpc::Status(grpc::StatusCode::UNKNOWN, "Unexpected failure"));
                }
                nextWrite();
            }
            void OnDone() override
            {
                spdlog::info("RPC completed for " + mPeer);
                mSubscribersCount->fetch_sub(1);
                delete this;
            }
            void OnCancel() override
            {
                spdlog::info("RPC canceled for " + mPeer);
            }
            void nextWrite()
            {
                std::chrono::milliseconds timeOut{15};
                while (mKeepRunning->load())
                {
                    try
                    {
                        auto nextPacket = mAllPacketsSubject->getNextPacket(mContext);
                        if (nextPacket)
                        {
                            ++mPacketsRead;
                            StartWrite(&*nextPacket);
                        }
                        else
                        {
                            std::this_thread::sleep_for(timeOut);
                        }
                    }
                    catch (const std::exception &e)
                    {
                        Finish(grpc::Status(grpc::StatusCode::UNKNOWN,
                               "Server error - packet read failed"));
                    }
                }
                spdlog::info("Application terminating subscription for " + mPeer);
                mAllPacketsSubject->unsubscribe(mContext);
                Finish(grpc::Status::OK);
            }
            grpc::CallbackServerContext *mContext{nullptr};
            ::AllPacketsSubject *mAllPacketsSubject{nullptr};
            std::string mPeer;
            std::chrono::microseconds mLastPacketRead{0};
            //UDataPacketImport::GRPC::SubscribeToAllStreamsResponse *mResponse{nullptr};
            uint64_t mPacketsRead{0};
            std::atomic<int> *mSubscribersCount{nullptr};
            std::atomic<bool> *mKeepRunning{nullptr};
        };
        return new Subscriber(mOptions.grpcAccessToken,
                              context,
                              &mAllPacketsSubject,
                              &mKeepRunning,
                              &mSubscribersCount);
    }

//private:
    std::thread mNotificationThread;
    ::ProgramOptions mOptions;
    std::unique_ptr<UDataPacketImport::SEEDLink::Client>
         mSEEDLinkClient{nullptr};
    ::AllPacketsSubject mAllPacketsSubject;
    std::function<void(std::vector<UDataPacketImport::Packet> &&)>
        mPacketBroadcastCallbackFunction
    {
        std::bind(&::ServiceImpl::importPackets, this,
                  std::placeholders::_1)
    };
    std::unique_ptr<moodycamel::ReaderWriterQueue<UDataPacketImport::Packet>>
        mImportQueue{nullptr};
    std::map<std::string, std::unique_ptr<::StreamMetrics>> mStreamMetrics;
    uint64_t mPacketsBroadcast{0};
    uint64_t mPacketsReceived{0};
    std::atomic<int> mSubscribersCount{0};
    std::atomic<bool> mKeepRunning{true};
};

class ServerImpl final
{
public:
    explicit ServerImpl(const ::ProgramOptions options) :
        mOptions(options),
        mService(options)
    {
        spdlog::info("In server c'tor");
    }

    void Run() 
    {
        auto address = mOptions.grpcHost + ":" 
                     + std::to_string(mOptions.grpcPort);

        grpc::ServerBuilder builder;
        if (mOptions.grpcServerKey.empty() ||
            mOptions.grpcServerCertificate.empty())
        {   
            spdlog::info("Initiating non-secured server SEEDLink bbroadcast server");
            builder.AddListeningPort(address,
                                     grpc::InsecureServerCredentials());
            builder.RegisterService(&mService);
        }   
        else
        {   
            spdlog::info("Creating secured SEEDLink broadcast server");
            grpc::SslServerCredentialsOptions::PemKeyCertPair keyCertPair
            {
                mOptions.grpcServerKey, // Private key
                mOptions.grpcServerCertificate // Public key (cert chain)
            };
            grpc::SslServerCredentialsOptions sslOptions; 
            sslOptions.pem_key_cert_pairs.emplace_back(keyCertPair);
            builder.AddListeningPort(address,
                                     grpc::SslServerCredentials(sslOptions));
            builder.RegisterService(&mService);
        }   
    //std::unique_ptr<grpc::Server> grpcServer(builder.BuildAndStart());
        spdlog::info("Server listening on " + address);
        mService.start();
        mServer = builder.BuildAndStart();
        handleMainThread();
    }
    ~ServerImpl()
    {
        spdlog::info("Stopping server");
        mService.stop();
        mServer->Shutdown();
    }
    // Calling thread from Run gets stuck here then fails through to
    // destructor
    void handleMainThread()
    {
        spdlog::debug("Main thread entering waiting loop");
        catchSignals();
        {
            while (!mStopRequested)
            {
                if (mInterrupted)
                {
                    spdlog::info("SIGINT/SIGTERM signal received!");
                    mStopRequested = true;
                    break;
                }
                std::unique_lock<std::mutex> lock(mStopMutex);
                mStopCondition.wait_for(lock,
                                        std::chrono::milliseconds {100},
                                        [this]
                                        {
                                              return mStopRequested;
                                        });
                lock.unlock();
            }
        }
        if (mStopRequested)
        {
            spdlog::debug("Stop request received.  Exiting...");
            //mServer->Shutdown();
            //stop(); 
           // mServer->Shutdown(); //
        }
    }
    /// Handles sigterm and sigint
    static void signalHandler(const int )
    {   
        mInterrupted = true;
    }   
    static void catchSignals()
    {   
        struct sigaction action;
        action.sa_handler = signalHandler;
        action.sa_flags = 0;
        sigemptyset(&action.sa_mask);
        sigaction(SIGINT,  &action, NULL);
        sigaction(SIGTERM, &action, NULL);
    }   
    /// Issues a stop notification 
    void issueStopNotification()
    {   
        spdlog::debug("Issuing stop notification...");
        {
            std::lock_guard<std::mutex> lock(mStopMutex);
            mStopRequested = true;
        }
        mStopCondition.notify_one();
    }
//private:
    mutable std::mutex mStopMutex;
    ::ProgramOptions mOptions;
    ServiceImpl mService;
    std::unique_ptr<grpc::Server> mServer;
    std::condition_variable mStopCondition;
    bool mStopRequested{false};
};

}

void runServer(const ::ProgramOptions &options)
{
/*
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    auto address = options.grpcHost + ":" 
                 + std::to_string(options.grpcPort);

    ServerImpl dataServer{options};

    grpc::ServerBuilder builder;
    if (options.grpcServerKey.empty() ||
        options.grpcServerCertificate.empty())
    {   
        spdlog::info("Initiating non-secured server SEEDLink bbroadcast server");
        builder.AddListeningPort(address,
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(&dataServer);
    }   
    else
    {   
        spdlog::info("Creating secured SEEDLink broadcast server");
        grpc::SslServerCredentialsOptions::PemKeyCertPair keyCertPair
        {
            options.grpcServerKey, // Private key
            options.grpcServerCertificate // Public key (cert chain)
        };
        grpc::SslServerCredentialsOptions sslOptions; 
        sslOptions.pem_key_cert_pairs.emplace_back(keyCertPair);
        builder.AddListeningPort(address,
                                 grpc::SslServerCredentials(sslOptions));
        builder.RegisterService(&dataServer);
    }   
    std::unique_ptr<grpc::Server> grpcServer(builder.BuildAndStart());


    dataServer.start();
    //grpcServer->Start();
    while (!terminateApplication)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds {100});
    }
    spdlog::info("Main thread exiting");
    grpcServer->Shutdown();
    dataServer.stop();
*/    
   
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
        //::runServer(programOptions);
        ServerImpl server{programOptions};
        server.Run();
    }
    catch (const std::exception &e)
    {
        spdlog::critical("Failed to run server because "
                       + std::string {e.what()});
        return EXIT_FAILURE;
    }
/*
    std::unique_ptr<::Publisher> publisher{nullptr};
    try 
    {   
        publisher = std::make_unique<::Publisher> (programOptions);
    }   
    catch (const std::exception &e) 
    {   
        spdlog::critical("Failed to create publisher; failed with "
                       + std::string {e.what()});
        return EXIT_FAILURE;
    }   
*/

    return EXIT_SUCCESS;
}

///--------------------------------------------------------------------------///
///                            Utility Functions                             ///
///--------------------------------------------------------------------------///
namespace
{

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

/// Read the program options from the command line
std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[])
{
    std::string iniFile;
    boost::program_options::options_description desc(R"""(
The uSEEDLinkBroadcast scrapes all packets from a SEEDLink import then
forwards these packets to gPRC subscriber(s).

    uSEEDLinkBroadcast --ini=seedLink.ini

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

UDataPacketImport::SEEDLink::ClientOptions
getSEEDLinkOptions(const boost::property_tree::ptree &propertyTree,
                   const std::string &clientName)
{
    namespace USL = UDataPacketImport::SEEDLink;
    USL::ClientOptions clientOptions;
    auto address = propertyTree.get<std::string> (clientName + ".address");
    auto port = propertyTree.get<uint16_t> (clientName + ".port", 18000);
    clientOptions.setAddress(address);
    clientOptions.setPort(port);
    for (int iSelector = 1; iSelector <= 32768; ++iSelector)
    {   
        std::string selectorName{clientName
                               + ".data_selector_"
                               + std::to_string(iSelector)};
        auto selectorString
            = propertyTree.get_optional<std::string> (selectorName);
        if (selectorString)
        {
            std::vector<std::string> splitSelectors;
            boost::split(splitSelectors, *selectorString,
                         boost::is_any_of(",|"));
            // A selector string can look like:
            // UU.FORK.HH?.01 | UU.CTU.EN?.01 | ....
            for (const auto &thisSplitSelector : splitSelectors)
            {
                std::vector<std::string> thisSelector; 
                auto splitSelector = thisSplitSelector;
                boost::algorithm::trim(splitSelector);

                boost::split(thisSelector, splitSelector,
                             boost::is_any_of(" \t"));
                USL::StreamSelector selector;
                if (splitSelector.empty())
                {
                    throw std::invalid_argument("Empty selector");
                }
                // Require a network
                boost::algorithm::trim(thisSelector.at(0));
                selector.setNetwork(thisSelector.at(0));
                // Add a station?
                if (splitSelector.size() > 1)
                {   
                    boost::algorithm::trim(thisSelector.at(1));
                    selector.setStation(thisSelector.at(1));
                }   
                // Add channel + location code + data type
                std::string channel{"*"};
                std::string locationCode{"??"};
                if (splitSelector.size() > 2)
                {
                    boost::algorithm::trim(thisSelector.at(2));
                    channel = thisSelector.at(2);
                }   
                if (splitSelector.size() > 3)
                {
                    boost::algorithm::trim(thisSelector.at(3));
                    locationCode = thisSelector.at(3);
                }   
                // Data type
                auto dataType = USL::StreamSelector::Type::All;
                if (splitSelector.size() > 4)
                {
                    boost::algorithm::trim(thisSelector.at(4));
                    if (thisSelector.at(4) == "D")
                    {
                        dataType = USL::StreamSelector::Type::Data;
                    }
                    else if (thisSelector.at(4) == "A")
                    {
                        dataType = USL::StreamSelector::Type::All; 
                    }
                    // TODO other data types
                }
                selector.setSelector(channel, locationCode, dataType);
                clientOptions.addStreamSelector(selector);
            } // Loop on selectors
        } // End check on selector string
    } // Loop on selectors
    return clientOptions;
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

    // gPRC
    options.grpcHost
        = propertyTree.get<std::string> ("gRPC.host",
                                         options.grpcHost);
    if (options.grpcHost.empty())
    {
        throw std::invalid_argument("gRPC end host must be specified");
    }
    options.grpcPort
        = propertyTree.get<uint16_t> ("gRPC.port", options.grpcPort);
    options.grpcEnableReflection
        = propertyTree.get<bool> ("gRPC.enableReflection",
                                  options.grpcEnableReflection);

    std::string grpcServerKey = ""; 
    grpcServerKey
        = propertyTree.get<std::string> ("gRPC.serverKey",
                                         grpcServerKey);
    std::string grpcServerCertificate = ""; 
    grpcServerCertificate
        = propertyTree.get<std::string> ("gRPC.serverCertificate",
                                         grpcServerCertificate);
    if (!grpcServerKey.empty() && !grpcServerCertificate.empty())
    {   
        if (!std::filesystem::exists(grpcServerKey))
        {
            throw std::invalid_argument("gRPC server key file "
                                      + grpcServerKey + " does not exist");
        }
        if (!std::filesystem::exists(grpcServerCertificate))
        {
            throw std::invalid_argument("gRPC server certifcate file "
                                      + grpcServerCertificate
                                      + " does not exist");
        }
        options.grpcServerKey = ::loadStringFromFile(grpcServerKey);
        options.grpcServerCertificate = ::loadStringFromFile(grpcServerCertificate);

    }

    options.grpcAccessToken
        = propertyTree.get<std::string> ("gRPC.accessToken",
                                         options.grpcAccessToken);
    if (!options.grpcAccessToken.empty() &&
        (options.grpcServerKey.empty() ||
         options.grpcServerCertificate.empty()))
    {
        throw std::invalid_argument(
            "Must set server certicate and key to use access token");
    }

    // SEEDLink properties
    if (propertyTree.get_optional<std::string> ("SEEDLink.address"))
    {   
        options.seedLinkClientOptions
             = ::getSEEDLinkOptions(propertyTree, "SEEDLink");
    }   

    return options;
}

}
