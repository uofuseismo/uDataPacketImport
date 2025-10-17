#include <csignal>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
//#include <grpcpp/ext/otel_plugin.h>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <spdlog/spdlog.h>
#include <concurrentqueue.h>
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "uDataPacketImport/seedLink/client.hpp"
#include "uDataPacketImport/seedLink/clientOptions.hpp"
#include "uDataPacketImport/seedLink/streamSelector.hpp"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "getNow.hpp"
#include "loadStringFromFile.hpp"

#define APPLICATION_NAME "uSEEDLinkBroadcast"
#define MAX_QUEUE_SIZE 4096

namespace
{

std::atomic<bool> mInterrupted{false};

struct ProgramOptions
{
    UDataPacketImport::SEEDLink::ClientOptions seedLinkClientOptions;
    std::string prometheusURL{"localhost:9090"};
    std::string applicationName{APPLICATION_NAME};
    std::string grpcHost;
    std::string grpcPublisherToken;
    std::string grpcClientCertificate;
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
*/
    size_t maxPublisherQueueSize{MAX_QUEUE_SIZE};
    uint16_t grpcPort{50000};
    int verbosity{3};
    bool preventFuturePackets{true};
};


[[nodiscard]] std::pair<std::string, bool> 
parseCommandLineOptions(int argc, char *argv[]);

std::shared_ptr<grpc::Channel> createChannel(const ::ProgramOptions &options);

[[nodiscard]] UDataPacketImport::SEEDLink::ClientOptions
getSEEDLinkOptions(const boost::property_tree::ptree &propertyTree,
                   const std::string &clientName);
void setVerbosityForSPDLOG(const int verbosity);

::ProgramOptions parseIniFile(const std::filesystem::path &iniFile);

}

class CustomAuthenticator : public grpc::MetadataCredentialsPlugin
{
public:
    CustomAuthenticator(const grpc::string &token) :
        mToken(token)
    {
    }
    grpc::Status GetMetadata(
        grpc::string_ref serviceURL, 
        grpc::string_ref methodName,
        const grpc::AuthContext &channelAuthContext,
        std::multimap<grpc::string, grpc::string> *metadata) override
    {
        metadata->insert(std::make_pair("x-custom-auth-token", mToken));
        return grpc::Status::OK;
    }
    
//private:
    grpc::string mToken;
};

class Publisher
{
public:
    explicit Publisher(const ::ProgramOptions options) :
        mPublisherQueue(options.maxPublisherQueueSize),
        mOptions(options)
    {
//        mChannel = ::createChannel(options);

        mSEEDLinkClient
            = std::make_unique<UDataPacketImport::SEEDLink::Client>
              (mPacketPublisherCallbackFunction,
               options.seedLinkClientOptions);
    
    }

    void start()
    {
        stop();
        mKeepRunning = true;
        //mPublisherThread = std::thread(&::Publisher::publishPacketsAsynchronously, this);
        mPublisherThread = std::thread(&::Publisher::publishPacketsSynchronously, this);
        //mSEEDLinkClientThread = std::thread(&::Publisher::runSEEDLinkClient, this);
        mSEEDLinkClient->start();
    }
/*
    void runSEEDLinkClient()
    {
        while (mKeepRunning)
        {
            mSEEDLinkClient->stop();
            mSEEDLinkClient->start();
            if (mKeepRunning)
            {
                spdlog::info("Attempting to restart SEEDLink client");
            }
            else
            {
                spdlog::info("SEEDLink client thread exiting");
                break;
            }
        } 
    }
*/

    void stop()
    {
        mKeepRunning = false;
        mSEEDLinkClient->stop();
        //if (mSEEDLinkClientThread.joinable()){mSEEDLinkClientThread.join();}
        if (mPublisherThread.joinable()){mPublisherThread.join();}
    }

    void publishPacketsSynchronously()
    {
        //mChannel = ::createChannel(mOptions);

        std::vector<std::chrono::seconds> retryInterval;
        retryInterval.push_back(std::chrono::seconds {5});
        retryInterval.push_back(std::chrono::seconds {10});
        retryInterval.push_back(std::chrono::seconds {20});


        int iReconnect{0};
        while (true)
        {

//  grpc::SslCredentialsOptions ssl_options;
//  ssl_options.pem_root_certs = mOptions.grpcClientCertificate;
//  auto channel = grpc::CreateChannel("localhost:50040",
//                                     grpc::SslCredentials(ssl_options));

            auto channel = ::createChannel(mOptions);
            grpc::ClientContext context;
            context.set_wait_for_ready(false);

            auto stub = UDataPacketImport::GRPC::BroadcastProxy::NewStub(channel);

            UDataPacketImport::GRPC::EndPublicationResponse response;
            std::unique_ptr<grpc::ClientWriter<UDataPacketImport::GRPC::Packet>>
                 writer(stub->PublishPackets(&context, &response));
            bool doReconnect{true};
            std::this_thread::sleep_for(std::chrono::milliseconds {100});
            if (channel->GetState(false) == GRPC_CHANNEL_READY)
            {
                iReconnect = 0;
                // Keep propagating packets
                while (mKeepRunning)
                {
                    UDataPacketImport::GRPC::Packet packet;
                    //spdlog::info("hey packet..." + std::to_string(mChannel->GetState(false)));
                    if (mPublisherQueue.try_dequeue(packet))
                    {
                        auto now = ::getNow();
                        std::chrono::microseconds startTime(packet.start_time_mus());
                        if (startTime < now - std::chrono::seconds {180*86400})
                        {
                            spdlog::warn("Will not propagate packet - has expired data");
                            continue;
                        }
                        std::chrono::microseconds endTime;
                        try
                        {
                            endTime = UDataPacketImport::getEndTime(packet);
                        }
                        catch (const std::exception &e)
                        {
                            spdlog::warn("Failed to get packet end time");
                            continue;
                        }
                        if (endTime > now)
                        {
                            spdlog::warn("Will not propagate packet - has future data");
                            continue;
                        }
                        if (!writer->Write(packet))
                        {
                            spdlog::warn("Failed to write packet!");
                            break; // Some type of error occurred - figure it out
                        }
                    }
                    else
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds {50});
                    }
                }
                // Finish up
                writer->WritesDone();
                auto status = writer->Finish();
                if (status.ok())
                {
                    doReconnect = false;
                    if (mKeepRunning)
                    {
                        spdlog::warn("Writer successfully finished - but why?");
                        issueStopNotification();
                    }
                    else
                    {
                        spdlog::info("Writer successfully haulted publication");
                    }
                    break;
                }
                else
                {
                    // Channel blinked out
                    if (status.error_code() == grpc::UNAVAILABLE)
                    {
                        doReconnect = true;
                    }
                    else
                    {
                        if (status.error_code() == grpc::UNAUTHENTICATED)
                        {
                            spdlog::critical(
                                "Token required to publish packets");
                        }
                        else
                        {
                            spdlog::critical("Unhandled grpc error: "
                                        + std::string {status.error_message()});
                        }
                        doReconnect = false;
                        if (mKeepRunning){issueStopNotification();}
                        break;
                    }
                }
            } // End check on channel being ready
            if (doReconnect)
            {
                if (!mKeepRunning){break;}
                if (iReconnect < retryInterval.size())
                {
                    auto timeOut = retryInterval.at(iReconnect); 
                    spdlog::info("Attempting reconnect in "
                               + std::to_string(timeOut.count()) + " (s)");
                    std::this_thread::sleep_for(timeOut);
                    iReconnect = iReconnect + 1;
                    continue;
                }
                else
                {
                    spdlog::error("Publisher reconnect limit exceeded");
                    if (mKeepRunning){issueStopNotification();}
                    break;
                }
            }
            spdlog::info("Not sure why i'm here");
            if (mKeepRunning){issueStopNotification();}
            break;
        } // Loop on retries
    }
 
/*
    void publishPacketsAsynchronously()
    {
        class AsyncPublisher : public grpc::ClientWriteReactor<UDataPacketImport::GRPC::Packet>
        {
        public:
            AsyncPublisher(UDataPacketImport::GRPC::BroadcastProxy::Stub *stub, 
                           moodycamel::ConcurrentQueue<UDataPacketImport::GRPC::Packet> *publisherQueue,
                           std::atomic<bool> *keepRunning) :
                mPublisherQueue(publisherQueue),
                mKeepRunningPublisher(keepRunning)
            {
                mContext.set_wait_for_ready(true);
                stub->async()->PublishPackets(&mContext, &mAPIResponse, this);
                // The route client uses this b/c some StartsWrites were invoked
                // indirectly from a delayed lambda.  I'll try it for now.
                AddHold();
                nextWrite();
                StartCall();
            }
            // Reacts to write completion.  If the write was successful, we then
            // write another point.
            void OnWriteDone(bool ok) override
            {
                nextWrite();
            } 
            // Reacts to RPC completion,records the RPC status, and notifies
            // condition variable waiting for this to finish.
            void OnDone(const grpc::Status &status) override
            {
                std::unique_lock<std::mutex> lock{mMutex};
                mStatus = status;
                mWriteDone = true;
                mConditionVariable.notify_one();
            }
            // Allows caller to wait for RPC to complete.
            grpc::Status Await(UDataPacketImport::GRPC::EndPublicationResponse *status)
            {
                std::unique_lock<std::mutex> lock{mMutex};
                mConditionVariable.wait(lock, [this] {return mWriteDone;});
                *mAPIResponsePointer = mAPIResponse; 
                return std::move(mStatus);
            }
        private:
            void nextWrite()
            {
                // Keep writing
                if (mKeepRunningPublisher->load())
                {
                    UDataPacketImport::GRPC::Packet packet;
                    if (mPublisherQueue->try_dequeue(packet))
                    {
                        spdlog::info("Submitting packet");
                        StartWrite(&packet);
                    }
                    else
                    {
                        // Empty queue -> wait then try again
                        std::this_thread::sleep_for(mWriteTimeOut);
                        nextWrite(); // Iterate
                    }
                }
                else
                {
                    // Publication is teriminated by caller.  Indicate we're done
                    // writing
                    StartWritesDone();
                    RemoveHold();
                }
            }
            std::mutex mMutex;
            std::condition_variable mConditionVariable;
            grpc::Status mStatus;
            grpc::ClientContext mContext;
            moodycamel::ConcurrentQueue<UDataPacketImport::GRPC::Packet> *mPublisherQueue{nullptr};
            UDataPacketImport::GRPC::Packet mPacketToWrite;
            UDataPacketImport::GRPC::EndPublicationResponse *mAPIResponsePointer{nullptr};
            UDataPacketImport::GRPC::EndPublicationResponse mAPIResponse;
            std::chrono::milliseconds mWriteTimeOut{10};
            std::atomic<bool> *mKeepRunningPublisher{nullptr};
            bool mWriteDone{false};
        };

        spdlog::info("Beginning packet publication RPC");
        std::unique_ptr<UDataPacketImport::GRPC::BroadcastProxy::Stub> stub
             = UDataPacketImport::GRPC::BroadcastProxy::NewStub(mChannel);
        UDataPacketImport::GRPC::EndPublicationResponse endPublicationResponse;
        AsyncPublisher publisher(stub.get(), 
                                 &mPublisherQueue,
                                 &mKeepRunning);
        auto status = publisher.Await(&endPublicationResponse);
        if (status.ok())
        {
            spdlog::info("Packet publication succesfully concluded.  RPC consumed "
                       + std::to_string(endPublicationResponse.packets_received())
                       + " RPC propagated "
                       + std::to_string(endPublicationResponse.packets_published()));
        }
        else
        {
            spdlog::error("Packet publication RPC failed");
        }
    }
*/
    /// Callback that propagates packets to the publisher thread
    void inputPacketsToQueueCallback(
        std::vector<UDataPacketImport::Packet> &&packets)
    {
        // Don't even bother
        if (packets.empty()){return;}
        // Normally we get one packet and write it so no point optimizing
        // queue operations with bulk inserts.  First, we clear some space.
        auto queueSize = mPublisherQueue.size_approx();
        if (queueSize + packets.size() > mMaxPublisherQueueSize)
        {
            for (size_t i = 0; i < queueSize; ++i)
            {
                queueSize = mPublisherQueue.size_approx();
                if (queueSize + packets.size() < mMaxPublisherQueueSize)
                {
                    break;
                }
                UDataPacketImport::GRPC::Packet work;
                if (!mPublisherQueue.try_dequeue(work))
                {
                    spdlog::warn("Failed to dequeu packet");
                }
            }
            return;
        }
        // Should be space - try writing
        for (auto &&packet : packets)
        {
            try
            {
                auto work = packet.toProtobuf();
                if (!mPublisherQueue.try_enqueue(std::move(work)))
                {
                    throw std::runtime_error("Failed to enqueue packet");
                }
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to write packet because "
                           + std::string {e.what()});
            }
        }
        /*
        // Try to clean the queue a bit
        spdlog::warn("Trying bulk write");
        auto queueSize = mPublisherQueue.size_approx();
        if (queueSize + packets.size() > mMaxPublisherQueueSize)
        {
            std::vector<UDataPacketImport::Packet>
                work(mMaxPublisherQueueSize - packets.size()); 
            if (!mPublisherQueue.try_dequeue_bulk(work.begin(), work.size()))
            {
                spdlog::warn("Failed to enqueue packets - trying slow way");
                for (size_t i = 0; i < queueSize; ++i)
                {
                    if (mPublisherQueue.size_approx() < mMaxPublisherQueueSize)
                    {
                        break;
                    }
                    UDataPacketImport::Packet workPacket;
                    if (!mPublisherQueue.try_dequeue(workPacket))
                    {
                        spdlog::warn("Failed to dequeue packet");
                    }
                }
            }
        }
        if (!mPublisherQueue.try_enqueue_bulk(packets.begin(), packets.size()))
        {
            spdlog::warn("Failed to enqueue packets - trying slow way");
            for (auto &packet : packets)
            {
                if (!mPublisherQueue.try_enqueue(std::move(packet)))
                {
                    spdlog::warn("Failed to enqueue packet");
                }
            }
        }
        */
    }
    /// Place for the main thread to sleep until someone wakes it up.
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
            spdlog::debug("Stop request received.  Terminating proxy...");
            stop();
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
    // Single channel-single reader
    std::thread mSEEDLinkClientThread;
    std::thread mPublisherThread;
    std::unique_ptr<UDataPacketImport::SEEDLink::Client> mSEEDLinkClient{nullptr};
    moodycamel::ConcurrentQueue<UDataPacketImport::GRPC::Packet> mPublisherQueue;
    std::function<void(std::vector<UDataPacketImport::Packet> &&)>
        mPacketPublisherCallbackFunction
    {
        std::bind(&::Publisher::inputPacketsToQueueCallback, this,
                  std::placeholders::_1)
    };
    ::ProgramOptions mOptions;
    //std::shared_ptr<grpc::Channel> mChannel{nullptr};
    mutable std::mutex mStopMutex;
    size_t mMaxPublisherQueueSize{MAX_QUEUE_SIZE};
    std::condition_variable mStopCondition;
    std::atomic<bool> mKeepRunning{true};
    bool mStopRequested{false};
};

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

    try
    {
        publisher->start();
    }
    catch (const std::exception &e)
    {
        spdlog::critical("Failed to run publisher; failed with "
                       + std::string {e.what()});
        return EXIT_FAILURE;
    }
    publisher->handleMainThread();

    return EXIT_SUCCESS;
}

///--------------------------------------------------------------------------///
///                            Utility Functions                             ///
///--------------------------------------------------------------------------///
namespace
{

std::shared_ptr<grpc::Channel> createChannel(const ::ProgramOptions &options)
{
    auto address = options.grpcHost + ":" + std::to_string(options.grpcPort);
/*
  grpc::SslCredentialsOptions ssl_options;
  ssl_options.pem_root_certs = options.grpcClientCertificate;
  return grpc::CreateChannel(address, //"localhost:50040",
                             grpc::SslCredentials(ssl_options));
*/
/*
    std::string retryPolicy =
    "{\"methodConfig\" : [{"
    "   \"name\" : [{\"service\": \"UDataPacketImport.GRPC.BroadcastPackets.PublishPackets\"}],"
    "   \"waitForReady\": true,"
    "   \"retryPolicy\": {"
    "     \"maxAttempts\": 4,"
    "     \"initialBackoff\": \"1s\","
    "     \"maxBackoff\": \"120s\","
    "     \"backoffMultiplier\": 1.0,"
    "     \"retryableStatusCodes\": [\"UNAVAILABLE\"]"
    "    }"
    "}]}";
//std::cout << retryPolicy << std::endl;
    grpc::ChannelArguments retryArguments;
    //retryArguments.SetString(GRPC_ARG_SERVICE_CONFIG);
    //retryArguments.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 2000); // 2s
    //retryArguments.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS,   120000); // 120s
    //retryArguments.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS,    10000); // 10s
    //retryArguments.SetServiceConfigJSON(retryPolicy);
*/
    //auto address = options.grpcHost + ":" + std::to_string(options.grpcPort);
    //std::shared_ptr<grpc::Channel> channel{nullptr};
    if (options.grpcPublisherToken.empty())
    {   
        if (options.grpcClientCertificate.empty())
        {
            spdlog::info("Creating non-secured client without token at "
                       + address);
            return grpc::CreateChannel(address,
                                       grpc::InsecureChannelCredentials());
//                                            retryArguments);
        }
        else
        {
            spdlog::info("Creating secured client without token at "
                       + address);
            grpc::SslCredentialsOptions sslOptions;
            sslOptions.pem_root_certs = options.grpcClientCertificate;
            return grpc::CreateChannel(address,
                                       grpc::SslCredentials(sslOptions));
        }
    }   
    else
    {
        auto callCredentials = grpc::MetadataCredentialsFromPlugin(
            std::unique_ptr<grpc::MetadataCredentialsPlugin> (
               new CustomAuthenticator(options.grpcPublisherToken)));
        if (options.grpcClientCertificate.empty())
        {
            throw std::runtime_error("gRPC forbids tokens without encyption");
            /*
            spdlog::info("Creating non-secured client with a token at "
                       + address);
            spdlog::warn("Token can be intercepted in flight");
            auto channelCredentials
                = grpc::CompositeChannelCredentials(
                      grpc::InsecureChannelCredentials(),
                      callCredentials);
            return grpc::CreateChannel(address, channelCredentials); //, retryArguments); CustomChannel
            */
        }
        else
        { 
            spdlog::info("Creating secured client with token at "
                       + address);
            grpc::SslCredentialsOptions sslOptions;
            sslOptions.pem_root_certs = options.grpcClientCertificate;
            auto channelCredentials
                = grpc::CompositeChannelCredentials(
                      grpc::SslCredentials(sslOptions),
                      callCredentials);
            return grpc::CreateChannel(address, channelCredentials);
        }
    }
    throw std::runtime_error("Uhandled channel creation route");
}


/// Read the program options from the command line
std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[])
{
    std::string iniFile;
    boost::program_options::options_description desc(R"""(
The uSEEDLinkBroadcast reads packets from a SEEDLink server
then propagates these packets to gRPC packet broadcast.
Example usage is

    uSEEDLinkBroadcast --ini=loader.ini

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

    // gRPC
    options.grpcHost
        = propertyTree.get<std::string> ("gRPC.host", options.grpcHost);
    if (options.grpcHost.empty())
    {
        throw std::invalid_argument("gRPC host must be specified");
    }
    options.grpcPort
        = propertyTree.get<uint16_t> ("gRPC.port", options.grpcPort);

    options.grpcPublisherToken
        = propertyTree.get<std::string> ("gRPC.publisherToken",
                                         options.grpcPublisherToken);
   
    std::string grpcClientCertificate = ""; 
    grpcClientCertificate
        = propertyTree.get<std::string> ("gRPC.clientCertificate",
                                         grpcClientCertificate);
    if (!grpcClientCertificate.empty())
    {
        if (!std::filesystem::exists(grpcClientCertificate))
        {
            throw std::invalid_argument("gRPC client certificate file "
                                      + grpcClientCertificate
                                      + " does not exist");
        }
        options.grpcClientCertificate = ::loadStringFromFile(grpcClientCertificate);
    }
    if (!options.grpcPublisherToken.empty() &&
        options.grpcClientCertificate.empty())
    {
        throw std::invalid_argument("Must set client certificate to use a token");
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
