#include <vector>
#include <string>
#include <csignal>
#include <thread>
#include <mutex>
#include <atomic>
#include <spdlog/spdlog.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/ext/otel_plugin.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/meter_provider.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/exporters/ostream/metric_exporter_factory.h>
#include <opentelemetry/exporters/prometheus/exporter_factory.h>
#include <opentelemetry/exporters/prometheus/exporter_options.h>
#include <opentelemetry/sdk/metrics/meter_context.h>
#include <opentelemetry/sdk/metrics/meter_context_factory.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/meter_provider_factory.h>
#include <opentelemetry/sdk/metrics/provider.h>
#include <opentelemetry/sdk/metrics/view/instrument_selector_factory.h>
#include <opentelemetry/sdk/metrics/view/meter_selector_factory.h>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <spdlog/spdlog.h>
//#include <readerwriterqueue.h>
#include "uDataPacketImport/grpc/subscriptionManager.hpp"
#include "uDataPacketImport/grpc/subscriptionManagerOptions.hpp"
#include "uDataPacketImport/grpc/streamOptions.hpp"
#include "uDataPacketImport/seedLink/subscriber.hpp"
#include "uDataPacketImport/seedLink/subscriberOptions.hpp"
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "uDataPacketImport/sanitizer/duplicatePacketDetector.hpp"
#include "uDataPacketImport/sanitizer/expiredPacketDetector.hpp"
#include "uDataPacketImport/sanitizer/futurePacketDetector.hpp"
#include "proto/dataPacketBroadcast.pb.h"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "loadStringFromFile.hpp"
#include "asynchronousWriter.hpp"

#define APPLICATION_NAME "uPacketSanitizer"

namespace
{
std::atomic<bool> mInterrupted{false};


struct PacketImport
{
    std::string host;
    uint16_t port;
    std::string clientCertificate;
    std::string clientToken;
};

struct ProgramOptions
{
    std::vector<::PacketImport> importersOptions;
    UDataPacketImport::Sanitizer::DuplicatePacketDetectorOptions
        duplicatePacketDetectorOptions;
    UDataPacketImport::Sanitizer::FuturePacketDetectorOptions 
        futurePacketDetectorOptions;
    UDataPacketImport::Sanitizer::ExpiredPacketDetectorOptions 
        expiredPacketDetectorOptions;
    UDataPacketImport::GRPC::SubscriptionManagerOptions
        subscriptionManagerOptions;
    std::string applicationName{APPLICATION_NAME};
    std::string prometheusURL{"localhost:9090"};
    std::string grpcHost{"localhost"};
    std::string grpcServerCertificate;
    std::string grpcServerKey;
    std::string grpcServerToken;
    std::vector<std::chrono::seconds> reconnectInterval
    {
        std::chrono::seconds {1},
        std::chrono::seconds {15},
        std::chrono::seconds {30}
    };
    uint16_t grpcPort{50050};
    int verbosity{3};
    int maximumNumberOfSubscribers{64};
    bool rejectExpiredPackets{true};
    bool rejectFuturePackets{true};
    bool rejectDuplicatePackets{true};
    bool grpcEnableReflection{false};
};

void setVerbosityForSPDLOG(const int verbosity);
std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[]);
::ProgramOptions parseIniFile(const std::filesystem::path &iniFile);

void initializeMetrics(const ProgramOptions &options)
{
    opentelemetry::exporter::metrics::PrometheusExporterOptions
        prometheusOptions;
    prometheusOptions.url = options.prometheusURL;
    auto prometheusExporter
        = opentelemetry::exporter::metrics::PrometheusExporterFactory::Create(
              prometheusOptions);

    // Initialize and set the global MeterProvider
    auto providerInstance 
        = opentelemetry::sdk::metrics::MeterProviderFactory::Create();
    auto *meterProvider
        = static_cast<opentelemetry::sdk::metrics::MeterProvider *>
          (providerInstance.get());
    meterProvider->AddMetricReader(std::move(prometheusExporter));

    //constexpr std::string_view otelVersion{"1.2.0"};
    //constexpr std::string_view otelSchema{"https://opentelemetry.io/schemas/1.2.0"};
 
    std::shared_ptr<opentelemetry::metrics::MeterProvider>
        provider(std::move(providerInstance));
    opentelemetry::sdk::metrics::Provider::SetMeterProvider(provider);
}

void cleanupMetrics()
{
     std::shared_ptr<opentelemetry::metrics::MeterProvider> none;
     opentelemetry::sdk::metrics::Provider::SetMeterProvider(none);
}

}

class ServiceImpl :
    public UDataPacketImport::GRPC::RealTimeBroadcast::CallbackService
{
public:
    ServiceImpl(const ProgramOptions &options) :
        mOptions(options)
    {
        if (mOptions.importersOptions.empty())
        { 
            throw std::invalid_argument("No imports defined");
        }

        if (mOptions.rejectExpiredPackets)
        {
            mExpiredPacketDetector
                = std::make_unique
                  <
                      UDataPacketImport::Sanitizer::ExpiredPacketDetector
                  > (mOptions.expiredPacketDetectorOptions);
        }
        if (mOptions.rejectFuturePackets)
        {
            mFuturePacketDetector
                = std::make_unique
                  <
                      UDataPacketImport::Sanitizer::FuturePacketDetector
                  > (mOptions.futurePacketDetectorOptions);
        }
        else
        {
            if (mOptions.subscriptionManagerOptions.getStreamOptions().
                requireOrdered())
            {
                spdlog::warn(
R"""(
Not rejecting future packets while requiring publishers insert new packets can
blind a broadcast if very future data is encountered because of a GPS slip.
)""");
            }
        }
        if (mOptions.rejectDuplicatePackets)
        {
            mDuplicatePacketDetector
                = std::make_unique
                  <
                      UDataPacketImport::Sanitizer::DuplicatePacketDetector
                  > (mOptions.duplicatePacketDetectorOptions);
        }
        // Create the mechanism by which we send `clean' packets
        mBroadcastSubscriptionManager
            = std::make_shared<UDataPacketImport::GRPC::SubscriptionManager>
              (mOptions.subscriptionManagerOptions);

        // Define the subsribers that clean the import gPRC stream(s)
        for (auto &importOptions : mOptions.importersOptions)
        {
            // Create the importers
            UDataPacketImport::SEEDLink::SubscriberOptions subscriberOptions;
            subscriberOptions.setAddress(
                importOptions.host + ":"
                             + std::to_string(importOptions.port));
            if (!importOptions.clientCertificate.empty())
            {
                subscriberOptions.setCertificate(
                    importOptions.clientCertificate);
                if (!importOptions.clientToken.empty())
                {
                    subscriberOptions.setToken(importOptions.clientToken);
                }
            }
            auto importer
                = std::make_unique<UDataPacketImport::SEEDLink::Subscriber>
                  (mAddPacketCallbackFunction, subscriberOptions);
            mDataImporters.push_back(std::move(importer));
        }
    }

    void checkAndPropagateInputPackets(
        UDataPacketImport::GRPC::Packet &&packet)
    {
        bool allow{true};
        if (allow && mExpiredPacketDetector)
        {
            try
            {
                allow = mExpiredPacketDetector->allow(packet);
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to test expired packet because "
                           + std::string {e.what()});
                allow = false;
            }
        }
        if (allow && mFuturePacketDetector)
        {
            try
            {
                allow = mFuturePacketDetector->allow(packet);
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to test future packet because " 
                           + std::string {e.what()});
                allow = false;
            }
        }
        if (allow && mDuplicatePacketDetector)
        {
            try
            {
                allow = mDuplicatePacketDetector->allow(packet);
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to test duplicate packet because " 
                           + std::string {e.what()});
                allow = false;
            }
        }
        if (allow)
        {
/*
UDataPacketImport::StreamIdentifier id{packet.stream_identifier()};
if (id.toString() == "WY.YNM.HHZ.01")
{
 std::cout << std::setprecision(16) << packet.start_time_mus()*1.e-6 << std::endl;
}
*/
            mBroadcastSubscriptionManager->addPacket(std::move(packet)); 
        }
    }

    void start()
    {
        stop();
        mKeepRunning = true;
        mDataImportersFuture.clear();
        for (auto &importer : mDataImporters)
        {
            mDataImportersFuture.push_back(importer->start());
        }
    }

    grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet> *
    Subscribe(
        grpc::CallbackServerContext *context,
        const UDataPacketImport::GRPC::SubscriptionRequest *request) override
    {
        return new ::AsynchronousWriterSubscribe(
                      context,
                      request,
                      mBroadcastSubscriptionManager,
                      &mKeepRunning,
                      mOptions.grpcServerToken,
                      mOptions.maximumNumberOfSubscribers);
    }   

    grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet> *
    SubscribeToAllStreams(
        grpc::CallbackServerContext *context,
        const UDataPacketImport::GRPC::SubscribeToAllStreamsRequest *request) override
    {
        return new ::AsynchronousWriterSubscribeToAll(
                      context,
                      request,
                      mBroadcastSubscriptionManager,
                      &mKeepRunning,
                      mOptions.grpcServerToken,
                      mOptions.maximumNumberOfSubscribers);
    }


/*
    void importPackets( )
    {
        while (mKeepRunning)
        {
            for (int k = 0; k < mOptions.reconnectInterval.size(); ++k)
            {
                UDataPacketImport::SEEDLink::SubscriberOptions options;
                std::shared_ptr<GRPC::Channel> channel{nullptr};
                try
                {
                    channel = grpc::CreateChannel(options.address,
                                                  grpc::InsecureChannelCredentials());
                }
                catch (const std::exception &e)
                {

                }
                UDataPacketImport::SEEDLink::Subscriber
                    subscriber{mAddPacketCallbackFunction, options};
                try
                {
                    //mSubscriberFuture = subscriber.start(); 
                }
                catch (const std::exception &e)
                {
                    spdlog::critical("Import failed with "
                                   + std::string {e.what()});
                    break;
                }
                if (!mKeepRunning){break;} 
            }
        } 
        if (mKeepRunning)
        {
            spdlog::critical("Import task ended prematurely");
            stop();
        }
    }
*/
    /// @brief Convenience function to check any of the futures throwing
    ///        an exception.
    [[nodiscard]] bool futuresOkay(const std::chrono::milliseconds &timeOut) const
    {
        for (auto &future : mDataImportersFuture)
        {
            try
            {
                auto status = future.wait_for(timeOut);
                if (status == std::future_status::ready)
                {
                    future.get();
                }   
            }   
            catch (const std::exception &e) 
            {
                spdlog::critical("Fatal error in gRPC import: "
                               + std::string {e.what()});
                return false; 
            }
        }
        return true;
    }
    // Stops the imports
    void stop()
    {
        mKeepRunning = false;
        for (auto &importer : mDataImporters)
        {
            importer->stop();
        }
        for (auto &future : mDataImportersFuture)
        {
            if (future.valid()){future.get();}
        }
    }

    ::ProgramOptions mOptions;
    mutable std::vector<std::future<void>> mDataImportersFuture;
    std::vector<std::unique_ptr<UDataPacketImport::SEEDLink::Subscriber>>
        mDataImporters;
    std::unique_ptr<UDataPacketImport::Sanitizer::ExpiredPacketDetector>
        mExpiredPacketDetector{nullptr};
    std::unique_ptr<UDataPacketImport::Sanitizer::FuturePacketDetector>
        mFuturePacketDetector{nullptr};
    std::unique_ptr<UDataPacketImport::Sanitizer::DuplicatePacketDetector>
        mDuplicatePacketDetector{nullptr};
    std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager>
        mBroadcastSubscriptionManager{nullptr};
    std::function<void (UDataPacketImport::GRPC::Packet &&)>
        mAddPacketCallbackFunction
    {
        std::bind(&::ServiceImpl::checkAndPropagateInputPackets, this,
                  std::placeholders::_1)
    };
    std::atomic<bool> mKeepRunning{false};
};

class ServerImpl final
{
public:
    explicit ServerImpl(const ::ProgramOptions options) :
        mOptions(options),
        mService(options)
    {   
        spdlog::debug("In ServerImpl constructor");
    }   
    void Run() 
    {   
        auto address = mOptions.grpcHost + ":" 
                     + std::to_string(mOptions.grpcPort);
        grpc::ServerBuilder builder;
        if (mOptions.grpcServerKey.empty() ||
            mOptions.grpcServerCertificate.empty())
        {
            spdlog::info(
                "Initiating non-secured server SEEDLink broadcast server");
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
                if (!mService.futuresOkay(std::chrono::milliseconds {5}))
                {
                    spdlog::critical(
                       "Futures exception caught; terminating app");
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
    ~ServerImpl()
    {
        spdlog::info("Stopping server");
        mService.stop();
        mServer->Shutdown();
    }
//private:
    mutable std::mutex mStopMutex;
    ::ProgramOptions mOptions;
    ::ServiceImpl mService;
    std::unique_ptr<grpc::Server> mServer{nullptr};
    std::condition_variable mStopCondition;
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

    // Get this service going
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
The uPacketSanitizer scrapes all packets from a gRPC broadcast and
attempts to purge duplicate, future, and expired packets.  These packets
are subsequently broadcast to subscribers via gRPC.

    uPacketSanitizer --ini=sanitizer.ini

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

    // Behavior
    options.rejectExpiredPackets 
        = propertyTree.get<bool> ("Sanitizer.rejectExpiredPackets",
                                  options.rejectExpiredPackets);
    if (options.rejectExpiredPackets)
    {
        auto iTimeMS
            = propertyTree.get<int>
              ("Sanitizer.maxFutureTimeInMilliSeconds", 0);
        std::chrono::milliseconds time{iTimeMS};
        options.futurePacketDetectorOptions.setMaxFutureTime(time);
    }

    options.rejectFuturePackets
        = propertyTree.get<bool> ("Sanitizer.rejectFuturePackets",
                                  options.rejectFuturePackets);
    if (options.rejectFuturePackets)
    {
        auto iTimeS
            = propertyTree.get<int>
              ("Sanitizer.maxExpiredTimeInSeconds", 0); 
        if (iTimeS <= 0)
        {
            throw std::invalid_argument(
               "maxExpiredTimeInSeconds must be positive");
        }
        std::chrono::seconds time{iTimeS};
        options.expiredPacketDetectorOptions.setMaxExpiredTime(time);
    }

    options.rejectDuplicatePackets
       = propertyTree.get<bool> ("Sanitizer.rejectDuplicatePackets",
                                 options.rejectDuplicatePackets);
    if (options.rejectDuplicatePackets)
    {
        auto circularBufferSize
            = propertyTree.get_optional<int>
              ("Sanitizer.duplicatePacketCircularBufferSize");
        if (circularBufferSize) 
        {
            if (*circularBufferSize <= 0)
            {
                 throw std::invalid_argument(
                    "duplicatePacketCircularBufferSize must be positive");
            }
            options.duplicatePacketDetectorOptions.setCircularBufferSize(
                *circularBufferSize);
        }
        else
        {
            auto iTime
                = propertyTree.get<int>
                  ("Sanitizer.duplicatePacketCircularBufferDurationInSeconds",
                   300);
            if (iTime <= 0)
            {
                throw std::invalid_argument(
                    "duplicatePacketCircularBufferDuration must be positive");
            }
            options.duplicatePacketDetectorOptions.setCircularBufferDuration(
                std::chrono::seconds {iTime});
        }
    }

    // Stream options 
    auto streamOptions = options.subscriptionManagerOptions.getStreamOptions();
    auto requireOrdered
        = propertyTree.get<bool> ("StreamOptions.requireOrdered",
                                  streamOptions.requireOrdered());
    if (requireOrdered)
    {
        streamOptions.enableRequireOrdered();
    }
    else
    {
        streamOptions.disableRequireOrdered();
    }
    auto maximumQueueSize
        = propertyTree.get<int> ("StreamOptions.maximumQueueSize",
                                 streamOptions.getMaximumQueueSize());
    if (maximumQueueSize <= 0)
    {
        throw std::invalid_argument(
           "Maximum stream queue size must be positive");
    }
    streamOptions.setMaximumQueueSize(maximumQueueSize);
    options.subscriptionManagerOptions.setStreamOptions(streamOptions);

    

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

    options.grpcServerToken
        = propertyTree.get<std::string> ("gRPC.serverToken",
                                         options.grpcServerToken);
    if (!options.grpcServerToken.empty() &&
        (options.grpcServerKey.empty() ||
         options.grpcServerCertificate.empty()))
    {   
        throw std::invalid_argument(
            "Must set server certicate and key to use access token");
    } 

    options.maximumNumberOfSubscribers
       = propertyTree.get<int> ("grpc.maximumNumberOfSubscribers",
                                options.maximumNumberOfSubscribers);
    if (options.maximumNumberOfSubscribers <= 0)
    {
        throw std::invalid_argument(
           "Maximum number of subscribers must be be positive");
    }

    // Read the import configs
    for (int k = 1; k <= std::numeric_limits<int>::max(); ++k)
    {
        auto section = "Import" + std::to_string(k);
        if (propertyTree.get_optional<std::string> (section + ".host"))
        {
            try
            {
                ::PacketImport importOptions;
                importOptions.host
                    = propertyTree.get<std::string> (section + ".host");
                importOptions.port
                    = propertyTree.get<uint16_t> (section + ".port");
                auto certificate
                    = propertyTree.get_optional<std::string>
                      (section + ".clientCertificate");
                if (certificate)
                {
                    std::filesystem::path certificatePath{*certificate};
                    if (std::filesystem::exists(certificatePath))
                    {
                        importOptions.clientCertificate
                            = ::loadStringFromFile(certificatePath);
                    }
                }
                auto token
                    = propertyTree.get_optional<std::string>
                      (section + ".clientToken"); 
                if (token)
                {
                    if (importOptions.clientCertificate.empty())
                    {
                        throw std::invalid_argument(
                           "clientCertificate required to use token in section "
                          + section);
                    }
                    if (token->empty())
                    {
                        throw std::invalid_argument("Token is empty");
                    }
                    importOptions.clientToken = *token;
                }
                // Check if this exists
                bool exists{false};
                for (const auto &item : options.importersOptions)
                {
                    if (item.host == importOptions.host &&
                        item.port == importOptions.port)
                    {
                        spdlog::warn("Import already defined; skipping");
                        exists = true;
                        break;
                    }
                }
                if (!exists)
                {
                    options.importersOptions.push_back(importOptions);
                }
            }
            catch (const std::exception &e)
            {
                spdlog::error("Failed to read section " + section
                            + " because " + std::string {e.what()});
                continue;
            }

        }
        else
        {
            break;
        }
    }
    if (options.importersOptions.empty())
    {
        throw std::invalid_argument(
           "Must specify data imports - e.g., [Import1]");
    }

    return options;
}

}
