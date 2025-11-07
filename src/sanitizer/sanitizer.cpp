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
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <spdlog/spdlog.h>
//#include <readerwriterqueue.h>
#include "uDataPacketImport/grpc/subscriptionManager.hpp"
#include "uDataPacketImport/seedLink/subscriber.hpp"
#include "uDataPacketImport/seedLink/subscriberOptions.hpp"
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "uDataPacketImport/sanitizer/expiredPacketDetector.hpp"
#include "uDataPacketImport/sanitizer/futurePacketDetector.hpp"
#include "proto/dataPacketBroadcast.pb.h"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "loadStringFromFile.hpp"

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
    UDataPacketImport::Sanitizer::FuturePacketDetectorOptions 
        futurePacketDetectorOptions;
    UDataPacketImport::Sanitizer::ExpiredPacketDetectorOptions 
        expiredPacketDetectorOptions;
    std::string applicationName{APPLICATION_NAME};
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
    bool rejectExpiredPackets{true};
    bool rejectFuturePackets{true};
    bool grpcEnableReflection{false};
};

void setVerbosityForSPDLOG(const int verbosity);
std::pair<std::string, bool> parseCommandLineOptions(int argc, char *argv[]);
::ProgramOptions parseIniFile(const std::filesystem::path &iniFile);

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
                = std::make_unique<
                     UDataPacketImport::Sanitizer::ExpiredPacketDetector
                  > (mOptions.expiredPacketDetectorOptions);
        }
        if (mOptions.rejectFuturePackets)
        {
            mFuturePacketDetector
                = std::make_unique<
                     UDataPacketImport::Sanitizer::FuturePacketDetector
                  > (mOptions.futurePacketDetectorOptions);
        }

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
        if (allow)
        {

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
/*
        for (auto &options : mOptions.packetImport)
        {
        }
*/
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
    std::function<void (UDataPacketImport::GRPC::Packet &&)>
        mAddPacketCallbackFunction
    {
        std::bind(&::ServiceImpl::checkAndPropagateInputPackets, this,
                  std::placeholders::_1)
    };
    bool mKeepRunning{false};
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
