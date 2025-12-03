#include <vector>
#include <atomic>
#include <future>
#include <thread>
#include <queue>
#include <mutex>
#include <cmath>
#include <string>
#include <chrono>
#include <limits>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "asynchronousWriter.hpp"
#include "uDataPacketImport/grpc/subscriptionManager.hpp"
#include "uDataPacketImport/grpc/subscriptionManagerOptions.hpp"
#include "uDataPacketImport/grpc/client.hpp"
#include "uDataPacketImport/grpc/clientOptions.hpp"
#include "uDataPacketImport/grpc/streamOptions.hpp"
#include "uDataPacketImport/grpc/stream.hpp"
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/v1/broadcast.grpc.pb.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#define SERVER_HOST "0.0.0.0:59598"
#define CLIENT_HOST "localhost:59598"

namespace
{
std::vector<UDataPacketImport::GRPC::V1::Packet> packetsToBroadcast;
std::vector<UDataPacketImport::GRPC::V1::Packet> packetsReceived;

[[nodiscard]] 
UDataPacketImport::GRPC::V1::Packet
    createPacket(const int number, const bool cwu)
{
    UDataPacketImport::Packet packet;
    UDataPacketImport::StreamIdentifier identifier;
    identifier.setNetwork("UU");
    if (cwu)
    {
        identifier.setStation("CWU");
    }
    else
    {
        identifier.setStation("SGU");
    }
    identifier.setChannel("HHZ");
    identifier.setLocationCode("01");
    packet.setStreamIdentifier(identifier);
    packet.setSamplingRate(100);
    std::chrono::microseconds startTime{std::chrono::seconds {number}};
    std::vector<int> data{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    packet.setStartTime(startTime);
    packet.setData(data);
    return packet.toProtobuf();
}

class ServiceImpl : public UDataPacketImport::GRPC::V1::RealTimeBroadcast::CallbackService
{
public:
    ServiceImpl()
    {
        UDataPacketImport::GRPC::SubscriptionManagerOptions options;
        UDataPacketImport::GRPC::StreamOptions streamOptions;
        streamOptions.enableRequireOrdered();
        options.setStreamOptions(streamOptions);
        mManager
            = std::make_shared<UDataPacketImport::GRPC::SubscriptionManager>
              (options);
    }
    ~ServiceImpl()
    {
        stop();
    }
    std::future<void> start()
    {
        return std::async(&ServiceImpl::publishPackets, this);
    }
    void publishPackets()
    {
        for (const auto &packet : packetsToBroadcast)
        {
            if (!mKeepRunning){break;}
            std::this_thread::sleep_for(std::chrono::milliseconds {10});
            //std::cout << "Broadcast packet" << std::endl;
            try
            {
                mManager->addPacket(packet);
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << std::endl;
                REQUIRE(false);
            }
        }
    }
    grpc::ServerUnaryReactor*
    GetAvailableStreams(
        grpc::CallbackServerContext *context,
        const UDataPacketImport::GRPC::V1::AvailableStreamsRequest *request,
        UDataPacketImport::GRPC::V1::AvailableStreamsResponse *availableStreamsResponse)
    {   
        return new ::AsynchronousGetAvailableStreamsReactor(
                      context,
                      *request,
                      availableStreamsResponse,
                      mManager,
                      apiKey);
    }   

    grpc::ServerWriteReactor<UDataPacketImport::GRPC::V1::Packet> *
    SubscribeToAllStreams(
        grpc::CallbackServerContext *context,
        const UDataPacketImport::GRPC::V1::SubscribeToAllStreamsRequest *request) override 
    {
        return new ::AsynchronousWriterSubscribeToAll(
                      context, request, mManager, &mKeepRunning, apiKey, maxSubscribers);
    }
    grpc::ServerWriteReactor<UDataPacketImport::GRPC::V1::Packet> *
    Subscribe(
        grpc::CallbackServerContext *context,
        const UDataPacketImport::GRPC::V1::SubscriptionRequest *request) override 
    {   
        return new ::AsynchronousWriterSubscribe(
                      context, request, mManager, &mKeepRunning, apiKey, maxSubscribers);
    }   

    void stop()
    {
        mKeepRunning = false;
        mManager->unsubscribeAll();
    }
    std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager> mManager{nullptr};
    std::string apiKey;
    int maxSubscribers{16};
    std::atomic<bool> mKeepRunning{true};
};

class ServerImpl
{
public:
    ~ServerImpl()
    {
        stop();
    }
    void run()
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(SERVER_HOST,
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(&mService);
        mBroadcastFuture = mService.start();
        mServer = builder.BuildAndStart();
    }
    void stop()
    {
        mService.stop();
        mServer->Shutdown();
        if (mBroadcastFuture.valid()){mBroadcastFuture.get();}
    }
    ::ServiceImpl mService;
    std::unique_ptr<grpc::Server> mServer{nullptr};
    std::future<void> mBroadcastFuture;
};

void subscribe(const bool doCancel = false,
               const bool subscribeToAll = true)
{
    grpc::ClientContext context;
    //Status status = stub_->GetFeature(&context, point, feature);
    auto channel
        = grpc::CreateChannel(CLIENT_HOST,
                              grpc::InsecureChannelCredentials());
    auto stub = UDataPacketImport::GRPC::V1::RealTimeBroadcast::NewStub(channel);
    grpc::Status status;
    if (subscribeToAll)
    {
        UDataPacketImport::GRPC::V1::SubscribeToAllStreamsRequest request;
        std::unique_ptr<grpc::ClientReader<UDataPacketImport::GRPC::V1::Packet> >
            reader( stub->SubscribeToAllStreams(&context, request) );
        UDataPacketImport::GRPC::V1::Packet packet;
        int iPacketsRead = 0;
        while (reader->Read(&packet)) 
        {
            //std::cout << "look at me right here e yes" << std::endl;
            //std::cout << packet.start_time_mus() << std::endl;
            if (iPacketsRead == 2 && doCancel){break;}
           ++iPacketsRead;
        }
        if (doCancel){context.TryCancel();}
        status = reader->Finish();
        if (status.ok()) 
        {
            std::cout << "SubscribeToAll rpc succeeded." << std::endl;
        }
        else
        {
            if (status.error_code() == grpc::StatusCode::CANCELLED)
            {
                std::cout << "SubscribeToAll canceled by client" << std::endl;
            }
            else 
            {
                std::cout << "SubscribeToAll rpc failed." << std::endl;
            }
        }
    }   
    else
    {
        auto doCWU = true;
        auto grpcIdentifier = createPacket(0, doCWU).stream_identifier(); // cwu
        UDataPacketImport::GRPC::V1::SubscriptionRequest request;
        *request.add_streams() = grpcIdentifier;
        std::unique_ptr<grpc::ClientReader<UDataPacketImport::GRPC::V1::Packet> >
            reader( stub->Subscribe(&context, request) );
        int iPacketsRead{0};
        UDataPacketImport::GRPC::V1::Packet packet;
        while (reader->Read(&packet))
        {
            //std::cout << "look at me right here e yes" << std::endl;
            std::cout << "ind sub" << packet.start_time_mus() << std::endl;
            if (iPacketsRead == 2 && doCancel){break;}
           ++iPacketsRead;
        }
        if (doCancel){context.TryCancel();}
    } 
}

void gotGRPCPacketCallback(UDataPacketImport::GRPC::V1::Packet &&packet)
{
    std::cout << "in here" << std::endl;
    UDataPacketImport::StreamIdentifier identifier{packet.stream_identifier()};
    std::cout << "got packet " << identifier.toString() << std::endl;
}

void subscribeWithClient(const bool doCancel = false,
                         const bool subscribeToAll = true)
{
    UDataPacketImport::GRPC::ClientOptions options;
    options.setAddress(CLIENT_HOST);
auto cwuPacket = ::createPacket(0, false);
UDataPacketImport::StreamIdentifier identifier{cwuPacket.stream_identifier()};
std::set<UDataPacketImport::StreamIdentifier> identifiers;
identifiers.insert(identifier);
assert(identifiers.size() == 1);
options.setStreamSelections(identifiers);

    std::function<void (UDataPacketImport::GRPC::V1::Packet &&)>
        gotPacketCallbackFunction
    {
        std::bind(&::gotGRPCPacketCallback, //this,
                  std::placeholders::_1)
    };

std::cout << "init" << std::endl;
    UDataPacketImport::GRPC::Client client{gotPacketCallbackFunction, options};

auto availableStreams = client.getAvailableStreams();
std::cout << availableStreams.size() << " streams available" << std::endl;

std::cout << "starting client" << std::endl;
    auto future = client.start(); 
    std::this_thread::sleep_for(std::chrono::seconds {2});
std::cout << "stopping client" << std::endl;
    client.stop();
    try
    {
        future.get();
    }   
    catch (const std::exception &e) 
    {  
        std::cerr << "Fatal error in client: " << e.what() << std::endl;
        //return false; 
    }
    //return true;
std::cout << "okay" << std::endl;
}


/*
void runServer()
{
  std::string serverAddress(SERVER_HOST);

  UDataPacketImport::GRPC::SubscriptionManagerOptions options;
  UDataPacketImport::GRPC::SubscriptionManager manager{options};
  ServerImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(serverAddress, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
}
*/

}

TEST_CASE("UDataPacketImport::GRPC::StreamOptions", "[streamOptions]")
{
    UDataPacketImport::GRPC::StreamOptions options;
    SECTION("Defaults")
    {
        REQUIRE(options.getMaximumQueueSize() == 8);
        REQUIRE(!options.requireOrdered());
    }
    options.setMaximumQueueSize(39);
    REQUIRE(options.getMaximumQueueSize() == 39);
    options.enableRequireOrdered();
    REQUIRE(options.requireOrdered());
    options.disableRequireOrdered();
    REQUIRE(!options.requireOrdered());
}

TEST_CASE("UDataPacketImport::GRPC::SubscriptionManagerOptions",
          "[subscriptionManagerOptions]")
{
    UDataPacketImport::GRPC::SubscriptionManagerOptions options;
    SECTION("Defaults")
    {
        REQUIRE(options.getStreamOptions().getMaximumQueueSize() == 8); 
        REQUIRE(!options.getStreamOptions().requireOrdered());
    }

    UDataPacketImport::GRPC::StreamOptions streamOptions;
    streamOptions.setMaximumQueueSize(39);
    streamOptions.enableRequireOrdered();
    options.setStreamOptions(streamOptions);
    REQUIRE(options.getStreamOptions().getMaximumQueueSize() == 39);
    REQUIRE(options.getStreamOptions().requireOrdered());
}

TEST_CASE("UDataPacketImport::GRPC::Stream")
{
    UDataPacketImport::GRPC::StreamOptions options;
    SECTION("Default")
    {
        std::vector<UDataPacketImport::GRPC::V1::Packet> localPackets;
        for (int i = 0; i < 3; ++i)
        {
            localPackets.push_back(::createPacket(i, true));
        }
        UDataPacketImport::GRPC::Stream stream{localPackets.at(0), options};
        // Become my own subscriber
        auto tid = std::this_thread::get_id();
        auto subscriberID = reinterpret_cast<uintptr_t> (&tid);
        REQUIRE_NOTHROW(stream.subscribe(subscriberID));
        REQUIRE_NOTHROW(stream.subscribe(subscriberID + 1));
        REQUIRE(stream.getNumberOfSubscribers() == 2);
        REQUIRE(stream.getSubscribers().size() == 2);
        REQUIRE(stream.getSubscribers().contains(subscriberID));
        REQUIRE(stream.getSubscribers().contains(subscriberID + 1));
        // Add the rest of the packets
        for (int i = 1; i < static_cast<int> (localPackets.size()); ++i)
        {
            stream.setLatestPacket(localPackets.at(i));
        }
        // Get the packets (these are cached) - note I missed the first packet
        for (int i = 1; i < static_cast<int> (localPackets.size()); ++i)
        {
            for (int j = 0; j < 2; ++j)
            {
                auto packetBack = stream.getNextPacket(subscriberID + j); 
                if (packetBack)
                {
                    REQUIRE(packetBack->start_time_mus()
                            == localPackets.at(i).start_time_mus());
                }
                else
                {
                    REQUIRE(false);
                }
            }
        }
        // Unsubscribe
        stream.unsubscribe(subscriberID);
        REQUIRE(stream.getNumberOfSubscribers() == 1);
        REQUIRE(stream.getSubscribers().size() == 1);
        REQUIRE(stream.getSubscribers().contains(subscriberID + 1));
        stream.unsubscribe(subscriberID + 1);
        REQUIRE(stream.getNumberOfSubscribers() == 0);
        REQUIRE(stream.getSubscribers().size() == 0);
    }

    SECTION("Overflow queue")
    {
        std::vector<UDataPacketImport::GRPC::V1::Packet> localPackets;
        options.setMaximumQueueSize(8);
        for (int i = 0; i < 10; ++i)
        {
            localPackets.push_back(::createPacket(i, true));
        }
        UDataPacketImport::GRPC::Stream stream{localPackets.at(0), options};
        auto tid = std::this_thread::get_id();
        auto subscriberID = reinterpret_cast<uintptr_t> (&tid);
        REQUIRE_NOTHROW(stream.subscribe(subscriberID));
        for (const auto &packet : localPackets)
        {
            stream.setLatestPacket(packet);
        }
 
        std::vector<UDataPacketImport::GRPC::V1::Packet> packetsBack;
        for (int i = 0; i < static_cast<int> (localPackets.size()); ++i)
        {
            auto packetBack = stream.getNextPacket(subscriberID); 
            if (!packetBack){break;}
            packetsBack.push_back(*packetBack);
        }
        stream.unsubscribe(subscriberID);
        // I should have missed the first 2 packets
        REQUIRE(packetsBack.size() == 8);
        for (int i = 0; i < static_cast<int> (packetsBack.size()); ++i)
        {
            REQUIRE(packetsBack.at(i).start_time_mus()
                    == localPackets.at(i + 2).start_time_mus());
        }
    }

    SECTION("Require ordered")
    {
        options.enableRequireOrdered();
        std::vector<UDataPacketImport::GRPC::V1::Packet> localPackets;
        for (int i = 3; i >= 0; --i)
        {
            localPackets.push_back(::createPacket(i, true));
        } 
        UDataPacketImport::GRPC::Stream stream{localPackets.at(0), options};
        // Become my own subscriber
        auto tid = std::this_thread::get_id();
        auto subscriberID = reinterpret_cast<uintptr_t> (&tid);
        REQUIRE_NOTHROW(stream.subscribe(subscriberID));
        // Set the first packet
        stream.setLatestPacket(localPackets.at(0));
        // Attempt to add the other packets (this should fail)
        // Add the rest of the packets
        for (int i = 1; i < static_cast<int> (localPackets.size()); ++i)
        {
            stream.setLatestPacket(localPackets.at(i));
        }
        auto packetBack = stream.getNextPacket(subscriberID);
        if (packetBack)
        {   
            REQUIRE(packetBack->start_time_mus() == localPackets.at(0).start_time_mus());
        }
        else
        {
            REQUIRE(false);
        }   
        // I should have skipped the previous packets so add a new one
        // that is valid
        auto newPacket = ::createPacket(4, true);
        stream.setLatestPacket(newPacket);
        packetBack = stream.getNextPacket(subscriberID);
        if (packetBack)
        {
            REQUIRE(packetBack->start_time_mus() == newPacket.start_time_mus());
        }
        else
        {
            REQUIRE(false);
        } 
        REQUIRE(stream.getNextPacket(subscriberID) == std::nullopt);
    }
}

TEST_CASE("UDataPacketImport::GRPC::SubscriptionManager",
          "[publisherTests]")
{
    for (int i = 0; i < 10; ++i)
    {
        packetsToBroadcast.push_back(::createPacket(i, true));
        packetsToBroadcast.push_back(::createPacket(i, false));
    }
 
    // Start server and accept connections
    ServerImpl server;
    //auto t1 = std::thread(&::ServerImpl::run, &server); 
    auto t1 = std::thread(&::ServerImpl::run, &server);
    std::this_thread::sleep_for(std::chrono::milliseconds {20});

    auto t2 = std::async(std::launch::async,
                         &::subscribeWithClient, false, true);
/*
    auto t3 = std::async(std::launch::async,
                         &::subscribe, true,  true);
*/
/*
    auto t4 = std::async(std::launch::async,
                         &::subscribe, false, false);
    auto t5 = std::async(std::launch::async,
                         &::subscribe, true,  false);
*/ 
    std::this_thread::sleep_for(std::chrono::seconds {4});
    std::cout << "wake up" << std::endl;
    server.stop();
    std::cout << "shut down" << std::endl;
    if (t1.joinable()){t1.join();}
    //if (t2.joinable()){t2.join();}
    try
    {
        t2.get();
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
    } 
/*
    try
    {
        t3.get();
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
    }
*/
/*
    try
    {
        t4.get();
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
    }
    try
    {
        t5.get();
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
    }
*/
}
