#include <vector>
#include <atomic>
#include <future>
#include <thread>
#include <mutex>
#include <cmath>
#include <string>
#include <chrono>
#include <limits>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "uDataPacketImport/grpc/subscriptionManager.hpp"
#include "uDataPacketImport/grpc/subscriptionManagerOptions.hpp"
#include "uDataPacketImport/grpc/streamOptions.hpp"
#include "uDataPacketImport/grpc/stream.hpp"
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.pb.h"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#define SERVER_HOST "0.0.0.0:59598"
#define CLIENT_HOST "localhost:59598"

namespace
{
std::vector<UDataPacketImport::GRPC::Packet> packetsToBroadcast;
std::vector<UDataPacketImport::GRPC::Packet> packetsReceived;

[[nodiscard]] 
UDataPacketImport::GRPC::Packet
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

class ServiceImpl : public UDataPacketImport::GRPC::RealTimeBroadcast::CallbackService
{
public:
    ServiceImpl()
    {
        UDataPacketImport::GRPC::SubscriptionManagerOptions options;
        UDataPacketImport::GRPC::StreamOptions streamOptions;
        streamOptions.enableRequireOrdered();
        options.setStreamOptions(streamOptions);
        mManager
            = std::make_unique<UDataPacketImport::GRPC::SubscriptionManager>
              (options);
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
            std::cout << "Broadcast packet" << std::endl;
            try
            {
                mManager->addPacket(packet);
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << std::endl;
            }
        }
    }
    void stop()
    {
        mKeepRunning = false;
    }
    std::unique_ptr<UDataPacketImport::GRPC::SubscriptionManager> mManager;
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
        std::vector<UDataPacketImport::GRPC::Packet> localPackets;
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
        auto response = stream.unsubscribe(subscriberID);
        REQUIRE(response.packets_read() == 2);
        REQUIRE(stream.getNumberOfSubscribers() == 1);
        response = stream.unsubscribe(subscriberID + 1);
        REQUIRE(response.packets_read() == 2);
        REQUIRE(stream.getNumberOfSubscribers() == 0);
    }
}

TEST_CASE("UDataPacketImport::GRPC::SubscriptionManager",
          "[publisherTests]")
{
    for (int i = 0; i < 3; ++i)
    {
        packetsToBroadcast.push_back(::createPacket(i, true));
        packetsToBroadcast.push_back(::createPacket(i, false));
    }
 
    ServerImpl server;
    auto t1 = std::thread(&::ServerImpl::run, &server); 

    std::this_thread::sleep_for(std::chrono::seconds {5});
    std::cout << "wake up" << std::endl;
    server.stop();
    std::cout << "shtu down" << std::endl;
    if (t1.joinable()){t1.join();}
}
