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
            std::cout << "Broadcast packet" << std::endl;
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
    grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet> *
    SubscribeToAllStreams(
        grpc::CallbackServerContext *context,
        const UDataPacketImport::GRPC::SubscribeToAllStreamsRequest *request) override 
    {
/*
        class Writer : public grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet> 
        {
        public:
            Writer(grpc::CallbackServerContext *context,
                   const UDataPacketImport::GRPC::SubscriptionRequest *request,
                   std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager> &subscriptionManager,
                   std::atomic<bool> *keepRunning) :
                mContext(context),
                mManager(subscriptionManager),
                mKeepRunning(keepRunning)
            {
                // Subscribe
                try
                {
                    mManager->subscribeToAll(context);
                }
                catch (const std::exception &e) 
                {
                    std::cerr << "Failed to subscribe because "
                              << e.what() << std::endl;
                    Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Failed to subscribe"));
                }
                nextWrite();
            }
            void OnWriteDone(bool ok) override 
            {
                if (!ok) 
                {
                    Finish(grpc::Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure"));
                }
                mWriteInProgress = false;
                mPacketsQueue.pop();
                nextWrite();
            }   
            void OnDone() override 
            {
                std::cout << "RPC Completed" << std::endl;
                if (mContext)
                {
                    mManager->unsubscribeFromAllOnCancel(mContext);
                }
                delete this;
            }   
            void OnCancel() override 
            { 
                std::cout << "RPC Cancelled" << std::endl;
                if (mContext)
                {
                    mManager->unsubscribeFromAllOnCancel(mContext);
                }
            }
        private:
            void nextWrite() 
            {
                UDataPacketImport::GRPC::Packet nextPacket;
                while (mKeepRunning->load() && !mContext->IsCancelled()) //next_feature_ != feature_list_->end()) 
                {
                    // Get any remaining packets on the queue on the wire
                    if (!mPacketsQueue.empty() && !mWriteInProgress)
                    {
                        const auto &packet = mPacketsQueue.front();
                        mWriteInProgress = true;
                        StartWrite(&packet);
                        return;
                    }
                    // Try to get more packets to write
                    if (mPacketsQueue.empty())
                    {
                        try
                        {
                            auto packetsBuffer
                                = mManager->getNextPacketsFromAllSubscriptions(
                                     mContext);
                            for (auto &packet : packetsBuffer)
                            {
                                if (mPacketsQueue.size() > mMaximumQueueSize)
                                {
                                    mPacketsQueue.pop();
                                }
                                mPacketsQueue.push(std::move(packet));
  std::cout << "got more" << std::endl;
                            }
                        }
                        catch (const std::exception &e)
                        {
                            std::cerr << e.what() << std::endl;
                        }
                    }
                    // Take a break
                    if (mPacketsQueue.empty())
                    {
                        std::this_thread::sleep_for(mTimeOut);
                    }
                }
                if (mContext->IsCancelled())
                {
                    std::cout << "Terminating acquisition because of client side cancel "
                              << mContext->peer() << std::endl;
                }
                else
                {
                    std::cout << "Terminating acquisition because of server side shutdown" << std::endl;
                }
                //if (mContext){mManager->unsubscribeFromAll(mContext);}
                Finish(grpc::Status::OK);
            }
            grpc::CallbackServerContext *mContext{nullptr};
            std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager> mManager{nullptr};
            std::queue<UDataPacketImport::GRPC::Packet> mPacketsQueue;
            std::atomic<bool> *mKeepRunning{nullptr};
            std::chrono::milliseconds mTimeOut{10};
            size_t mMaximumQueueSize{2048};
            bool mWriteInProgress{false};
        };
*/
        return new ::AsynchronousWriterSubscribeToAll(
                      context, request, mManager, &mKeepRunning);
    }
/*
    grpc::Status ListFeatures(ServerContext* context,
                      const routeguide::Rectangle* rectangle,
                      ServerWriter<Feature>* writer) override {
    auto lo = rectangle->lo();
    auto hi = rectangle->hi();
    long left = (std::min)(lo.longitude(), hi.longitude());
    long right = (std::max)(lo.longitude(), hi.longitude());
    long top = (std::max)(lo.latitude(), hi.latitude());
    long bottom = (std::min)(lo.latitude(), hi.latitude());
    for (const Feature& f : feature_list_) {
      if (f.location().longitude() >= left &&
          f.location().longitude() <= right &&
          f.location().latitude() >= bottom && f.location().latitude() <= top) {
        writer->Write(f);
      }   
    }   
    return Status::OK;
  }
*/

    void stop()
    {
        mKeepRunning = false;
        mManager->unsubscribeAll();
    }
    std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager> mManager{nullptr};
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

void subscribe()
{
bool doCancel = false;
std::cout << "im here" << std::endl;
    grpc::ClientContext context;
    //Status status = stub_->GetFeature(&context, point, feature);
    auto channel
        = grpc::CreateChannel(CLIENT_HOST,
                              grpc::InsecureChannelCredentials());
    auto stub = UDataPacketImport::GRPC::RealTimeBroadcast::NewStub(channel);
    UDataPacketImport::GRPC::SubscribeToAllStreamsRequest request;
    std::unique_ptr<grpc::ClientReader<UDataPacketImport::GRPC::Packet> > reader(
        stub->SubscribeToAllStreams(&context, request));
std::cout << "sub" << std::endl;
    UDataPacketImport::GRPC::Packet packet;
int i =0;
    while (reader->Read(&packet)) 
    {
        //std::cout << "look at me right here e yes" << std::endl;
        std::cout << packet.start_time_mus() << std::endl;
if (i == 2 && doCancel){break;}
++i;
/*
      std::cout << "Found feature called " << feature.name() << " at "
                << feature.location().latitude() / kCoordFactor_ << ", "
                << feature.location().longitude() / kCoordFactor_ << std::endl;
*/
    }   
if (doCancel){context.TryCancel();}
//std::cout << "Done" << std::endl;
    auto status = reader->Finish();
    if (status.ok()) {
      std::cout << "SubscribeToAll rpc succeeded." << std::endl;
    } else {
      std::cout << "SubscribeToAll rpc failed." << std::endl;
    }   

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

/*
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

    SECTION("Overflow queue")
    {
        std::vector<UDataPacketImport::GRPC::Packet> localPackets;
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
 
        std::vector<UDataPacketImport::GRPC::Packet> packetsBack;
        for (int i = 0; i < static_cast<int> (localPackets.size()); ++i)
        {
            auto packetBack = stream.getNextPacket(subscriberID); 
            if (!packetBack){break;}
            packetsBack.push_back(*packetBack);
        }
        auto response = stream.unsubscribe(subscriberID);
        REQUIRE(response.packets_read() == 8);
        // I should have missed the first 2 packets
        for (int i = 0; i < static_cast<int> (packetsBack.size()); ++i)
        {
            REQUIRE(packetsBack.at(i).start_time_mus()
                    == localPackets.at(i + 2).start_time_mus());
        }
    }

    SECTION("Require ordered")
    {
        options.enableRequireOrdered();
        std::vector<UDataPacketImport::GRPC::Packet> localPackets;
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
*/

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
    std::this_thread::sleep_for(std::chrono::milliseconds {10});

    //auto t2 = std::thread(&::subscribe);
    auto t2 = std::async(std::launch::async, &::subscribe);
    auto t3 = std::async(std::launch::async, &::subscribe);
 
    std::this_thread::sleep_for(std::chrono::seconds {5});
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
    try
    {
        t3.get();
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
    }
}
