#include <atomic>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <grpc/grpc.h>
#include <grpcpp/alarm.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <readerwriterqueue.h>
#include <spdlog/spdlog.h>
//#include <moodycamel/readerwriterqueue.h>
#include "uDataPacketImport/seedLink/subscriber.hpp"
#include "uDataPacketImport/seedLink/subscriberOptions.hpp"
#include "uDataPacketImport/packet.hpp"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "proto/dataPacketBroadcast.pb.h"
#include "src/isEmpty.hpp"

using namespace UDataPacketImport::SEEDLink;

namespace
{

bool matches(const UDataPacketImport::GRPC::StreamIdentifier &identifier,
             const std::string_view &networkSelector)
{
    auto network = ::convertString(identifier.network());
    if (network.empty())
    {
        throw std::runtime_error("network is empty in packet");
    }
    // We'll take anything
    if (networkSelector.empty()){return true;}
    // Direct hit; e.g., UU == UU 
    if (network == networkSelector){return true;}
    // Network codes are pretty limited
    if (networkSelector == "??")
    {
        return true;
    }
    return false;
}

bool matches(const UDataPacketImport::GRPC::StreamIdentifier &identifier,
             const std::string_view &networkSelector,
             const std::string_view &stationSelector)
{
    if (!matches(identifier, networkSelector)){return false;}
    auto station = ::convertString(identifier.station());
    if (station.empty())
    {
        throw std::runtime_error("station is empty in packet");
    }
    // We'll take anything
    if (stationSelector.empty()){return true;}
    // Direct hit; e.g., CTU == CTU
    if (station == stationSelector){return true;}
    // Wild card all stations
    if (stationSelector == "*"){return true;}
    if (stationSelector.size() == station.size())
    {
        for (size_t i = 0 ; i < stationSelector.size(); ++i)
        {
            if (stationSelector[i] != '?' && 
                stationSelector[i] != station[i])
            {
                return false;
            }
        }
        return true;
    }
    return false;
}

bool matches(const UDataPacketImport::GRPC::StreamIdentifier &identifier,
             const std::string_view &networkSelector,
             const std::string_view &stationSelector,
             const std::string_view &channelSelector)
{
    if (!matches(identifier, networkSelector, stationSelector)){return false;}
    auto channel = ::convertString(identifier.channel());
    if (channel.empty())
    {
        throw std::runtime_error("channel is empty in packet");
    }
    // We'll take anything
    if (channelSelector.empty()){return true;}
    // Direct hit; e.g., HHZ == HHZ
    if (channel == channelSelector){return true;}
    // Wild card all channels
    if (channelSelector == "*"){return true;}
    if (channelSelector.size() == channel.size())
    {
        for (size_t i = 0 ; i < channelSelector.size(); ++i)
        {
            if (channelSelector[i] != '?' &&  
                channelSelector[i] != channel[i])
            {
                return false;
            }
        }
        return true;
    }
    return false;
}

bool matches(const UDataPacketImport::GRPC::StreamIdentifier &identifier,
             const std::string_view &networkSelector,
             const std::string_view &stationSelector,
             const std::string_view &channelSelector,
             const std::string_view &locationCodeSelector)
{
    if (!::matches(identifier,
                   networkSelector, stationSelector, channelSelector))
    {
        return false;
    }
    auto locationCode = ::convertString(identifier.location_code());
    // We'll take anything
    if (locationCodeSelector.empty()){return true;}
    // Direct hit; e.g., 01 == 01
    if (locationCode == locationCodeSelector){return true;}
    // This happens a lot
    if (locationCode.size() == locationCodeSelector.size())
    {
        for (size_t i = 0 ; i < channelSelector.size(); ++i)
        {
            if (locationCodeSelector[i] != '?' &&
                locationCodeSelector[i] != locationCode[i])
            {
                return false;
            }
        }
        return true;
    }
    return false;
}

std::shared_ptr<grpc::Channel> createChannel(const SubscriberOptions &options)
{
    auto address = options.getAddress();
    spdlog::info("Will connect to gRPC SEEDLink publisher at " + address);
    std::shared_ptr<grpc::Channel> channel{nullptr};
    channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    return channel;
}

}

class Subscriber::SubscriberImpl
{
public:
    SubscriberImpl() = delete;
    SubscriberImpl(
        const std::function<void (UDataPacketImport::GRPC::Packet &&)> &callback,
        const SubscriberOptions &options) :
        mOptions(options),
        mGetPacketCallback(callback)
    {
        mInitialized = true;
    }
    [[nodiscard]] std::future<void> start()
    {
        stop();
        mKeepRunning = true;
        auto result = std::async(&SubscriberImpl::acquirePackets, this);
        return result;
    }
    void stop()
    {
        mKeepRunning = false;
    }
/*
    void acquirePackets()
    {
        while (mKeepRunning)
        {
std::this_thread::sleep_for(std::chrono::seconds {1});
        }
        if (!mKeepRunning)
        {
            spdlog::critical("gRPC SEEDLink subscriber terminating early");
            throw std::runtime_error("gRPC SEEDLink premature end of import");
        }
        spdlog::info("Thread exiting acquisition loop");
    }
*/
    void acquirePackets()
    {
/*
        class Reader :
            public grpc::ClientReadReactor<UDataPacketImport::GRPC::Packet>
        {
        public:
            Reader(UDataPacketImport::GRPC::SEEDLinkBroadcast::Stub *stub,
                   const UDataPacketImport::GRPC::SubscribeToAllStreamsRequest &request,
                   const std::function<void (UDataPacketImport::GRPC::Packet &&)> &callback,
                   std::atomic<bool> *keepRunning) :
                mStub(stub),
                mGetPacketCallback(callback),
                mKeepRunning(keepRunning)
            {
                mContext.set_wait_for_ready(true);
                mStub->async()->Subscribe(&mContext, &request, this);
                StartRead(&mPacket);
                StartCall();
            }
            void OnReadDone(bool ok) override
            {
                if (ok)
                {
spdlog::info("got one");
                    mGetPacketCallback(std::move(mPacket));
                    if (mKeepRunning->load())
                    {
                        StartRead(&mPacket);
                    }
                    else
                    {
                        spdlog::debug("Failing through; subscription canceled");
                    }
                }
                else
                {
                    spdlog::info("didn't get one");
                }
            }
            void OnDone(const grpc::Status &status) override
            {
                spdlog::info("In OnDone");
                if (status.ok())
                {
                    spdlog::info("Subscription terminated on server side");
                }
                mStatus = status;
                mRPCCompleted = true;
                mAwaitConditionVariable.notify_one();
            }
            grpc::Status await()
            {
                std::unique_lock<std::mutex> lock(mConditionVariableMutex);
                mAwaitConditionVariable.wait(lock, [this] 
                {
                    return mRPCCompleted;
                }); 
                return std::move(mStatus);
            }
        private:
            std::mutex mConditionVariableMutex;
            std::condition_variable mAwaitConditionVariable;
            UDataPacketImport::GRPC::SEEDLinkBroadcast::Stub *mStub{nullptr};
            grpc::ClientContext mContext;
            grpc::Status mStatus;
            UDataPacketImport::GRPC::Packet mPacket; 
            std::function<void (UDataPacketImport::GRPC::Packet &&)> mGetPacketCallback;
            std::atomic<bool> *mKeepRunning{nullptr};
            bool mRPCCompleted{false};
        };
*/
        // Begin the activity of connecting and starting the RPC
        int nReconnect{0};
        auto reconnectSchedule = mOptions.getReconnectSchedule();
        while (mKeepRunning)
        {
            spdlog::info("Subscribing to SEEDLink gRPC broadcast");
            auto channel = ::createChannel(mOptions);
            auto stub
                = UDataPacketImport::GRPC::SEEDLinkBroadcast::NewStub(channel);
            grpc::ClientContext context;
            context.set_wait_for_ready(false);
            //mConnected = true;
            UDataPacketImport::GRPC::SubscribeToAllStreamsRequest request;
            std::unique_ptr<grpc::ClientReader<UDataPacketImport::GRPC::Packet>>
                reader(stub->Subscribe(&context, request));
            UDataPacketImport::GRPC::Packet packet;
            spdlog::debug("Beginning SEEDLink gRPC subscription loop");
            while (reader->Read(&packet))
            {
                if (!mKeepRunning)
                {
                    spdlog::info(
                        "Subscription loop leaving b/c of application termination");
                    context.TryCancel();
                    break;
                }   
                try
                {
                    mGetPacketCallback(std::move(packet));
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Failed to propagate packet because "
                               + std::string{e.what()});
                }
            }
            spdlog::debug("Exited subscription loop");
            auto status = reader->Finish();
/*
            Reader packetReader{stub.get(),
                                request,
                                mGetPacketCallback,
                                &mKeepRunning};
            // Have this thread wait for the RPC to complete
            auto status = packetReader.await();
*/
            if (status.ok())
            {
                spdlog::info("Subscription status ended succesfully");
                nReconnect = 0;
                if (!mKeepRunning){break;}
                //mConnected = false;
            }
            else
            {
                //mConnected = false;
                bool doReconnect{false};
                if (status.error_code() == grpc::UNAVAILABLE)
                {
                    spdlog::warn("Service is unavailable ("
                               + status.error_message() + ")");
                    if (mKeepRunning)
                    {
                        doReconnect = true;
                    }
                }
                else if (status.error_code() == grpc::CANCELLED)
                {
                    if (!mKeepRunning)
                    {
                        spdlog::debug("RPC cancellation successful");
                    }
                    else
                    {
                        spdlog::warn("Server side cancellation");
                        doReconnect = true;
                    }
                }
                else
                {
                    spdlog::info("Exited subscription RPC with error code "
                               + std::to_string(status.error_code())
                               + " and message " + status.error_message());
                    if (status.error_code() == grpc::CANCELLED)
                    {
                        if (!mKeepRunning)
                        {
                            throw std::runtime_error("Server canceled request");
                        }
                    }
                }
                if (doReconnect)
                {
                    std::this_thread::sleep_for(
                       reconnectSchedule.at(nReconnect)); 
                    nReconnect = nReconnect + 1;
                    if (nReconnect >=
                        static_cast<int> (reconnectSchedule.size()))
                    {   
                        throw std::runtime_error(
                            "Max number of reconnects hit - terminating");
                    }   
                }
            }
            //mConnected = false;
        } // Loop on keep running
        if (mKeepRunning)
        {
            throw std::runtime_error("SEEDLink subscriber failed");
        }
        spdlog::info("SEEDLink subscriber thread leaving");
    }
    SubscriberOptions mOptions;
    std::function<void (UDataPacketImport::GRPC::Packet &&)> mGetPacketCallback;
    std::atomic<bool> mKeepRunning{true};
    //std::atomic<bool> mConnected{false};
    bool mInitialized{false};
};

/// Constructor
Subscriber::Subscriber(
    const std::function<void (UDataPacketImport::GRPC::Packet &&)> &callback, 
    const SubscriberOptions &options) :
    //IClient(callback),
    pImpl(std::make_unique<SubscriberImpl> (callback, options))
{
}

/// Run the subscriber
std::future<void> Subscriber::start()
{
    return pImpl->start();
}

/// Stop the subscriber
void Subscriber::stop()
{
    pImpl->stop();
}

bool Subscriber::isInitialized() const noexcept
{
    return pImpl->mInitialized;
}

/*
bool Subscriber::isConnected() const noexcept
{
    return pImpl->mConnected.load();
}

void Subscriber::connect()
{
    return;
}
*/

bool Subscriber::isRunning() const noexcept
{
    return pImpl->mKeepRunning.load();
}

UDataPacketImport::IAcquisition::Type Subscriber::getType() const noexcept
{
    return UDataPacketImport::IAcquisition::Type::gRPC;
}

/// Destructor
Subscriber::~Subscriber() = default;
