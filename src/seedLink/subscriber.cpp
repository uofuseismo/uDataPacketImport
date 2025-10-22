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

}

class Subscriber::SubscriberImpl
{
public:
    void getPackets()
    {
        class Reader : public grpc::ClientReadReactor<UDataPacketImport::GRPC::Packet> 
        {
        public:
            Reader(UDataPacketImport::GRPC::SEEDLinkBroadcast::Stub *stub,
                   const UDataPacketImport::GRPC::SubscribeToAllStreamsRequest &request)
            {
                stub->async()->Subscribe(&mContext, &request, this);
                StartRead(&mPacket);
            }
            void OnReadDone(bool ok)
            {
                if (ok)
                {
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
            }
            void OnDone(const grpc::Status &s) override
            {
                spdlog::info("Subscription terminated on server side");
            }
        private:
            grpc::ClientContext mContext;
            UDataPacketImport::GRPC::Packet mPacket; 
            std::atomic<bool> *mKeepRunning{nullptr};
            std::function<void (UDataPacketImport::GRPC::Packet &&)> mGetPacketCallback;
        };
    }
    SubscriberOptions mOptions;
    std::function<void (UDataPacketImport::GRPC::Packet &&)> mGetPacketCallback;
    std::atomic<bool> mKeepRunning{true};
};

/// Constructor
Subscriber::Subscriber(const SubscriberOptions &options) :
    pImpl(std::make_unique<SubscriberImpl> ())
{
    if (!options.hasAddress())
    {
        throw std::invalid_argument("Address not set");
    }
}

/// Destructor
Subscriber::~Subscriber() = default;
