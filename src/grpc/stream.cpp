#include <mutex>
#include <unordered_map>
#include <queue>
#include <spdlog/spdlog.h>
#include <grpcpp/server.h>
#include "uDataPacketImport/grpc/stream.hpp"
#include "uDataPacketImport/grpc/streamOptions.hpp"
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "src/getNow.hpp"

using namespace UDataPacketImport::GRPC;

namespace
{
class PacketQueue
{
public:
    explicit PacketQueue(const UDataPacketImport::GRPC::StreamOptions &options) :
        mQueueSize{static_cast<size_t> (options.getMaximumQueueSize())},
        mRequiredOrdered{options.requireOrdered()}
    {
    }
    void insert(const UDataPacketImport::GRPC::Packet &packetIn)
    {
        auto packet = packetIn;
        insert(std::move(packet));
    } 
    bool insert(UDataPacketImport::GRPC::Packet &&packet)
    {
        bool inserted{false};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        if (mRequiredOrdered)
        {
            // Expired or out of order?
            auto packetStartTime
                = std::chrono::microseconds {packet.start_time_mus()};
            if (packetStartTime <= mLatestSample)
            {
                return false;
            }
        }
        // Run with it
        auto packetTime = UDataPacketImport::getEndTime(packet);
        // Usually queue should be empty but just incase
        if (mQueue.size() == mQueueSize)
        {
            mQueue.pop();
        }
        auto packetEndTime = UDataPacketImport::getEndTime(packet);
        mQueue.push(std::move(packet));
        }
        return true;      
    }
    [[nodiscard]] std::optional<UDataPacketImport::GRPC::Packet> popFront()
    {
        std::lock_guard<std::mutex> lock(mMutex);
        if (mQueue.empty()){return std::nullopt;}
        std::optional<UDataPacketImport::GRPC::Packet> result(mQueue.front());
        mQueue.pop();
        return result;
    }
    mutable std::mutex mMutex;
    std::queue<UDataPacketImport::GRPC::Packet> mQueue;
    //moodycamel::ReaderWriterQueue<UDataPacketImport::GRPC::Packet> mQueue;
    std::chrono::microseconds mLatestSample{0};
    size_t mQueueSize{8};
    bool mRequiredOrdered{true};
};
       
}

class Stream::StreamImpl
{
public:
    // Constructor
    StreamImpl(UDataPacketImport::GRPC::Packet &&packet,
               const UDataPacketImport::GRPC::StreamOptions &options) :
        mStreamOptions(options)
    {
        UDataPacketImport::StreamIdentifier
            identifier{packet.stream_identifier()};
#ifndef NDEBUG
        assert(!identifier.toStringView().empty());
#endif
        mIdentifier = identifier.toString();
        mIdentifierStringView = mIdentifier;
        try
        {
            if (!setLatestPacket(std::move(packet)))
            {
                spdlog::warn("Failed to set first packet for " + mIdentifier);
            }
        }
        catch (const std::exception &e)
        {
            spdlog::warn("Failed to add first packet for "
                       + mIdentifier + " because " + std::string {e.what()});
        }
    }
    // Set the latest packet
    [[nodiscard]]
    bool setLatestPacket(UDataPacketImport::GRPC::Packet &&packetIn)
    {
        UDataPacketImport::StreamIdentifier
            identifier{packetIn.stream_identifier()};
        if (identifier.toStringView() != mIdentifierStringView)
        {
            spdlog::error("Cannot add " + identifier.toString()
                         + " to stream " + mIdentifier);
            return false;
        }
        bool errorsDetected{false};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        for (auto &subscriber : mSubscribers)
        {
            auto packet = packetIn; 
            auto queue = subscriber.second.get();
            if (!queue->insert(std::move(packet)))
            {
                spdlog::warn("Failed to enqueue packet for "
                           + subscriber.first->peer());
                errorsDetected = true;
            }
        }
        }
        return errorsDetected; 
    }
    // Subscribe
    void subscribe(grpc::CallbackServerContext *context)
    {   
        if (context == nullptr){throw std::invalid_argument("Context is null");}
        bool wasAdded{false};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        if (mSubscribers.contains(context))
        {
            spdlog::debug(context->peer() + " already subscribed to "
                        + mIdentifier);
            return;
        }
        // Add it
        spdlog::debug(context->peer() + " subscribing to  "
                    + mIdentifier);
        auto newQueue
            = std::make_unique<::PacketQueue> (mStreamOptions);
        wasAdded = mSubscribers.insert( {context, std::move(newQueue)} ).second;
        }
        if (wasAdded)
        {
            spdlog::debug(context->peer() + " subscribed to " + mIdentifier);
        }
        throw std::runtime_error(context->peer() + " failed to subscribe to "
                               + mIdentifier);
    }
    // Unsubscribe one particular customer
    void unsubscribe(grpc::CallbackServerContext *context)
    {
        if (context == nullptr){throw std::invalid_argument("Context is null");}
        bool wasUnsubscribed{false};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto index = mSubscribers.find(context);
        if (index != mSubscribers.end())
        {
            mSubscribers.erase(index);
            wasUnsubscribed = true;
        }
        else
        {
            wasUnsubscribed = false;
        }
        }
        if (wasUnsubscribed)
        {
            spdlog::debug(context->peer() + " unsubscribing from  " 
                        + mIdentifier);
        }
        else
        {
            spdlog::debug(context->peer() + " never subscribed to "
                        + mIdentifier);
        }
    }
    // Unsubscribe everyone
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
            spdlog::info("Purged " + std::to_string(count)
                       + " subscribers from " + mIdentifier );
        }
    }
    // Convenience function for subscriber to get next packet 
    [[nodiscard]] std::optional<UDataPacketImport::GRPC::Packet>
        getNextPacket(grpc::CallbackServerContext *context) const noexcept
    {
        UDataPacketImport::GRPC::Packet packet;
        bool notFound{false};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mSubscribers.find(context);
        if (idx == mSubscribers.end())
        {
            notFound = true;
        }
        else
        {
            return idx->second->popFront();
        }
        }
        if (notFound)
        {
            spdlog::warn(context->peer() + " not subscribed to " + mIdentifier);
        }
        return std::nullopt;
    }           

    mutable std::mutex mMutex; 
    std::unordered_map
    <
        grpc::CallbackServerContext *,
        std::unique_ptr<::PacketQueue> 
    > mSubscribers;
    StreamOptions mStreamOptions;
    std::string mIdentifier;
    std::string_view mIdentifierStringView;
    size_t mSubscriberQueueSize{8};
    bool mRequiredOrdered{false};
};

/// Constructor
Stream::Stream(UDataPacketImport::GRPC::Packet &&packet,
               const UDataPacketImport::GRPC::StreamOptions &options) :
    pImpl(std::make_unique<StreamImpl> (std::move(packet), options))
{
}

/// Destructor
Stream::~Stream() = default;

/// Get identifier
[[nodiscard]] const std::string_view Stream::getIdentifier() const noexcept
{
    return pImpl->mIdentifierStringView;
}

/// Sets the latest packet
void Stream::setLatestPacket(const UDataPacketImport::GRPC::Packet &packet)
{
    auto copy = packet;
    setLatestPacket(std::move(packet));
}

void Stream::setLatestPacket(UDataPacketImport::GRPC::Packet &&packet)
{
    if (!pImpl->setLatestPacket(std::move(packet)))
    {
        spdlog::warn("Errors detected while adding latest packet");
    }
}

/// Comparison
bool UDataPacketImport::GRPC::operator<(const Stream &lhs, const Stream &rhs)
{
    return lhs.getIdentifier() < rhs.getIdentifier();
} 
