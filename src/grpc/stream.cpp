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
    void insert(UDataPacketImport::GRPC::Packet &&packet)
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
                return; // true; //false;
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
        mLatestSample = packetEndTime;
        }
        //return true;      
    }
    /*
    [[nodiscard]] uint64_t getNumberOfPacketsRead() const noexcept
    {
        return mPacketsRead.load();
    }
    */
    [[nodiscard]] std::optional<UDataPacketImport::GRPC::Packet> popFront()
    {
        std::optional<UDataPacketImport::GRPC::Packet> result{std::nullopt};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        if (mQueue.empty()){return std::nullopt;}
        result
            = std::move
              (
                 std::optional<UDataPacketImport::GRPC::Packet> 
                 (
                    std::move(mQueue.front())
                 )
              );
        mQueue.pop();
        }
        //mPacketsRead.fetch_add(1);
        return result;
    }
    mutable std::mutex mMutex;
    std::queue<UDataPacketImport::GRPC::Packet> mQueue;
    std::chrono::microseconds mLatestSample
    {
        std::numeric_limits<int64_t>::lowest()
    };
    size_t mQueueSize{8};
    //std::atomic<uint64_t> mPacketsRead{0};
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
        mIdentifier = identifier.toProtobuf();
        mIdentifierString = identifier.toString();
        mIdentifierStringView = mIdentifierString;
        try
        {
            if (!setLatestPacket(std::move(packet)))
            {
                spdlog::warn("Failed to set first packet for "
                           + mIdentifierString);
            }
        }
        catch (const std::exception &e)
        {
            spdlog::warn("Failed to add first packet for "
                       + mIdentifierString + " because "
                       + std::string {e.what()});
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
                         + " to stream " + mIdentifierString);
            return false;
        }
        bool success{true};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        for (auto &subscriber : mSubscribers)
        {
            auto packet = packetIn; 
            auto queue = subscriber.second.get();
            try
            {
                queue->insert(std::move(packet));
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to enqueue packet for "
                           + std::to_string(subscriber.first) 
                           + " because " + std::string {e.what()});
                success = false;
            }
/*
            if (!queue->insert(std::move(packet)))
            {
                spdlog::warn("Failed to enqueue packet for "
                           + std::to_string(subscriber.first));
//                           + subscriber.first->peer());
                success = false;
            }
*/
        }
        }
        return success;
    }
    // Subscribe
    void subscribe(const uintptr_t contextAddress)
    {
        auto contextAddressString = std::to_string(contextAddress);
        //if (context == nullptr){throw std::invalid_argument("Context is null");}
        bool wasAdded{false};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        if (mSubscribers.contains(contextAddress))
        {
            spdlog::debug(contextAddressString //context->peer()
                        + " already subscribed to "
                        + mIdentifierString);
            return;
        }
        // Add it
        spdlog::debug(contextAddressString + " subscribing to "
                    + mIdentifierString);
        auto newQueue
            = std::make_unique<::PacketQueue> (mStreamOptions);
        if (newQueue == nullptr)
        {
            throw std::runtime_error("Failed to create queue for "
                                   + contextAddressString);
        }
        std::pair<uintptr_t, std::unique_ptr<::PacketQueue>>
            newSubscriberPair{contextAddress, std::move(newQueue)};
        wasAdded = mSubscribers.insert(std::move(newSubscriberPair)).second;
        }
        if (wasAdded)
        {
            spdlog::debug(contextAddressString
                        + " subscribed to " + mIdentifierString);
            return;
        }
        throw std::runtime_error("Failed to subscribe "
                               + contextAddressString + " to stream "
                               + mIdentifierString);
    }
    // Unsubscribe one particular customer
    void unsubscribe(const uintptr_t contextAddress)
    {
        bool wasUnsubscribed{false};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto index = mSubscribers.find(contextAddress);
        if (index != mSubscribers.end())
        {
            mSubscribers.erase(index);
            wasUnsubscribed = true;
        }
        else
        {
            wasUnsubscribed = false;
        }
        } // End lock
        if (wasUnsubscribed)
        {
            spdlog::debug(std::to_string(contextAddress) //context->peer()
                        + " unsubscribing from  " 
                        + mIdentifierString);
        }
        else
        {
            spdlog::debug(std::to_string(contextAddress) //context->peer()
                        + " never subscribed to "
                        + mIdentifierString);
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
                       + " subscribers from " + mIdentifierString );
        }
    }
    // Convenience function for subscriber to get next packet 
    [[nodiscard]] std::optional<UDataPacketImport::GRPC::Packet>
        getNextPacket(const uintptr_t contextAddress) const noexcept
    {
        UDataPacketImport::GRPC::Packet packet;
        bool notFound{false};
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mSubscribers.find(contextAddress);
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
            spdlog::warn(std::to_string(contextAddress) //context->peer()
                       + " not subscribed to " + mIdentifierString);
        }
        return std::nullopt;
    }
    [[nodiscard]] int getNumberOfSubscribers() const noexcept
    {
        std::lock_guard<std::mutex> lock(mMutex);
        return static_cast<int> (mSubscribers.size());
    }
    [[nodiscard]]
    bool isSubscribed(const uintptr_t contextAddress) const noexcept
    {
        std::lock_guard<std::mutex> lock(mMutex);
        return mSubscribers.contains(contextAddress);
    }

    mutable std::mutex mMutex; 
    std::unordered_map
    <
        uintptr_t, //grpc::CallbackServerContext *,
        std::unique_ptr<::PacketQueue> 
    > mSubscribers;
    StreamOptions mStreamOptions;
    UDataPacketImport::GRPC::StreamIdentifier mIdentifier;
    std::string mIdentifierString;
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

Stream::Stream(const UDataPacketImport::GRPC::Packet &packet,
               const UDataPacketImport::GRPC::StreamOptions &options) :
    pImpl(std::make_unique<StreamImpl> 
    (
        std::move(UDataPacketImport::GRPC::Packet {packet}), options)
    )
{
}


/// Destructor
Stream::~Stream() = default;

/// Get identifier
[[nodiscard]] std::string Stream::getIdentifier() const noexcept
{
    return pImpl->mIdentifierString;
}

/// Sets the latest packet
void Stream::setLatestPacket(const UDataPacketImport::GRPC::Packet &packetIn)
{
    UDataPacketImport::GRPC::Packet packet{packetIn};
    setLatestPacket(std::move(packet));
}

void Stream::setLatestPacket(UDataPacketImport::GRPC::Packet &&packet)
{
    if (!pImpl->setLatestPacket(std::move(packet)))
    {
        spdlog::warn("Errors detected while adding latest packet");
    }
}

void Stream::subscribe(const uintptr_t contextAddress)
{
    pImpl->subscribe(contextAddress);
}

/*
void Stream::subscribe(grpc::CallbackServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Context is null");
    }
    auto contextAddress = reinterpret_cast<uintptr_t> (context);
    subscribe(contextAddress);
    //pImpl->subscribe(context);
}
*/

std::optional<UDataPacketImport::GRPC::Packet>
    Stream::getNextPacket(const uintptr_t contextAddress) const noexcept
{
    return pImpl->getNextPacket(contextAddress);
}

void Stream::unsubscribe(const uintptr_t contextAddress)
{
    pImpl->unsubscribe(contextAddress);
}

/*
UnsubscribeResponse Stream::unsubscribe(grpc::CallbackServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Context is null");
    }
    auto contextAddress = reinterpret_cast<uintptr_t> (context);
    return unsubscribe(contextAddress);
    //return pImpl->unsubscribe(context);
}
*/

int Stream::getNumberOfSubscribers() const noexcept
{
    return pImpl->getNumberOfSubscribers();
}

/// Subscribed?
bool Stream::isSubscribed(const uintptr_t contextAddress) const noexcept
{
    return pImpl->isSubscribed(contextAddress);
}

/// Comparison
bool UDataPacketImport::GRPC::operator<(const Stream &lhs, const Stream &rhs)
{
    return lhs.getIdentifier() < rhs.getIdentifier();
} 

void Stream::unsubscribeAll()
{
    pImpl->unsubscribeAll();    
}

