#include <mutex>
#include <atomic>
#include <map>
#include <set>
#ifndef NDEBUG
#include <cassert>
#endif
#include <spdlog/spdlog.h>
#include <grpcpp/server.h>
#include "uDataPacketImport/grpc/subscriptionManager.hpp"
#include "uDataPacketImport/grpc/subscriptionManagerOptions.hpp"
#include "uDataPacketImport/grpc/stream.hpp"
#include "uDataPacketImport/grpc/streamOptions.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "uDataPacketImport/packet.hpp"

using namespace UDataPacketImport::GRPC;

class SubscriptionManager::SubscriptionManagerImpl
{
public:
    explicit SubscriptionManagerImpl(
        const SubscriptionManagerOptions &options) :
        mOptions(options)
    {
        mStreamOptions = mOptions.getStreamOptions();
    }
    void setLatestPacket(UDataPacketImport::GRPC::Packet &&packet)
    {
        bool addedPacket{false};
        bool createdStream{false};
        std::string errorMessage;
        UDataPacketImport::StreamIdentifier
            streamIdentifier{packet.stream_identifier()}; 
        auto streamIdentifierString = streamIdentifier.toString();
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mStreamsMap.find(streamIdentifierString);
        if (idx != mStreamsMap.end())
        {
            //spdlog::debug("SubscriptionManagerImpl found "
            //            + streamIdentifierString
            //            + "; adding packet...");
            idx->second->setLatestPacket(std::move(packet));
            return;
        }
        // Create stream
        try
        {
            spdlog::info("SubscriptionManagerImpl creating stream "
                       + streamIdentifierString);
            auto stream
                = std::make_unique<Stream> (std::move(packet), mStreamOptions);
            std::pair
            <
                std::string,
                std::unique_ptr<Stream>
            > newStreamPair{streamIdentifierString, std::move(stream)};
            auto added = mStreamsMap.insert(std::move(newStreamPair)).second;
            if (added)
            {
                spdlog::info("SubscriptionManagerImpl added "
                           + streamIdentifierString
                           + " to streams map.  Map size is "
                           + std::to_string(mStreamsMap.size()));
                createdStream = true;
            }
            else
            {
                spdlog::warn("SubscriptionManagerImpl failed to add "
                           + streamIdentifierString
                           + " to streams map");
                createdStream = false;
            }
#ifndef NDEBUG
            assert(mStreamsMap.contains(streamIdentifierString));
#endif
        }
        catch (const std::exception &e)
        {
            errorMessage = std::string{e.what()};
        }
        if (!createdStream)
        {
            spdlog::critical("SubscriptionManagerImpl failed to create stream "
                           + streamIdentifierString
                           + " because " + errorMessage);
            return;
        }
        idx = mStreamsMap.find(streamIdentifierString);
        // Add any pending subscriptions to the stream
        if (idx != mStreamsMap.end())
        {
            for (auto &pendingSubscription : mPendingSubscriptions)
            {
                for (auto &desiredStreamIdentifier : pendingSubscription.second)
                {
                    if (streamIdentifier == desiredStreamIdentifier)
                    {
                        spdlog::info("Adding " 
                                   + pendingSubscription.first->peer()
                                   + " to stream " + streamIdentifierString);
                        auto contextAddress
                            = reinterpret_cast<uintptr_t>
                              (pendingSubscription.first);
                        idx->second->subscribe(contextAddress); //pendingSubscription.first);
                        pendingSubscription.second.erase(
                            desiredStreamIdentifier);
                    }
                }
                // This guy is fully subscribed
                if (pendingSubscription.second.empty())
                {
                    mPendingSubscriptions.erase(pendingSubscription.first);
                    spdlog::info("All pending subscriptions filled for "
                               + pendingSubscription.first->peer());
                }
            }
        }
        else
        {
            spdlog::warn("This is strange - can't find stream I just added");
        }
        }
    } 
    [[nodiscard]] UDataPacketImport::GRPC::UnsubscribeResponse
        unsubscribe(grpc::CallbackServerContext *context,
                    const UnsubscribeRequest &request)
    {
        UDataPacketImport::StreamIdentifier
            streamIdentifier{request.stream_identifier()};
        auto streamIdentifierString = streamIdentifier.toString();
        UDataPacketImport::GRPC::UnsubscribeResponse response;
        *response.mutable_stream_identifier() = streamIdentifier.toProtobuf();
        response.set_packets_read(0);
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mStreamsMap.find(streamIdentifierString);
        if (idx != mStreamsMap.end())
        {
            auto contextAddress = reinterpret_cast<uintptr_t> (context);
            response = idx->second->unsubscribe(contextAddress); 
            mStreamsMap.erase(idx);
        }
        // Ensure there's nothing pending
        mPendingSubscriptions.erase(context); 
        }
        return response;
    }
    mutable std::mutex mMutex;
    SubscriptionManagerOptions mOptions;
    StreamOptions mStreamOptions;
    std::map
    <
        std::string, //UDataPacketImport::StreamIdentifier,
        std::unique_ptr<Stream>
    > mStreamsMap;
    std::map
    <
        grpc::CallbackServerContext *,
        std::set<UDataPacketImport::StreamIdentifier>
    > mPendingSubscriptions;
    std::atomic<bool> mNumberOfSubscribers{0};
};

/// Constructor
SubscriptionManager::SubscriptionManager(
    const SubscriptionManagerOptions &options) :
    pImpl(std::make_unique<SubscriptionManagerImpl> (options))
{
}

/// Destructor
SubscriptionManager::~SubscriptionManager() = default;

/// Add a packet
void SubscriptionManager::addPacket(
    const UDataPacketImport::GRPC::Packet &packet)
{
    addPacket(std::move(UDataPacketImport::GRPC::Packet {packet}));
}

void SubscriptionManager::addPacket(
    UDataPacketImport::GRPC::Packet &&packet)
{
    // Won't get far without this
    if (!packet.has_stream_identifier())
    {
        throw std::invalid_argument("Stream identifier not set");
    }
    if (packet.sampling_rate() <= 0)
    {
        throw std::invalid_argument("Sampling rate not positive");
    }
    pImpl->setLatestPacket(std::move(packet));
}
