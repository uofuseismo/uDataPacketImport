#include <mutex>
#include <atomic>
#include <map>
#include <set>
#include <spdlog/spdlog.h>
#include <grpcpp/server.h>
#include "uDataPacketImport/grpc/subscriptionManager.hpp"
#include "uDataPacketImport/grpc/stream.hpp"
#include "uDataPacketImport/grpc/streamOptions.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "uDataPacketImport/packet.hpp"

using namespace UDataPacketImport::GRPC;

class SubscriptionManager::SubscriptionManagerImpl
{
public:
    void setLatestPacket(UDataPacketImport::GRPC::Packet &&packet)
    {
        bool addedPacket{false};
        bool createdStream{false};
        std::string errorMessage;
        UDataPacketImport::StreamIdentifier
            streamIdentifier{packet.stream_identifier()}; 
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mStreamsMap.find(streamIdentifier);
        if (idx != mStreamsMap.end())
        {
            idx->second->setLatestPacket(std::move(packet));
            return;
        }
        // Create stream
        try
        {
            auto stream
                = std::make_unique<Stream>
                  (std::move(packet), mStreamOptions);
            idx = mStreamsMap.insert(
                std::pair{streamIdentifier, std::move(stream)} ).first;
        }
        catch (const std::exception &e)
        {
            errorMessage = std::string{e.what()};
        }
        if (!createdStream)
        {
            spdlog::critical("Failed to create stream "
                           + streamIdentifier.toString()
                           + " because " + errorMessage);
            return;
        }
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
                                   + " to stream " + streamIdentifier.toString());
                        idx->second->subscribe(pendingSubscription.first);
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
            spdlog::warn("This is strange");
        }
        }
    } 
    [[nodiscard]] UDataPacketImport::GRPC::UnsubscribeResponse
        unsubscribe(grpc::CallbackServerContext *context,
                    const UnsubscribeRequest &request)
    {
        UDataPacketImport::StreamIdentifier
            streamIdentifier{request.stream_identifier()};
        UDataPacketImport::GRPC::UnsubscribeResponse response;
        *response.mutable_stream_identifier() = streamIdentifier.toProtobuf();
        response.set_packets_read(0);
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mStreamsMap.find(streamIdentifier);
        if (idx != mStreamsMap.end())
        {
            response = idx->second->unsubscribe(context); 
            mStreamsMap.erase(idx);
        }
        // Ensure there's nothing pending
        mPendingSubscriptions.erase(context); 
        }
        return response;
    }
    mutable std::mutex mMutex;
    StreamOptions mStreamOptions;
    std::map
    <
        UDataPacketImport::StreamIdentifier,
        std::unique_ptr<Stream>
    > mStreamsMap;
    std::map
    <
        grpc::CallbackServerContext *,
        std::set<UDataPacketImport::StreamIdentifier>
    > mPendingSubscriptions;
    std::atomic<bool> mNumberOfSubscribers{0};
};

/// Destructor
SubscriptionManager::~SubscriptionManager() = default;
