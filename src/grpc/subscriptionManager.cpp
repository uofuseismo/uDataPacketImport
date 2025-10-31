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
                = std::make_unique<Stream> (std::move(packet), mStreamOptions);
            createdStream = true;
        }
        catch (const std::exception &e)
        {
            errorMessage = std::string{e.what()};
        }
        }
        if (!createdStream)
        {
            spdlog::critical("Failed to create stream "
                           + streamIdentifier.toString()
                           + " because " + errorMessage);
            return;
        }
        // Add any pending streams
        for (const auto &pendingSubscription : mPendingSubscriptions)
        {
            for (auto &desiredStreamIdentifier : pendingSubscription.second)
            {
                if (streamIdentifier == desiredStreamIdentifier)
                {
                    spdlog::info("Adding " 
                               + pendingSubscription.first->peer()
                               + " to stream " + streamIdentifier.toString());
                    //pendingSubscription.first
                }
            }
        } 
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
