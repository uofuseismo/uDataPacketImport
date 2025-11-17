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
            // Callback server
            for (auto &pendingSubscription :
                 mPendingCallbackServerContextSubscriptions)
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
                        idx->second->subscribe(contextAddress);
                        pendingSubscription.second.erase(
                            desiredStreamIdentifier);
                    }
                }
                // This guy is fully subscribed
                if (pendingSubscription.second.empty())
                {
                    mPendingCallbackServerContextSubscriptions.erase(
                        pendingSubscription.first);
                    spdlog::info("All pending subscriptions filled for "
                               + pendingSubscription.first->peer());
                }
            }
            for (auto &pendingSubscription :
                 mPendingCallbackServerContextSubscribeToAll)
            {
                spdlog::info("Adding " 
                           + pendingSubscription->peer()
                           + " to stream " + streamIdentifierString);
                auto contextAddress
                   = reinterpret_cast<uintptr_t>
                     (pendingSubscription);
                idx->second->subscribe(contextAddress);
            }

            // Synchronous server
            for (auto &pendingSubscription : 
                 mPendingServerContextSubscriptions)
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
                        idx->second->subscribe(contextAddress);
                        pendingSubscription.second.erase(
                            desiredStreamIdentifier);
                    }
                }
                // This guy is fully subscribed
                if (pendingSubscription.second.empty())
                {
                    mPendingServerContextSubscriptions.erase(
                        pendingSubscription.first);
                    spdlog::info("All pending subscriptions filled for "
                               + pendingSubscription.first->peer());
                }
            }

            for (auto &pendingSubscription :
                 mPendingServerContextSubscribeToAll)
            {
                spdlog::info("Adding " 
                           + pendingSubscription->peer()
                           + " to stream " + streamIdentifierString);
                auto contextAddress
                   = reinterpret_cast<uintptr_t>
                     (pendingSubscription);
                idx->second->subscribe(contextAddress);
            }

        }
        else
        {
            spdlog::warn("This is strange - can't find stream I just added");
        }
        }
    } 
    /// Asynchronous unsubscribe from all streams
    UDataPacketImport::GRPC::UnsubscribeFromAllStreamsResponse
        unsubscribeFromAll(grpc::CallbackServerContext *context)
    {
        UDataPacketImport::GRPC::UnsubscribeFromAllStreamsResponse
            response;
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto contextAddress = reinterpret_cast<uintptr_t> (context);
        for (auto &stream : mStreamsMap)
        {
            try
            {
                auto streamResponse
                    = stream.second->unsubscribe(contextAddress);
                response.mutable_stream_response()->Add(
                    std::move(streamResponse));
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to unsubscribe "
                           + context->peer() + " from " 
                           + stream.first + " because "
                           + std::string {e.what()});
            }
        }
        mPendingCallbackServerContextSubscribeToAll.erase(context);
        }
        return response;
    }
    /// Synchronous unsubscribe from all streams
    UDataPacketImport::GRPC::UnsubscribeFromAllStreamsResponse
        unsubscribeFromAll(grpc::ServerContext *context)
    {   
        UDataPacketImport::GRPC::UnsubscribeFromAllStreamsResponse
            response;
        {
        std::lock_guard<std::mutex> lock(mMutex);
        auto contextAddress = reinterpret_cast<uintptr_t> (context);
        for (auto &stream : mStreamsMap)
        {
            try
            {
                auto streamResponse
                    = stream.second->unsubscribe(contextAddress);
                response.mutable_stream_response()->Add(
                    std::move(streamResponse));
            }
            catch (const std::exception &e) 
            {
                spdlog::warn("Failed to unsubscribe "
                           + context->peer() + " from " 
                           + stream.first + " because "
                           + std::string {e.what()});
            }
        }
        mPendingServerContextSubscribeToAll.erase(context);
        }
        return response;
    }   
    /// Asynchronous subscribe to all streams 
    void subscribeToAll(grpc::CallbackServerContext *context)
    {
        {
        std::lock_guard<std::mutex> lock(mMutex);
        if (mPendingCallbackServerContextSubscribeToAll.contains(context))
        {
            spdlog::info(context->peer() + " already waiting");
            return;
        }
        auto contextAddress = reinterpret_cast<uintptr_t> (context);
        for (auto &stream : mStreamsMap)
        {
            try
            {
                stream.second->subscribe(contextAddress);
 spdlog::info("Subscribed " + context->peer() + " to " + stream.second->getIdentifier());
            } 
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to subscribe "
                           + context->peer() + " to " 
                           + stream.first + " because "
                           + std::string {e.what()});
            }
        }
        mPendingCallbackServerContextSubscribeToAll.insert(context);
        }
    }
    /// Synchronous subsribe to all streams
    void subscribeToAll(grpc::ServerContext *context)
    {   
        {   
        std::lock_guard<std::mutex> lock(mMutex);
        if (mPendingServerContextSubscribeToAll.contains(context))
        {
            return;
        }
        auto contextAddress = reinterpret_cast<uintptr_t> (context);
        for (auto &stream : mStreamsMap)
        {   
            try 
            {
                stream.second->subscribe(contextAddress);
            }
            catch (const std::exception &e) 
            {   
                spdlog::warn("Failed to subscribe "
                           + context->peer() + " to " 
                           + stream.first + " because "
                           + std::string {e.what()});
            }   
        }
        mPendingServerContextSubscribeToAll.insert(context);
        }   
    }   
    /// Asynchronous unsubscribe
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
        mPendingCallbackServerContextSubscriptions.erase(context); 
        }
        return response;
    }
    /// Synchronous unsubscribe
    [[nodiscard]] UDataPacketImport::GRPC::UnsubscribeResponse
        unsubscribe(grpc::ServerContext *context,
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
        mPendingServerContextSubscriptions.erase(context); 
        }
        return response;
    }   
    /// Get next packet
    [[nodiscard]] std::vector<UDataPacketImport::GRPC::Packet>
        getNextPacketsFromAllSubsriptions(
            const uintptr_t contextAddress) const noexcept
    {
        std::vector<UDataPacketImport::GRPC::Packet> result;
        result.reserve(64);
        {
        std::lock_guard<std::mutex> lock(mMutex);
        for (const auto &stream : mStreamsMap)
        {
            for (int i = 0; i < std::numeric_limits<int>::max(); ++i)
            {
                auto packet = stream.second->getNextPacket(contextAddress);
                if (packet)
                {
                    result.push_back(std::move(*packet));
                }
                else
                {
                    break;
                }
            }
        }
        }
        return result;
    }   

    void unsubscribeAll()
    {
        {
        std::lock_guard<std::mutex> lock(mMutex);
        for (auto &stream : mStreamsMap)
        {
             stream.second->unsubscribeAll();
        }
        mPendingCallbackServerContextSubscribeToAll.clear();
        mPendingServerContextSubscribeToAll.clear(); 
        }
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
    > mPendingCallbackServerContextSubscriptions;
    std::map
    <
        grpc::ServerContext *,
        std::set<UDataPacketImport::StreamIdentifier>
    > mPendingServerContextSubscriptions;
    std::set
    <
        grpc::CallbackServerContext *
    > mPendingCallbackServerContextSubscribeToAll;
    std::set
    <
        grpc::ServerContext *
    > mPendingServerContextSubscribeToAll;
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

/// Unsubscribe from all
/*
void SubscriptionManager::unsubscribeFromAll(
    grpc::ServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Server context is null");
    }
}

void SubscriptionManager::unsubscribeFromAll(
    grpc::CallbackServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Callback server context is null");
    }
}
*/


/// Subscribe to all
void SubscriptionManager::subscribeToAll(
    grpc::ServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Server context is null");
    }
    pImpl->subscribeToAll(context);
}

void SubscriptionManager::subscribeToAll(
    grpc::CallbackServerContext *context)
{
    if (context == nullptr)
    {   
        throw std::invalid_argument("Callback server context is null");
    }   
    pImpl->subscribeToAll(context);
}

/// Unsubscribe from all
void SubscriptionManager::unsubscribeFromAllOnCancel(
    grpc::ServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Server context is null");
    }   
    pImpl->unsubscribeFromAll(context);
}

void SubscriptionManager::unsubscribeFromAllOnCancel(
    grpc::CallbackServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Callback server context is null");
    }   
    pImpl->unsubscribeFromAll(context);
}

UDataPacketImport::GRPC::UnsubscribeFromAllStreamsResponse
    SubscriptionManager::unsubscribeFromAll(grpc::ServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Server context is null");
    }
    return pImpl->unsubscribeFromAll(context);
}

UDataPacketImport::GRPC::UnsubscribeFromAllStreamsResponse 
   SubscriptionManager::unsubscribeFromAll(grpc::CallbackServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Callback server context is null");
    }
    return pImpl->unsubscribeFromAll(context);
}


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

std::vector<UDataPacketImport::GRPC::Packet>
    SubscriptionManager::getNextPacketsFromAllSubscriptions(
    grpc::CallbackServerContext *context) const
{
    if (context == nullptr)
    {   
        throw std::invalid_argument("Callback server context is null");
    }
    auto contextAddress = reinterpret_cast<uintptr_t> (context);
    return pImpl->getNextPacketsFromAllSubsriptions(contextAddress);
}

std::vector<UDataPacketImport::GRPC::Packet>
    SubscriptionManager::getNextPacketsFromAllSubscriptions(
    grpc::ServerContext *context) const
{       
    if (context == nullptr)
    {                      
        throw std::invalid_argument("Callback server context is null");
    }   
    auto contextAddress = reinterpret_cast<uintptr_t> (context);
    return pImpl->getNextPacketsFromAllSubsriptions(contextAddress);
}       

void SubscriptionManager::unsubscribeAll()
{
    pImpl->unsubscribeAll();
}
