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

namespace
{
std::set<UDataPacketImport::StreamIdentifier> 
    toSet(const UDataPacketImport::GRPC::SubscriptionRequest &request)
{
    std::set<UDataPacketImport::StreamIdentifier> identifiers;
    for (const auto &grpcStreamIdentifier : request.streams())
    {   
        UDataPacketImport::StreamIdentifier identifier{grpcStreamIdentifier};
        // N.B. insert does nothing if key exists
        identifiers.insert(std::move(identifier));
    }
    if (identifiers.empty())
    {
        throw std::invalid_argument("No streams requested in subscription");
    }
    return identifiers;
}
}

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
                bool wasUpdated{false};
                auto pendingSubscriptions = pendingSubscription.second;
                for (auto &desiredStreamIdentifier : pendingSubscriptions)
                {
                    if (streamIdentifier == desiredStreamIdentifier)
                    {
                        wasUpdated = true;
                        auto contextAddress
                            = reinterpret_cast<uintptr_t>
                              (pendingSubscription.first);
                        idx->second->subscribe(contextAddress);
                        pendingSubscription.second.erase(
                            desiredStreamIdentifier);
                        spdlog::info("Added subscriber " 
                                   + pendingSubscription.first->peer()
                                   + " to stream " + streamIdentifierString);
                    }
                }
                pendingSubscription.second = pendingSubscriptions;
            }
            // Clean up the callback
            for (auto it = mPendingCallbackServerContextSubscriptions.cbegin();
                 it != mPendingCallbackServerContextSubscriptions.cend();
                 )
            {
                // This guy is fully subscribed
                if (it->second.empty())
                {
                    spdlog::info("All pending subscriptions filled for "
                               + it->first->peer());
                    mPendingCallbackServerContextSubscriptions.erase(it++);
                }
                else
                {
                    ++it;
                }
            }
            for (auto &pendingSubscription :
                 mPendingCallbackServerContextSubscribeToAll)
            {
                auto contextAddress
                   = reinterpret_cast<uintptr_t>
                     (pendingSubscription);
                idx->second->subscribe(contextAddress);
                spdlog::info("Added " 
                           + pendingSubscription->peer()
                           + " to stream " + streamIdentifierString);
            }

            // Synchronous server
            /*
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
            */
        }
        else
        {
            spdlog::warn("This is strange - can't find stream I just added");
        }
        }
    } 
    /// Asynchronous unsubscribe from all streams
    void unsubscribeFromAll(grpc::CallbackServerContext *context)
    {
        {
        std::lock_guard<std::mutex> lock(mMutex);
        mNumberOfSubscribers =-1;
        auto contextAddress = reinterpret_cast<uintptr_t> (context);
        for (auto &stream : mStreamsMap)
        {
            try
            {
                stream.second->unsubscribe(contextAddress);
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
    }
    /// Synchronous unsubscribe from all streams
    /*
    void unsubscribeFromAll(grpc::ServerContext *context)
    {
        {
        std::lock_guard<std::mutex> lock(mMutex);
        mNumberOfSubscribers =-1;
        auto contextAddress = reinterpret_cast<uintptr_t> (context);
        for (auto &stream : mStreamsMap)
        {
            try
            {
                stream.second->unsubscribe(contextAddress);
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
    }
    */
    /// Asynchronous subscribe to streams
    void subscribe(grpc::CallbackServerContext *context,
                   const std::set<UDataPacketImport::StreamIdentifier>
                       &streamIdentifiers)
    {
        if (streamIdentifiers.empty()){return;}
        auto contextAddress = reinterpret_cast<uintptr_t> (context);
        auto peer = context->peer();
        {
        std::lock_guard<std::mutex> lock(mMutex);
        mNumberOfSubscribers =-1;
        for (const auto &streamIdentifier : streamIdentifiers)
        {
            auto streamIdentifierString = streamIdentifier.toString();
            auto idx = mStreamsMap.find(streamIdentifierString);
            if (idx != mStreamsMap.end())
            {
                try
                {
                    idx->second->subscribe(contextAddress);
                    spdlog::debug("Subscribed " + peer
                                + " to " + streamIdentifierString);
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Failed to subscribe "
                               + peer + " to "
                               + streamIdentifierString + " because "
                               + std::string {e.what()});
                }
            }
            else
            {
                // Check our pending subscriptions for this context
                auto jdx
                    = mPendingCallbackServerContextSubscriptions.find(context);
                if (jdx != mPendingCallbackServerContextSubscriptions.end())
                {
                    // The context already has this subscription pending
                    if (jdx->second.contains(streamIdentifier))
                    {
                        spdlog::debug(peer
                                    + " already has a pending subscription for " 
                                    + streamIdentifierString);
                    }
                    else
                    {
                        // Now it's pending
                        jdx->second.insert(streamIdentifier); 
                    }
                }
                else
                {
                    // Need a new context with a new pending subscription
                    std::set<UDataPacketImport::StreamIdentifier>
                        tempSet{streamIdentifier};
                    mPendingCallbackServerContextSubscriptions.insert(
                        std::pair {context, std::move(tempSet)}
                    ); 
                }
            }
        }
        }
    }
    /// Synchronous subscribe to streams
    /*
    void subscribe(
        grpc::ServerContext *context,
        const std::set<UDataPacketImport::StreamIdentifier> &streamIdentifiers)
    {
        if (streamIdentifiers.empty()){return;}
        auto contextAddress = reinterpret_cast<uintptr_t> (context);
        auto peer = context->peer();
        {
        std::lock_guard<std::mutex> lock(mMutex);
        mNumberOfSubscribers =-1;
        for (const auto &streamIdentifier : streamIdentifiers)
        {
            auto streamIdentifierString = streamIdentifier.toString();
            auto idx = mStreamsMap.find(streamIdentifierString);
            if (idx != mStreamsMap.end())
            {
                try
                {   
             
                    idx->second->subscribe(contextAddress);
                    spdlog::debug("Subscribed " + peer
                                + " to " + streamIdentifierString);
                }   
                catch (const std::exception &e) 
                {   
                    spdlog::warn("Failed to subscribe "
                               + peer + " to "
                               + streamIdentifierString + " because "
                               + std::string {e.what()});
                }   
            }
            else
            {   
                // Check our pending subscriptions for this context
                auto jdx 
                    = mPendingServerContextSubscriptions.find(context);
                if (jdx != mPendingServerContextSubscriptions.end())
                {
                    // The context already has this subscription pending
                    if (jdx->second.contains(streamIdentifier))
                    {
                        spdlog::debug(peer
                                    + " already has a pending subscription for "
                                    + streamIdentifierString);
                    } 
                    else
                    {
                        // Now it's pending
                        jdx->second.insert(streamIdentifier); 
                    }
                }   
                else
                {
                    // Need a new context with a new pending subscription
                    std::set<UDataPacketImport::StreamIdentifier>
                        tempSet{streamIdentifier};
                    mPendingServerContextSubscriptions.insert(
                        std::pair {context, std::move(tempSet)}
                    );
                }
            }
        }
        } 
    }
    */

    void unsubscribeOnCancel(
        grpc::CallbackServerContext *context,
        const std::set<UDataPacketImport::StreamIdentifier> &streamIdentifiers)
    {
        auto contextAddress = reinterpret_cast<uintptr_t> (context);
        auto peer = context->peer();
        {
        std::lock_guard<std::mutex> lock(mMutex);
        mNumberOfSubscribers =-1;
        for (const auto &streamIdentifier : streamIdentifiers)
        {
            const auto streamIdentifierString = streamIdentifier.getStringReference();
            auto idx = mStreamsMap.find(streamIdentifierString);
            if (idx != mStreamsMap.end())
            {
                try
                {
                    idx->second->unsubscribe(contextAddress);
                    spdlog::debug("Unsubscribed " + peer
                                + " from " + streamIdentifierString);
                }
                catch (const std::exception &e)
                {
                    spdlog::debug("Failed to unsubscribe " + peer
                                + " from " + streamIdentifierString
                                + " because " + std::string {e.what()});
                }
            }
        }
        mPendingCallbackServerContextSubscriptions.erase(context);
        }
    }

    /// Asynchronous subscribe to all streams 
    void subscribeToAll(grpc::CallbackServerContext *context)
    {
        auto peer = context->peer();
        {
        std::lock_guard<std::mutex> lock(mMutex);
        mNumberOfSubscribers =-1;
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
                spdlog::debug("Subscribed " + peer
                            + " to " + stream.second->getIdentifier());
            } 
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to subscribe "
                           + peer + " to " 
                           + stream.first + " because "
                           + std::string {e.what()});
            }
        }
        mPendingCallbackServerContextSubscribeToAll.insert(context);
        }
    }
    /*
    /// Synchronous subsribe to all streams
    void subscribeToAll(grpc::ServerContext *context)
    {
        auto peer = context->peer();
        {   
        std::lock_guard<std::mutex> lock(mMutex);
        mNumberOfSubscribers =-1;
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
                spdlog::debug("Subscribed " + peer
                            + " to " + stream.second->getIdentifier());
            }
            catch (const std::exception &e) 
            {   
                spdlog::warn("Failed to subscribe "
                           + peer + " to " 
                           + stream.first + " because "
                           + std::string {e.what()});
            }   
        }
        mPendingServerContextSubscribeToAll.insert(context);
        }
    }   
    */
    /// Asynchronous unsubscribe
    void unsubscribe(grpc::CallbackServerContext *context,
                     const UnsubscribeRequest &request)
    {
        UDataPacketImport::StreamIdentifier
            streamIdentifier{request.stream_identifier()};
        auto streamIdentifierString = streamIdentifier.toString();
        {
        std::lock_guard<std::mutex> lock(mMutex);
        mNumberOfSubscribers =-1;
        auto idx = mStreamsMap.find(streamIdentifierString);
        if (idx != mStreamsMap.end())
        {
            auto contextAddress = reinterpret_cast<uintptr_t> (context);
            idx->second->unsubscribe(contextAddress);
            mStreamsMap.erase(idx);
        }
        // Ensure there's nothing pending
        mPendingCallbackServerContextSubscriptions.erase(context); 
        }
    }
    /// Synchronous unsubscribe
    /*
    void unsubscribe(grpc::ServerContext *context,
                     const UnsubscribeRequest &request)
    {
        UDataPacketImport::StreamIdentifier
            streamIdentifier{request.stream_identifier()};
        auto streamIdentifierString = streamIdentifier.toString();
        {
        std::lock_guard<std::mutex> lock(mMutex);
        mNumberOfSubscribers =-1;
        auto idx = mStreamsMap.find(streamIdentifierString);
        if (idx != mStreamsMap.end())
        {
            auto contextAddress = reinterpret_cast<uintptr_t> (context);
            idx->second->unsubscribe(contextAddress); 
            mStreamsMap.erase(idx);
        }
        // Ensure there's nothing pending
        mPendingServerContextSubscriptions.erase(context); 
        }
    }
    */
    /// Get next packet for a particular context
    [[nodiscard]] std::vector<UDataPacketImport::GRPC::Packet>
        getNextPackets(const uintptr_t contextAddress,
                       const std::set<UDataPacketImport::StreamIdentifier> &identifiers) const noexcept
    {
        std::vector<UDataPacketImport::GRPC::Packet> result;
        result.reserve(32);
        {
        std::lock_guard<std::mutex> lock(mMutex);
        for (const auto &identifier : identifiers)
        {
            auto idx = mStreamsMap.find(identifier.getStringReference());
            if (idx != mStreamsMap.end())
            {
#ifndef NDEBUG
                assert(idx->second->isSubscribed(contextAddress));
#endif
                for (int i = 0; i < std::numeric_limits<int>::max(); ++i)
                {
                    auto packet = idx->second->getNextPacket(contextAddress);
                    if (packet)
                    {
                        result.push_back(std::move(*packet));
                    }
                    else
                    {
                        break;
                    }
                }
            } // End check on subscribed
        }
        }
        return result;
    }
    /// Get next packet
    [[nodiscard]] std::vector<UDataPacketImport::GRPC::Packet>
        getNextPacketsFromAllSubscriptions(
            const uintptr_t contextAddress) const noexcept
    {
        std::vector<UDataPacketImport::GRPC::Packet> result;
        result.reserve(32);
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
        mNumberOfSubscribers =-1;
        for (auto &stream : mStreamsMap)
        {
             stream.second->unsubscribeAll();
        }
        mPendingCallbackServerContextSubscribeToAll.clear();
        //mPendingServerContextSubscribeToAll.clear(); 
        }
    }

    [[nodiscard]] int getNumberOfSubscribers() const noexcept
    {
        {
        std::lock_guard<std::mutex> lock(mMutex);
        if (mNumberOfSubscribers < 0)
        {
            std::set<uintptr_t> allSubscribers;
            for (const auto &stream : mStreamsMap)
            {
                auto subscribers = stream.second->getSubscribers();
                allSubscribers.insert(subscribers.begin(), subscribers.end());
            }
            if (allSubscribers.empty())
            {
                mNumberOfSubscribers
                   = mPendingCallbackServerContextSubscriptions.size();
            }
            else
            {
                mNumberOfSubscribers = static_cast<int> (allSubscribers.size());
            }
        }
        return mNumberOfSubscribers;
        }
    }

    [[nodiscard]] std::vector<UDataPacketImport::StreamIdentifier> getAvailableStreams() const
    {
        std::vector<UDataPacketImport::StreamIdentifier> identifiers;
        {
        std::lock_guard<std::mutex> lock(mMutex);
        identifiers.reserve(mStreamsMap.size());
        for (const auto &stream : mStreamsMap)
        {
            try
            {
                identifiers.insert( mStreamsMap->second->getStreamIdentifier() );
            }
            catch (const std::exception &e)
            {
                spdlog::warn(e.what());
            }
        }
        }
        return identifiers;
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
    /*
    std::map
    <
        grpc::ServerContext *,
        std::set<UDataPacketImport::StreamIdentifier>
    > mPendingServerContextSubscriptions;
    */
    std::set
    <
        grpc::CallbackServerContext *
    > mPendingCallbackServerContextSubscribeToAll;
    /*
    std::set
    <
        grpc::ServerContext *
    > mPendingServerContextSubscribeToAll;
    */
    mutable int mNumberOfSubscribers{-1};
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
    pImpl->unsubscribeFromAll(context);
}
*/

void SubscriptionManager::unsubscribeFromAll(
    grpc::CallbackServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Callback server context is null");
    }
    pImpl->unsubscribeFromAll(context);
}

/// Subscribe
/*
void SubscriptionManager::subscribe(
    grpc::ServerContext *context,
    const UDataPacketImport::GRPC::SubscriptionRequest &request)
{
    auto identifiers = ::toSet(request); // Throws
    pImpl->subscribe(context, identifiers);
}
*/

void SubscriptionManager::subscribe(
    grpc::CallbackServerContext *context,
    const UDataPacketImport::GRPC::SubscriptionRequest &request)
{
    auto identifiers = ::toSet(request); // Throws
    pImpl->subscribe(context, identifiers);
}

/// Subscribe to all
/*
void SubscriptionManager::subscribeToAll(
    grpc::ServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Server context is null");
    }
    pImpl->subscribeToAll(context);
}
*/

void SubscriptionManager::subscribeToAll(
    grpc::CallbackServerContext *context)
{
    if (context == nullptr)
    {   
        throw std::invalid_argument("Callback server context is null");
    }   
    pImpl->subscribeToAll(context);
}

/// Unsbubscribe 
void SubscriptionManager::unsubscribe(
    grpc::CallbackServerContext *context,
    const std::set<UDataPacketImport::StreamIdentifier> &streamIdentifiers)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Callback server context is null");
    }
    pImpl->unsubscribeOnCancel(context, streamIdentifiers);
}

/// Unsubscribe from all
/*
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
*/

/*
void SubscriptionManager::unsubscribeFromAll(
    grpc::ServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Server context is null");
    }
    pImpl->unsubscribeFromAll(context);
}

void SubscriptionManager::unsubscribeFromAll(
   grpc::CallbackServerContext *context)
{
    if (context == nullptr)
    {
        throw std::invalid_argument("Callback server context is null");
    }
    pImpl->unsubscribeFromAll(context);
}
*/

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
    return pImpl->getNextPacketsFromAllSubscriptions(contextAddress);
}

std::vector<UDataPacketImport::GRPC::Packet>
    SubscriptionManager::getNextPackets(
    grpc::CallbackServerContext *context,
    const std::set<UDataPacketImport::StreamIdentifier> &subscriptions) const
{           
    if (context == nullptr)
    {                          
        throw std::invalid_argument("Callback server context is null");
    }
    auto contextAddress = reinterpret_cast<uintptr_t> (context);
    return pImpl->getNextPackets(contextAddress, subscriptions);
}    

/*
std::vector<UDataPacketImport::GRPC::Packet>
    SubscriptionManager::getNextPacketsFromAllSubscriptions(
    grpc::ServerContext *context) const
{       
    if (context == nullptr)
    {                      
        throw std::invalid_argument("Callback server context is null");
    }   
    auto contextAddress = reinterpret_cast<uintptr_t> (context);
    return pImpl->getNextPacketsFromAllSubscriptions(contextAddress);
}       
*/

/// Allows server to purge all clients
void SubscriptionManager::unsubscribeAll()
{
    pImpl->unsubscribeAll();
}

/// Gets the number of subscribers
int SubscriptionManager::getNumberOfSubscribers() const noexcept
{
    return pImpl->getNumberOfSubscribers();    
}

/// Gets all the current streams
std::set<UDataPacket::StreamIdentifier>
    SubscriptionManager::getAvailableStreams() const
{
    auto availableStreams = pImpl->getAvailableStreams();
    std::set<UDataPacket::StreamIdentifier> result;
    for (const auto &s : availableStreams)
    {
        result.insert( UDataPacketImport::StreamIdentifier {s} );
    }
    return result;
}
