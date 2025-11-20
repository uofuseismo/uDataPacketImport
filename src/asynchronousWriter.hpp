#ifndef UDATA_PACKET_IMPORT_WRITER_HPP
#define UDATA_PACKET_IMPORT_WRITER_HPP
#include <atomic>
#include <chrono>
#include <set>
#include <string>
#include <queue>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>
#include "uDataPacketImport/grpc/subscriptionManager.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.pb.h"
#include "proto/dataPacketBroadcast.grpc.pb.h"

namespace
{

bool validateClient(const grpc::CallbackServerContext *context,
                    const std::string &accessToken,
                    const std::string &peer)
{
    if (accessToken.empty()){return true;}
    for (const auto &item : context->client_metadata())
    {
        if (item.first == "x-custom-auth-token")
        {
            if (item.second == accessToken)
            {
                spdlog::info("Validated " + peer + "'s token");
                return true;
            }
        }
    }
    return false;
}

class AsynchronousWriterSubscribeToAll :
    public grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet>
{
public:
    AsynchronousWriterSubscribeToAll(
         grpc::CallbackServerContext *context,
         const UDataPacketImport::GRPC::SubscribeToAllStreamsRequest *request,
         std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager>
             &subscriptionManager,
         std::atomic<bool> *keepRunning,
         const std::string &accessToken = "",
         const int maximumNumberOfSubscribers = 128) :
             mContext(context),
             mManager(subscriptionManager),
             mKeepRunning(keepRunning),
             mMaximumNumberOfSubscribers(maximumNumberOfSubscribers)
    {
        // Authenticate
        mPeer = context->peer();
        if (!accessToken.empty()) 
        {
            if (!::validateClient(context, accessToken, mPeer))
            {
                spdlog::info(mPeer + " rejected");
                grpc::Status status{grpc::StatusCode::UNAUTHENTICATED,
R"""(
Client must provide access token in x-custom-auth-token header field
)"""};
                Finish(status);
            }
        }
        // Subscribe
        try
        {
            auto nSubscribers = mManager->getNumberOfSubscribers();
            if (nSubscribers >= mMaximumNumberOfSubscribers)
            {
                spdlog::warn("Currently at " 
                           + std::to_string(nSubscribers)
                           + ".  Resource exhausted.");
                grpc::Status status{grpc::StatusCode::RESOURCE_EXHAUSTED,
                                    "Max subscribers hit - try again later"};
                Finish(status);
            }
            spdlog::info("Subscribing " + mPeer + " to all streams");
            mManager->subscribeToAll(context);
            spdlog::info("Subscription manager is now managing "
                       + std::to_string(mManager->getNumberOfSubscribers())
                       + " subscribers");
        }
        catch (const std::exception &e) 
        {
            spdlog::warn("Failed to subscribe because "
                       + std::string {e.what()});
            Finish(grpc::Status(grpc::StatusCode::INTERNAL,
                                "Failed to subscribe"));
        }
        // Start
        nextWrite();
     }

     ~AsynchronousWriterSubscribeToAll()
     {
        spdlog::debug("In destructor");
     }

     void OnWriteDone(bool ok) override 
     {
        if (!ok) 
        {
            if (mContext)
            {   
                if (mContext->IsCancelled())
                {   
                    return Finish(grpc::Status::CANCELLED);
                }
            }   
            return Finish(grpc::Status(grpc::StatusCode::UNKNOWN,
                                       "Unexpected failure"));
        }
        // Packet is flushed; can now safely purge the element to write
        mWriteInProgress = false;
        mPacketsQueue.pop();
        // Start next write
        nextWrite();
     }   
     // This needs to perform quickly.  I should do blocking work but
     // this is my last ditch effort to evict the context from the 
     // subscription manager..
     void OnDone() override 
     {
         spdlog::info("Subscribe to all RPC completed for " + mPeer);
         if (mContext)
         {
             mManager->unsubscribeFromAll(mContext);
         }
         delete this;
     }   

     void OnCancel() override 
     { 
         spdlog::info("Subscribe to all RPC cancelled for " + mPeer);
         if (mContext)
         {
             mManager->unsubscribeFromAll(mContext);
         }
     }

private:
    void nextWrite() 
    {
        // Keep running either until the server or client quits
        while (mKeepRunning->load())
        {
            // Cancel means we leave now
            if (mContext->IsCancelled()){break;}

            // Get any remaining packets on the queue on the wire
            if (!mPacketsQueue.empty() && !mWriteInProgress)
            {
                const auto &packet = mPacketsQueue.front();
                mWriteInProgress = true;
                StartWrite(&packet);
                return;
            }
            // I've cleared the queue and/or I have packets in flight.
            // Try to get more packets to write while I `wait.'
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
                            spdlog::warn(
                               "RPC writer queue exceeded - popping element");
                            mPacketsQueue.pop();
                         }
                         mPacketsQueue.push(std::move(packet));
                    }
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Failed to get next packet because "
                               + std::string {e.what()});
                }
            }
            // No new packets were acquired and I'm not waiting for a write.
            // Give me stream manager a break.
            if (mPacketsQueue.empty() && !mWriteInProgress)
            {
                std::this_thread::sleep_for(mTimeOut);
            }
        } // Loop on server still running
        if (mContext)
        {
            // The context is still valid so try to remove it from the
            // subscriptoins.  This can be the case whether the server is
            // shutting down or the client bailed.
            mManager->unsubscribeFromAll(mContext);
            if (mContext->IsCancelled())
            {
                spdlog::info("Terminating acquisition for " 
                           + mPeer
                           + " because of client side cancel");
                Finish(grpc::Status::CANCELLED);
            }
            else
            {
                spdlog::info("Terminating acquisition for "
                           + mPeer
                           + " because of server side cancel");
                Finish(grpc::Status::OK);
            }
        }
        else
        {
            spdlog::warn("The context for " + mPeer + " has disappeared");
        }
    }
    grpc::CallbackServerContext *mContext{nullptr};
    std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager> mManager{nullptr};
    std::atomic<bool> *mKeepRunning{nullptr};
    std::queue<UDataPacketImport::GRPC::Packet> mPacketsQueue;
    std::string mPeer;
    std::chrono::milliseconds mTimeOut{20};
    size_t mMaximumQueueSize{2048};
    int mMaximumNumberOfSubscribers{128};
    bool mWriteInProgress{false};
};

//----------------------------------------------------------------------------//

class AsynchronousWriterSubscribe :
    public grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet>
{
public:
    AsynchronousWriterSubscribe(
         grpc::CallbackServerContext *context,
         const UDataPacketImport::GRPC::SubscriptionRequest *request,
         std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager>
             &subscriptionManager,
         std::atomic<bool> *keepRunning,
         const std::string &accessToken = "",
         const int maximumNumberOfSubscribers = 128) :
             mContext(context),
             mSubscriptionRequest(*request),
             mManager(subscriptionManager),
             mKeepRunning(keepRunning),
             mMaximumNumberOfSubscribers(maximumNumberOfSubscribers)
    {
        // Authenticate
        mPeer = context->peer();
        if (!accessToken.empty()) 
        {
            if (!::validateClient(context, accessToken, mPeer))
            {
                spdlog::info(mPeer + " rejected");
                grpc::Status status{grpc::StatusCode::UNAUTHENTICATED,
R"""(
Client must provide access token in x-custom-auth-token header field
)"""};
                Finish(status);
            }
        }
        // Ensure the client is requesting streams
        if (mSubscriptionRequest.streams_size() == 0)
        {
            grpc::Status status{grpc::StatusCode::INVALID_ARGUMENT,
R"""(
Client must provide specify at least one stream to which to subscribe.
)"""};
            Finish(status);
        }
        // Subscribe
        try
        {
            auto nSubscribers = mManager->getNumberOfSubscribers();
            if (nSubscribers >= mMaximumNumberOfSubscribers)
            { 
                spdlog::warn("Currently at " 
                           + std::to_string(nSubscribers)
                           + ".  Resource exhausted.");
                grpc::Status status{grpc::StatusCode::RESOURCE_EXHAUSTED,
                                    "Max subscribers hit - try again later"};
                Finish(status);
            }
            spdlog::info("Subscribing " + mPeer);
            for (const auto &stream : mSubscriptionRequest.streams())
            {   
                mStreamIdentifiers.insert( 
                    UDataPacketImport::StreamIdentifier {stream});
            }
            if (mStreamIdentifiers.empty())
            {
                grpc::Status status{grpc::StatusCode::INVALID_ARGUMENT,
                             "No streams specified - check stream identifiers"};
                Finish(status);
            }
            spdlog::info("Subscribing " + mPeer
                       + " to " + std::to_string(mStreamIdentifiers.size())
                       + " streams");
            mManager->subscribe(context, mSubscriptionRequest);
            spdlog::info("Subscription manager is now managing "
                       + std::to_string(mManager->getNumberOfSubscribers())
                       + " subscribers");
        }
        catch (const std::invalid_argument &e)
        {
            spdlog::warn("Failed to subscribe because of invalid request "
                       + std::string {e.what()});
            Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                           "Malformed request - check the stream identifiers"));
        }
        catch (const std::exception &e) 
        {
            spdlog::warn("Failed to subscribe because "
                       + std::string {e.what()});
            Finish(grpc::Status(grpc::StatusCode::INTERNAL,
                                "Failed to subscribe"));
        }
        // Start
        nextWrite();
     }

     void OnWriteDone(bool ok) override 
     {
        if (!ok) 
        {
            if (mContext)
            {
                if (mContext->IsCancelled())
                {
                    return Finish(grpc::Status::CANCELLED);
                }
            }
            return Finish(grpc::Status(grpc::StatusCode::UNKNOWN,
                                       "Unexpected Failure"));
        }
        // Packet is flushed; can now safely purge the element to write
        mWriteInProgress = false;
        mPacketsQueue.pop();
        // Start next write
        nextWrite();
     }   

     void OnDone() override 
     {
         spdlog::debug("Subscribe RPC completed for " + mPeer);
         if (mContext)
         {
             mManager->unsubscribe(mContext, mStreamIdentifiers);
         }
         delete this;
     }   

     void OnCancel() override 
     { 
         spdlog::debug("Subscribe RPC cancelled for " + mPeer);
         if (mContext)
         {
             mManager->unsubscribe(mContext, mStreamIdentifiers);
         }
     }

private:
    void nextWrite() 
    {
        // Keep running either until the server or client quits
        while (mKeepRunning->load() && !mContext->IsCancelled())
        {
            // Get any remaining packets on the queue on the wire
            if (!mPacketsQueue.empty() && !mWriteInProgress)
            {
                const auto &packet = mPacketsQueue.front();
                mWriteInProgress = true;
                StartWrite(&packet);
                return;
            }
            // I've cleared the queue and/or I have packets in flight.
            // Try to get more packets to write while I `wait.'
            if (mPacketsQueue.empty())
            {
                try
                {
                    auto packetsBuffer
                        = mManager->getNextPackets(mContext,
                                                   mStreamIdentifiers);
                    for (auto &packet : packetsBuffer)
                    {
                        if (mPacketsQueue.size() > mMaximumQueueSize)
                        {
                            spdlog::warn(
                               "RPC writer queue exceeded - popping element");
                            mPacketsQueue.pop();
                         }
                         mPacketsQueue.push(std::move(packet));
                    }
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Failed to get next packet because "
                               + std::string {e.what()});
                }
            }
            // No new packets were acquired and I'm not waiting for a write.
            // Give me stream manager a break.
            if (mPacketsQueue.empty() && !mWriteInProgress)
            {
                std::this_thread::sleep_for(mTimeOut);
            }
        } // Loop on server still running
        if (mContext)
        {
            // The context is still valid so try to remove from the
            // subscriptions.  This can be the case whether the server is
            // shutting down or the client bailed.
            mManager->unsubscribe(mContext, mStreamIdentifiers);
            if (mContext->IsCancelled())
            {
                spdlog::debug("Terminating acquisition for " 
                            + mPeer
                            + " because of client side cancel");
            }
            else
            {
                spdlog::debug("Terminating acquisition for "
                            + mPeer
                            + " because of server side cancel");
            }
        }
        else
        {
            spdlog::warn("Context has disappeared for " + mPeer);
        }
        Finish(grpc::Status::OK);
    }
    grpc::CallbackServerContext *mContext{nullptr};
    UDataPacketImport::GRPC::SubscriptionRequest mSubscriptionRequest;
    std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager> mManager{nullptr};
    std::atomic<bool> *mKeepRunning{nullptr};
    std::queue<UDataPacketImport::GRPC::Packet> mPacketsQueue;
    std::set<UDataPacketImport::StreamIdentifier> mStreamIdentifiers;
    std::string mPeer;
    std::chrono::milliseconds mTimeOut{20};
    size_t mMaximumQueueSize{2048};
    int mMaximumNumberOfSubscribers{128};
    bool mWriteInProgress{false};
};

}
#endif
