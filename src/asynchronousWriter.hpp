#ifndef UDATA_PACKET_IMPORT_WRITER_HPP
#define UDATA_PACKET_IMPORT_WRITER_HPP
#include <atomic>
#include <chrono>
#include <string>
#include <queue>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>
#include "uDataPacketImport/grpc/subscriptionManager.hpp"
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
         const std::string &accessToken = "") :
             mContext(context),
             mManager(subscriptionManager),
             mKeepRunning(keepRunning)
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
            spdlog::info("Subscribing " + mPeer);
            mManager->subscribeToAll(context);
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
            Finish(grpc::Status(grpc::StatusCode::UNKNOWN,
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
         spdlog::info("Subscribe to all RPC completed for " + mPeer);
         if (mContext)
         {
             mManager->unsubscribeFromAllOnCancel(mContext);
         }
         delete this;
     }   

     void OnCancel() override 
     { 
         spdlog::info("Subscribe to all RPC cancelled for " + mPeer);
         if (mContext)
         {
             mManager->unsubscribeFromAllOnCancel(mContext);
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
        } // Loop on server/client
        if (mContext->IsCancelled())
        {
            spdlog::info("Terminating acquisition for " 
                        + mContext->peer()
                        + " because of client side cancel");
        }
        else
        {
            spdlog::info("Terminating acquisition for "
                        + mContext->peer()
                        + " because of server side cancel");
        }
        Finish(grpc::Status::OK);
    }
    grpc::CallbackServerContext *mContext{nullptr};
    std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager> mManager{nullptr};
    std::atomic<bool> *mKeepRunning{nullptr};
    std::queue<UDataPacketImport::GRPC::Packet> mPacketsQueue;
    std::string mPeer;
    std::chrono::milliseconds mTimeOut{20};
    size_t mMaximumQueueSize{2048};
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
         const std::string &accessToken = "") :
             mContext(context),
             mSubscriptionRequest(request),
             mManager(subscriptionManager),
             mKeepRunning(keepRunning)
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
        if (mSubscriptionRequest == nullptr)
        {
            spdlog::critical("Subscription request pointer is null");
            Finish(grpc::Status(grpc::StatusCode::INTERNAL,
                                "Failed to subscribe"));
        }
        if (mSubscriptionRequest->streams_size() == 0)
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
            spdlog::info("Subscribing " + mPeer);
            //mManager->subscribe(context, *mSubscriptionRequest); // TODO
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
            Finish(grpc::Status(grpc::StatusCode::UNKNOWN,
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
         spdlog::debug("RPC completed for " + mPeer);
         if (mContext)
         {
             //mManager->unsubscribeFromAllOnCancel(mContext); TODO
         }
         delete this;
     }   

     void OnCancel() override 
     { 
         spdlog::debug("RPC cancelled for " + mPeer);
         if (mContext)
         {
             //mManager->unsubscribeFromAllOnCancel(mContext); TODO
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
/*
                    auto packetsBuffer
                        = mManager->getNextPacketsFromAllSubscriptions(
                              mContext); // TODO
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
*/
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
        } // Loop on server/client
        if (mContext->IsCancelled())
        {
            spdlog::debug("Terminating acquisition for " 
                        + mContext->peer()
                        + " because of client side cancel");
        }
        else
        {
            spdlog::debug("Terminating acquisition for "
                        + mContext->peer()
                        + " because of server side cancel");
        }
        Finish(grpc::Status::OK);
    }
    grpc::CallbackServerContext *mContext{nullptr};
    const UDataPacketImport::GRPC::SubscriptionRequest *mSubscriptionRequest{nullptr};
    std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager> mManager{nullptr};
    std::atomic<bool> *mKeepRunning{nullptr};
    std::queue<UDataPacketImport::GRPC::Packet> mPacketsQueue;
    std::string mPeer;
    std::chrono::milliseconds mTimeOut{20};
    size_t mMaximumQueueSize{2048};
    bool mWriteInProgress{false};
};

}
#endif
