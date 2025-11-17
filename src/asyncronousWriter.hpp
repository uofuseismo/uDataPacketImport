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

class AsyncronousWriter :
    public grpc::ServerWriteReactor<UDataPacketImport::GRPC::Packet>
{
public:
    AsyncronousWriter(
         grpc::CallbackServerContext *context,
         const UDataPacketImport::GRPC::SubscriptionRequest *request,
         std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager>
             &subscriptionManager,
         std::atomic<bool> *keepRunning) :
             mContext(context),
             mManager(subscriptionManager),
             mKeepRunning(keepRunning)
    {
        // Subscribe
        try
        {
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
         spdlog::debug("RPC Completed");
         if (mContext)
         {
             mManager->unsubscribeFromAllOnCancel(mContext);
         }
         delete this;
     }   

     void OnCancel() override 
     { 
         spdlog::debug("RPC Cancelled");
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
    std::shared_ptr<UDataPacketImport::GRPC::SubscriptionManager> mManager{nullptr};
    std::queue<UDataPacketImport::GRPC::Packet> mPacketsQueue;
    std::atomic<bool> *mKeepRunning{nullptr};
    std::chrono::milliseconds mTimeOut{10};
    size_t mMaximumQueueSize{2048};
    bool mWriteInProgress{false};
};

}
#endif
