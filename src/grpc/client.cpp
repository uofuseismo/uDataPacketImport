#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <spdlog/spdlog.h>
#include <grpcpp/grpcpp.h>
#include "uDataPacketImport/grpc/client.hpp"
#include "uDataPacketImport/grpc/clientOptions.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.pb.h"
#include "proto/dataPacketBroadcast.grpc.pb.h"

using namespace UDataPacketImport::GRPC;

namespace
{

class CustomAuthenticator : public grpc::MetadataCredentialsPlugin
{       
public:     
    CustomAuthenticator(const grpc::string &token) :
        mToken(token)
    {
    }
    grpc::Status GetMetadata(
        grpc::string_ref serviceURL, 
        grpc::string_ref methodName,
        const grpc::AuthContext &channelAuthContext,
        std::multimap<grpc::string, grpc::string> *metadata) override
    {
        metadata->insert(std::make_pair("x-custom-auth-token", mToken));
        return grpc::Status::OK;
    }
    
//private:
    grpc::string mToken;
};

/// Creates the channel
std::shared_ptr<grpc::Channel>
    createChannel(const std::string &address,
                  const std::string &clientCertificate,
                  const std::string &apiKey)
{
    if (!clientCertificate.empty())
    {
        if (!apiKey.empty())
        {
            spdlog::debug("Creating secure channel with API key to "
                        + address); 
            auto callCredentials = grpc::MetadataCredentialsFromPlugin(
                std::unique_ptr<grpc::MetadataCredentialsPlugin> (
                    new ::CustomAuthenticator(apiKey)));
            grpc::SslCredentialsOptions sslOptions;
            sslOptions.pem_root_certs = clientCertificate;
            auto channelCredentials
                = grpc::CompositeChannelCredentials(
                      grpc::SslCredentials(sslOptions),
                      callCredentials);
            return grpc::CreateChannel(address, channelCredentials);
        }
        spdlog::debug("Creating secure channel without API key to "
                    + address);
        grpc::SslCredentialsOptions sslOptions;
        sslOptions.pem_root_certs = clientCertificate;
        return grpc::CreateChannel(address,
                                   grpc::SslCredentials(sslOptions));
     }
     spdlog::debug("Creating non-secure channel to " + address);
     return grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
}

class ReadAll : public grpc::ClientReadReactor<UDataPacketImport::GRPC::Packet>
{
public:
    ReadAll(//UDataPacketImport::GRPC::RealTimeBroadcast::Stub *stub,
            std::shared_ptr<grpc::Channel> channel,
            const std::function<void (UDataPacketImport::GRPC::Packet &&)> &callback,
            std::atomic<bool> *keepRunning,
            const std::string &apiKey = "") :
        mCallback(callback),
        mKeepRunning(keepRunning)
    {
/*
        // Authenticate the user?
        if (!apiKey.empty())
        {
            auto callCredentials = grpc::MetadataCredentialsFromPlugin(
                std::unique_ptr<grpc::MetadataCredentialsPlugin> (
                    new ::CustomAuthenticator(apiKey));
            grpc::SslCredentialsOptions sslOptions;
            sslOptions.pem_root_certs = options.grpcClientCertificate;
            auto channelCredentials
                = grpc::CompositeChannelCredentials(
                      grpc::SslCredentials(sslOptions),
                      callCredentials);
            return grpc::CreateChannel(address, channelCredentials);
        }
        else
        {
            return grpc::InsecureChannelCredentials());
        }
 */      
        // Okay, make the stub
        mStub = UDataPacketImport::GRPC::RealTimeBroadcast::NewStub (channel);
        mStub->async()->SubscribeToAllStreams(&mClientContext, &mRequest, this); 
        StartRead(&mPacket);
        StartCall();
    }
    /*
    void OnWriteDone(bool ok) override
    {
        if (ok)
        {
            if (!mKeepRunning->load())
            {
                spdlog::info("Client submitting a cancel request");
                mClientContext.TryCancel();
            }
            else
            {
                StartRead(&mPacket);
            }
        }
    }
    */
    void OnReadDone(bool ok) override
    {
        if (ok)
        {
            if (mKeepRunning->load())
            {
                try
                {
                    auto copy = mPacket;
                    mCallback(std::move(copy)); 
                }
                catch (const std::exception &e)
                {
                    spdlog::warn("Failed to propagate packet because "
                               + std::string {e.what()});
                }
                StartRead(&mPacket);
            }
            else
            {
                spdlog::info("Client submitting a cancel request");
                mClientContext.TryCancel();
            }
        }
    }

    void OnDone(const grpc::Status &status) override
    {
        mStatus = status;
        if (!mStatus.ok())
        {
            if (mStatus.error_code() == grpc::StatusCode::CANCELLED)
            {
                spdlog::info("Client successfully canceled RPC");
            }
            else
            {
                spdlog::warn("RPC failed with error code "
                           + std::to_string(mStatus.error_code()) 
                           + ": " + mStatus.error_message());
            }
        } 
        std::unique_lock<std::mutex> lock(mMutex);
        mDone = true;
        mConditionVariable.notify_all();
    }

    grpc::Status await()
    {
        std::unique_lock<std::mutex> lock(mMutex);
        mConditionVariable.wait(lock, [this]
                                {
                                   return mDone;
                                });
        return std::move(mStatus);
    }

    std::mutex mMutex;
    UDataPacketImport::GRPC::Packet mPacket;
    std::function<void (UDataPacketImport::GRPC::Packet &&)> mCallback;
    std::unique_ptr<UDataPacketImport::GRPC::RealTimeBroadcast::Stub> mStub{nullptr};
    std::condition_variable mConditionVariable;
    grpc::ClientContext mClientContext;
    UDataPacketImport::GRPC::SubscribeToAllStreamsRequest mRequest;
    grpc::Status mStatus;
    std::atomic<bool> *mKeepRunning{nullptr};
    bool mDone{false};
};

}

class ClientOptions
{
public:
 
};

class Client::ClientImpl
{
public:
    ClientImpl(
        const std::function<void (UDataPacketImport::GRPC::Packet &&)> &callback,
        const ClientOptions &options) :
        mCallback(callback),
        mOptions(options)
    {
        mInitialized = true;
    }
    void stop()
    {
        mKeepRunning = false;
    }
    std::future<void> start()
    {
        stop();
        mKeepRunning = true;
    }
    void acquirePackets()
    {
        if (mOptions.subscribeToAllStreams())
        {

        }
        else
        {
            auto streamSelections = mOptions.getStreamSelections();
            if (streamSelections == std::nullopt)
            {
                throw std::runtime_error("Stream selections are null");
            }
            UDataPacketImport::GRPC::SubscriptionRequest request;
            for (const auto &stream : *streamSelections)
            {
                *request.add_streams() = stream.toProtobuf();
            }
        }
    }
    std::function<void (UDataPacketImport::GRPC::Packet &&) > mCallback;
    UDataPacketImport::GRPC::ClientOptions mOptions;
    std::atomic<bool> mKeepRunning{true};
    bool mInitialized{false};
};

/// Constructor
Client::Client(
    const std::function<void (UDataPacketImport::GRPC::Packet &&)> &callback,
    const ClientOptions &options) :
    pImpl(std::make_unique<ClientImpl> (callback, options))
{
}

/// Destructor
Client::~Client() = default;

/// Running?
bool Client::isRunning() const noexcept
{
    return pImpl->mKeepRunning.load();
}

/// Start
std::future<void> Client::start()
{
    if (!isInitialized()){throw std::runtime_error("Client not initialized");}
    return pImpl->start(); 
}

/// Stop
void Client::stop()
{
    pImpl->stop();
}

/// Initialized?
bool Client::isInitialized() const noexcept
{
    return pImpl->mInitialized;
}

/// Type
std::string Client::getType() const noexcept
{
    return "gRPC";
}
