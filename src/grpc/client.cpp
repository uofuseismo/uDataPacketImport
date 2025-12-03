#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <spdlog/spdlog.h>
#include <grpcpp/grpcpp.h>
#include "uDataPacketImport/grpc/client.hpp"
#include "uDataPacketImport/grpc/clientOptions.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/v1/broadcast.grpc.pb.h"
#include "proto/v1/packet.pb.h"

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
    createChannel(const ClientOptions &options)
{
    auto address = options.getAddress();
    auto clientCertificate = options.getCertificate();
    if (clientCertificate)
    {
#ifndef NDEBUG
        assert(!clientCertificate->empty());
#endif
        auto apiKey = options.getToken();
        if (apiKey)
        {
#ifndef NDEBUG
            assert(!apiKey->empty());
#endif
            spdlog::debug("Creating secure channel with API key to "
                        + address); 
            auto callCredentials = grpc::MetadataCredentialsFromPlugin(
                std::unique_ptr<grpc::MetadataCredentialsPlugin> (
                    new ::CustomAuthenticator(*apiKey)));
            grpc::SslCredentialsOptions sslOptions;
            sslOptions.pem_root_certs = *clientCertificate;
            auto channelCredentials
                = grpc::CompositeChannelCredentials(
                      grpc::SslCredentials(sslOptions),
                      callCredentials);
            return grpc::CreateChannel(address, channelCredentials);
        }
        spdlog::debug("Creating secure channel without API key to "
                    + address);
        grpc::SslCredentialsOptions sslOptions;
        sslOptions.pem_root_certs = *clientCertificate;
        return grpc::CreateChannel(address,
                                   grpc::SslCredentials(sslOptions));
     }
     spdlog::debug("Creating non-secure channel to " + address);
     return grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
}

class Reader :
    public grpc::ClientReadReactor<UDataPacketImport::GRPC::V1::Packet>
{
public:
    Reader(UDataPacketImport::GRPC::V1::RealTimeBroadcast::Stub *stub,
           const std::function<void (UDataPacketImport::GRPC::V1::Packet &&)> &callback,
           std::atomic<bool> *keepRunning,
           int *nReconnect) :
        mStub(stub),
        mGRPCPacketCallback(callback),
        mKeepRunning(keepRunning),
        mReconnect(nReconnect),
        mSubscribeToAll(true),
        mUseGRPCPacketCallback(true)
    {
        mStub->async()->SubscribeToAllStreams(&mClientContext,
                                              &mSubscribeToAllRequest,
                                              this); 
        StartRead(&mPacket); // Specify where to store received response
        StartCall(); // Activate RPC
    }

    Reader(UDataPacketImport::GRPC::V1::RealTimeBroadcast::Stub *stub,
           const std::function<void (UDataPacketImport::Packet &&)> &callback,
           std::atomic<bool> *keepRunning,
           int *nReconnect) :
        mStub(stub),
        mPacketCallback(callback),
        mKeepRunning(keepRunning),
        mReconnect(nReconnect),
        mSubscribeToAll(true),
        mUseGRPCPacketCallback(false)
    {   
        mStub->async()->SubscribeToAllStreams(&mClientContext,
                                              &mSubscribeToAllRequest,
                                              this); 
        StartRead(&mPacket); // Specify where to store received response
        StartCall(); // Activate RPC
    }   

    Reader(UDataPacketImport::GRPC::V1::RealTimeBroadcast::Stub *stub,
           const std::function<void (UDataPacketImport::GRPC::V1::Packet &&)> &callback,
           std::atomic<bool> *keepRunning,
           int *nReconnect,
           const std::set<UDataPacketImport::StreamIdentifier> &identifiers) :
           mStub(stub),
        mGRPCPacketCallback(callback),
        mKeepRunning(keepRunning),
        mReconnect(nReconnect),
        mSubscribeToAll(false),
        mUseGRPCPacketCallback(true)
    {   
        if (identifiers.empty())
        {
            throw std::invalid_argument("No stream selections specified");
        }
        mSubscriptionRequest.clear_streams();
        for (const auto &identifier : identifiers)
        {
            *mSubscriptionRequest.add_streams() = identifier.toProtobuf();
        }
        mStub->async()->Subscribe(&mClientContext, &mSubscriptionRequest, this);
        StartRead(&mPacket); // Specify where to store received response
        StartCall(); // Activate RPC
    }   

    Reader(UDataPacketImport::GRPC::V1::RealTimeBroadcast::Stub *stub,
           const std::function<void (UDataPacketImport::Packet &&)> &callback,
           std::atomic<bool> *keepRunning,
           int *nReconnect,
           const std::set<UDataPacketImport::StreamIdentifier> &identifiers) :
           mStub(stub),
        mPacketCallback(callback),
        mKeepRunning(keepRunning),
        mReconnect(nReconnect),
        mSubscribeToAll(false),
        mUseGRPCPacketCallback(false)
    {
        if (identifiers.empty())
        {
            throw std::invalid_argument("No stream selections specified");
        }
        mSubscriptionRequest.clear_streams();
        for (const auto &identifier : identifiers)
        {
            *mSubscriptionRequest.add_streams() = identifier.toProtobuf();
        }
        mStub->async()->Subscribe(&mClientContext, &mSubscriptionRequest, this);
        StartRead(&mPacket); // Specify where to store received response
        StartCall(); // Activate RPC
    }

    /*
    void OnWriteDone(bool ok) override
    {
        if (ok)
        {
            if (!mKeepRunning->load())
            {
                spdlog::info("Client submitting a cancel request");
                mSentCancel = true;
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
            if (mReconnect){*mReconnect = 0;} // Something clearly came through - reset counter
            try
            {
                if (mUseGRPCPacketCallback)
                {
                    //auto copy = mPacket;
                    mGRPCPacketCallback(std::move(mPacket)); 
                }
                else
                {
                    UDataPacketImport::Packet packet{mPacket};
                    mPacketCallback(std::move(packet)); 
                }
            }
            catch (const std::exception &e)
            {
                spdlog::warn("::Read failed to propagate packet because "
                           + std::string {e.what()});
            }
            if (mKeepRunning->load())
            {
                StartRead(&mPacket);
            }
        }
    }

    void OnDone(const grpc::Status &status) override
    {
        if (!status.ok())
        {
            if (status.error_code() == grpc::StatusCode::CANCELLED)
            {
                spdlog::info("::Reader client successfully canceled RPC");
            }
            else
            {
                spdlog::warn("::Reader RPC failed with error code "
                           + std::to_string(mStatus.error_code()) 
                           + ": " + mStatus.error_message());
            }
        }
        {
        std::unique_lock<std::mutex> lock(mMutex);
        mStatus = status;
        mDone = true;
        mConditionVariable.notify_all();
        }
    }

    [[nodiscard]] grpc::Status await()
    {
        while (!mDone)
        {
            if (!mKeepRunning->load())
            {
                if (!mSentCancel.load())
                {
                    mSentCancel = true;
                    mClientContext.TryCancel();
                } 
            }
            std::unique_lock<std::mutex> lock(mMutex);
            mConditionVariable.wait_for(lock,
                                        std::chrono::milliseconds {50},
                                        [this]
                                        {
                                            return mDone;
                                        });
            lock.unlock();
        }
        return std::move(mStatus);
/*
        std::unique_lock<std::mutex> lock(mMutex);
        mConditionVariable.wait(lock, [this]
                                {
                                   return mDone;
                                });
        return std::move(mStatus);
*/
    }

    std::mutex mMutex;
    UDataPacketImport::GRPC::V1::Packet mPacket;
    std::unique_ptr<UDataPacketImport::GRPC::V1::RealTimeBroadcast::Stub> mStub{nullptr};
    std::function<void (UDataPacketImport::GRPC::V1::Packet &&)> mGRPCPacketCallback;
    std::function<void (UDataPacketImport::Packet &&)> mPacketCallback;
    std::condition_variable mConditionVariable;
    grpc::ClientContext mClientContext;
    UDataPacketImport::GRPC::V1::SubscribeToAllStreamsRequest
        mSubscribeToAllRequest;
    UDataPacketImport::GRPC::V1::SubscriptionRequest mSubscriptionRequest; 
    grpc::Status mStatus;
    std::atomic<bool> *mKeepRunning{nullptr};
    int *mReconnect{nullptr};
    bool mSubscribeToAll{false};
    bool mUseGRPCPacketCallback{true};
    bool mDone{false};
    std::atomic<bool> mSentCancel{false};
};

[[nodiscard]] std::set<UDataPacketImport::StreamIdentifier>
    getAvailableStreams(const ClientOptions &options)
{
    auto channel = ::createChannel(options);
    auto stub
        = UDataPacketImport::GRPC::V1::RealTimeBroadcast::NewStub(channel);
    grpc::ClientContext context;
    constexpr std::chrono::seconds timeOut{3};
    context.set_deadline(std::chrono::high_resolution_clock::now() + timeOut);
    std::mutex mutex;
    std::condition_variable conditionVariable;
    UDataPacketImport::GRPC::V1::AvailableStreamsRequest request;
    UDataPacketImport::GRPC::V1::AvailableStreamsResponse response;
    grpc::Status status;
    bool done{false};
    stub->async()->GetAvailableStreams(
         &context, &request, &response,
         [&mutex, &conditionVariable, &done, &status]
         (grpc::Status returnedStatus)
    {
        status = std::move(returnedStatus);
        std::lock_guard<std::mutex> lock(mutex);
        done = true;
        conditionVariable.notify_one();
    });
    std::unique_lock<std::mutex> lock(mutex);
    while (!done)
    {
        conditionVariable.wait(lock);
    }
    // Take action
    if (status.ok())
    { 
        spdlog::debug("Successfully determined there are " 
                    + std::to_string(response.stream_identifiers().size())
                    + " streams available");
        std::set<UDataPacketImport::StreamIdentifier> result;
        for (const auto &stream : response.stream_identifiers())
        {
            result.insert( UDataPacketImport::StreamIdentifier {stream} );
        }
        return result;
    }
    else
    {
        auto error = "Available streams request failed with "
                   + std::to_string(static_cast<int> (status.error_code()))
                   + ": " 
                   + status.error_message();
        throw std::runtime_error(error);
    }
}


}

class ClientOptions
{
public:
 
};

class Client::ClientImpl
{
public:
    ClientImpl(
        const std::function<void (UDataPacketImport::GRPC::V1::Packet &&)> &callback,
        const ClientOptions &options) :
        mGRPCCallback(callback),
        mOptions(options),
        mUseGRPCPacketCallback(true)
    {
        mInitialized = true;
    }
    ClientImpl(
        const std::function<void (UDataPacketImport::Packet &&)> &callback,
        const ClientOptions &options) :
        mPacketCallback(callback),
        mOptions(options),
        mUseGRPCPacketCallback(false)
    {
        mInitialized = true;
    }   
    void stop()
    {
        mKeepRunning.store(false);
    }
    std::future<void> start()
    {
        stop();
        mKeepRunning.store(true);
        auto result = std::async(&ClientImpl::acquirePackets, this);
        return result;
    }
    [[nodiscard]] bool shouldReconnect(const grpc::Status &status) const
    {
        // Successful termination
        if (status.ok())
        {
            spdlog::info("Subscription ended successfully");
            return mKeepRunning.load() ? true : false;
        }
        else // Handle the errors
        {
            // Most there was a cancellation
            if (status.error_code() == grpc::CANCELLED)
            {
                // Server quit, let's try someone else
                if (mKeepRunning.load())
                {
                    spdlog::warn("Client received server side cancellation");
                    return true;
                }
                else
                {
                    spdlog::info("Client RPC cancellation successful");
                    return false;
                }
            } 
            // No one home - try again (provided I want to keep running?)
            else if (status.error_code() == grpc::UNAVAILABLE)
            {
                spdlog::warn("Service is unavailable ("
                           + status.error_message() + ")");
                return mKeepRunning.load() ? true : false;
            }
            // Service might be oversubscribed try again and hopefully scalable
            // backend has something else available
            else if (status.error_code() == grpc::RESOURCE_EXHAUSTED)
            {
                spdlog::warn("Resource exhausted");
                return mKeepRunning.load() ? true : false;
            }
            // My creds are probably bad - no amount of retries will help
            else if (status.error_code() == grpc::PERMISSION_DENIED)
            {
                spdlog::critical(
                    "Permission deneid - check API key");
                throw std::runtime_error("Permission denied");
            }
            else if (status.error_code() == grpc::UNAUTHENTICATED)
            {
                spdlog::critical(
                    "Invalid authentication credentials - check API key");
                throw std::runtime_error("Invalid authentication credentials");
            }
            else if (status.error_code() == grpc::INVALID_ARGUMENT)
            {
                spdlog::critical(
                    "Invalid argument provided to RPC; RPC returned "
                  + status.error_message());
                throw std::runtime_error("Invalid arguments in RPC");
            }
            else
            {
                spdlog::info("Exited subscription RPC with error code "
                           + std::to_string(status.error_code())
                           + " and message " + status.error_message());
                throw std::runtime_error("Unhandled error"); 
            }
        }
#ifndef NDEBUG
        assert(false);
#else
        spdlog::critical("Unhandled section of code");
        return false;
#endif
    }
    void acquirePackets()
    {
        // Begin the activity of connecting and starting the RPC
        int nReconnect{0};
        auto reconnectSchedule = mOptions.getReconnectSchedule();
        while (mKeepRunning.load())
        {
            auto channel = ::createChannel(mOptions);
            auto stub
                = UDataPacketImport::GRPC::V1::RealTimeBroadcast::NewStub(channel);
            grpc::Status status;
            grpc::ClientContext context;
            context.set_wait_for_ready(false);
            if (!mOptions.getStreamSelections())
            {
                spdlog::info("Subscribing to all streams");
                UDataPacketImport::GRPC::V1::SubscribeToAllStreamsRequest request;
                std::unique_ptr<grpc::ClientReader<UDataPacketImport::GRPC::V1::Packet>>
                    reader(stub->SubscribeToAllStreams(&context, request));
                UDataPacketImport::GRPC::V1::Packet packet;
                while (reader->Read(&packet))
                {
                    if (!mKeepRunning)
                    {
                        spdlog::info(
                            "Subscription loop leaving b/c of application termination");
                        break;
                    }
                    try
                    {
                        if (mUseGRPCPacketCallback)
                        {
                            mGRPCCallback(std::move(packet));
                        }
                        else
                        {
                            UDataPacketImport::Packet copy{packet};
                            mPacketCallback(std::move(copy));
                        }
                    }
                    catch (const std::exception &e) 
                    {
                       spdlog::warn("Failed to propagate packet because "
                                  + std::string{e.what()});
                    }
                }
                spdlog::debug("Exited subscription loop");
                if (!mKeepRunning){context.TryCancel();}
                status = reader->Finish();
            }
            else
            {
                //std::cout << "hey" << std::endl;
                UDataPacketImport::GRPC::V1::SubscriptionRequest request;
                auto selections = mOptions.getStreamSelections();
#ifndef NDEBUG
                assert(selections);
#endif
                for (const auto &selection : *selections)
                {
                      *request.add_streams() = selection.toProtobuf();
                }
                spdlog::info("Subscribing to "
                           + std::to_string(request.streams_size())
                           + " streams");
                std::unique_ptr<grpc::ClientReader<UDataPacketImport::GRPC::V1::Packet>>
                    reader(stub->Subscribe(&context, request));
                UDataPacketImport::GRPC::V1::Packet packet;
                while (reader->Read(&packet))
                {
                    if (!mKeepRunning)
                    {
                        spdlog::info(
                            "Subscription loop leaving b/c of application termination");
                        break;
                    }
                    try
                    {
                        if (mUseGRPCPacketCallback)
                        {
                            mGRPCCallback(std::move(packet));
                        }
                        else
                        {
                            UDataPacketImport::Packet copy{packet};
                            mPacketCallback(std::move(copy));
                        }
                    }
                    catch (const std::exception &e) 
                    {
                       spdlog::warn("Failed to propagate packet because "
                                  + std::string{e.what()});
                    }
                }
                spdlog::debug("Exited subscription loop");
                if (!mKeepRunning){context.TryCancel();}
                status = reader->Finish();
            }

/*
            spdlog::debug("Creating gRPC subscription channel");
            grpc::Status status;
            {
            auto channel = ::createChannel(mOptions);
            std::unique_ptr<UDataPacketImport::GRPC::RealTimeBroadcast::Stub> stub
                = UDataPacketImport::GRPC::RealTimeBroadcast::NewStub(channel);
            if (mOptions.getStreamSelections())
            { 
                spdlog::info("Subscribing to selection of streams");
                if (mUseGRPCPacketCallback)
                {
                    ::Reader reader(stub.get(), mGRPCCallback,
                                    &mKeepRunning, &nReconnect,
                                    *mOptions.getStreamSelections());
                    status = reader.await();
                }
                else
                {
                    ::Reader reader(stub.get(), mPacketCallback,
                                    &mKeepRunning, &nReconnect,
                                    *mOptions.getStreamSelections());
                    status = reader.await();
                }
            }
            else
            {
                spdlog::info("Subscribing to all streams");
                if (mUseGRPCPacketCallback)
                {
                    ::Reader reader(stub.get(), mGRPCCallback,
                                    &mKeepRunning, &nReconnect);
                    status = reader.await();
                }
                else
                {
                    ::Reader reader(stub.get(), mPacketCallback,
                                    &mKeepRunning, &nReconnect);
                    status = reader.await();
                }
            }
            }
*/
            bool doReconnect{true};
            try
            {
                doReconnect = shouldReconnect(status);
            }
            catch (const std::exception &e)
            {
                spdlog::critical("Unknown reconnect plan: "
                               + std::string {e.what()});
                break;
            }
            if (doReconnect)
            {
                if (nReconnect >=
                    static_cast<int> (reconnectSchedule.size()))
                {
                    throw std::runtime_error(
                        "Max number of reconnects hit - terminating");
                }
                spdlog::info("Sleeping until next reconnect"); 
                std::this_thread::sleep_for(
                    reconnectSchedule.at(nReconnect)); 
                nReconnect = nReconnect + 1;
            }
            else
            {
                spdlog::info("Leaving reconnect loop");
                break;
            }
        } // Loop on keep running
        if (mKeepRunning.load())
        {
            spdlog::warn("Client likely hit max number of reconnects - throwing");
            throw std::runtime_error("Subscriber failed");
        }
        spdlog::info("Subscriber thread leaving");
    }
    std::function<void (UDataPacketImport::GRPC::V1::Packet &&) > mGRPCCallback;
    std::function<void (UDataPacketImport::Packet &&) > mPacketCallback;
    UDataPacketImport::GRPC::ClientOptions mOptions;
    std::atomic<bool> mKeepRunning{true};
    bool mSubscribeToAll{true};
    bool mInitialized{false};
    bool mUseGRPCPacketCallback{true};
};

/// Constructor
Client::Client(
    const std::function<void (UDataPacketImport::GRPC::V1::Packet &&)> &callback,
    const ClientOptions &options) :
    pImpl(std::make_unique<ClientImpl> (callback, options))
{
}

/// Constructor
Client::Client(
    const std::function<void (UDataPacketImport::Packet &&)> &callback,
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

/// Available streams
std::set<UDataPacketImport::StreamIdentifier>
Client::getAvailableStreams() const
{
    if (!isInitialized())
    {
        throw std::runtime_error("Client not initialized");
    }
    return ::getAvailableStreams(pImpl->mOptions);
}
