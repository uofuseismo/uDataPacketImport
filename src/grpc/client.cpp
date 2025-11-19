#include <functional>
#include <atomic>
#include <grpcpp/grpcpp.h>
#include "uDataPacketImport/grpc/client.hpp"

using namespace UDataPacketImport::GRPC;

namespace
{

}

class ClientOptions
{
public:
 
};

class Client::ClientImpl
{
public:
    ClientImpl(
        const std::function<void (UDataPacketImport::GRPC::Packet &&)> &callback) :
        mCallback(callback)
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

    }
    std::function<void (UDataPacketImport::GRPC::Packet &&) > mCallback;
    std::atomic<bool> mKeepRunning{true};
    bool mInitialized{false};
};

/// Constructor
Client::Client(
    const std::function<void (UDataPacketImport::GRPC::Packet &&)> &callback,
    const ClientOptions &options) :
    pImpl(std::make_unique<ClientImpl> (callback))
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
