#include <vector>
#include "uDataPacketImport/client.hpp"
#include "uDataPacketImport/packet.hpp"

using namespace UDataPacketImport;

class IClient::IClientImpl
{
public:
    explicit IClientImpl(
        const std::function
        <
            void (std::vector<UDataPacketImport::Packet> &&)
        > &callback) :
        mCallback(callback),
        mHaveCallback(true)
    {   
    }   
    std::function<void (std::vector<UDataPacketImport::Packet> &&)>
        mCallback;
    bool mHaveCallback{false};
};

/// Constructor
IClient::IClient(
    const std::function
    <
        void (std::vector<UDataPacketImport::Packet> &&)
    > &callback) :
    pImpl(std::make_unique<IClientImpl> (callback))
{
}

/// Destructor
IClient::~IClient() = default;

/// Applies the callback
void IClient::operator()(
    std::vector<UDataPacketImport::Packet> &&packets)
{
    if (!pImpl->mHaveCallback){throw std::runtime_error("Callback not set");}
    pImpl->mCallback(std::move(packets));
}

/// Implement a single packet
void IClient::operator()(UDataPacketImport::Packet &&packet)
{
    std::vector<UDataPacketImport::Packet> packets{std::move(packet)};
    this->operator()(std::move(packets));
} 
    
