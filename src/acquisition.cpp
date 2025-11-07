#include <vector>
#include "uDataPacketImport/acquisition.hpp"
#include "uDataPacketImport/packet.hpp"
#include "proto/dataPacketBroadcast.pb.h"

using namespace UDataPacketImport;

/// Destructor
IAcquisition::~IAcquisition() = default;

/*
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
*/

/*
/// Constructor
IClient::IClient(
    const std::function
    <
        void (std::vector<UDataPacketImport::Packet> &&)
    > &callback) :
    pImpl(std::make_unique<IClientImpl> (callback))
{
}
*/

/// Applies the callback
/*
void IClient::operator()(
    std::vector<UDataPacketImport::Packet> &&packets)
{
    //if (!pImpl->mHaveCallback){throw std::runtime_error("Callback not set");}
    for (auto &packet : packets)
    {
        this->operator()(std::move(packet));
    }
    //pImpl->mCallback(std::move(packets));
}
*/

/// Implement a single packet
/*
void IClient::operator()(const UDataPacketImport::Packet &packet)
{
    //std::vector<UDataPacketImport::Packet> packets{std::move(packet)};
    this->operator()(std::move(packet.toProtobuf()));
} 
*/
