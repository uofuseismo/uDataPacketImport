#include <string>
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "isEmpty.hpp"

using namespace UDataPacketImport;

class StreamIdentifier::StreamIdentifierImpl
{
public:
    void setString()
    {
        mString.clear();
        if (!mNetwork.empty() &&
            !mStation.empty() &&
            !mChannel.empty() &&
            !mLocationCode.empty())
        {
            mString = mNetwork + "."
                    + mStation + "."
                    + mChannel + "."
                    + mLocationCode;
        }
        mStringView = mString; 
    }
    std::string mNetwork;
    std::string mStation;
    std::string mChannel;
    std::string mLocationCode;   
    std::string mString;
    std::string_view mStringView;
};

/// Constructor
StreamIdentifier::StreamIdentifier() :
    pImpl(std::make_unique<StreamIdentifierImpl> ())
{
}

/// Constructor
StreamIdentifier::StreamIdentifier(
    const std::string_view &network,
    const std::string_view &station,
    const std::string_view &channel,
    const std::string_view &locationCode)
{
    StreamIdentifier temp;
    temp.setNetwork(network);
    temp.setStation(station);
    temp.setChannel(channel);
    temp.setLocationCode(locationCode);
    *this = std::move(temp);
}

/// Constructor
StreamIdentifier::StreamIdentifier(
    const UDataPacketImport::GRPC::StreamIdentifier &streamIdentifier) :
    pImpl(std::make_unique<StreamIdentifierImpl> ())
{
    StreamIdentifier identifier;
    identifier.setNetwork(streamIdentifier.network());
    identifier.setStation(streamIdentifier.station());
    identifier.setChannel(streamIdentifier.channel());
    if (streamIdentifier.has_location_code())
    {
        identifier.setLocationCode(streamIdentifier.location_code());
    }
    else
    {
        identifier.setLocationCode("");
    }
    *this = std::move(identifier);
}

/// Copy constructor
StreamIdentifier::StreamIdentifier(const StreamIdentifier &identifier)
{
    *this = identifier;
}

/// Move constructor
StreamIdentifier::StreamIdentifier(StreamIdentifier &&identifier) noexcept
{
    *this = std::move(identifier);
}

/// Copy assignment
StreamIdentifier& 
StreamIdentifier::operator=(const StreamIdentifier &identifier)
{
    if (&identifier == this){return *this;}
    pImpl = std::make_unique<StreamIdentifierImpl> (*identifier.pImpl);
    return *this;
}

/// Move assignment
StreamIdentifier& 
StreamIdentifier::operator=(StreamIdentifier &&identifier) noexcept
{
    if (&identifier == this){return *this;}
    pImpl = std::move(identifier.pImpl);
    return *this;
}

/// Reset class
void StreamIdentifier::clear() noexcept
{
    pImpl->mNetwork.clear();
    pImpl->mStation.clear();
    pImpl->mChannel.clear();
    pImpl->mLocationCode.clear();
    pImpl->setString();
}

/// Destructor
StreamIdentifier::~StreamIdentifier() = default;

/// Network
void StreamIdentifier::setNetwork(const std::string_view &network)
{
    auto s = ::convertString(network);
    if (::isEmpty(s)){throw std::invalid_argument("Network is empty");}
    pImpl->mNetwork = std::move(s);
    pImpl->setString();
}

std::string StreamIdentifier::getNetwork() const
{
    if (!hasNetwork()){throw std::runtime_error("Network not set yet");}
    return pImpl->mNetwork;
}

bool StreamIdentifier::hasNetwork() const noexcept
{
    return !pImpl->mNetwork.empty();
}

/// Station
void StreamIdentifier::setStation(const std::string_view &station)
{
    auto s = ::convertString(station);
    if (::isEmpty(s)){throw std::invalid_argument("Station is empty");}
    pImpl->mStation = std::move(s);
    pImpl->setString();
}

std::string StreamIdentifier::getStation() const
{
    if (!hasStation()){throw std::runtime_error("Station not set yet");}
    return pImpl->mStation;
}

bool StreamIdentifier::hasStation() const noexcept
{
    return !pImpl->mStation.empty();
}

/// Channel
void StreamIdentifier::setChannel(const std::string_view &channel)
{
    auto s = ::convertString(channel);
    if (::isEmpty(s)){throw std::invalid_argument("Channel is empty");}
    pImpl->mChannel = std::move(s);
    pImpl->setString();
}

std::string StreamIdentifier::getChannel() const
{
    if (!hasChannel()){throw std::runtime_error("Channel not set yet");}
    return pImpl->mChannel;
}

bool StreamIdentifier::hasChannel() const noexcept
{
    return !pImpl->mChannel.empty();
}

/// Location code
void StreamIdentifier::setLocationCode(const std::string_view &locationCode)
{
    auto s = ::convertString(locationCode);
    if (::isEmpty(locationCode))
    {
        pImpl->mLocationCode = "--";
    }
    else
    {
        pImpl->mLocationCode = std::move(s);
    }
    pImpl->setString();
}

std::string StreamIdentifier::getLocationCode() const
{
    if (!hasLocationCode())
    {   
        throw std::runtime_error("Location code not set yet");
    }   
    return pImpl->mLocationCode;
}

bool StreamIdentifier::hasLocationCode() const noexcept
{
    return !pImpl->mLocationCode.empty();
}

/// To name
std::string StreamIdentifier::toString() const
{
    if (pImpl->mString.empty())
    {
        if (!hasNetwork()){throw std::runtime_error("Network not set");}
        if (!hasStation()){throw std::runtime_error("Station not set");}
        if (!hasChannel()){throw std::runtime_error("Channel not set");}
        if (!hasLocationCode())
        {
            throw std::runtime_error("Location code not set");
        }
    }
    return pImpl->mString;
}

const std::string &StreamIdentifier::getStringReference() const
{
    if (pImpl->mString.empty())
    {   
        if (!hasNetwork()){throw std::runtime_error("Network not set");}
        if (!hasStation()){throw std::runtime_error("Station not set");}
        if (!hasChannel()){throw std::runtime_error("Channel not set");}
        if (!hasLocationCode())
        {   
            throw std::runtime_error("Location code not set");
        }   
    }   
    return *&pImpl->mString;
}

const std::string_view StreamIdentifier::toStringView() const
{
    if (pImpl->mString.empty())
    {   
        if (!hasNetwork()){throw std::runtime_error("Network not set");}
        if (!hasStation()){throw std::runtime_error("Station not set");}
        if (!hasChannel()){throw std::runtime_error("Channel not set");}
        if (!hasLocationCode())
        {
            throw std::runtime_error("Location code not set");
        }   
    }   
    return pImpl->mStringView;
}

UDataPacketImport::GRPC::StreamIdentifier StreamIdentifier::toProtobuf() const
{
    UDataPacketImport::GRPC::StreamIdentifier result;
    result.set_network(std::move(getNetwork()));
    result.set_station(std::move(getStation()));
    result.set_channel(std::move(getChannel()));
    result.set_location_code(std::move(getLocationCode()));
    return result;
}

bool UDataPacketImport::operator<(const StreamIdentifier &lhs,
                                  const StreamIdentifier &rhs)
{
    return lhs.toStringView() < rhs.toStringView();
}

bool UDataPacketImport::operator==(const StreamIdentifier &lhs,
                                   const StreamIdentifier &rhs)
{
    return lhs.toStringView() == rhs.toStringView();
}

