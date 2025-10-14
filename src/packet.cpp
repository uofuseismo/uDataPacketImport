#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <cmath>
#ifndef NDEBUG
#include <cassert>
#endif
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "isEmpty.hpp"

#define MESSAGE_TYPE "US8::MessageFormats::Broadcasts::DataPacket"
#define MESSAGE_VERSION "1.0.0"

using namespace UDataPacketImport;

namespace
{

std::string convertString(const std::string &s) 
{
    auto temp = s;
    temp.erase(std::remove(temp.begin(), temp.end(), ' '), temp.end());
    std::transform(temp.begin(), temp.end(), temp.begin(), ::toupper);
    return temp;
}

}

class Packet::PacketImpl
{
public:
    [[nodiscard]] int size() const
    {   
        if (mDataType == Packet::DataType::Unknown)
        {
            return 0;
        }
        else if (mDataType == Packet::DataType::Integer32)
        {   
            return static_cast<int> (mInteger32Data.size());
        }   
        else if (mDataType == Packet::DataType::Float)
        {
            return static_cast<int> (mFloatData.size());
        }
        else if (mDataType == Packet::DataType::Double)
        {
            return static_cast<int> (mDoubleData.size());
        }
        else if (mDataType == Packet::DataType::Integer64)
        {   
            return static_cast<int> (mInteger64Data.size());
        }
#ifndef NDEBUG
        assert(false);
#else
        throw std::runtime_error("Unhandled data type in packet impl size");
#endif
    }
    void clearData()
    {
        mInteger32Data.clear();
        mInteger64Data.clear();
        mFloatData.clear();
        mDoubleData.clear();
        mDataType = Packet::DataType::Unknown;
    }
    void setData(std::vector<int> &&data)
    {
        if (data.empty()){return;}
        clearData();
        mInteger32Data = std::move(data);
        mDataType = Packet::DataType::Integer32;
        updateEndTime();
    }
    void setData(std::vector<float> &&data)
    {
        if (data.empty()){return;}
        clearData();
        mFloatData = std::move(data);
        mDataType = Packet::DataType::Float;
        updateEndTime();
    }
    void setData(std::vector<double> &&data)
    {
        if (data.empty()){return;}
        clearData();
        mDoubleData = std::move(data);
        mDataType = Packet::DataType::Double;
        updateEndTime();
    }
    void setData(std::vector<int64_t> &&data)
    {
        if (data.empty()){return;}
        clearData();
        mInteger64Data = std::move(data);
        mDataType = Packet::DataType::Integer64;
        updateEndTime();
    }
    void updateEndTime()
    {
        mEndTimeMicroSeconds = mStartTimeMicroSeconds;
        auto nSamples = size();  
        if (nSamples > 0 && mSamplingRate > 0)
        {
            auto traceDuration
                = std::round( ((nSamples - 1)/mSamplingRate)*1000000 );
            auto iTraceDuration = static_cast<int64_t> (traceDuration);
            std::chrono::microseconds traceDurationMuS{iTraceDuration};
            mEndTimeMicroSeconds = mStartTimeMicroSeconds + traceDurationMuS;
        }
    }
    StreamIdentifier mIdentifier;
    std::vector<int> mInteger32Data;
    std::vector<int64_t> mInteger64Data;
    std::vector<float> mFloatData;
    std::vector<double> mDoubleData;
    std::chrono::microseconds mStartTimeMicroSeconds{0};
    std::chrono::microseconds mEndTimeMicroSeconds{0};
    double mSamplingRate{0};
    Packet::DataType mDataType{Packet::DataType::Unknown};
    bool mHasIdentifier = false;
};

/// Clear class
void Packet::clear() noexcept
{
    pImpl->clearData();
    pImpl->mIdentifier.clear();
    pImpl->mHasIdentifier = false;
    constexpr std::chrono::microseconds zeroMuS{0};
    pImpl->mStartTimeMicroSeconds = zeroMuS;
    pImpl->mEndTimeMicroSeconds = zeroMuS;
    pImpl->mSamplingRate = 0;
}

/// Constructor
Packet::Packet() :
    pImpl(std::make_unique<PacketImpl> ())
{
}

/// Copy constructor
Packet::Packet(const Packet &packet)
{
    *this = packet;
}

/// Move constructor
Packet::Packet(Packet &&packet) noexcept
{
    *this = std::move(packet);
}

/// Construct from protobuf
Packet::Packet(const UDataPacketImport::GRPC::Packet &packet)
{
    Packet temp;

    StreamIdentifier streamIdentifier{packet.stream_identifier()};
    temp.setStreamIdentifier(std::move(streamIdentifier));
    temp.setSamplingRate(packet.sampling_rate());
    temp.setStartTime(std::chrono::microseconds {packet.start_time_mus()});

    auto dataType = packet.data_type();
    if (dataType == UDataPacketImport::GRPC::Packet_DataType_Integer32 &&
        packet.data32i().size() > 0)
    {
        std::vector<int> work;
        work.reserve(packet.data32i().size());
        for (const auto &v : packet.data32i()){work.push_back(v);}
        temp.setData(std::move(work));
    }
    else if (dataType == UDataPacketImport::GRPC::Packet_DataType_Double &&
             packet.data64f().size() > 0)
    {
        std::vector<double> work;
        for (const auto &v : packet.data64f()){work.push_back(v);}
        temp.setData(std::move(work));
    }
    else if (dataType == UDataPacketImport::GRPC::Packet_DataType_Integer64 &&
             packet.data64i().size())
    {
        std::vector<int64_t> work;
        for (const auto &v : packet.data64i()){work.push_back(v);}
        temp.setData(std::move(work));
    }
    else if (dataType == UDataPacketImport::GRPC::Packet_DataType_Float &&
             packet.data32f().size())
    {
        std::vector<float> work;
        for (const auto &v : packet.data32f()){work.push_back(v);}
        temp.setData(std::move(work));
    }

    *this = std::move(temp);
}

/// Copy assignment
Packet& Packet::operator=(const Packet &packet)
{
    if (&packet == this){return *this;}
    pImpl = std::make_unique<PacketImpl> (*packet.pImpl);
    return *this;
}

/// Move assignment
Packet& Packet::operator=(Packet &&packet) noexcept
{
    if (&packet == this){return *this;}
    pImpl = std::move(packet.pImpl);
    return *this;
}

/// Destructor
Packet::~Packet() = default;

/// Identifier
void Packet::setStreamIdentifier(const StreamIdentifier &identifier)
{
    StreamIdentifier copy{identifier};
    setStreamIdentifier(std::move(copy));
}

void Packet::setStreamIdentifier(StreamIdentifier &&identifier)
{
    if (!identifier.hasNetwork())
    { 
        throw std::invalid_argument("Network not set");
    }
    if (!identifier.hasStation())
    {
        throw std::invalid_argument("Station not set");
    }
    if (!identifier.hasChannel())
    {
        throw std::invalid_argument("Channel not set");
    }
    if (!identifier.hasLocationCode())
    {
        throw std::invalid_argument("Location code not set");
    }
    pImpl->mIdentifier = std::move(identifier);
    pImpl->mHasIdentifier = true;
}

const StreamIdentifier &Packet::getStreamIdentifierReference() const
{
    if (!hasStreamIdentifier()){throw std::runtime_error("Identifier not set");}
    return *&pImpl->mIdentifier;
}

StreamIdentifier Packet::getStreamIdentifier() const
{
    if (!hasStreamIdentifier()){throw std::runtime_error("Identifier not set");}
    return pImpl->mIdentifier;
}

bool Packet::hasStreamIdentifier() const noexcept
{
    return pImpl->mHasIdentifier;
}

/// Sampling rate
void Packet::setSamplingRate(const double samplingRate) 
{
    if (samplingRate <= 0)
    {
        throw std::invalid_argument("samplingRate = "
                                  + std::to_string(samplingRate)
                                  + " must be positive");
    }
    pImpl->mSamplingRate = samplingRate;
    pImpl->updateEndTime();
}

double Packet::getSamplingRate() const
{
    if (!hasSamplingRate()){throw std::runtime_error("Sampling rate not set");}
    return pImpl->mSamplingRate;
}

bool Packet::hasSamplingRate() const noexcept
{
    return (pImpl->mSamplingRate > 0);     
}

/// Number of samples
int Packet::getNumberOfSamples() const noexcept
{
    return pImpl->size();
}

/// Start time
void Packet::setStartTime(const double startTime) noexcept
{
    auto iStartTimeMuS = static_cast<int64_t> (std::round(startTime*1.e6));
    std::chrono::microseconds startTimeMuS{iStartTimeMuS};
    setStartTime(startTimeMuS);
}

void Packet::setStartTime(
    const std::chrono::microseconds &startTime) noexcept
{
    pImpl->mStartTimeMicroSeconds = startTime;
    pImpl->updateEndTime();
}

std::chrono::microseconds Packet::getStartTime() const noexcept
{
    return pImpl->mStartTimeMicroSeconds;
}

std::chrono::microseconds Packet::getEndTime() const
{
    if (!hasSamplingRate())
    {   
        throw std::runtime_error("Sampling rate not set");
    }   
    if (getNumberOfSamples() < 1)
    {   
        throw std::runtime_error("No samples in signal");
    }   
    return pImpl->mEndTimeMicroSeconds;
}

/// Sets the data
template<typename U>
void Packet::setData(std::vector<U> &&x)
{
    pImpl->setData(std::move(x));
    pImpl->updateEndTime();
}

template<typename U>
void Packet::setData(const std::vector<U> &x)
{
    auto xWork = x;
    setData(std::move(xWork));
}

template<typename U>
void Packet::setData(const int nSamples, const U *x)
{
    // Invalid
    if (nSamples < 0){throw std::invalid_argument("nSamples not positive");}
    if (x == nullptr){throw std::invalid_argument("x is NULL");}
    std::vector<U> data(nSamples);
    std::copy(x, x + nSamples, data.begin());
    setData(std::move(data));
}

/// Gets the data
template<typename U>
std::vector<U> Packet::getData() const noexcept
{
    std::vector<U> result;
    auto nSamples = getNumberOfSamples();
    if (nSamples < 1){return result;}
    result.resize(nSamples);
    auto dataType = getDataType();
    if (dataType == DataType::Integer32)
    {   
        std::copy(pImpl->mInteger32Data.begin(),
                  pImpl->mInteger32Data.end(),
                  result.begin());
    }   
    else if (dataType == DataType::Float)
    {   
        std::copy(pImpl->mFloatData.begin(),
                  pImpl->mFloatData.end(),
                  result.begin());
    }   
    else if (dataType == DataType::Double)
    {   
        std::copy(pImpl->mDoubleData.begin(),
                  pImpl->mDoubleData.end(),
                  result.begin());
    }   
    else if (dataType == DataType::Integer64)
    {   
        std::copy(pImpl->mInteger64Data.begin(),
                  pImpl->mInteger64Data.end(),
                  result.begin());
    }   
    else
    {   
#ifndef NDEBUG
        assert(false);
#endif
        constexpr U zero{0};
        std::fill(result.begin(), result.end(), zero); 
    }   
    return result;
}

const void* Packet::getDataPointer() const noexcept
{
    if (getNumberOfSamples() < 1){return nullptr;}
    auto dataType = getDataType();
    if (dataType == DataType::Integer32)
    {   
        return pImpl->mInteger32Data.data();
    }   
    else if (dataType == DataType::Float)
    {   
        return pImpl->mFloatData.data();
    }   
    else if (dataType == DataType::Double)
    {   
        return pImpl->mDoubleData.data();
    }   
    else if (dataType == DataType::Integer64)
    {   
        return pImpl->mInteger64Data.data();
    }   
    else if (dataType  == DataType::Unknown)
    {   
        return nullptr;
    }   
#ifndef NDEBUG
    else 
    {   
        assert(false);
    }   
#endif
    return nullptr;
}

/// Data type
Packet::DataType Packet::getDataType() const noexcept
{
    return pImpl->mDataType;
}

/// Protobuf
UDataPacketImport::GRPC::Packet Packet::toProtobuf() const
{
    if (!hasStreamIdentifier()){throw std::runtime_error("Identifier not set");}
    UDataPacketImport::GRPC::StreamIdentifier identifier;
    identifier.set_network(pImpl->mIdentifier.getNetwork());
    identifier.set_station(pImpl->mIdentifier.getStation());
    identifier.set_channel(pImpl->mIdentifier.getChannel());
    identifier.set_location_code(pImpl->mIdentifier.getLocationCode());

    UDataPacketImport::GRPC::Packet packet;
    *packet.mutable_stream_identifier() = std::move(identifier);
 
    packet.set_start_time_mus(getStartTime().count());
    packet.set_sampling_rate(getSamplingRate());

    packet.set_data_type(
        UDataPacketImport::GRPC::Packet_DataType_Unknown);

    auto nSamples = getNumberOfSamples();
    if (nSamples > 0)
    {
        auto dataType = getDataType();
        if (dataType == Packet::DataType::Integer32)
        {
            packet.set_data_type(
                UDataPacketImport::GRPC::Packet_DataType_Integer32);
            *packet.mutable_data32i() = {pImpl->mInteger32Data.begin(),
                                         pImpl->mInteger32Data.end()};
        }
        else if (dataType == Packet::DataType::Double)
        {
            packet.set_data_type(
                UDataPacketImport::GRPC::Packet_DataType_Double);
            *packet.mutable_data64f() = {pImpl->mDoubleData.begin(),
                                         pImpl->mDoubleData.end()};
        }
        else if (dataType == Packet::DataType::Float)
        {
            packet.set_data_type(
                UDataPacketImport::GRPC::Packet_DataType_Float);
            *packet.mutable_data32f() = {pImpl->mFloatData.begin(),
                                         pImpl->mFloatData.end()};
        }
        else if (dataType == Packet::DataType::Integer64)
        {
            packet.set_data_type(
                UDataPacketImport::GRPC::Packet_DataType_Integer64);
            *packet.mutable_data64i() = {pImpl->mInteger64Data.begin(),
                                         pImpl->mInteger64Data.end()};
        }
        else
        {
#ifndef NDEBUG
            assert(false);
#endif
            packet.set_data_type(
                UDataPacketImport::GRPC::Packet_DataType_Unknown);
        } 
    }

    return packet; 
}

std::chrono::microseconds UDataPacketImport::getEndTime(
    const UDataPacketImport::GRPC::Packet &packet)
{
    auto startTimeMuS = packet.start_time_mus();
    auto samplingRate = packet.sampling_rate();
    if (samplingRate <= 0)
    {
        throw std::invalid_argument("Sampling rate must be positive");
    }
    auto samplingPeriodMuS
        = static_cast<int64_t> (std::round(1000000/samplingRate));
    auto dataType = packet.data_type();
    int nSamples{0};
    if (dataType == UDataPacketImport::GRPC::Packet_DataType_Integer32)
    {
        nSamples = static_cast<int> (packet.data32i().size());
    }
    else if (dataType == UDataPacketImport::GRPC::Packet_DataType_Integer64)
    {
        nSamples = static_cast<int> (packet.data64i().size());
    }
    else if (dataType == UDataPacketImport::GRPC::Packet_DataType_Double)
    {
        nSamples = static_cast<int> (packet.data64f().size());
    }
    else if (dataType == UDataPacketImport::GRPC::Packet_DataType_Float)
    {
        nSamples = static_cast<int> (packet.data32f().size());
    }
    else
    {
        throw std::invalid_argument("No data type");
    }
    if (nSamples < 1){throw std::invalid_argument("No data");}
    auto endTimeMuS = startTimeMuS + (nSamples - 1)*samplingPeriodMuS;
    return std::chrono::microseconds {endTimeMuS};
}

///--------------------------------------------------------------------------///
///                               Template Instantiation                     ///
///--------------------------------------------------------------------------///
template void UDataPacketImport::Packet::setData(const std::vector<double> &);
template void UDataPacketImport::Packet::setData(const std::vector<float> &);
template void UDataPacketImport::Packet::setData(const std::vector<int> &);
template void UDataPacketImport::Packet::setData(const std::vector<int64_t> &);

template void UDataPacketImport::Packet::setData(std::vector<double> &&);
template void UDataPacketImport::Packet::setData(std::vector<float> &&);
template void UDataPacketImport::Packet::setData(std::vector<int> &&);
template void UDataPacketImport::Packet::setData(std::vector<int64_t> &&);

template void UDataPacketImport::Packet::setData(const int, const double *);
template void UDataPacketImport::Packet::setData(const int, const float *);
template void UDataPacketImport::Packet::setData(const int, const int *);
template void UDataPacketImport::Packet::setData(const int, const int64_t *);

template std::vector<int> UDataPacketImport::Packet::getData<int> () const noexcept;
template std::vector<double> UDataPacketImport::Packet::getData<double> () const noexcept;
template std::vector<float> UDataPacketImport::Packet::getData<float> () const noexcept;
template std::vector<int64_t> UDataPacketImport::Packet::getData<int64_t> () const noexcept;

