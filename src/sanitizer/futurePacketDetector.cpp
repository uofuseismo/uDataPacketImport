#include <iostream>
#include <string>
#include <chrono>
#include <mutex>
#include <set>
#include <spdlog/spdlog.h>
#include "uDataPacketImport/sanitizer/futurePacketDetector.hpp"
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/dataPacketBroadcast.grpc.pb.h"
#include "src/getNow.hpp"
//#include "toName.hpp"

using namespace UDataPacketImport::Sanitizer;

namespace
{

[[nodiscard]] std::string toName(const UDataPacketImport::Packet &packet)
{
    auto name = packet.getStreamIdentifierReference().toString(); 
    return name;
}

[[nodiscard]] std::string toName(const UDataPacketImport::GRPC::Packet &packet)
{
    UDataPacketImport::StreamIdentifier identifier{packet.stream_identifier()};
    return identifier.toString();
}

}

class FuturePacketDetectorOptions::FuturePacketDetectorOptionsImpl
{
public:
    std::chrono::microseconds mMaxFutureTime{0};
    std::chrono::seconds mLogBadDataInterval{3600};
};

/// Constructor
FuturePacketDetectorOptions::FuturePacketDetectorOptions() :
    pImpl(std::make_unique<FuturePacketDetectorOptionsImpl> ())
{
}

/// Copy constructor
FuturePacketDetectorOptions::FuturePacketDetectorOptions(
    const FuturePacketDetectorOptions &options)
{
    *this = options;
}

/// Copy assignment
FuturePacketDetectorOptions&
FuturePacketDetectorOptions::operator=(
    const FuturePacketDetectorOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<FuturePacketDetectorOptionsImpl> (*options.pImpl);
    return *this;
}
 
/// Move assignment
FuturePacketDetectorOptions&
FuturePacketDetectorOptions::operator=(
    FuturePacketDetectorOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Max future time
void FuturePacketDetectorOptions::setMaxFutureTime(
    const std::chrono::microseconds &duration)
{
    if (duration.count() < 0)
    {
        spdlog::warn("Future time is negative");
    } 
    pImpl->mMaxFutureTime = duration;
}

std::chrono::microseconds
    FuturePacketDetectorOptions::getMaxFutureTime() const noexcept
{
    return pImpl->mMaxFutureTime;
}

/// Logging interval
void FuturePacketDetectorOptions::setLogBadDataInterval(
    const std::chrono::seconds &interval) noexcept
{
    pImpl->mLogBadDataInterval = interval;
    if (interval.count() < 0)
    {
        pImpl->mLogBadDataInterval = std::chrono::seconds {-1};
    }
}

std::chrono::seconds
    FuturePacketDetectorOptions::getLogBadDataInterval() const noexcept
{
    return pImpl->mLogBadDataInterval;
}

/// Destructor
FuturePacketDetectorOptions::~FuturePacketDetectorOptions() = default;

///--------------------------------------------------------------------------///

class FuturePacketDetector::FuturePacketDetectorImpl
{
public:
    FuturePacketDetectorImpl(const FuturePacketDetectorImpl &impl)
    {
        *this = impl;
    }
    explicit FuturePacketDetectorImpl(const FuturePacketDetectorOptions &options) :
        mOptions(options),
        mMaxFutureTime(options.getMaxFutureTime()),
        mLogBadDataInterval(options.getLogBadDataInterval())
    {
        // This might be okay if you really want to account for telemetry
        // lags.  But that's a dangerous game so I'll let the user know.
        //if (mMaxFutureTime.count() < 0)
        //{
        //    spdlog::warn("Max future time is negative");
        //}
        if (mLogBadDataInterval.count() >= 0)
        {
            mLogBadData = true;
        }
        else
        {
            mLogBadData = false;
        }
    }
    /// Checks the packet
    template<typename U>
    [[nodiscard]] bool allow(const U &packet)
    {
        std::chrono::microseconds packetEndTime;
        if constexpr (std::is_same<UDataPacketImport::Packet, U>::value)
        {
            packetEndTime = packet.getEndTime(); // Throws
        }
        else if constexpr (std::is_same<UDataPacketImport::GRPC::Packet, U>::value)
        {
            packetEndTime = UDataPacketImport::getEndTime(packet); // Throws
        }
        else
        {
#ifndef NDEBUG
            assert(false);
#else
            spdlog::critical("Unhandled template");
            throw std::runtime_error("Unhandled template");
#endif
        }
        // Computing the current time after the scraping the ring is
        // conservative.  Basically, when the max future time is zero,
        // this allows for a zero-latency, 1 sample packet, to be
        // successfully passed through.
        auto nowMuSeconds = ::getNow();
        auto latestTime  = nowMuSeconds + mMaxFutureTime;
        // Packet contains data after max allowable time?
        bool allow = (packetEndTime <= latestTime) ? true : false;
        // (Safely) handle logging
        try
        {
            logBadData(allow, packet, nowMuSeconds);
        }
        catch (const std::exception &e)
        {
            spdlog::warn("Error detect in logBadData: "
                       + std::string {e.what()});
        }
        return allow;
    }
    /// Logs the bad packets
    template<typename U>
    void logBadData(const bool allow,
                    const U &packet,
                    const std::chrono::microseconds &nowMuSec)
    {
        if (!mLogBadData){return;}
        std::string name;
        try
        {
            if (!allow){name = ::toName(packet);}
        }
        catch (...)
        {
            spdlog::warn("Could not extract name of packet");
        }
        auto nowSeconds
            = std::chrono::duration_cast<std::chrono::seconds> (nowMuSec);
        {
        std::lock_guard<std::mutex> lockGuard(mMutex); 
        try
        {
            if (!name.empty() && !mFutureChannels.contains(name))
            {
                mFutureChannels.insert(name);
            }
        }
        catch (...)
        {
            spdlog::warn("Failed to add " + name + " to set");
        }
        if (nowSeconds >= mLastLogTime + mLogBadDataInterval)
        {
            if (!mFutureChannels.empty())
            {
                std::string message{"Future data detected for:"};
                for (const auto &channel : mFutureChannels)
                {
                    message = message + " " + channel;
                }
                spdlog::info(message);
                mFutureChannels.clear();
                mLastLogTime = nowSeconds;
            }
        }
        }
    }
    FuturePacketDetectorImpl& operator=(const FuturePacketDetectorImpl &impl)
    {
        if (&impl == this){return *this;}
        {
        std::lock_guard<std::mutex> lockGuard(impl.mMutex);
        mFutureChannels = impl.mFutureChannels;
        mLastLogTime = impl.mLastLogTime; 
        }
        mOptions = impl.mOptions;
        mMaxFutureTime = impl.mMaxFutureTime;
        mLogBadDataInterval = impl.mLogBadDataInterval;
        mLogBadData = impl.mLogBadData;
        return *this;
    }
//private:
    mutable std::mutex mMutex;
    FuturePacketDetectorOptions mOptions;
    std::set<std::string> mFutureChannels;
    std::chrono::microseconds mMaxFutureTime{0};
    std::chrono::seconds mLastLogTime{0};
    std::chrono::seconds mLogBadDataInterval{3600};
    bool mLogBadData{true};
};

/// Constructor with options
FuturePacketDetector::FuturePacketDetector(
    const FuturePacketDetectorOptions &options) :
    pImpl(std::make_unique<FuturePacketDetectorImpl> (options))
{
}

/// Copy constructor
FuturePacketDetector::FuturePacketDetector(
    const FuturePacketDetector &testFutureDataPacket)
{
    *this = testFutureDataPacket;
}

/// Move constructor
FuturePacketDetector::FuturePacketDetector(
    FuturePacketDetector &&testFutureDataPacket) noexcept
{
    *this = std::move(testFutureDataPacket);
}

/// Copy assignment
FuturePacketDetector& 
FuturePacketDetector::operator=(const FuturePacketDetector &detector)
{
    if (&detector == this){return *this;}
    pImpl = std::make_unique<FuturePacketDetectorImpl> (*detector.pImpl);
    return *this;
}

/// Move assignment
FuturePacketDetector&
FuturePacketDetector::operator=(
    FuturePacketDetector &&detector) noexcept
{
    if (&detector == this){return *this;}
    pImpl = std::move(detector.pImpl);
    return *this;
}

/// Destructor
FuturePacketDetector::~FuturePacketDetector() = default;

/// Allow future packet?
bool FuturePacketDetector::allow(
    const UDataPacketImport::Packet &packet) const
{
    return pImpl->allow(packet);
}

bool FuturePacketDetector::allow(
    const UDataPacketImport::GRPC::Packet &packet) const
{
    return pImpl->allow(packet);
}

bool FuturePacketDetector::operator()(
    const UDataPacketImport::GRPC::Packet &packet) const
{
    return allow(packet);
}

bool FuturePacketDetector::operator()(
    const UDataPacketImport::Packet &packet) const
{
    return allow(packet);
}

