#ifndef UDATA_PACKET_IMPORT_SANITIZER_EXPIRED_PACKET_DETECTOR_HPP
#define UDATA_PACKET_IMPORT_SANITIZER_EXPIRED_PACKET_DETECTOR_HPP
#include <chrono>
#include <string>
#include <memory>
namespace UDataPacketImport
{
 class Packet;
}
namespace UDataPacketImport::GRPC
{
 class Packet;
}
namespace UDataPacketImport::Sanitizer
{

class ExpiredPacketDetectorOptions
{
public:
    /// @brief Constructor.
    ExpiredPacketDetectorOptions();
    /// @brief Copy constructor.
    ExpiredPacketDetectorOptions(const ExpiredPacketDetectorOptions &options);
    /// @brief Move constructor.
    ExpiredPacketDetectorOptions(ExpiredPacketDetectorOptions &&options) noexcept;

    /// @result If any sample in the packet has a time that precees the current
    ///         time minus getMaxExpiredTime() then the packet is rejected.
    [[nodiscard]] std::chrono::microseconds getMaxExpiredTime() const noexcept;
   
    /// @result Data streams appearing to have expired data are logged at this
    ///         interval.
    [[nodiscard]] std::chrono::seconds getLogBadDataInterval() const noexcept;
 
    /// @brief Destructor.
    ~ExpiredPacketDetectorOptions();

    /// @brief Copy assignment.
    ExpiredPacketDetectorOptions& operator=(const ExpiredPacketDetectorOptions &options);
    /// @brief Move constructor.
    ExpiredPacketDetectorOptions& operator=(ExpiredPacketDetectorOptions &&options) noexcept;
private:
    class ExpiredPacketDetectorOptionsImpl;
    std::unique_ptr<ExpiredPacketDetectorOptionsImpl> pImpl;
};
}
namespace UDataPacketImport::Sanitizer
{
/// @class ExpiredPacketDetector futurePacketDetector.hpp
/// @brief Tests whether or not a packet contains data from the future.  This
///        indicates that there is a timing error.
/// @copyright Ben Baker (University of Utah) distributed under the MIT NO AI
///            license.
class ExpiredPacketDetector
{
public:
    /// @brief Constructs the expired data detector.
    explicit ExpiredPacketDetector(const ExpiredPacketDetectorOptions &options);
    /// @brief Copy constructor.
    ExpiredPacketDetector(const ExpiredPacketDetector &detector);
    /// @brief Move constructor.
    ExpiredPacketDetector(ExpiredPacketDetector &&detector) noexcept;

    /// @param[in] packet  The protobuf representation of a data packet.
    /// @result True indicates the data packet does not appear to have any expired data.
    [[nodiscard]] bool allow(const UDataPacketImport::GRPC::Packet &packet) const;
    /// @param[in] packet  The client library representation of a data packet.
    /// @result True indicates the data packet does not appear to have any expired data.
    [[nodiscard]] bool allow(const UDataPacketImport::Packet &packet) const;

    /// @result True indicates the data packet does not appear to have any expired data.
    [[nodiscard]] bool operator()(const UDataPacketImport::GRPC::Packet &packet) const;
    /// @result True indicates the data packet does not appear to have any expired data.
    [[nodiscard]] bool operator()(const UDataPacketImport::Packet &packet) const;

    /// @brief Destructor.
    ~ExpiredPacketDetector();
    /// @brief Copy assignment.
    ExpiredPacketDetector& operator=(const ExpiredPacketDetector &detector);
    /// @brief Move constructor.
    ExpiredPacketDetector& operator=(ExpiredPacketDetector &&detector) noexcept;

    ExpiredPacketDetector() = delete;
private:
    class ExpiredPacketDetectorImpl;
    std::unique_ptr<ExpiredPacketDetectorImpl> pImpl;
};
}
#endif
