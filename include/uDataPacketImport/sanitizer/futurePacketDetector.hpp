#ifndef UDATA_PACKET_IMPORT_SANITIZER_FUTURE_PACKET_DETECTOR_HPP
#define UDATA_PACKET_IMPORT_SANITIZER_FUTURE_PACKET_DETECTOR_HPP
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

class FuturePacketDetectorOptions
{
public:
    /// @brief Constructor.
    FuturePacketDetectorOptions();
    /// @brief Copy constructor.
    FuturePacketDetectorOptions(const FuturePacketDetectorOptions &options);
    /// @brief Move constructor.
    FuturePacketDetectorOptions(FuturePacketDetectorOptions &&options) noexcept;

    /// @result If any sample in the packet has a time that exceeds the current
    ///         time plus getMaxFutureTime() then the packet is rejected.
    [[nodiscard]] std::chrono::microseconds getMaxFutureTime() const noexcept;
   
    /// @result Data streams appearing to have future data are logged at this
    ///         interval.
    [[nodiscard]] std::chrono::seconds getLogBadDataInterval() const noexcept;
 
    /// @brief Destructor.
    ~FuturePacketDetectorOptions();

    /// @brief Copy assignment.
    FuturePacketDetectorOptions& operator=(const FuturePacketDetectorOptions &options);
    /// @brief Move constructor.
    FuturePacketDetectorOptions& operator=(FuturePacketDetectorOptions &&options) noexcept;
private:
    class FuturePacketDetectorOptionsImpl;
    std::unique_ptr<FuturePacketDetectorOptionsImpl> pImpl;
};
}
namespace UDataPacketImport::Sanitizer
{
/// @class FuturePacketDetector futurePacketDetector.hpp
/// @brief Tests whether or not a packet contains data from the future.  This
///        indicates that there is a timing error.
/// @copyright Ben Baker (University of Utah) distributed under the MIT NO AI
///            license.
class FuturePacketDetector
{
public:
    /// @brief Constructs the future data detector.
    explicit FuturePacketDetector(const FuturePacketDetectorOptions &options);
    /// @brief Copy constructor.
    FuturePacketDetector(const FuturePacketDetector &detector);
    /// @brief Move constructor.
    FuturePacketDetector(FuturePacketDetector &&detector) noexcept;

    /// @param[in] packet  The protobuf representation of a data packet.
    /// @result True indicates the data packet does not appear to have any future data.
    [[nodiscard]] bool allow(const UDataPacketImport::GRPC::Packet &packet) const;
    /// @param[in] packet  The client library representation of a data packet.
    /// @result True indicates the data packet does not appear to have any future data.
    [[nodiscard]] bool allow(const UDataPacketImport::Packet &packet) const;

    /// @result True indicates the data packet does not appear to have any future data.
    [[nodiscard]] bool operator()(const UDataPacketImport::GRPC::Packet &packet) const;
    /// @result True indicates the data packet does not appear to have any future data.
    [[nodiscard]] bool operator()(const UDataPacketImport::Packet &packet) const;

    /// @brief Destructor.
    ~FuturePacketDetector();
    /// @brief Copy assignment.
    FuturePacketDetector& operator=(const FuturePacketDetector &detector);
    /// @brief Move constructor.
    FuturePacketDetector& operator=(FuturePacketDetector &&detector) noexcept;

    FuturePacketDetector() = delete;
private:
    class FuturePacketDetectorImpl;
    std::unique_ptr<FuturePacketDetectorImpl> pImpl;
};
}
#endif
