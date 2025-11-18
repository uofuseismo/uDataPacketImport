#ifndef UDATA_PACKET_IMPORT_SANITIZER_DUPLICATE_PACKET_DETECTOR_HPP
#define UDATA_PACKET_IMPORT_SANITIZER_DUPLICATE_PACKET_DETECTOR_HPP
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

/// @class DuplicatePacketDetectorOptions duplicatePacketDetectorOptions.hpp
/// @brief Tests whether or not a packet has already passed through the system.
/// @copyright Ben Baker (University of Utah) distributed under the MIT NO AI
///            license.
class DuplicatePacketDetectorOptions
{
public:
    /// @brief Constructor.
    DuplicatePacketDetectorOptions();
    /// @brief Copy constructor.
    DuplicatePacketDetectorOptions(const DuplicatePacketDetectorOptions &options);
    /// @brief Move constructor.
    DuplicatePacketDetectorOptions(DuplicatePacketDetectorOptions &&options) noexcept;

    /// @brief Sets the number of packets in the circular buffer.
    /// @param[in] circularBufferSize  The number of packets in the circular
    ///                                buffer.
    /// @throws std::invalid_argument if this is not positive.
    void setCircularBufferSize(int circularBufferSize);
    /// @result The number of packets in the circular buffer.
    [[nodiscard]] std::optional<int> getCircularBufferSize() const noexcept;

    /// @brief Sets the approximate duration of the circular buffer.
    /// @param[in] circularBufferDuration  The approximate duration of the
    ///                                    circular buffer.
    /// @throws std::invalid_argument if this is not positive.
    void setCircularBufferDuration(const std::chrono::seconds &circularBufferDuration);
    /// @result The approximate circular buffer expressed as a duration.
    [[nodiscard]] std::optional<std::chrono::seconds> getCircularBufferDuration() const noexcept;
   
    /// @brief Sets the interval at which to log expired data.
    /// @param[in] logInterval  The interval at which to log data. 
    /// @note Setting this to a negative value disables logging.
    void setLogBadDataInterval(const std::chrono::seconds &logInterval) noexcept;
    /// @result Data streams appearing to have expired data are logged at this
    ///         interval.  By default bad data is logged every hour.
    [[nodiscard]] std::chrono::seconds getLogBadDataInterval() const noexcept;
 
    /// @brief Destructor.
    ~DuplicatePacketDetectorOptions();

    /// @brief Copy assignment.
    DuplicatePacketDetectorOptions& operator=(const DuplicatePacketDetectorOptions &options);
    /// @brief Move constructor.
    DuplicatePacketDetectorOptions& operator=(DuplicatePacketDetectorOptions &&options) noexcept;
private:
    class DuplicatePacketDetectorOptionsImpl;
    std::unique_ptr<DuplicatePacketDetectorOptionsImpl> pImpl;
};
/// @brief Tests whether or not this packet may have been previously processed
///        This works by comparing the packet's header (start and end time)
///        to previous packets collected in a circular buffer.  Additionally,
///        it can detect GPS slips.  For example, if an older packet arrives
///        with times contained between earlier process packets then it is also
///        rejected.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class DuplicatePacketDetector
{
public:
    /// @brief Constructs a duplicate packet checker. 
    explicit DuplicatePacketDetector(
        const DuplicatePacketDetectorOptions &options);
    /// @brief Copy constructor.
    DuplicatePacketDetector(const DuplicatePacketDetector &detector);
    /// @brief Move constructor.
    DuplicatePacketDetector(DuplicatePacketDetector &&detector) noexcept;

    /// @param[in] packet   The packet to test.
    /// @result True indicates the data does not appear to be a duplicate.
    [[nodiscard]] bool allow(const UDataPacketImport::GRPC::Packet &packet) const;
    /// @param[in] packet   The packet to test.
    /// @result True indicates the data does not appear to be a duplicate.
    [[nodiscard]] bool allow(const UDataPacketImport::Packet &packet) const;

    /// @result True indicates the data does not appear to be a duplicate.
    [[nodiscard]] bool operator()(const UDataPacketImport::GRPC::Packet &packet) const;
    /// @result True indicates the data does not appear to be a duplicate.
    [[nodiscard]] bool operator()(const UDataPacketImport::Packet &packet) const;

    /// @brief Destructor.
    ~DuplicatePacketDetector();
    /// @brief Copy assignment.
    DuplicatePacketDetector& operator=(const DuplicatePacketDetector &detector);
    /// @brief Move constructor.
    DuplicatePacketDetector& operator=(DuplicatePacketDetector &&detector) noexcept;
private:
    class DuplicatePacketDetectorImpl;
    std::unique_ptr<DuplicatePacketDetectorImpl> pImpl;
};
}
#endif
