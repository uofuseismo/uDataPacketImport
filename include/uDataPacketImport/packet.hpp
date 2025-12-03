#ifndef UDATA_PACKET_IMPORT_PACKET_HPP
#define UDATA_PACKET_IMPORT_PACKET_HPP
#include <vector>
#include <chrono>
#include <memory>
namespace UDataPacketImport::GRPC::V1
{
  class Packet;
}
namespace UDataPacketImport
{
  class StreamIdentifier;
}
namespace UDataPacketImport
{
class Packet
{
public:
    /// @brief Defines the data type.
    enum class DataType
    {   
        Integer32,
        Integer64,
        Float,
        Double,
        Unknown
    };  
public:
    /// @name Constructors
    /// @{

    /// @brief Constructor.
    Packet();
    /// @brief Copy constructor.
    /// @param[in] packet  The data packet from which to initialize this class.
    Packet(const Packet &packet);
    /// @brief Move constructor.
    /// @param[in,out] packet  The data packet from which to initialize this
    ///                        class.  On exit, packet's behavior is undefined.
    Packet(Packet &&packet) noexcept;
    /// @brief Constructs class from a protobuf message.
    /// @param[in] packet  A protobuf representation of hte packet from which
    ///                    to construct this class.
    explicit Packet(const UDataPacketImport::GRPC::V1::Packet &packet);
    /// @}

    /// @brief Sets the stream identifier.
    /// @param[in,out] identifier  The stream identifier.  On exit, identifier's
    ///                            behavior is undefined.
    /// @throws std::invalid_argument if the network, station, channel, or
    ///         location code is not set.
    void setStreamIdentifier(StreamIdentifier &&identifier);
    /// @brief Sets the stream identifier.
    /// @param[in] identifier  The stream identifier.
    /// @throws std::invalid_argument if the network, station, channel, or
    ///         location code is not set.
    void setStreamIdentifier(const StreamIdentifier &identifier);
    /// @result The stream identifier.
    /// @throws std::runtime_error if \c hasIdentifier() is false.
    [[nodiscard]] StreamIdentifier getStreamIdentifier() const;
    /// @result A reference to the stream identifier.
    /// @throws std::runtime_error if \c hasIdentifier() is false.
    [[nodiscard]] const StreamIdentifier &getStreamIdentifierReference() const;
    /// @result True indicates that the identifier was set.
    [[nodiscard]] bool hasStreamIdentifier() const noexcept;

    /// @brief Sets the station name.
    /// @param[in] station   The station name.
    /// @throws std::invalid_argument if station is empty.
    void setStation(const std::string &station);
    /// @result The station name.
    /// @throws std::runtime_error if \c hasStation() is false.
    [[nodiscard]] std::string getStation() const;
    /// @result True indicates that the station name was set.
    [[nodiscard]] bool hasStation() const noexcept;

    /// @brief Sets the channel name.
    /// @param[in] channel  The channel name.
    /// @throws std::invalid_argument if channel is empty.
    void setChannel(const std::string &channel);
    /// @result The channel name.
    /// @throws std::runtime_error if the channel was not set.
    [[nodiscard]] std::string getChannel() const;
    /// @result True indicates that the channel was set.
    [[nodiscard]] bool hasChannel() const noexcept;

    /// @brief Sets the location code.
    /// @param[in] locationCode  The location code.
    /// @throws std::invalid_argument if location is empty.
    void setLocationCode(const std::string &locationCode);
    /// @brief Sets the location code.
    /// @throws std::runtime_error if \c hasLocationCode() is false.
    [[nodiscard]] std::string getLocationCode() const;
    /// @result True indicates that the location code was set.
    [[nodiscard]] bool hasLocationCode() const noexcept;

    /// @brief Sets the sampling rate for data in the packet.
    /// @param[in] samplingRate  The sampling rate in Hz.
    /// @throws std::invalid_argument if samplingRate is not positive.
    void setSamplingRate(double samplingRate);
    /// @result The sampling rate of the packet in Hz.
    /// @throws std::runtime_error if \c hasSamplingRate() is false.
    [[nodiscard]] double getSamplingRate() const;
    /// @result True indicates that the sampling rate was set.
    [[nodiscard]] bool hasSamplingRate() const noexcept; 
    /// @}

    /// @name Optional Information
    /// @{

    /// @param[in] startTime  The UTC start time in seconds from the epoch
    ///                       (Jan 1, 1970).
    void setStartTime(double startTime) noexcept;
    /// @param[in] startTime  The UTC start time in microseconds
    ///                       from the epoch (Jan 1, 1970). 
    void setStartTime(const std::chrono::microseconds &startTime) noexcept;
    /// @result The UTC start time in microseconds from the epoch.
    [[nodiscard]] std::chrono::microseconds getStartTime() const noexcept;
    //[[nodiscard]] int64_t getStartTime() const noexcept;
    /// @result The UTC time in microseconds from the epoch of the last sample.
    /// @throws std::runtime_error if \c hasSamplingRate() is false or
    ///         \c getNumberOfSamples() is 0.
    [[nodiscard]] std::chrono::microseconds getEndTime() const;
    /// @}

    /// @name Data
    /// @{

    /// @brief Sets the time series data in this packet.
    /// @param[in,out] data  The time series data.  On exit, data's behavior
    ///                      is undefined.
    template<typename U> void setData(std::vector<U> &&data);
    /// @brief Sets the time series data in this packet.
    /// @param[in] data  The time series data.
    template<typename U> void setData(const std::vector<U> &data);
    /// @brief Sets the time series data in this packet.
    /// @param[in] nSamples  The nubmer of samples in the signal.
    /// @param[in] data      The time series data.  This is an array whose
    ///                      dimension is [nSamples].
    /// @throws std::invalid_argument if data is null.
    ///  
    template<typename U> void setData(int nSamples, const U *data);
    /// @result The time series currently set on the packet. 
    template<typename U>
    [[nodiscard]] std::vector<U> getData() const noexcept;
    /// @result The data type.
    [[nodiscard]] DataType getDataType() const noexcept;
    /// @result A reference to the time series currently set on the packet.
    [[nodiscard]] const std::vector<double> &getDataReference() const noexcept;
    /// @result A pointer to the underlying data packet.  This is an array whose
    ///         dimensions is [\c getNumberOfSamples()] 
    [[nodiscard]] const void *getDataPointer() const noexcept;
    /// @result The number of data samples in the packet.
    [[nodiscard]] int getNumberOfSamples() const noexcept;
    /// @}

    /// @result A protobuf representation of the data packet.
    /// @throws std::runtime_error if any required parameters are not set.
    [[nodiscard]] GRPC::V1::Packet toProtobuf() const;

    /// @name Destructors
    /// @{

    /// @brief Resets the class and releases all memory.
    void clear() noexcept;
    /// @brief Destructor.
    ~Packet();
    /// @}

    /// @name Operators
    /// @{

    /// @brief Copy assignment.
    /// @param[in] packet  The packet to copy to this class.
    /// @result A deep copy of the input packet.
    Packet& operator=(const Packet &packet);
    /// @brief Move assignment.
    /// @param[in,out] packet  The packet whose memory will be moved to
    ///                        this class.
    /// @result The memory from packet moved to this.
    Packet& operator=(Packet &&packet) noexcept; 
    /// @}

private:
    class PacketImpl;
    std::unique_ptr<PacketImpl> pImpl;
};
[[nodiscard]] std::chrono::microseconds getEndTime(const UDataPacketImport::GRPC::V1::Packet &packet);
}
#endif
