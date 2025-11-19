#ifndef UDATA_PACKET_IMPORT_STREAM_IDENTIFIER_HPP
#define UDATA_PACKET_IMPORT_STREAM_IDENTIFIER_HPP
#include <string_view>
#include <string>
#include <memory>
namespace UDataPacketImport::GRPC
{
 class StreamIdentifier;
}
namespace UDataPacketImport
{
class StreamIdentifier
{
public:
    /// @brief Constructor.
    StreamIdentifier();
    /// @brief Copy constructor.
    /// @param[in] identifier  The stream identifier from which to initialize
    ///                        this class.
    StreamIdentifier(const StreamIdentifier &identifier);
    /// @brief Move constructor.
    /// @param[in] identifier  The stream identifier from which to initialize
    ///                        this class.  On exit, identifier is undefined.
    StreamIdentifier(StreamIdentifier &&identifier) noexcept;
    /// @brief Constructs from a network, station, and channel name.
    /// @param[in] network  The network code - e.g, UU.
    /// @param[in] station  The station name - e.g., CTU.
    /// @param[in] channel  The channel code - e.g., HHZ.
    /// @param[in] locationCode   The location code - e.g., 01.
    StreamIdentifier(const std::string_view &network,
                     const std::string_view &station,
                     const std::string_view &channel,
                     const std::string_view &locationCode);
    /// @brief Constructs from a gRPC protobuf.
    /// @param[in] identifier  The protobuf stream identifier.
    explicit StreamIdentifier(const UDataPacketImport::GRPC::StreamIdentifier &identifier);

    /// @brief Sets the network code.
    /// @param[in] network  The network code.
    /// @throws std::invalid_argument if network is empty.
    void setNetwork(const std::string_view &network);
    /// @result The network code.
    /// @throws std::runtime_error if \c hasNetwork() is false.
    [[nodiscard]] std::string getNetwork() const;
    /// @result True indicates that the network was set.
    [[nodiscard]] bool hasNetwork() const noexcept;

    /// @brief Sets the station name.
    /// @param[in] station   The station name.
    /// @throws std::invalid_argument if station is empty.
    void setStation(const std::string_view &station);
    /// @result The station name.
    /// @throws std::runtime_error if \c hasStation() is false.
    [[nodiscard]] std::string getStation() const;
    /// @result True indicates that the station name was set.
    [[nodiscard]] bool hasStation() const noexcept;

    /// @brief Sets the channel name.
    /// @param[in] channel  The channel name.
    /// @throws std::invalid_argument if channel is empty.
    void setChannel(const std::string_view &channel);
    /// @result The channel name.
    /// @throws std::runtime_error if the channel was not set.
    [[nodiscard]] std::string getChannel() const;
    /// @result True indicates that the channel was set.
    [[nodiscard]] bool hasChannel() const noexcept;

    /// @brief Sets the location code.
    /// @param[in] locationCode  The location code.
    /// @throws std::invalid_argument if location is empty.
    void setLocationCode(const std::string_view &locationCode);
    /// @brief Sets the location code.
    /// @throws std::runtime_error if \c hasLocationCode() is false.
    [[nodiscard]] std::string getLocationCode() const;
    /// @result True indicates that the location code was set.
    [[nodiscard]] bool hasLocationCode() const noexcept;

    /// @result A string representation of the identifier encoded as
    /// @throws std::runtime_error if the network, station, channel, or location
    ///         code is not set.
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] const std::string &getStringReference() const;
    /// @result A string view representation of the identifier encoded as
    /// @throws std::runtime_error if the network, station, channel, or location
    ///         code is not set.
    /// @note You should prefer \c toString() as it is safer.
    [[nodiscard]] const std::string_view toStringView() const;

    /// @result Converts the class to a protobuf.
    [[nodiscard]] UDataPacketImport::GRPC::StreamIdentifier toProtobuf() const;

    /// @name Destructors
    /// @{

    /// @brief Resets the class.
    void clear() noexcept;
    /// @brief Destructor.
    ~StreamIdentifier();
    /// @}

    /// @name Operators
    /// @{

    /// @brief Copy assignment.
    /// @result A deep copy of the stream identifier.
    StreamIdentifier &operator=(const StreamIdentifier &identifier);
    /// @brief Move assignment.
    /// @result The identifier moved to this.
    StreamIdentifier &operator=(StreamIdentifier &&identifier) noexcept;
    /// @}
private:
    class StreamIdentifierImpl;
    std::unique_ptr<StreamIdentifierImpl> pImpl;
};
bool operator<(const StreamIdentifier &, const StreamIdentifier &rhs);
bool operator==(const StreamIdentifier &, const StreamIdentifier &rhs);
}
#endif
