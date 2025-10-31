#ifndef UDATA_PACKET_IMPORT_GRPC_STREAM_HPP
#define UDATA_PACKET_IMPORT_GRPC_STREAM_HPP
#include <string>
#include <string_view>
#include <optional>
#include <memory>
namespace UDataPacketImport::GRPC
{
 class Packet;
 class StreamOptions;
}
namespace UDataPacketImport::GRPC
{
/// @brief A stream is the product of data from a given channel.  In the context
///        of an observer pattern this is the subject.  In this case a 
///        client (un)subscribes to a stream and actively fetches new data.
///        Conversely, a publisher adds the packet to the stream.  This
///        implementation enables this with a queue.
class Stream
{
public:
    /// @brief Creates a stream from the given packet.
    /// @param[in] packet   The packet from which to create this stream.
    Stream(UDataPacketImport::GRPC::Packet &&packet,
           const StreamOptions &options);

    /// @result The stream identifier.
    [[nodiscard]] const std::string_view getIdentifier() const noexcept;

    /// @brief Sets the latest packet.
    void setLatestPacket(UDataPacketImport::GRPC::Packet &&packet);
    /// @brief Sets the latest packet.
    void setLatestPacket(const UDataPacketImport::GRPC::Packet &packet);

    /// @brief Gets the next packet from the stream.
    [[nodiscard]] std::optional<UDataPacketImport::GRPC::Packet>
         getNextPacket(grpc::CallbackServerContext *context) const noexcept;

    /// @brief Subscribes to the stream.
    void subscribe(grpc::CallbackServerContext *context);    

    /// @brief Unsubscribes from the stream.
    void unsubscribe(grpc::CallbackServerContext *context);

    /// @brief Unsubscribes all from the stream.
    void unsubscribeAll();

    /// @brief Destructor.
    ~Stream();

    Stream() = delete;
    Stream(const Stream &) = delete;
    Stream(Stream &&stream) noexcept = delete;
    Stream& operator=(const Stream &) = delete;
    Stream& operator=(Stream &&) noexcept = delete;
private:
    class StreamImpl;
    std::unique_ptr<StreamImpl> pImpl;
};
[[nodiscard]] bool operator<(const Stream &lhs, const Stream &rhs);
}
#endif
