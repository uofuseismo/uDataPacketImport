#ifndef UDATA_PACKET_IMPORT_GRPC_STREAM_HPP
#define UDATA_PACKET_IMPORT_GRPC_STREAM_HPP
#include <string>
#include <cstdint>
#include <string_view>
#include <optional>
#include <memory>
#include <set>
namespace UDataPacketImport::GRPC
{
 class Packet;
 class StreamIdentifier;
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
    /// @param[in,out] packet   The packet from which to create this stream.
    /// @param[in] options      The stream options.
    Stream(UDataPacketImport::GRPC::Packet &&packet,
           const StreamOptions &options);
    Stream(const UDataPacketImport::GRPC::Packet &packet,
           const StreamOptions &options);


    /// @result The stream identifier.
    [[nodiscard]] std::string getIdentifier() const noexcept;
    /// @result The stream identifier.
    [[nodiscard]] UDataPacketImport::GRPC::StreamIdentifier getStreamIdentifier() const;

    /// @brief Sets the latest packet.
    void setLatestPacket(UDataPacketImport::GRPC::Packet &&packet);
    /// @brief Sets the latest packet.
    void setLatestPacket(const UDataPacketImport::GRPC::Packet &packet);

    /// @brief Gets the next packet from the stream.
    [[nodiscard]] std::optional<UDataPacketImport::GRPC::Packet>
         getNextPacket(uintptr_t contextAddress) const noexcept;
    /// @result Gets the current number of subscribers.
    [[nodiscard]] int getNumberOfSubscribers() const noexcept;
    /// @result The current subscribers.
    [[nodiscard]] std::set<uintptr_t> getSubscribers() const noexcept;

    /// @brief Subscribes to a stream.
    /// @param[in] contextAddress  The memory address of the grpc context.
    /// @note The context address can be created using something like: 
    ///       grpc::CallbackServerContext *context;
    ///       auto contextAddress = reinterpret_cast<uintptr_t> (context);
    void subscribe(uintptr_t contextAddress);
    /// @result True indicates the context is subscribed.
    [[nodiscard]] bool isSubscribed(uintptr_t contextAddress) const noexcept;

    /// @brief Unsubscribes from the stream.
    /// @param[in] contextAddress  The memory address of the grpc context.
    void unsubscribe(uintptr_t contextAddress);

    /// @brief Unsubscribes all subscribes from the stream.
    /// @note This is useful for when the server running the subscription
    ///       manager wants to shut down and purge all clients.
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
