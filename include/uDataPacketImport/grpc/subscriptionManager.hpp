#ifndef UDATA_PACKET_IMPORT_GRPC_SUBSCRIPTION_MANAGER_HPP
#define UDATA_PACKET_IMPORT_GRPC_SUBSCRIPTION_MANAGER_HPP
#include <memory>
namespace UDataPacketImport::GRPC
{
  class Packet;
  class StreamOptions;
}
namespace UDataPacketImport::GRPC
{
class SubscriptionManager
{
public:

    /// @name Client Interface
    /// @{

    /// @brief Allows a client to subscribe to a stream.
    void subscribeToAll(grpc::CallbackServerContext *context );

    /// @brief Allows a client to unsubscribe from all streams.
    void unsubscribeFromAll(grpc::CallbackServerContext *context);

    /// @brief Allows a client to unsubscribe from a stream.
    void unsubscribe(grpc::CallbackServerContext *context );
    /// @}

    /// @name Producer Interface
    /// @{

    /// @brief Used by a publisher to add a packet.
    void addPacket(UDataPacketImport::GRPC::Packet &&packet);

    /// @}

    /// @result The number of streams.
    [[nodiscard]] int getNumberOfStreams() const noexcept;

    /// @result The number of subscribers.
    [[nodiscard]] int getNumberOfSubscribers() const noexcept;

    /// @brief Destructor.
    ~SubscriptionManager();

    SubscriptionManager(const SubscriptionManager &) = delete;
    SubscriptionManager& operator=(const SubscriptionManager &) = delete;
private:
    class SubscriptionManagerImpl;
    std::unique_ptr<SubscriptionManagerImpl> pImpl;
};
}
#endif
