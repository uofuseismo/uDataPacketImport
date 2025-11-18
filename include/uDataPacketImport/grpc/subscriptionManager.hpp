#ifndef UDATA_PACKET_IMPORT_GRPC_SUBSCRIPTION_MANAGER_HPP
#define UDATA_PACKET_IMPORT_GRPC_SUBSCRIPTION_MANAGER_HPP
#include <grpcpp/grpcpp.h>
#include <memory>
namespace UDataPacketImport::GRPC
{
  class Packet;
  class StreamOptions;
  class UnsubscribeFromAllStreamsResponse;
  class UnsubscribeResponse;
  class SubscriptionRequest;
  class SubscriptionManagerOptions;
}
namespace UDataPacketImport::GRPC
{
/// @class SubscriptionManager "subscriptionManager.hpp"
/// @brief The subscription manager is the interface between data producers and
///        data clients.  This class allows clients to subscribe to or
///        unsubscribe from data streams.  Likewise, it allows producers to
///        make streams of data available to clients for consumption.
/// @copyright Ben Baker (University of Utah) distributed under the MIT license.
class SubscriptionManager
{
public:
    /// @brief Constructor.
    explicit SubscriptionManager(const SubscriptionManagerOptions &options);

    /// @name Client Interface
    /// @{

    /// @brief Allows a client to subscribe to all streams.
    void subscribeToAll(grpc::ServerContext *context);
    void subscribeToAll(grpc::CallbackServerContext *context);

    /// @brief Allows a client to subsribe to a list of streams.
    void subscribe(grpc::ServerContext *context,
                   const SubscriptionRequest &request);
    void subscribe(grpc::CallbackServerContext *context,
                   const SubscriptionRequest &request);

    /// @brief Allows a client to unsubscribe from all streams.
    [[nodiscard]] UnsubscribeFromAllStreamsResponse
        unsubscribeFromAll(grpc::ServerContext *context);
    [[nodiscard]] UnsubscribeFromAllStreamsResponse
        unsubscribeFromAll(grpc::CallbackServerContext *context);
    void unsubscribeFromAllOnCancel(grpc::ServerContext *context);
    void unsubscribeFromAllOnCancel(grpc::CallbackServerContext *context);

    /// @brief Allows a client to unsubscribe from a stream.
    [[nodiscard]] UnsubscribeResponse unsubscribe(uintptr_t contextAddress);
    [[nodiscard]] UnsubscribeResponse
        unsubscribe(grpc::ServerContext *context);
    [[nodiscard]] UnsubscribeResponse unsubscribe(grpc::CallbackServerContext *context);

    [[nodiscard]] std::vector<UDataPacketImport::GRPC::Packet>
        getNextPacketsFromAllSubscriptions(grpc::CallbackServerContext *context) const;
    [[nodiscard]] std::vector<UDataPacketImport::GRPC::Packet>
        getNextPacketsFromAllSubscriptions(grpc::ServerContext *context) const;
    /// @}

    /// @name Producer Interface
    /// @{

    /// @brief Used by a publisher to add a packet.
    void addPacket(const UDataPacketImport::GRPC::Packet &packet);
    void addPacket(UDataPacketImport::GRPC::Packet &&packet);

    /// @}

    /// @result The number of streams.
    [[nodiscard]] int getNumberOfStreams() const noexcept;

    /// @result The number of subscribers.
    [[nodiscard]] int getNumberOfSubscribers() const noexcept;

    void unsubscribeAll();

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
