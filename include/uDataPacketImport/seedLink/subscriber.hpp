#ifndef UDATA_PACKET_IMPORT_SEED_LINK_SUBSCRIBER_HPP
#define UDATA_PACKET_IMPORT_SEED_LINK_SUBSCRIBER_HPP
#include <functional>
#include <memory>
#include <uDataPacketImport/acquisition.hpp>
namespace UDataPacketImport::GRPC
{
 class Packet;
}
namespace UDataPacketImport::SEEDLink
{
 class SubscriberOptions;
}
namespace UDataPacketImport::SEEDLink
{
/// @class Subscriber "subscriber.hpp"
/// @brief Subscribes to the gRPC SEEDLink data packet broadcast.
///        Since it is important to get data "in the door" the broadcast
///        simply propagates everything it receives.  Therefore, it is the job
///        of the client to filter streams as it deems appropriate. 
class Subscriber final : public UDataPacketImport::IAcquisition
{
public:
    /// @brief Constructs the subscriber from the given options.
    Subscriber(const std::function<void (UDataPacketImport::GRPC::Packet &&)> &callback,
               const SubscriberOptions &options);

    /// @brief Initialized?
    [[nodiscard]] bool isInitialized() const noexcept final;

    //void connect() final;

    /// @result True indicates the client is streaming data from the
    ///         gRPC endpoint.
    [[nodiscard]] bool isRunning() const noexcept final;

    /// @brief Runs the packet acquisition.
    [[nodiscard]] std::future<void> start() final;

    /// @brief Stops the packet acquisition.
    void stop() final;

    /// @brief Destructor.  
    ~Subscriber() override;

    [[nodiscard]] UDataPacketImport::IAcquisition::Type getType() const noexcept final;

    Subscriber() = delete;
    Subscriber(const Subscriber &) = delete;
    Subscriber(Subscriber &&subscriber) noexcept = delete;
private:
    class SubscriberImpl;
    std::unique_ptr<SubscriberImpl> pImpl;
};
}
#endif
