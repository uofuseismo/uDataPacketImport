#ifndef UDATA_PACKET_IMPORT_GRPC_SUBSCRIPTION_MANAGER_OPTIONS_HPP
#define UDATA_PACKET_IMPORT_GRPC_SUBSCRIPTION_MANAGER_OPTIONS_HPP
#include <memory>
namespace UDataPacketImport::GRPC
{
 class StreamOptions;
}
namespace UDataPacketImport::GRPC
{
class SubscriptionManagerOptions
{
public:
    /// @brief Constructor.
    SubscriptionManagerOptions();
    /// @brief Copy constructor.
    SubscriptionManagerOptions(const SubscriptionManagerOptions &options);
    /// @brief Move constructor.
    SubscriptionManagerOptions(SubscriptionManagerOptions &&options) noexcept;

    /// @brief Sets the options defining the behavior of the data streams.
    /// @param[in] options   The data streams options. 
    void setStreamOptions(const StreamOptions &options);
    /// @result The options defining the behavior of the data streams.
    [[nodiscard]] StreamOptions getStreamOptions() const noexcept;

    /// @brief Destructor.
    ~SubscriptionManagerOptions();
    /// @brief Copy assignment.
    SubscriptionManagerOptions& operator=(const SubscriptionManagerOptions &options);
    /// @brief Move assignment.
    SubscriptionManagerOptions& operator=(SubscriptionManagerOptions &&options) noexcept;
private:
    class SubscriptionManagerOptionsImpl;
    std::unique_ptr<SubscriptionManagerOptionsImpl> pImpl;
};
}
#endif
