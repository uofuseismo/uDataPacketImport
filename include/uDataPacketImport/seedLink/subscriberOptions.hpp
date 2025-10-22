#ifndef UDATA_PACKET_IMPORT_SEED_LINK_SUBSCRIBER_OPTIONS_HPP
#define UDATA_PACKET_IMPORT_SEED_LINK_SUBSCRIBER_OPTIONS_HPP
#include <memory>
namespace UDataPacketImport::SEEDLink
{
class SubscriberOptions
{
public:
    /// @brief Constructor.
    SubscriberOptions();
    /// @brief Copy constructor.
    /// @param[in] options  The options class from which to initialize
    ///                     this class. 
    SubscriberOptions(const SubscriberOptions &options);
    /// @brief Move constructor.
    /// @param[in,out] options  The options class from which to initialize
    ///                         this class.  On exit, options's behavior
    ///                         is undefined.
    SubscriberOptions(SubscriberOptions &&options) noexcept;

    /// @brief The connection address - e.g., localhost:50000.
    /// @param[in] address   The address of the gRPC endpoint.
    void setAddress(const std::string &address);
    /// @result The connection address.
    [[nodiscard]] std::string getAddress() const;
    /// @result True indicates the address was set. 
    [[nodiscard]] bool hasAddress() const noexcept;

    /// Destructor.
    ~SubscriberOptions();

    /// @brief Copy assigment.
    /// @param[in] options  The options to copy to this.
    /// @result A deep copy of options.
    SubscriberOptions& operator=(const SubscriberOptions &options);
    /// @brief Move assignment.
    /// @param[in,out] options  The options whose memory will be moved to
    ///                         this.  On exit, options's behavior is
    ///                         undefined. 
    /// @result The memory from options moved to this.
    SubscriberOptions& operator=(SubscriberOptions &&options) noexcept;
private:
    class SubscriberOptionsImpl;
    std::unique_ptr<SubscriberOptionsImpl> pImpl;
};
}
#endif
