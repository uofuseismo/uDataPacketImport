#ifndef UDATA_PACKET_IMPORT_GRPC_CLIENT_OPTIONS_HPP
#define UDATA_PACKET_IMPORT_GRPC_CLIENT_OPTIONS_HPP
#include <string>
#include <set>
#include <chrono>
#include <memory>
#include <optional>
namespace UDataPacketImport
{
 class StreamIdentifier;
}
namespace UDataPacketImport::GRPC
{
/// @class ClientOptions 
/// @brief Sets the gRPC client options. 
/// @copyright Ben Baker (University of Utah) distributed under the MIT NO AI
///            license.
class ClientOptions
{
public:
    /// @brief Constructor.
    ClientOptions();
    /// @brief Copy assignment.
    ClientOptions(const ClientOptions &options);
    /// @brief Move assignment.
    ClientOptions(ClientOptions &&options) noexcept;

    /// @brief Sets the gRPC endpoint.
    void setAddress(const std::string &address);
    /// @result The gRPC endpoint.
    /// @throws std::runtime_error if \c hasAddress() is false.
    [[nodiscard]] std::string getAddress() const;
    /// @result True indicates the address was set.
    [[nodiscard]] bool hasAddress() const noexcept;

    /// @brief If the publishers blinks out then the subscriber will try
    ///        to reconnect according to this schedule.  Afterwards, the
    ///        subscriber fails.
    [[nodiscard]] std::vector<std::chrono::seconds> getReconnectSchedule() const noexcept;

    /// @brief Sets the SSL/TLS certificate.
    /// @param[in] certificate   The certificate.
    void setCertificate(const std::string &certificate);
    /// @result The certificate.
    [[nodiscard]] std::optional<std::string> getCertificate() const noexcept;

    /// @brief Sets a custom access token.
    /// @note This requires a certificate to work.
    void setToken(const std::string &token);
    /// @result The access token.
    [[nodiscard]] std::optional<std::string> getToken() const noexcept;

    /// @brief Enables subscribing to all streams.  
    /// @note This invalidates any stream selections.
    void enableSubscribeToAllStreams() noexcept;
    /// @result True indicates the client will subscribe to all streams.
    /// @note This is the default behavior.
    [[nodiscard]] bool subscribeToAllStreams() const noexcept;

   
    /// @brief Sets the streams selections.
    /// @param[in] desiredStreams  The desired channels to stream.
    /// @note This will disable subscribe to all.
    void setStreamSelections(const std::set<UDataPacketImport::StreamIdentifier> &desiredStreams);
    /// @note If \c subcribeToAllStreams() is true then this is disregarded.
    [[nodiscard]] std::optional<std::set<UDataPacketImport::StreamIdentifier>> getStreamSelections() const noexcept;

    /// @brief Destructor.
    ~ClientOptions();

    /// @brief Copy assignment.
    /// @param[in] options  The options class to copy to this.
    /// @result A deep copy of the options.
    ClientOptions &operator=(const ClientOptions &options);
    /// @brief Move assignment.
    /// @param[in,out] options  The options class from which to initialize this
    ///                         class.  On exit, the behavior of options is
    ///                         undefined.
    /// @result The memory from options moved to this.
    ClientOptions &operator=(ClientOptions &&options) noexcept;

private:
    class ClientOptionsImpl;
    std::unique_ptr<ClientOptionsImpl> pImpl; 
};
}
#endif
