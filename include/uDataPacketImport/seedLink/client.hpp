#ifndef UDATA_PACKET_IMPORT_SEEDLINK_CLIENT_HPP
#define UDATA_PACKET_IMPORT_SEEDLINK_CLIENT_HPP
#include <uDataPacketImport/client.hpp>
#include <memory>
namespace UDataPacketImport
{
 class Packet;
}
namespace UDataPacketImport::SEEDLink
{
 class ClientOptions;
}
namespace UDataPacketImport::SEEDLink
{
/// @class Client "client.hpp"
/// @brief Reads data packets from a SEEDLink server and propagates those 
///        packets to the calling application.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class Client : public UDataPacketImport::IClient
{
public:
    /// @brief Defines the SEEDLink packet reader client.
    /// @param[in] callback  The mechanism by which packets are propagated from
    ///                      SEEDLink to this application.
    /// @param[in] options   The SEEDLink client options.
    Client(const std::function<void (std::vector<UDataPacketImport::Packet> &&packets)> &callback,
           const ClientOptions &options);
    /// @brief Connects to SEEDLink.
    void connect() final;
    /// @brief Starts the import thread.
    void start() final;
    /// @brief Stops the import thread.
    void stop() final;
    /// @result True indicates the class is initialized and ready to start.
    [[nodiscard]] bool isInitialized() const noexcept final;
    /// @result True indicates the SEEDLink client is connected.
    [[nodiscard]] bool isConnected() const noexcept final;
    /// @result Indicates that this is a SEEDLink client.
    [[nodiscard]] UDataPacketImport::IClient::Type getType() const noexcept final;
    /// @brief Destructor.
    ~Client() override;
    Client() = delete;
private:
    class ClientImpl;
    std::unique_ptr<ClientImpl> pImpl;
};
}
#endif
