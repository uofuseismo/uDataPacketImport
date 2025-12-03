#ifndef UDATA_PACKET_IMPORT_SEEDLINK_ACQUISITION_HPP
#define UDATA_PACKET_IMPORT_SEEDLINK_ACQUISITION_HPP
#include <uDataPacketImport/acquisition.hpp>
#include <future>
#include <memory>
namespace UDataPacketImport::GRPC::V1
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
/// @brief Acquires data packets from a SEEDLink server and propagates those 
///        packets to the calling application.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class Client : public UDataPacketImport::IAcquisition
{
public:
    /// @brief Defines the SEEDLink packet reader client.
    /// @param[in] callback  The mechanism by which packets are propagated from
    ///                      SEEDLink to this application.
    /// @param[in] options   The SEEDLink client options.
    Client(const std::function<void (UDataPacketImport::GRPC::V1::Packet &&packet)> &callback,
           const ClientOptions &options);
    /// @brief Connects to SEEDLink.
    //void connect() final;
    /// @brief Starts the import thread.
    [[nodiscard]] std::future<void> start() final;
    /// @brief Stops the import thread.
    void stop() final;
    /// @result True indicates the class is initialized and ready to start.
    [[nodiscard]] bool isInitialized() const noexcept final;
    /// @result True indicates the SEEDLink client is acquiring data.
    [[nodiscard]] bool isRunning() const noexcept final;
    /// @result Indicates that this is a SEEDLink client.
    [[nodiscard]] std::string getType() const noexcept final;
    /// @brief Destructor.
    ~Client() override;
    Client() = delete;
private:
    class ClientImpl;
    std::unique_ptr<ClientImpl> pImpl;
};
}
#endif
