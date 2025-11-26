#ifndef UDATA_PACKET_IMPORT_GRPC_CLIENT_HPP
#define UDATA_PACKET_IMPORT_GRPC_CLIENT_HPP
#include <memory>
#include <functional>
#include <uDataPacketImport/packet.hpp>
#include <uDataPacketImport/acquisition.hpp>
namespace UDataPacketImport::GRPC
{
 class ClientOptions;
}
namespace UDataPacketImport::GRPC
{
class Client : public UDataPacketImport::IAcquisition
{
public:
    /// @brief Defines the gRPC packet reader client.
    /// @param[in] callback  The mechanism by which packets are propagated from
    ///                      gRPC to this application.  This callback uses the
    ///                      low-level protobuf which is slightly faster but
    ///                      more inconvenient for use in an applicaiton.
    /// @param[in] options   The client options.
    Client(const std::function<void (UDataPacketImport::GRPC::Packet &&packet)> &callback,
           const ClientOptions &options);
    /// @brief Defines the gRPC packet reader client.
    /// @param[in] callback  The mechanism by which packets are propagated from
    ///                      gRPC to this application.  This callback uses the
    ///                      library packet specification which is slightly
    ///                      slower but more convenient for use in an
    ///                      application.
    /// @param[in] options   The client options.
    Client(const std::function<void (UDataPacketImport::Packet &&packet)> &callback,
           const ClientOptions &options);
    /// @brief Starts the import thread.
    /// @result A future so as to catch exceptions.
    [[nodiscard]] std::future<void> start() final;
    /// @brief Stops the import thread.
    void stop() final;
    /// @result True indicates the class is initialized and ready to start.
    [[nodiscard]] bool isInitialized() const noexcept final;
    /// @result True indicates the client is acquiring data.
    [[nodiscard]] bool isRunning() const noexcept final;
    /// @result Indicates that this is a client.
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
