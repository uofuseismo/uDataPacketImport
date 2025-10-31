#ifndef UDATA_PACKET_IMPORT_EXTERNAL_CLIENT_HPP
#define UDATA_PACKET_IMPORT_EXTERNAL_CLIENT_HPP
#include <functional>
#include <memory>
#include <vector>
namespace UDataPacketImport
{
 class Packet;
}
namespace UDataPacketImport
{
/// @class IClient "client.hpp"
/// @brief An abstract base class that imports data from some seismic import
///        interface - e.g., SEEDLink, Earthworm, etc.  A client is a 
///        long-lived thread that listens to a feed.
/// @copyright Ben Baker (University of Utah) distributed under the MIT NO AI
///            license.
class IClient
{
public:
    /// @brief Defines the import type.
    enum class Type
    {
        SEEDLink,
        gRPC
    };
public:
    /// @brief Constructor with a callback.
    /// @param[in] callback   Defines how consumed packets from the import
    ///                       are propagated are propagated to the next phase
    ///                       of processing.
    explicit IClient(const std::function<void (std::vector<UDataPacketImport::Packet> &&packets)> &callback);
    /// @brief Destructor.
    virtual ~IClient();
    /// @brief Connects the client to the data source.
    virtual void connect() = 0;
    /// @brief Starts the acquisition.
    virtual void start() = 0;
    /// @brief Terminates the acquisition.
    virtual void stop() = 0;
    /// @result The client type.
    virtual Type getType() const noexcept = 0;
    /// @result True indicates the client is ready to receive 
    ///         data packets.
    [[nodiscard]] virtual bool isInitialized() const noexcept = 0;
    /// @result True indicates the client is connected.
    [[nodiscard]] virtual bool isConnected() const noexcept = 0;
    /// @brief Passes the packets read from the import to the next processing
    ///        phase.
    /// @param[in,out] packets  The packets to send to propagate.
    ///                         On exit,  the behavior of packets is undefined.
    virtual void operator()(std::vector<UDataPacketImport::Packet> &&packets);
    /// @brief Passes the packet read from the import to the next processing
    ///        phase.
    /// @param[in,out] packet  The packet to propagate.
    ///                        On exit, packet's behavior is undefined.
    virtual void operator()(UDataPacketImport::Packet &&packet);
    /// @brief Constructor.
    IClient() = delete;
private:
    class IClientImpl;
    std::unique_ptr<IClientImpl> pImpl;
};
}
#endif
