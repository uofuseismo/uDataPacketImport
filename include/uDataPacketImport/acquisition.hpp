#ifndef UDATA_PACKET_IMPORT_EXTERNAL_ACQUISITION_HPP
#define UDATA_PACKET_IMPORT_EXTERNAL_ACQUISITION_HPP
#include <future>
#include <string>
namespace UDataPacketImport
{
/// @class IAcquisition "acquisition.hpp"
/// @brief An abstract base class that acquires data from some seismic import
///        interface - e.g., SEEDLink, gRPC, Earthworm, etc.  An acquisition is
///        a long-lived thread that listens to a feed.
/// @copyright Ben Baker (University of Utah) distributed under the MIT NO AI
///            license.
class IAcquisition
{
public:
    /// @brief Starts the acquisition.
    /// @result A future that can be used to detect exceptions thrown by the 
    ///         subscribing thread.
    virtual std::future<void> start() = 0;
    /// @brief Terminates the acquisition.
    virtual void stop() = 0;
    /// @result The client type.
    virtual std::string getType() const noexcept = 0;
    /// @result True indicates the client is ready to receive 
    ///         data packets.
    [[nodiscard]] virtual bool isInitialized() const noexcept = 0;
    /// @result True indicates the client is connected.
    [[nodiscard]] virtual bool isRunning() const noexcept = 0;
    /// @brief Destructor.
    virtual ~IAcquisition();
};
}
#endif
