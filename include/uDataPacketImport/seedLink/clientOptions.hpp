#ifndef UDATA_PACKET_IMPORT_SEED_LINK_CLIENT_OPTIONS_HPP
#define UDATA_PACKET_IMPORT_SEED_LINK_CLIENT_OPTIONS_HPP
#include <chrono>
#include <vector>
#include <memory>
#include <filesystem>
namespace UDataPacketImport::SEEDLink
{
 class StreamSelector;
}
namespace UDataPacketImport::SEEDLink
{
/// @class ClientOptions "clientOptions.hpp" "uDataPacketImport/seedLink/clientOptions.hpp"
/// @brief Defines the options used by the SEEDLink client.
/// @copyright Ben Baker (University of Utah) distributed under the
///            NO AI MIT license.
class ClientOptions
{
public:
    /// @name Constructors
    /// @{

    /// @brief Constructor.
    ClientOptions();
    /// @brief Copy constructor.
    /// @param[in] options  The options from which to initialize this class.
    ClientOptions(const ClientOptions &options);
    /// @brief Move constructor.
    /// @param[in,out] options  The options from which to initialize this class.
    ///                         On exit, option's behavior is undefined.
    ClientOptions(ClientOptions &&options) noexcept;
    /// @}

    /// @name Operators
    /// @{

    /// @brief Copy assignment operator.
    /// @param[in] options  The options class to copy to this.
    /// @result A deep copy of the options.
    ClientOptions& operator=(const ClientOptions &options);
    /// @brief Move assignment operator.
    /// @param[in,out] options  The options class whose memory will be moved
    ///                         to this.  On exit, option's behavior is
    ///                         undefined.
    /// @result The memory from options moved to this.
    ClientOptions& operator=(ClientOptions &&options) noexcept;
    /// @}

    /// @name Properties
    /// @{

    /// @brief Sets the address of the SEEDLink server.
    /// @param[in] address  The IP address of the SEEDLink server.
    void setAddress(const std::string &address);
    /// @result The IP address of the SEEDLink server.  By default this is
    ///         rtserve.iris.washington.edu
    [[nodiscard]] std::string getAddress() const noexcept;

    /// @brief Sets the port number of the SEEDLink server.
    /// @param[in] port  The port of the server.
    void setPort(uint16_t port) noexcept;
    /// @result The port number of the SEEDLink server.  By default this 18000.
    [[nodiscard]] uint16_t getPort() const noexcept;

    /// @brief Packets are read from SEED Link, put onto an internal queue,
    ///        then sent out via the databroadcast.  This sets the maximum
    ///        internal queue size.  After this point, the oldest packets
    ///        are popped from the queue.
    /// @param[in] maximumQueueSize  The maximum queue size in number of
    ///                              packets.
    /// @throws std::invalid_argument if this is not positive.
    void setMaximumInternalQueueSize(int maximumQueueSize);
    /// @result The maximum internal queue size in packets.
    [[nodiscard]] int getMaximumInternalQueueSize() const noexcept;

    /// @brief Sets the SEEDLink client's state file.  The state file
    ///        contains a list of sequence numbers written during
    ///        clean shutdown.  When the client resumes these numbers
    ///        are used to resume data streams.
    /// @param[in] stateFile  The path to the state file. 
    /// @throw std::runtime_error if the path to the state cannot be made.
    void setStateFile(const std::filesystem::path &stateFile);
    /// @result The path to the state file.
    /// @throws std::runtime_error if \c haveStateFile() is false.
    [[nodiscard]] std::filesystem::path getStateFile() const; 
    /// @result True indicates the state file was set.
    [[nodiscard]] bool haveStateFile() const noexcept;

    /// @brief Controls the interval in which the state file is written.
    /// @param[in] interval   After this many packets are written the state
    ///                       file will be updated.
    void setStateFileUpdateInterval(uint16_t interval) noexcept;
    /// @result The state file update interval in packets.  The default is 100.
    [[nodiscard]] uint16_t getStateFileUpdateInterval() const noexcept;

    /// @brief Specifies the size in bytes of the SEED records.
    ///        Traditionally, this is 512 however RockToSLink may use
    ///        128 or 256.
    /// @param[in] recordSize  The SEED record size.
    /// @throw std::invalid_argument if the record size is not 128, 256, or 512.
    void setSEEDRecordSize(int recordSize);
    /// @result The SEED record size in bytes.  By default this is 512.
    [[nodiscard]] int getSEEDRecordSize() const noexcept;

    /// @brief After this many seconds elapses the network the SEED Link
    ///        connection will be reset.
    /// @param[in] timeOut  The time out in seconds.  If this is 0
    ///                     then this will be disabled.
    void setNetworkTimeOut(const std::chrono::seconds &timeOut);
    /// @result After this many seconds the network connection be reset.
    ///         By default this is zero.
    [[nodiscard]] std::chrono::seconds getNetworkTimeOut() const noexcept;
    /// @brief The network reconnect delay in seconds.
    /// @param[in] delay  The network re-connect delay in seconds.
    void setNetworkReconnectDelay(const std::chrono::seconds &timeOut);
    /// @result The network re-connect delay in seconds.
    [[nodiscard]] std::chrono::seconds getNetworkReconnectDelay() const noexcept;

    /// @brief Adds a stream selector.
    /// @throws std::invalid_argument if the selector is not properly set.
    void addStreamSelector(const StreamSelector &selector);
    /// @result The stream selectors. 
    [[nodiscard]] std::vector<StreamSelector> getStreamSelectors() const noexcept;
    /// @}

    /// @name Destructors
    /// @{

    /// @brief Resets the class and releases memory.
    void clear() noexcept;
    /// @brief Destructor.
    ~ClientOptions();
    /// @}
private:
    class ClientOptionsImpl;
    std::unique_ptr<ClientOptionsImpl> pImpl;
};
}
#endif
