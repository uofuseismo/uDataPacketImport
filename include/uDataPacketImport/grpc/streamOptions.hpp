#ifndef UDATA_PACKET_IMPORT_GRPC_STREAM_OPTIONS_HPP
#define UDATA_PACKET_IMPORT_GRPC_STREAM_OPTIONS_HPP
#include <memory>
namespace UDataPacketImport::GRPC
{

class StreamOptions
{
public:
    /// @brief Constructor.
    StreamOptions();
    /// @brief Copy constructor.
    StreamOptions(const StreamOptions &options);
    /// @brief Move constructor.
    StreamOptions(StreamOptions &&options) noexcept;

    /// @brief Sets the maximum number of packets to buffer in the queue
    ///        for the client.
    /// @param[in] maximumQueueSize  The maximum queue size to buffer.
    /// @throws std::invalid_argument if maximumQueueSize is not positive.
    void setMaximumQueueSize(int maximumQueueSize);
    /// @result The maximum queue size.  By default this is 8 packets. 
    [[nodiscard]] int getMaximumQueueSize() const noexcept;

    /// @brief Allows the publisher to always insert its most recently
    ///        acquired packet.
    void disableOrdered() noexcept;
    /// @brief Requires the publisher only insert packets that contain more 
    ///        recent data than any packet previously acquired.
    /// @note If enabled it is very important to screen out future data.
    ///       What can happen is a packet from, say, 40 years in the future
    ///       can effectively prevent all previously acquired packets from
    ///       entering the queue.
    void enableOrdered() noexcept;
    /// @result True requires the publisher maintain order.  This means
    ///         that the publisher cannot add a packet to the queue unless
    ///         it contains new data.  If this is false, then the publisher
    ///         will add its most recently acquired packet to the queue.
    /// @note The default is false.
    [[nodiscard]] bool requireOrdered() const noexcept;

    /// @brief Copy assignment.
    StreamOptions& operator=(const StreamOptions &options);
    /// @brief Move assignment.
    StreamOptions& operator=(StreamOptions &&options) noexcept;

    /// @brief Destructor.
    ~StreamOptions();
private:
     class StreamOptionsImpl;
     std::unique_ptr<StreamOptionsImpl> pImpl;
};

}
#endif
