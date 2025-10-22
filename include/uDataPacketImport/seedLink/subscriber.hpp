#ifndef UDATA_PACKET_IMPORT_SEED_LINK_SUBSCRIBER_HPP
#define UDATA_PACKET_IMPORT_SEED_LINK_SUBSCRIBER_HPP
#include <memory>
namespace UDataPacketImport
{
 class Packet;
}
namespace UDataPacketImport::SEEDLink
{
 class SubscriberOptions;
}
namespace UDataPacketImport::SEEDLink
{
/// @class Subscriber
/// @brief Subscribes to the gRPC SEEDLink data packet broadcast.
///        Since it is important to get data "in the door" the broadcast
///        simply propagates everything it receives.  Therefore, it is the job
///        of the client to filter streams as it deems appropriate. 
class Subscriber
{
public:
    /// @brief Constructs the subscriber from the given options.
    explicit Subscriber(const SubscriberOptions &options);

    /// @brief Destructor.  
    ~Subscriber();
private:
    class SubscriberImpl;
    std::unique_ptr<SubscriberImpl> pImpl;
};
}
#endif
