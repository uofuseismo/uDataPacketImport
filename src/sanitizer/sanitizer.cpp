#include <string>
#include <spdlog/spdlog.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/ext/otel_plugin.h>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <spdlog/spdlog.h>
//#include <readerwriterqueue.h>
//#include "uDataPacketImport/seedLink/client.hpp"
//#include "uDataPacketImport/seedLink/clientOptions.hpp"
//#include "uDataPacketImport/seedLink/streamSelector.hpp"
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "uDataPacketImport/sanitizer/expiredPacketDetector.hpp"
#include "uDataPacketImport/sanitizer/futurePacketDetector.hpp"
#include "proto/dataPacketBroadcast.pb.h"
#include "proto/dataPacketBroadcast.grpc.pb.h"

struct ProgramOptions
{
    UDataPacketImport::Sanitizer::FuturePacketDetectorOptions 
        futurePacketDetectorOptions;
    UDataPacketImport::Sanitizer::ExpiredPacketDetectorOptions 
        expiredPacketDetectorOptions;
    bool rejectExpiredPackets{true};
    bool rejectFuturePackets{true};
};

class ServiceImpl :
    public UDataPacketImport::GRPC::RealTimeBroadcast::CallbackService
{
public:
    ServiceImpl(const ProgramOptions &options) :
        mOptions(options)
    {
        if (mOptions.rejectExpiredPackets)
        {
            mExpiredPacketDetector
                = std::make_unique<
                     UDataPacketImport::Sanitizer::ExpiredPacketDetector
                  > (mOptions.expiredPacketDetectorOptions);
        }
        if (mOptions.rejectFuturePackets)
        {
            mFuturePacketDetector
                = std::make_unique<
                     UDataPacketImport::Sanitizer::FuturePacketDetector
                  > (mOptions.futurePacketDetectorOptions);
        }
    }

    void processPackets(UDataPacketImport::GRPC::Packet &&packet)
    {
        bool allow{true};
        if (allow && mExpiredPacketDetector)
        {
            try
            {
                allow = mExpiredPacketDetector->allow(packet);
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to test expired packet because "
                           + std::string {e.what()});
                allow = false;
            }
        } 
        if (allow && mFuturePacketDetector)
        {
            try
            {
                allow = mFuturePacketDetector->allow(packet);
            }
            catch (const std::exception &e)
            {
                spdlog::warn("Failed to test future packet because " 
                           + std::string {e.what()});
                allow = false;
            }
        }
        if (allow)
        {

        }
    }

    ::ProgramOptions mOptions;
    std::unique_ptr<UDataPacketImport::Sanitizer::ExpiredPacketDetector>
        mExpiredPacketDetector{nullptr};
    std::unique_ptr<UDataPacketImport::Sanitizer::FuturePacketDetector>
        mFuturePacketDetector{nullptr};
    
};

int main(int argc, char *argv[])
{
    return EXIT_SUCCESS;
}
