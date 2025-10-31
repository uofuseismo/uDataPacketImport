#include "uDataPacketImport/grpc/streamOptions.hpp"

using namespace UDataPacketImport::GRPC;

class StreamOptions::StreamOptionsImpl
{
public:
    int mMaximumQueueSize{8};
    bool mRequireOrdered{false};
};

/// Constructor
StreamOptions::StreamOptions() :
    pImpl(std::make_unique<StreamOptionsImpl> ())
{
}

/// Copy constructor
StreamOptions::StreamOptions(const StreamOptions &options)
{
    *this = options;
}

/// Move constructor
StreamOptions::StreamOptions(StreamOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
StreamOptions& StreamOptions::operator=(const StreamOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<StreamOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
StreamOptions& StreamOptions::operator=(StreamOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}
 
/// Destructor
StreamOptions::~StreamOptions() = default;
