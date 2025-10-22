#include <string>
#include <algorithm>
#include "uDataPacketImport/seedLink/subscriberOptions.hpp"

using namespace UDataPacketImport::SEEDLink;

class SubscriberOptions::SubscriberOptionsImpl
{
public:
    std::string mAddress;
};

/// Constructor
SubscriberOptions::SubscriberOptions() :
    pImpl(std::make_unique<SubscriberOptionsImpl> ())
{
}

/// Copy constructor
SubscriberOptions::SubscriberOptions(const SubscriberOptions &options)
{
    *this = options;
}

/// Move constructor
SubscriberOptions::SubscriberOptions(SubscriberOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
SubscriberOptions& 
SubscriberOptions::operator=(const SubscriberOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<SubscriberOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
SubscriberOptions& 
SubscriberOptions::operator=(SubscriberOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Address
void SubscriberOptions::setAddress(const std::string &address)
{
    std::string temp{address};
    temp.erase(std::remove(temp.begin(), temp.end(), ' '), temp.end());
    if (temp.empty())
    {
        throw std::invalid_argument("Address is empty");
    }
    pImpl->mAddress = temp; 
}

std::string SubscriberOptions::getAddress() const
{
    if (!hasAddress()){throw std::runtime_error("Address not set");}
    return pImpl->mAddress;
}

bool SubscriberOptions::hasAddress() const noexcept
{
    return !pImpl->mAddress.empty();
}

/// Destructor
SubscriberOptions::~SubscriberOptions() = default;
