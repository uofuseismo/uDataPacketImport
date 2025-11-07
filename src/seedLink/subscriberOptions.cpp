#include <string>
#include <chrono>
#include "uDataPacketImport/seedLink/subscriberOptions.hpp"

using namespace UDataPacketImport::SEEDLink;

class SubscriberOptions::SubscriberOptionsImpl
{
public:
    std::string mAddress;
    std::string mCertificate;
    std::string mToken;
    std::vector<std::chrono::seconds> mReconnectSchedule
    {
        std::chrono::seconds {5},
        std::chrono::seconds {15},
        std::chrono::seconds {60}
    };
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

/// Certificate
void SubscriberOptions::setCertificate(const std::string &certificate)
{
    if (certificate.empty())
    {
        throw std::invalid_argument("Certificate is empty");
    }
    pImpl->mCertificate = certificate;
}

std::optional<std::string> SubscriberOptions::getCertificate() const noexcept
{
    return !pImpl->mCertificate.empty() ?
           std::optional<std::string> (pImpl->mCertificate) : std::nullopt;
}

/// Token
void SubscriberOptions::setToken(const std::string &token)
{
    if (token.empty()){throw std::invalid_argument("Token is empty");}
    pImpl->mToken = token;
}

std::optional<std::string> SubscriberOptions::getToken() const noexcept
{
    return !pImpl->mToken.empty() ?
           std::optional<std::string> (pImpl->mToken) : std::nullopt;
}

/// Reconnect interval
std::vector<std::chrono::seconds> 
SubscriberOptions::getReconnectSchedule() const noexcept
{
    return pImpl->mReconnectSchedule;
}

/// Destructor
SubscriberOptions::~SubscriberOptions() = default;
