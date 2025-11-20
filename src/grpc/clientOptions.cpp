#include <set>
#include <chrono>
#include "uDataPacketImport/grpc/clientOptions.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"

using namespace UDataPacketImport::GRPC;

class ClientOptions::ClientOptionsImpl
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
    std::set<UDataPacketImport::StreamIdentifier> mStreamSelections;
    bool mSubscribeToAll{true};
};

/// Constructor
ClientOptions::ClientOptions() :
    pImpl(std::make_unique<ClientOptionsImpl> ())
{
}

/// Copy constructor
ClientOptions::ClientOptions(const ClientOptions &options)
{
    *this = options;
}

/// Move constructor
ClientOptions::ClientOptions(ClientOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
ClientOptions& ClientOptions::operator=(const ClientOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<ClientOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
ClientOptions& ClientOptions::operator=(ClientOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Destructor
ClientOptions::~ClientOptions() = default;

/// Desired streams
void ClientOptions::enableSubscribeToAllStreams() noexcept
{
    pImpl->mStreamSelections.clear();
    pImpl->mSubscribeToAll = true;
}

bool ClientOptions::subscribeToAllStreams() const noexcept
{
    return pImpl->mSubscribeToAll;
}

void ClientOptions::setStreamSelections(
    const std::set<UDataPacketImport::StreamIdentifier> &desiredStreams)
{
    for (const auto &s : desiredStreams)
    {
        try
        {
            if (s.getStringReference().empty())
            {
                throw std::invalid_argument("Desired stream not properly set");
            }
        }
        catch (const std::exception &e)
        {
            throw std::invalid_argument("Selected stream is invalid because "
                                      + std::string {e.what()});
        }
    }
    if (desiredStreams.empty())
    {
        throw std::invalid_argument("No streams specified");
    }
    pImpl->mStreamSelections = desiredStreams;
    pImpl->mSubscribeToAll = false;
}
 
std::optional<std::set<UDataPacketImport::StreamIdentifier>>
    ClientOptions::getStreamSelections() const noexcept
{
    if (subscribeToAllStreams())
    {
        return std::nullopt;
    }
    return std::optional< std::set<UDataPacketImport::StreamIdentifier> >
           (pImpl->mStreamSelections);
}


/// Address
void ClientOptions::setAddress(const std::string &address)
{
    std::string temp{address};
    temp.erase(std::remove(temp.begin(), temp.end(), ' '), temp.end());
    if (temp.empty())
    {   
        throw std::invalid_argument("Address is empty");
    }   
    pImpl->mAddress = temp; 
}

std::string ClientOptions::getAddress() const
{
    if (!hasAddress()){throw std::runtime_error("Address not set");}
    return pImpl->mAddress;
}

bool ClientOptions::hasAddress() const noexcept
{
    return !pImpl->mAddress.empty();
}

/// Certificate
void ClientOptions::setCertificate(const std::string &certificate)
{
    if (certificate.empty())
    {
        throw std::invalid_argument("Certificate is empty");
    }
    pImpl->mCertificate = certificate;
}

std::optional<std::string> ClientOptions::getCertificate() const noexcept
{
    return !pImpl->mCertificate.empty() ?
           std::optional<std::string> (pImpl->mCertificate) : std::nullopt;
}

/// Token
void ClientOptions::setToken(const std::string &token)
{
    if (token.empty()){throw std::invalid_argument("Token is empty");}
    pImpl->mToken = token;
}

std::optional<std::string> ClientOptions::getToken() const noexcept
{
    return !pImpl->mToken.empty() ?
           std::optional<std::string> (pImpl->mToken) : std::nullopt;
}

/// Reconnect interval
std::vector<std::chrono::seconds>
ClientOptions::getReconnectSchedule() const noexcept
{
    return pImpl->mReconnectSchedule;
}

