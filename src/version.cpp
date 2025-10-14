#include <string>
#include "uDataPacketImport/version.hpp"

using namespace UDataPacketImport;

int Version::getMajor() noexcept
{
    return UDataPacketImport_MAJOR;
}

int Version::getMinor() noexcept
{
    return UDataPacketImport_MINOR;
}

int Version::getPatch() noexcept
{
    return UDataPacketImport_PATCH;
}

bool Version::isAtLeast(const int major, const int minor,
                        const int patch) noexcept
{
    if (UDataPacketImport_MAJOR < major){return false;}
    if (UDataPacketImport_MAJOR > major){return true;}
    if (UDataPacketImport_MINOR < minor){return false;}
    if (UDataPacketImport_MINOR > minor){return true;}
    if (UDataPacketImport_PATCH < patch){return false;}
    return true;
}

std::string Version::getVersion() noexcept
{
    std::string version{UDataPacketImport_VERSION};
    return version;
}

