#include <string>
#include "uDataPacketImport/seedLink/clientOptions.hpp"
#include "uDataPacketImport/seedLink/streamSelector.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

TEST_CASE("UDataPacketImport::SEEDLink::StreamSelector", "[streamSelector]")
{
    namespace USD = UDataPacketImport::SEEDLink;
    USD::StreamSelector selector;
    const std::string network{"UU"};
    const std::string station{"*"};
    const std::string channel{"HH?"};
    REQUIRE_NOTHROW(selector.setNetwork(network));
    REQUIRE_NOTHROW(selector.setStation(station));
    REQUIRE(selector.getNetwork() == network);
    REQUIRE(selector.getStation() == station);
    SECTION("No location code")
    {
        selector.setSelector(channel, USD::StreamSelector::Type::Data);
        REQUIRE(selector.getSelector() == "??HH?.D");
    }
    SECTION("Location code")
    {
        selector.setSelector("", "01", USD::StreamSelector::Type::Data);
        REQUIRE(selector.getSelector() == "01*.D");
    }
}

TEST_CASE("UDataPacketImport::SEEDLink::ClientOptions", "[clientOptions]")
{
    namespace USD = UDataPacketImport::SEEDLink;
    //USD::StreamSelector selector;

}
