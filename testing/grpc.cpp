#include <vector>
#include <cmath>
#include <string>
#include <chrono>
#include <limits>
#include "uDataPacketImport/grpc/streamOptions.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

TEST_CASE("UDataPacketImport::GRPC::StreamOptions", "[streamOptions]")
{
    UDataPacketImport::GRPC::StreamOptions options;
    SECTION("Defaults")
    {
        REQUIRE(options.getMaximumQueueSize() == 8);
        REQUIRE(!options.requireOrdered());
    }
    options.setMaximumQueueSize(39);
    REQUIRE(options.getMaximumQueueSize() == 39);
    options.enableRequireOrdered();
    REQUIRE(options.requireOrdered());
    options.disableRequireOrdered();
    REQUIRE(!options.requireOrdered());
}

