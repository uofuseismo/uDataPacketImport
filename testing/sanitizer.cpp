#include <chrono>
#include <string>
#include "uDataPacketImport/sanitizer/expiredPacketDetector.hpp"
#include "uDataPacketImport/sanitizer/futurePacketDetector.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "uDataPacketImport/packet.hpp"
#include "proto/dataPacketBroadcast.pb.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

TEST_CASE("UDataPacketImport::Sanitizer::FuturePacketDetector",
          "[expiredData]")
{
    UDataPacketImport::StreamIdentifier identifier;
    identifier.setNetwork("UU");
    identifier.setStation("MOUT");
    identifier.setChannel("HHZ");
    identifier.setLocationCode("01");
    UDataPacketImport::Packet packet;
    packet.setStreamIdentifier(identifier);
    packet.setSamplingRate(1); // 1 sps helps with subsequent test
    packet.setData(std::vector<int> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    constexpr std::chrono::microseconds maxFutureTime{1000};
    constexpr std::chrono::seconds logBadDataInterval{-1};
    UDataPacketImport::Sanitizer::FuturePacketDetectorOptions options;
    options.setMaxFutureTime(maxFutureTime);
    options.setLogBadDataInterval(logBadDataInterval);
    REQUIRE(options.getMaxFutureTime() == maxFutureTime);
    REQUIRE(options.getLogBadDataInterval().count() < 0);
    UDataPacketImport::Sanitizer::FuturePacketDetector detector{options};

    SECTION("ValidData")
    {
        packet.setStartTime(0.0);
        REQUIRE(detector.allow(packet));
        REQUIRE(detector.allow(packet.toProtobuf()));
    }
    auto now = std::chrono::high_resolution_clock::now();
    auto nowMuSeconds
        = std::chrono::time_point_cast<std::chrono::microseconds>
          (now).time_since_epoch();
    SECTION("FutureData")
    {
        packet.setStartTime(nowMuSeconds - std::chrono::microseconds {100});
        REQUIRE(!detector.allow(packet));
        REQUIRE(!detector.allow(packet.toProtobuf()));
    }
    SECTION("Copy")
    {
        auto detectorCopy = detector;
        packet.setStartTime(nowMuSeconds - std::chrono::microseconds {100});
        REQUIRE(!detectorCopy.allow(packet));
        REQUIRE(!detectorCopy.allow(packet.toProtobuf()));
    }
}

TEST_CASE("UDataPacketImport::Sanitizer::ExpiredPacketDetector",
          "[expiredData]")
{
    UDataPacketImport::StreamIdentifier identifier;
    identifier.setNetwork("UU");
    identifier.setStation("EKU");
    identifier.setChannel("HHZ");
    identifier.setLocationCode("01");
    UDataPacketImport::Packet packet;
    packet.setStreamIdentifier(identifier);
    packet.setSamplingRate(100.0);
    packet.setData(std::vector<int64_t> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    constexpr std::chrono::microseconds maxExpiredTime{1000};
    constexpr std::chrono::seconds logBadDataInterval{-1};
    UDataPacketImport::Sanitizer::ExpiredPacketDetectorOptions options;
    options.setMaxExpiredTime(maxExpiredTime);
    options.setLogBadDataInterval(logBadDataInterval);
    REQUIRE(options.getMaxExpiredTime() == maxExpiredTime);
    REQUIRE(options.getLogBadDataInterval().count() < 0); 

    UDataPacketImport::Sanitizer::ExpiredPacketDetector detector{options};
    SECTION("ValidData")
    {
        auto now = std::chrono::high_resolution_clock::now();
        auto nowMuSeconds
            = std::chrono::time_point_cast<std::chrono::microseconds>
              (now).time_since_epoch();
        packet.setStartTime(std::chrono::microseconds {nowMuSeconds});
        REQUIRE(detector.allow(packet));
        REQUIRE(detector.allow(packet.toProtobuf()));
    }
    SECTION("ExpiredData")
    {
        auto now = std::chrono::high_resolution_clock::now();
        auto nowMuSeconds
            = std::chrono::time_point_cast<std::chrono::microseconds>
              (now).time_since_epoch();
        // Sometimes it executes too fast so we need to subtract a little
        // tolerance 
        packet.setStartTime(std::chrono::microseconds {nowMuSeconds}
                          - maxExpiredTime
                          - std::chrono::microseconds{1});
        REQUIRE(!detector.allow(packet));
        REQUIRE(!detector.allow(packet.toProtobuf()));
    }
    SECTION("Copy")
    {
        auto detectorCopy = detector;
        auto now = std::chrono::high_resolution_clock::now();
        auto nowMuSeconds
            = std::chrono::time_point_cast<std::chrono::microseconds>
              (now).time_since_epoch();
        packet.setStartTime(std::chrono::microseconds {nowMuSeconds}
                          - maxExpiredTime
                          - std::chrono::microseconds{1});
        REQUIRE(!detectorCopy.allow(packet));
        REQUIRE(!detectorCopy.allow(packet.toProtobuf()));
    }
}

