#include <chrono>
#include <vector>
#include <string>
#include <random>
#include <cmath>
#include <numeric>
#include "uDataPacketImport/sanitizer/expiredPacketDetector.hpp"
#include "uDataPacketImport/sanitizer/futurePacketDetector.hpp"
#include "uDataPacketImport/sanitizer/duplicatePacketDetector.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "uDataPacketImport/packet.hpp"
#include "proto/dataPacketBroadcast.pb.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

TEST_CASE("UDataPacketImport::Sanitizer::FuturePacketDetector",
          "[futureData]")
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
    // N.B. valgrind runs too slow - can either lower the sampling rate or
    // make the packet longer
    std::vector<int64_t> packetData(100);
    std::iota(packetData.begin(), packetData.end(), 1);
    packet.setData(packetData);
    constexpr std::chrono::microseconds maxExpiredTime{10000}; // 0.01 seconds (packet duration is 0.99 s)
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
        packet.setStartTime(nowMuSeconds); //std::chrono::microseconds {nowMuSeconds});
        REQUIRE(detector.allow(packet)); // Fails in valgrind if packet is too small
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

TEST_CASE("UDataPacketImport::Sanitizer::DuplicatePacketDetector",
          "[duplicateData]")
{
    // Random packet sizes
    std::random_device randomDevice;
    std::mt19937 generator(188382);
    std::uniform_int_distribution<> uniformDistribution(250, 350);

    // Define a base packet
    UDataPacketImport::StreamIdentifier identifier;
    identifier.setNetwork("UU");
    identifier.setStation("CTU");
    identifier.setChannel("HHZ");
    identifier.setLocationCode("01");

    const double samplingRate{100};
    UDataPacketImport::Packet packet;
    packet.setStreamIdentifier(identifier);
    packet.setSamplingRate(samplingRate); 

    // Define a start time
    auto now = std::chrono::high_resolution_clock::now();
    auto nowSeconds
        = std::chrono::time_point_cast<std::chrono::seconds>
          (now).time_since_epoch();
    double startTime = nowSeconds.count() - 600; // Don't mess with future
    packet.setStartTime(startTime);

    // Business as usual - all data comes in on time and in order
    SECTION("All good data")
    {   
        const std::chrono::seconds logBadDataInterval{0};
        const int circularBufferSize{15};

        UDataPacketImport::Sanitizer::DuplicatePacketDetectorOptions options;
        options.setCircularBufferSize(circularBufferSize);
        options.setLogBadDataInterval(logBadDataInterval);

        UDataPacketImport::Sanitizer::DuplicatePacketDetector detector{options};
        int cumulativeSamples{0}; 
        int nExamples = 2*circularBufferSize;
        for (int iPacket = 0; iPacket < nExamples; iPacket++)
        {
            auto packetStartTime = startTime + cumulativeSamples/samplingRate;
            std::vector<int> data(uniformDistribution(generator), 0); 
            cumulativeSamples
                = cumulativeSamples + static_cast<int> (data.size()); 
            packet.setStartTime(packetStartTime);
            packet.setData(data);
            REQUIRE(detector.allow(packet));
        }
    }   

    SECTION("Every other is a duplicate")
    {   
        const std::chrono::seconds logBadDataInterval{-1};
        const int circularBufferSize{15};

        UDataPacketImport::Sanitizer::DuplicatePacketDetectorOptions options;
        options.setCircularBufferSize(circularBufferSize);
        options.setLogBadDataInterval(logBadDataInterval);

        UDataPacketImport::Sanitizer::DuplicatePacketDetector detector{options};
        int cumulativeSamples{0}; 
        int nExamples = 2*circularBufferSize;
        for (int iPacket = 0; iPacket < nExamples; iPacket++)
        {
            auto packetStartTime = startTime + cumulativeSamples/samplingRate;
            std::vector<int> data(uniformDistribution(generator), 0); 
            cumulativeSamples
                = cumulativeSamples + static_cast<int> (data.size()); 
            packet.setStartTime(packetStartTime);
            packet.setData(data);
            CHECK(detector.allow(packet.toProtobuf()));
            CHECK(!detector.allow(packet));
        }
    }

    SECTION("Out of order with duplicates")
    {
        const std::chrono::seconds logBadDataInterval{-1};
        const int circularBufferSize{15};

        UDataPacketImport::Sanitizer::DuplicatePacketDetectorOptions options;
        options.setCircularBufferSize(circularBufferSize);
        options.setLogBadDataInterval(logBadDataInterval);

        UDataPacketImport::Sanitizer::DuplicatePacketDetector detector{options};

        std::vector<UDataPacketImport::Packet> packets;
        int cumulativeSamples{0};
        for (int iPacket = 0; iPacket < circularBufferSize; iPacket++)
        {
            auto packetStartTime = startTime + cumulativeSamples/samplingRate;
            std::vector<int> data(uniformDistribution(generator), 0);
            cumulativeSamples
                = cumulativeSamples + static_cast<int> (data.size());
            packet.setStartTime(packetStartTime);
            packet.setData(data);
            packets.push_back(packet);
        }
        std::shuffle(packets.begin(), packets.end(), generator);

        for (const auto &outOfOrderPacket : packets)
        {
            //std::cout << std::setprecision(16) << "hey " << outOfOrderPacket.getStartTime().count()*1.e-6 << std::endl;
            REQUIRE(detector.allow(outOfOrderPacket));
            CHECK(!detector.allow(outOfOrderPacket.toProtobuf()));
        }
    }

    SECTION("Timing slips")
    {   
        const std::chrono::seconds logBadDataInterval{-1};
        const int circularBufferSize{15};

        UDataPacketImport::Sanitizer::DuplicatePacketDetectorOptions options;
        options.setCircularBufferSize(circularBufferSize);
        options.setLogBadDataInterval(logBadDataInterval);

        UDataPacketImport::Sanitizer::DuplicatePacketDetector detector{options};

        int cumulativeSamples{0}; 
        // Load it
        int nExamples = circularBufferSize;
        std::vector<UDataPacketImport::Packet> packets;
        for (int iPacket = 0; iPacket < nExamples; iPacket++)
        {   
            auto packetStartTime = startTime + cumulativeSamples/samplingRate;
            std::vector<int> data(uniformDistribution(generator), 0); 
            cumulativeSamples
                = cumulativeSamples + static_cast<int> (data.size());
            packet.setStartTime(packetStartTime);
            packet.setData(data);
            if (iPacket%2 == 0)
            {
                CHECK(detector.allow(packet.toProtobuf()));
            }
            else
            {
                CHECK(detector.allow(packet));
            }
            packets.push_back(packet);
        }   

        // Throw some timing slips in there
        auto firstPacket = packets.front();
        firstPacket.setStartTime(
            firstPacket.getStartTime().count()*1.e-6
          - (firstPacket.getNumberOfSamples() - 1)
           /firstPacket.getSamplingRate()/2.);
        CHECK(!detector.allow(firstPacket));
        for (int iPacket = 0; iPacket < nExamples; iPacket++)
        {   
            auto thisPacket = packets.at(iPacket);
            double packetStartTime = thisPacket.getStartTime().count()*1.e-6
                                   + (thisPacket.getNumberOfSamples() - 1)
                                     /thisPacket.getSamplingRate()/2;
            thisPacket.setStartTime(packetStartTime);
            CHECK(!detector.allow(thisPacket));
        }
    }   


}

