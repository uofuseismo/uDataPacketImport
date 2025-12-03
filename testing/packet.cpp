#include <vector>
#include <set>
#include <map>
#include <cmath>
#include <string>
#include <chrono>
#include <limits>
#include "uDataPacketImport/packet.hpp"
#include "uDataPacketImport/streamIdentifier.hpp"
#include "proto/v1/packet.pb.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

TEST_CASE("UDataPacketImport::StreamIdentifier", "[streamIdentifier]")
{
    const std::string network{"UU"};
    const std::string station{"FTU"};
    const std::string channel{"HHN"};
    const std::string locationCode{"01"};
    UDataPacketImport::StreamIdentifier identifier;

    REQUIRE_NOTHROW(identifier.setNetwork(network));
    REQUIRE_NOTHROW(identifier.setStation(station));
    REQUIRE_NOTHROW(identifier.setChannel(channel));
    REQUIRE_NOTHROW(identifier.setLocationCode(locationCode));

    REQUIRE(identifier.getNetwork() == network);
    REQUIRE(identifier.getStation() == station);
    REQUIRE(identifier.getChannel() == channel);
    REQUIRE(identifier.getLocationCode() == locationCode);
    REQUIRE(identifier.toString() == "UU.FTU.HHN.01");
    REQUIRE(identifier.getStringReference() == "UU.FTU.HHN.01");

    SECTION("From Protobuf")
    {
        UDataPacketImport::GRPC::V1::StreamIdentifier proto;
        proto.set_network(network);
        proto.set_station(station);
        proto.set_channel(channel);
        proto.set_location_code(locationCode);

        UDataPacketImport::StreamIdentifier id2{proto};
        REQUIRE(id2.getNetwork() == network);
        REQUIRE(id2.getStation() == station);
        REQUIRE(id2.getChannel() == channel);
        REQUIRE(id2.getLocationCode() == locationCode);
    }

    SECTION("To Protobuf")
    {
        auto proto = identifier.toProtobuf();
        REQUIRE(proto.network() == network);
        REQUIRE(proto.station() == station);
        REQUIRE(proto.channel() == channel);
        REQUIRE(proto.location_code() == locationCode);
    }

    SECTION("Set")
    {
        UDataPacketImport::StreamIdentifier id1;
        id1.setNetwork("UU"); id1.setStation("CWU");
        id1.setChannel("HHZ"); id1.setLocationCode("01");

        UDataPacketImport::StreamIdentifier id2;
        id2.setNetwork("UU"); id2.setStation("CWU"); 
        id2.setChannel("HHN"); id2.setLocationCode("01");

        UDataPacketImport::StreamIdentifier id3;
        id3.setNetwork("UU"); id3.setStation("CWU"); 
        id3.setChannel("HHE"); id3.setLocationCode("01");

        UDataPacketImport::StreamIdentifier id4;
        id4.setNetwork("UU"); id4.setStation("TMU"); 
        id4.setChannel("HHZ"); id4.setLocationCode("01");

        std::set<UDataPacketImport::StreamIdentifier> ids;
        ids.insert(id1);
        ids.insert(id2);
        ids.insert(id3);
        REQUIRE(ids.size() == 3);
        REQUIRE(ids.contains(id1));
        REQUIRE(ids.contains(id2));
        REQUIRE(ids.contains(id3));
        REQUIRE(!ids.contains(id4)); 
    }
      
}


TEST_CASE("UDataPacketImport::Packet", "[packet]")
{
    const std::string network{"UU"};
    const std::string station{"FTU"};
    const std::string channel{"HHN"};
    const std::string locationCode{"01"};
    const double samplingRate{100};
    const std::chrono::microseconds startTime{1759952887000000};
    UDataPacketImport::StreamIdentifier identifier;

    REQUIRE_NOTHROW(identifier.setNetwork(network));
    REQUIRE_NOTHROW(identifier.setStation(station));
    REQUIRE_NOTHROW(identifier.setChannel(channel));
    REQUIRE_NOTHROW(identifier.setLocationCode(locationCode));
 
    UDataPacketImport::Packet packet;
    REQUIRE_NOTHROW(packet.setStreamIdentifier(identifier));
    REQUIRE_NOTHROW(packet.setSamplingRate(samplingRate));
    REQUIRE_NOTHROW(packet.setStartTime(startTime));
     
    REQUIRE(packet.getStreamIdentifierReference().getNetwork() == network); 
    REQUIRE(packet.getStreamIdentifier().getNetwork() == network); 
    REQUIRE(std::abs(packet.getSamplingRate() - samplingRate) < 1.e-14);
    REQUIRE(packet.getStartTime() == startTime);

    SECTION("double start time")
    {
        packet.setStartTime(startTime.count()*1.e-6);
        REQUIRE(packet.getStartTime() == startTime);
    }

    SECTION("integer32")
    {
        std::vector<int> data{1, 2, 3, 4};
        auto dataTemp = data;
        REQUIRE_NOTHROW(packet.setData(std::move(dataTemp)));
        REQUIRE(packet.getEndTime().count() == startTime.count() + 30000); 
        REQUIRE(packet.getDataType() ==
                UDataPacketImport::Packet::DataType::Integer32);
        auto dataBack = packet.getData<int> ();
        REQUIRE(dataBack.size() == data.size());
        REQUIRE(packet.getNumberOfSamples() == static_cast<int> (data.size()));
        for (int i = 0; i < static_cast<int> (data.size()); ++i)
        {
            REQUIRE(dataBack.at(i) == data.at(i));
        }
        REQUIRE(packet.getNumberOfSamples() == static_cast<int> (data.size()));

        SECTION("to protobuf")
        {
            auto proto = packet.toProtobuf(); 
            auto identifier = proto.stream_identifier();
            REQUIRE(identifier.network() == network);
            REQUIRE(identifier.station() == station);
            REQUIRE(identifier.channel() == channel);
            REQUIRE(identifier.location_code() == locationCode);
            REQUIRE(std::abs(proto.sampling_rate() - samplingRate) < 1.e-14);
            REQUIRE(proto.start_time_mus() == startTime.count());
            std::vector<int> work;
            work.reserve(proto.data32i().size());
            for (const auto &v : proto.data32i())
            {
                work.push_back(v);
            }
            REQUIRE(work.size() == data.size());
            for (int i = 0; i < static_cast<int> (data.size()); ++i)
            {
                REQUIRE(work.at(i) == data.at(i));
            }
        }
    }

    SECTION("integer64")
    {   
        std::vector<int64_t> data{4, 3, 2, 1}; 
        REQUIRE_NOTHROW(packet.setData(data));
        REQUIRE(packet.getEndTime().count() == startTime.count() + 30000); 
        REQUIRE(packet.getDataType() ==
                UDataPacketImport::Packet::DataType::Integer64);
        auto dataBack = packet.getData<int64_t> (); 
        REQUIRE(dataBack.size() == data.size());
        REQUIRE(packet.getNumberOfSamples() == static_cast<int> (data.size()));
        for (int i = 0; i < static_cast<int> (data.size()); ++i)
        {
            REQUIRE(dataBack.at(i) == data.at(i));
        }

        SECTION("to protobuf")
        {
            auto proto = packet.toProtobuf(); 
            std::vector<int64_t> work;
            work.reserve(proto.data64i().size());
            for (const auto &v : proto.data64i())
            {
                work.push_back(v);
            }
            REQUIRE(work.size() == data.size());
            for (int i = 0; i < static_cast<int> (data.size()); ++i)
            {
                REQUIRE(work.at(i) == data.at(i));
            }
        }
    }

    SECTION("double")
    {   
        std::vector<double> data{4, 2, 3, 1}; 
        REQUIRE_NOTHROW(packet.setData(data));
        REQUIRE(packet.getEndTime().count() == startTime.count() + 30000); 
        REQUIRE(packet.getDataType() ==
                UDataPacketImport::Packet::DataType::Double); 
        auto dataBack = packet.getData<double> (); 
        REQUIRE(dataBack.size() == data.size());
        REQUIRE(packet.getNumberOfSamples() == static_cast<int> (data.size()));
        for (int i = 0; i < static_cast<int> (data.size()); ++i)
        {
            REQUIRE(std::abs(dataBack.at(i) - data.at(i)) < 1.e-14);
        }

        SECTION("to protobuf")
        {
            auto proto = packet.toProtobuf(); 
            std::vector<double> work;
            work.reserve(proto.data64f().size());
            for (const auto &v : proto.data64f())
            {
                work.push_back(v);
            }
            REQUIRE(work.size() == data.size());
            for (int i = 0; i < static_cast<int> (data.size()); ++i)
            {
                REQUIRE(std::abs(work.at(i) - data.at(i)) < 1.e-14);
            }
        }
    }

    SECTION("float")
    {
        std::vector<float> data{4, 1, 2, 3};  
        REQUIRE_NOTHROW(packet.setData(data));
        REQUIRE(packet.getEndTime().count() == startTime.count() + 30000);
        REQUIRE(packet.getDataType() ==
                UDataPacketImport::Packet::DataType::Float);
        auto dataBack = packet.getData<float> (); 
        REQUIRE(dataBack.size() == data.size());
        REQUIRE(packet.getNumberOfSamples() == static_cast<int> (data.size()));
        for (int i = 0; i < static_cast<int> (data.size()); ++i)
        {
            REQUIRE(std::abs(dataBack.at(i) - data.at(i)) < 1.e-7);
        }

        SECTION("to protobuf")
        {
            auto proto = packet.toProtobuf(); 
            std::vector<float> work;
            work.reserve(proto.data32f().size());
            for (const auto &v : proto.data32f())
            {
                work.push_back(v);
            }
            REQUIRE(work.size() == data.size());
            for (int i = 0; i < static_cast<int> (data.size()); ++i)
            {
                REQUIRE(std::abs(work.at(i) - data.at(i)) < 1.e-7);
            }
        }
    }

}

