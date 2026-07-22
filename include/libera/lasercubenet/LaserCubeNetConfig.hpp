#pragma once

#include "libera/core/BufferEstimator.hpp"
#include "libera/net/NetConfig.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <system_error>
#include <vector>

namespace libera::lasercubenet {

struct LaserCubeNetConfig {
    static constexpr std::uint16_t ALIVE_PORT = 45456;
    static constexpr std::uint16_t COMMAND_PORT = 45457;
    static constexpr std::uint16_t DATA_PORT = 45458;

    static constexpr std::uint8_t CMD_GET_FULL_INFO = 0x77;
    static constexpr std::uint8_t CMD_ENABLE_BUFFER_RESPONSE = 0x78;
    static constexpr std::uint8_t CMD_SET_OUTPUT = 0x80;
    static constexpr std::uint8_t CMD_SET_ILDA_RATE = 0x82;
    static constexpr std::uint8_t CMD_GET_RINGBUFFER_FREE = 0x8a;
    static constexpr std::uint8_t CMD_SAMPLE_DATA = 0xa9;

    static constexpr std::size_t MAX_POINTS_PER_PACKET = 140; // fits within MTU
    static constexpr int SAFETY_HEADROOM_PACKETS = 2;
    static constexpr std::size_t SAFETY_HEADROOM_POINTS =
        MAX_POINTS_PER_PACKET * SAFETY_HEADROOM_PACKETS;

    static std::uint32_t clampPointRate(std::uint32_t pointRate, std::uint32_t maxPointRate) {
        if (maxPointRate > 0 && pointRate > maxPointRate) {
            return maxPointRate;
        }
        return pointRate;
    }

    static int targetBufferPoints(std::uint32_t pointRate,
                                  int bufferCapacity,
                                  std::chrono::milliseconds targetLatency) {
        return core::BufferEstimator::targetBufferPoints(
            pointRate,
            bufferCapacity,
            targetLatency,
            static_cast<int>(MAX_POINTS_PER_PACKET),
            static_cast<int>(SAFETY_HEADROOM_POINTS));
    }
};

// Injectable network endpoints and deadlines for deterministic loopback tests.
// Defaults preserve the production LaserCubeNet broadcast behavior.
struct LaserCubeNetNetworkConfig {
    // These caps reject accidental near-infinite waits while remaining far
    // above every production default. OperationControl callers can therefore
    // use the configured values as strict per-call latency bounds.
    static constexpr std::size_t MAX_DISCOVERY_DESTINATIONS = 64;
    static constexpr auto MAX_SOCKET_TIMEOUT = std::chrono::seconds(1);
    static constexpr auto MAX_DISCOVERY_DURATION = std::chrono::seconds(60);

    std::vector<std::string> discoveryDestinations{"255.255.255.255"};
    std::string localBindAddress{"0.0.0.0"};
    std::uint16_t discoveryBindPort = LaserCubeNetConfig::COMMAND_PORT;
    std::uint16_t commandPort = LaserCubeNetConfig::COMMAND_PORT;
    std::uint16_t dataPort = LaserCubeNetConfig::DATA_PORT;
    std::chrono::milliseconds sendTimeout{200};
    // Preserve the original LaserCubeNet polling cadence when no seam is used.
    std::chrono::milliseconds receivePollTimeout{500};
    std::chrono::milliseconds discoveryWindow{1000};
    std::chrono::milliseconds discoveryInterval{250};

    // A discovery response must come from the configured command port. For
    // injected unicast destinations, its source address must also be one of
    // those destinations. The production limited-broadcast destination is the
    // deliberate exception because any controller on the local IPv4 segment
    // may answer it.
    [[nodiscard]] bool acceptsDiscoveryResponse(
        const std::string& sourceAddress,
        std::uint16_t sourcePort) const {
        if (sourcePort != commandPort) {
            return false;
        }

        std::error_code addressError;
        const auto source = net::asio::ip::make_address(sourceAddress, addressError);
        if (addressError || !source.is_v4()) {
            return false;
        }
        for (const auto& destination : discoveryDestinations) {
            addressError.clear();
            const auto expected = net::asio::ip::make_address(destination, addressError);
            if (addressError || !expected.is_v4()) {
                continue;
            }
            if (expected.to_v4().to_uint() == 0xffffffffU || expected == source) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::error_code validate() const {
        if (discoveryDestinations.empty() ||
            discoveryDestinations.size() > MAX_DISCOVERY_DESTINATIONS ||
            localBindAddress.empty() || discoveryBindPort == 0 ||
            commandPort == 0 || dataPort == 0 ||
            !validSocketTimeout(sendTimeout) || !validSocketTimeout(receivePollTimeout) ||
            !validDiscoveryDuration(discoveryWindow) ||
            !validDiscoveryDuration(discoveryInterval) ||
            receivePollTimeout > discoveryWindow) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::error_code addressError;
        const auto localAddress = net::asio::ip::make_address(localBindAddress, addressError);
        if (addressError || !localAddress.is_v4()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        for (const auto& destination : discoveryDestinations) {
            addressError.clear();
            const auto address = net::asio::ip::make_address(destination, addressError);
            if (addressError || !address.is_v4()) {
                return std::make_error_code(std::errc::invalid_argument);
            }
        }
        return {};
    }

private:
    static bool validSocketTimeout(std::chrono::milliseconds duration) {
        return duration > std::chrono::milliseconds::zero() &&
               duration <= MAX_SOCKET_TIMEOUT;
    }

    static bool validDiscoveryDuration(std::chrono::milliseconds duration) {
        return duration > std::chrono::milliseconds::zero() &&
               duration <= MAX_DISCOVERY_DURATION;
    }
};

} // namespace libera::lasercubenet
