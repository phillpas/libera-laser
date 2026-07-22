#include "libera/lasercubenet/LaserCubeNetManager.hpp"
#include "libera/net/NetService.hpp"
#include "libera/net/UdpSocket.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;

namespace {

void check(bool condition, const char* expression, int line) {
    if (condition) return;
    std::cerr << "CHECK failed at line " << line << ": " << expression << '\n';
    std::abort();
}

#define CHECK(expression) check(static_cast<bool>(expression), #expression, __LINE__)

void writeLe16(std::uint8_t* output, std::uint16_t value) {
    output[0] = static_cast<std::uint8_t>(value & 0xffU);
    output[1] = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
}

void writeLe32(std::uint8_t* output, std::uint32_t value) {
    output[0] = static_cast<std::uint8_t>(value & 0xffU);
    output[1] = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
    output[2] = static_cast<std::uint8_t>((value >> 16U) & 0xffU);
    output[3] = static_cast<std::uint8_t>((value >> 24U) & 0xffU);
}

class LoopbackDiscovery final {
public:
    explicit LoopbackDiscovery(std::uint16_t port)
        : io(libera::net::shared_io_context()), socket(*io) {
        CHECK(!socket.open_v4());
        std::error_code addressError;
        const auto address = libera::net::asio::ip::make_address("127.0.0.1", addressError);
        CHECK(!addressError);
        CHECK(!socket.bind(address, port));
        worker = std::thread([this] { run(); });
    }

    ~LoopbackDiscovery() {
        running.store(false);
        if (worker.joinable()) worker.join();
        socket.close();
    }

private:
    std::array<std::uint8_t, 64> statusPacket() const {
        std::array<std::uint8_t, 64> packet{};
        packet[3] = 1;
        packet[4] = 13;
        writeLe32(&packet[10], 30000);
        writeLe32(&packet[14], 30000);
        writeLe16(&packet[19], 1000);
        writeLe16(&packet[21], 1000);
        for (std::size_t index = 0; index < 6; ++index) {
            packet[26 + index] = static_cast<std::uint8_t>(index + 1);
        }
        packet[32] = 127;
        packet[35] = 1;
        packet[37] = 10;
        constexpr char name[] = "LaserCube Test";
        for (std::size_t index = 0; index < sizeof(name); ++index) {
            packet[38 + index] = static_cast<std::uint8_t>(name[index]);
        }
        return packet;
    }

    void run() {
        while (running.load()) {
            std::array<std::uint8_t, 8> input{};
            libera::net::udp::endpoint sender;
            std::size_t received = 0;
            const auto error = socket.recv_from(
                input.data(), input.size(), sender, received, 20ms, false);
            if (error || received != 1 ||
                input[0] != libera::lasercubenet::LaserCubeNetConfig::CMD_GET_FULL_INFO) {
                continue;
            }
            const auto status = statusPacket();
            CHECK(!socket.send_to(status.data(), status.size(), sender, 50ms, false));
        }
    }

    std::shared_ptr<libera::net::asio::io_context> io;
    libera::net::UdpSocket socket;
    std::atomic<bool> running{true};
    std::thread worker;
};

void injectableDiscoveryUsesOnlyConfiguredLoopbackEndpoint() {
    constexpr std::uint16_t commandPort = 47457;
    LoopbackDiscovery cube(commandPort);

    libera::lasercubenet::LaserCubeNetNetworkConfig config;
    config.discoveryDestinations = {"127.0.0.1"};
    config.localBindAddress = "127.0.0.1";
    config.discoveryBindPort = static_cast<std::uint16_t>(commandPort + 10U);
    config.commandPort = commandPort;
    config.dataPort = static_cast<std::uint16_t>(commandPort + 1U);
    config.sendTimeout = 50ms;
    config.receivePollTimeout = 10ms;
    config.discoveryWindow = 50ms;
    config.discoveryInterval = 5ms;

    libera::lasercubenet::LaserCubeNetManager manager(config);
    const auto deadline = std::chrono::steady_clock::now() + 500ms;
    for (;;) {
        const auto discovered = manager.discover();
        if (!discovered.empty()) {
            CHECK(discovered.size() == 1);
            CHECK(discovered.front()->idValue() == "010203040506");
            break;
        }
        CHECK(std::chrono::steady_clock::now() < deadline);
        std::this_thread::yield();
    }
    manager.closeAll();
}

} // namespace

int main() {
    injectableDiscoveryUsesOnlyConfiguredLoopbackEndpoint();
    return 0;
}
