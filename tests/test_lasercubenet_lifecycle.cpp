#include "libera/lasercubenet/LaserCubeNetController.hpp"
#include "libera/net/NetService.hpp"
#include "libera/net/UdpSocket.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

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

class LoopbackCube final {
public:
    explicit LoopbackCube(std::uint16_t commandPort)
        : port(commandPort), io(libera::net::shared_io_context()), socket(*io) {
        CHECK(!socket.open_v4());
        std::error_code addressError;
        const auto address = libera::net::asio::ip::make_address("127.0.0.1", addressError);
        CHECK(!addressError);
        CHECK(!socket.bind(address, port));
        worker = std::thread([this] { run(); });
    }

    ~LoopbackCube() {
        running.store(false);
        if (worker.joinable()) {
            worker.join();
        }
        socket.close();
    }

    libera::lasercubenet::LaserCubeNetStatus initialStatus() const {
        libera::lasercubenet::LaserCubeNetStatus status;
        status.payloadVersion = 0;
        status.firmwareMajor = 1;
        status.firmwareMinor = 13;
        status.firmwareVersion = "1.13";
        status.outputEnabled = true;
        status.pointRate = 30000;
        status.pointRateMax = 30000;
        status.bufferFree = 500;
        status.bufferMax = 1000;
        status.serialNumber = "010203040506";
        status.ipAddress = "127.0.0.1";
        status.modelNumber = 10;
        status.modelName = "LaserCube Test";
        return status;
    }

    std::vector<std::uint8_t> commands() const {
        std::lock_guard<std::mutex> lock(mutex);
        return receivedCommands;
    }

    void dropStatus(bool drop) { dropFullStatus.store(drop); }

private:
    std::array<std::uint8_t, 64> statusPacket() const {
        std::array<std::uint8_t, 64> packet{};
        packet[2] = 0;
        packet[3] = 1;
        packet[4] = 13;
        packet[5] = outputEnabled.load() ? 0x01 : 0x00;
        writeLe32(&packet[10], 30000);
        writeLe32(&packet[14], 30000);
        writeLe16(&packet[19], bufferFree.load());
        writeLe16(&packet[21], 1000);
        packet[23] = 255;
        packet[24] = 25;
        packet[25] = 1;
        for (std::size_t index = 0; index < 6; ++index) {
            packet[26 + index] = static_cast<std::uint8_t>(index + 1);
        }
        packet[32] = 127;
        packet[33] = 0;
        packet[34] = 0;
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
            std::array<std::uint8_t, 64> input{};
            libera::net::udp::endpoint sender;
            std::size_t received = 0;
            const auto error = socket.recv_from(
                input.data(), input.size(), sender, received, 25ms, false);
            if (error) {
                continue;
            }
            if (received == 0) {
                continue;
            }
            {
                std::lock_guard<std::mutex> lock(mutex);
                receivedCommands.push_back(input[0]);
            }
            if (input[0] == libera::lasercubenet::LaserCubeNetConfig::CMD_SET_OUTPUT &&
                received >= 2) {
                outputEnabled.store(input[1] != 0);
            } else if (input[0] ==
                       libera::lasercubenet::LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER) {
                bufferFree.store(1000);
            } else if (input[0] ==
                           libera::lasercubenet::LaserCubeNetConfig::CMD_GET_FULL_INFO &&
                       !dropFullStatus.load()) {
                const auto packet = statusPacket();
                socket.send_to(packet.data(), packet.size(), sender, 50ms, false);
            }
        }
    }

    std::uint16_t port;
    std::shared_ptr<libera::net::asio::io_context> io;
    libera::net::UdpSocket socket;
    std::atomic<bool> running{true};
    std::atomic<bool> outputEnabled{true};
    std::atomic<std::uint16_t> bufferFree{500};
    std::atomic<bool> dropFullStatus{false};
    mutable std::mutex mutex;
    std::vector<std::uint8_t> receivedCommands;
    std::thread worker;
};

libera::lasercubenet::LaserCubeNetNetworkConfig testNetwork(std::uint16_t port) {
    libera::lasercubenet::LaserCubeNetNetworkConfig config;
    config.discoveryDestinations = {"127.0.0.1"};
    config.localBindAddress = "127.0.0.1";
    config.discoveryBindPort = static_cast<std::uint16_t>(port + 10U);
    config.commandPort = port;
    config.dataPort = static_cast<std::uint16_t>(port + 1U);
    config.sendTimeout = 50ms;
    config.receivePollTimeout = 20ms;
    return config;
}

void assertSequence(const std::vector<std::uint8_t>& actual,
                    const std::vector<std::uint8_t>& expected) {
    CHECK(actual.size() >= expected.size());
    for (std::size_t index = 0; index < expected.size(); ++index) {
        CHECK(actual[index] == expected[index]);
    }
}

void lifecycleOrdersRemoteCommandsAndRequiresFreshStatus() {
    constexpr std::uint16_t port = 47457;
    LoopbackCube cube(port);
    const auto status = cube.initialStatus();
    libera::lasercubenet::LaserCubeNetControllerInfo info(status);
    libera::lasercubenet::LaserCubeNetController controller(info, testNetwork(port));

    const auto connected = controller.connectDark(
        info, libera::lasercubenet::LaserCubeNetOperation::withTimeout(500ms));
    CHECK(connected);
    CHECK(connected->evidence ==
           libera::lasercubenet::LaserCubeNetRemoteEvidence::DeviceReportedDisabled);
    CHECK(!connected->status.outputEnabled);
    CHECK(connected->bufferConfirmedEmpty);
    assertSequence(
        cube.commands(),
        {libera::lasercubenet::LaserCubeNetConfig::CMD_SET_OUTPUT,
         libera::lasercubenet::LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER,
         libera::lasercubenet::LaserCubeNetConfig::CMD_SET_OUTPUT,
         libera::lasercubenet::LaserCubeNetConfig::CMD_GET_FULL_INFO});

    controller.setPointCallback([](const libera::core::PointFillRequest& request,
                                   std::vector<libera::core::LaserPoint>& points) {
        points.assign(request.minimumPointsRequired, libera::core::LaserPoint{});
    });
    const auto enabled = controller.enableOutput(
        libera::lasercubenet::LaserCubeNetOperation::withTimeout(500ms));
    CHECK(enabled);
    CHECK(enabled->evidence ==
           libera::lasercubenet::LaserCubeNetRemoteEvidence::DeviceReportedEnabled);
    CHECK(controller.isArmed());

    const auto disabled = controller.disableDark(
        libera::lasercubenet::LaserCubeNetOperation::withTimeout(500ms));
    CHECK(disabled);
    CHECK(!controller.isArmed());
    CHECK(disabled->evidence ==
           libera::lasercubenet::LaserCubeNetRemoteEvidence::DeviceReportedDisabled);
}

void missingFreshStatusFailsWithinDeadline() {
    constexpr std::uint16_t port = 47557;
    LoopbackCube cube(port);
    const auto status = cube.initialStatus();
    libera::lasercubenet::LaserCubeNetControllerInfo info(status);
    libera::lasercubenet::LaserCubeNetController controller(info, testNetwork(port));
    CHECK(controller.connectDark(
        info, libera::lasercubenet::LaserCubeNetOperation::withTimeout(500ms)));

    cube.dropStatus(true);
    const auto start = std::chrono::steady_clock::now();
    const auto disabled = controller.disableDark(
        libera::lasercubenet::LaserCubeNetOperation::withTimeout(100ms));
    const auto elapsed = std::chrono::steady_clock::now() - start;
    CHECK(!disabled);
    CHECK(elapsed < 250ms);
    CHECK(!controller.isArmed());
}

void cancelledOperationSendsNothing() {
    constexpr std::uint16_t port = 47657;
    LoopbackCube cube(port);
    const auto status = cube.initialStatus();
    libera::lasercubenet::LaserCubeNetControllerInfo info(status);
    libera::lasercubenet::LaserCubeNetController controller(info, testNetwork(port));
    const auto operation = libera::lasercubenet::LaserCubeNetOperation{
        std::chrono::steady_clock::now() + 1s,
        [] { return true; }};
    CHECK(!controller.connectDark(info, operation));
    CHECK(cube.commands().empty());
}

void closeSerializesWithPendingReceive() {
    auto io = libera::net::shared_io_context();
    libera::net::UdpSocket socket(*io);
    CHECK(!socket.open_v4());
    std::error_code addressError;
    const auto address = libera::net::asio::ip::make_address("127.0.0.1", addressError);
    CHECK(!addressError);
    CHECK(!socket.bind(address, 47757));

    std::atomic<bool> receiveEntered{false};
    std::thread receiver([&] {
        std::array<std::uint8_t, 1> input{};
        libera::net::udp::endpoint sender;
        std::size_t received = 0;
        receiveEntered.store(true, std::memory_order_release);
        const auto error = socket.recv_from(
            input.data(), input.size(), sender, received, 1s, false);
        CHECK(error == libera::net::asio::error::operation_aborted);
    });

    while (!receiveEntered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(20ms);
    socket.close();
    receiver.join();
}

} // namespace

int main() {
    lifecycleOrdersRemoteCommandsAndRequiresFreshStatus();
    missingFreshStatusFailsWithinDeadline();
    cancelledOperationSendsNothing();
    closeSerializesWithPendingReceive();
    return 0;
}
