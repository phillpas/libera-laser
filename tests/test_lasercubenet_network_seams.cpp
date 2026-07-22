#include "libera/lasercubenet/LaserCubeNetManager.hpp"
#include "libera/net/UdpSocket.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

using Clock = std::chrono::steady_clock;
using libera::lasercubenet::LaserCubeNetConfig;
using libera::lasercubenet::LaserCubeNetManager;
using libera::lasercubenet::LaserCubeNetNetworkConfig;

void check(bool condition, const char* expression, int line) {
    if (condition) return;
    std::cerr << "CHECK failed at line " << line << ": " << expression << '\n';
    std::abort();
}

#define CHECK(expression) check(static_cast<bool>(expression), #expression, __LINE__)

class Event final {
public:
    void signal() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            signalled = true;
        }
        changed.notify_all();
    }

    bool waitUntil(Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_until(lock, deadline, [this] { return signalled; });
    }

private:
    std::mutex mutex;
    std::condition_variable changed;
    bool signalled{false};
};

libera::net::asio::ip::address address(const std::string& text) {
    std::error_code error;
    auto result = libera::net::asio::ip::make_address(text, error);
    CHECK(!error);
    return result;
}

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
    LoopbackDiscovery(std::string bindAddress, std::uint8_t serialSeed)
        : io(), socket(io), serialSeed(serialSeed) {
        CHECK(!socket.open_v4());
        bindError = socket.bind(address(bindAddress), LaserCubeNetConfig::COMMAND_PORT, false);
        if (bindError) {
            socket.close();
            return;
        }
        worker = std::thread([this] { run(); });
    }

    ~LoopbackDiscovery() {
        running.store(false);
        socket.close();
        if (worker.joinable()) worker.join();
    }

    bool waitForProbe(Clock::time_point deadline) { return probeReceived.waitUntil(deadline); }
    [[nodiscard]] std::error_code getBindError() const { return bindError; }

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
            packet[26 + index] = static_cast<std::uint8_t>(serialSeed + index);
        }
        packet[37] = 10;
        constexpr char name[] = "LaserCube Test";
        std::copy(std::begin(name), std::end(name), packet.begin() + 38);
        return packet;
    }

    void run() {
        while (running.load()) {
            std::array<std::uint8_t, 8> input{};
            libera::net::udp::endpoint sender;
            std::size_t received = 0;
            const auto error = socket.recv_from(
                input.data(), input.size(), sender, received, 100ms, false);
            if (error || received != 1 || input[0] != LaserCubeNetConfig::CMD_GET_FULL_INFO) {
                continue;
            }
            probeReceived.signal();
            const auto status = statusPacket();
            CHECK(!socket.send_to(status.data(), status.size(), sender, 100ms, false));
        }
    }

    libera::net::asio::io_context io;
    libera::net::UdpSocket socket;
    std::uint8_t serialSeed;
    std::atomic<bool> running{true};
    Event probeReceived;
    std::thread worker;
    std::error_code bindError;
};

LaserCubeNetNetworkConfig twoCubeNetwork() {
    LaserCubeNetNetworkConfig config;
    config.discoveryDestinations = {"127.0.0.1", "127.0.0.2"};
    config.localBindAddress = "127.0.0.3";
    // All three processes share the unchanged production command/data ports.
    config.discoveryBindPort = LaserCubeNetConfig::COMMAND_PORT;
    config.commandPort = LaserCubeNetConfig::COMMAND_PORT;
    config.dataPort = LaserCubeNetConfig::DATA_PORT;
    config.sendTimeout = 50ms;
    config.receivePollTimeout = 10ms;
    config.discoveryWindow = 100ms;
    config.discoveryInterval = 10ms;
    return config;
}

void discoversTwoDevicesOnSharedStandardPorts() {
    LoopbackDiscovery first("127.0.0.1", 1);
    LoopbackDiscovery second("127.0.0.2", 11);
    CHECK(!first.getBindError());
    CHECK(!second.getBindError());
    LaserCubeNetManager manager(twoCubeNetwork());
    CHECK(!manager.getNetworkError());

    const auto deadline = Clock::now() + 1s;
    CHECK(first.waitForProbe(deadline));
    CHECK(second.waitForProbe(deadline));

    std::vector<std::unique_ptr<libera::core::ControllerInfo>> discovered;
    do {
        discovered = manager.discover();
        if (discovered.size() != 2) std::this_thread::yield();
    } while (discovered.size() != 2 && Clock::now() < deadline);

    CHECK(discovered.size() == 2);
    std::vector<std::string> ids;
    for (const auto& device : discovered) ids.push_back(device->idValue());
    std::sort(ids.begin(), ids.end());
    CHECK(ids[0] == "010203040506");
    CHECK(ids[1] == "0B0C0D0E0F10");
    manager.closeAll();
}

void productionDefaultsHaveExactParity() {
    const LaserCubeNetNetworkConfig config;
    CHECK(config.discoveryDestinations == std::vector<std::string>{"255.255.255.255"});
    CHECK(config.localBindAddress == "0.0.0.0");
    CHECK(config.discoveryBindPort == LaserCubeNetConfig::COMMAND_PORT);
    CHECK(config.commandPort == LaserCubeNetConfig::COMMAND_PORT);
    CHECK(config.dataPort == LaserCubeNetConfig::DATA_PORT);
    CHECK(config.sendTimeout == 200ms);
    CHECK(config.receivePollTimeout == 500ms);
    CHECK(config.discoveryWindow == 1000ms);
    CHECK(config.discoveryInterval == 250ms);
    CHECK(!config.validate());
}

void discoveryResponseSenderValidationIsExact() {
    auto config = twoCubeNetwork();
    CHECK(config.acceptsDiscoveryResponse("127.0.0.1", LaserCubeNetConfig::COMMAND_PORT));
    CHECK(config.acceptsDiscoveryResponse("127.0.0.2", LaserCubeNetConfig::COMMAND_PORT));
    CHECK(!config.acceptsDiscoveryResponse("127.0.0.4", LaserCubeNetConfig::COMMAND_PORT));
    CHECK(!config.acceptsDiscoveryResponse("127.0.0.1", LaserCubeNetConfig::COMMAND_PORT + 1));
    CHECK(!config.acceptsDiscoveryResponse("not-an-address", LaserCubeNetConfig::COMMAND_PORT));

    const LaserCubeNetNetworkConfig production;
    CHECK(production.acceptsDiscoveryResponse("192.0.2.10", LaserCubeNetConfig::COMMAND_PORT));
    CHECK(!production.acceptsDiscoveryResponse("192.0.2.10",
                                               LaserCubeNetConfig::COMMAND_PORT + 1));
}

void rejectsInvalidConfiguration() {
    auto expectInvalid = [](LaserCubeNetNetworkConfig config) {
        CHECK(config.validate() == std::make_error_code(std::errc::invalid_argument));
        LaserCubeNetManager manager(std::move(config));
        CHECK(manager.getNetworkError() == std::make_error_code(std::errc::invalid_argument));
        manager.closeAll();
    };

    auto config = LaserCubeNetNetworkConfig{};
    config.discoveryDestinations.clear();
    expectInvalid(config);
    config = LaserCubeNetNetworkConfig{};
    config.discoveryDestinations.assign(
        LaserCubeNetNetworkConfig::MAX_DISCOVERY_DESTINATIONS + 1, "127.0.0.1");
    expectInvalid(config);
    config = LaserCubeNetNetworkConfig{};
    config.discoveryDestinations = {"not-an-address"};
    expectInvalid(config);
    config = LaserCubeNetNetworkConfig{};
    config.discoveryDestinations = {"::1"};
    expectInvalid(config);
    config = LaserCubeNetNetworkConfig{};
    config.localBindAddress = "not-an-address";
    expectInvalid(config);

    for (int portField = 0; portField < 3; ++portField) {
        config = LaserCubeNetNetworkConfig{};
        if (portField == 0) config.discoveryBindPort = 0;
        if (portField == 1) config.commandPort = 0;
        if (portField == 2) config.dataPort = 0;
        expectInvalid(config);
    }

    for (int durationField = 0; durationField < 4; ++durationField) {
        config = LaserCubeNetNetworkConfig{};
        if (durationField == 0) config.sendTimeout = 0ms;
        if (durationField == 1) config.receivePollTimeout = -1ms;
        if (durationField == 2) config.discoveryWindow = 61s;
        if (durationField == 3) config.discoveryInterval = 61s;
        expectInvalid(config);
    }

    config = LaserCubeNetNetworkConfig{};
    config.receivePollTimeout = 2s;
    expectInvalid(config);
    config = LaserCubeNetNetworkConfig{};
    config.sendTimeout = 1001ms;
    expectInvalid(config);
    config = LaserCubeNetNetworkConfig{};
    config.receivePollTimeout = 1001ms;
    config.discoveryWindow = 2s;
    expectInvalid(config);
}

void reportsBindFailureWithoutStartingDiscovery() {
    libera::net::asio::io_context io;
    libera::net::UdpSocket occupied(io);
    CHECK(!occupied.open_v4());
    CHECK(!occupied.bind(address("127.0.0.1"), 0));
    std::error_code endpointError;
    const auto occupiedPort = occupied.raw().local_endpoint(endpointError).port();
    CHECK(!endpointError);
    CHECK(occupiedPort != 0);

    auto config = LaserCubeNetNetworkConfig{};
    config.discoveryDestinations = {"127.0.0.1"};
    config.localBindAddress = "127.0.0.1";
    config.discoveryBindPort = occupiedPort;
    LaserCubeNetManager manager(config);
    CHECK(manager.getNetworkError() == libera::net::asio::error::address_in_use);
    CHECK(manager.discover().empty());
    manager.closeAll();
    occupied.close();
}

std::chrono::milliseconds receiveTimeoutWithExecutor(libera::net::asio::io_context& io) {
    libera::net::UdpSocket socket(io);
    CHECK(!socket.open_v4());
    CHECK(!socket.bind(address("127.0.0.1"), 0));

    std::array<std::uint8_t, 8> packet{};
    libera::net::udp::endpoint sender;
    std::size_t received = 0;
    const auto start = Clock::now();
    const auto error = socket.recv_from(
        packet.data(), packet.size(), sender, received, 25ms, false);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
    CHECK(error == libera::net::asio::error::timed_out);
    CHECK(received == 0);
    socket.close();
    return elapsed;
}

void deadlineDoesNotDependOnStoppedExecutor() {
    libera::net::asio::io_context io;
    io.stop();
    const auto elapsed = receiveTimeoutWithExecutor(io);
    CHECK(elapsed >= 20ms);
    CHECK(elapsed <= 100ms);
}

void deadlineDoesNotDependOnStalledExecutor() {
    libera::net::asio::io_context io;
    auto work = libera::net::asio::make_work_guard(io);
    Event executorEntered;
    Event releaseExecutor;
    libera::net::asio::post(io, [&] {
        executorEntered.signal();
        CHECK(releaseExecutor.waitUntil(Clock::now() + 1s));
    });
    std::thread executor([&] { io.run(); });
    CHECK(executorEntered.waitUntil(Clock::now() + 1s));

    const auto elapsed = receiveTimeoutWithExecutor(io);
    CHECK(elapsed >= 20ms);
    CHECK(elapsed <= 100ms);

    releaseExecutor.signal();
    work.reset();
    io.stop();
    executor.join();
}

void closeWakesBlockedReceive() {
    libera::net::asio::io_context io;
    io.stop();
    libera::net::UdpSocket socket(io);
    CHECK(!socket.open_v4());
    CHECK(!socket.bind(address("127.0.0.1"), 0));

    Event receiverStarted;
    std::error_code receiveError;
    std::thread receiver([&] {
        std::array<std::uint8_t, 8> packet{};
        libera::net::udp::endpoint sender;
        std::size_t received = 0;
        receiverStarted.signal();
        receiveError = socket.recv_from(
            packet.data(), packet.size(), sender, received, 5s, false);
    });
    CHECK(receiverStarted.waitUntil(Clock::now() + 1s));

    const auto start = Clock::now();
    socket.close();
    receiver.join();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
    CHECK(receiveError == libera::net::asio::error::operation_aborted);
    CHECK(elapsed <= 100ms);
}

class SilentEndpoint final {
public:
    explicit SilentEndpoint(std::string bindAddress) : socket(io) {
        CHECK(!socket.open_v4());
        CHECK(!socket.bind(address(bindAddress), LaserCubeNetConfig::COMMAND_PORT));
        worker = std::thread([this] {
            std::array<std::uint8_t, 8> input{};
            libera::net::udp::endpoint sender;
            std::size_t received = 0;
            const auto error = socket.recv_from(
                input.data(), input.size(), sender, received, 1s, false);
            if (!error && received == 1 && input[0] == LaserCubeNetConfig::CMD_GET_FULL_INFO) {
                probeReceived.signal();
            }
        });
    }

    ~SilentEndpoint() {
        socket.close();
        if (worker.joinable()) worker.join();
    }

    bool waitForProbe(Clock::time_point deadline) { return probeReceived.waitUntil(deadline); }

private:
    libera::net::asio::io_context io;
    libera::net::UdpSocket socket;
    Event probeReceived;
    std::thread worker;
};

void managerShutdownWakesReceiveAndIntervalWaits() {
    SilentEndpoint endpoint("127.0.0.1");
    auto config = LaserCubeNetNetworkConfig{};
    config.discoveryDestinations = {"127.0.0.1"};
    config.localBindAddress = "127.0.0.1";
    config.discoveryBindPort = 45459;
    LaserCubeNetManager manager(config);
    CHECK(!manager.getNetworkError());
    CHECK(endpoint.waitForProbe(Clock::now() + 1s));

    const auto start = Clock::now();
    manager.closeAll();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
    CHECK(elapsed <= 100ms);
}

} // namespace

int main(int argc, char** argv) {
    const bool onlyTwoDevice = argc == 2 && std::string(argv[1]) == "--two-device";
    const bool withoutTwoDevice = argc == 2 && std::string(argv[1]) == "--without-two-device";
    CHECK(argc == 1 || onlyTwoDevice || withoutTwoDevice);

    if (onlyTwoDevice) {
        discoversTwoDevicesOnSharedStandardPorts();
        return 0;
    }

    productionDefaultsHaveExactParity();
    discoveryResponseSenderValidationIsExact();
    rejectsInvalidConfiguration();
    reportsBindFailureWithoutStartingDiscovery();
    deadlineDoesNotDependOnStoppedExecutor();
    deadlineDoesNotDependOnStalledExecutor();
    closeWakesBlockedReceive();
    managerShutdownWakesReceiveAndIntervalWaits();
    if (!withoutTwoDevice) {
        discoversTwoDevicesOnSharedStandardPorts();
    }
    return 0;
}
