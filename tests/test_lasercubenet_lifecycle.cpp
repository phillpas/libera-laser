#include "libera/lasercubenet/LaserCubeNetManager.hpp"
#include "libera/net/NetService.hpp"
#include "libera/net/UdpSocket.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

using libera::lasercubenet::LaserCubeNetConfig;
using libera::lasercubenet::LaserCubeNetController;
using libera::lasercubenet::LaserCubeNetControllerInfo;
using libera::lasercubenet::LaserCubeNetLifecycleResult;
using libera::lasercubenet::LaserCubeNetManager;
using libera::lasercubenet::LaserCubeNetNetworkConfig;
using libera::lasercubenet::LaserCubeNetOperation;
using libera::lasercubenet::LaserCubeNetRemoteEvidence;
using libera::lasercubenet::LaserCubeNetStatus;
using Clock = std::chrono::steady_clock;

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
    for (std::size_t index = 0; index < 4; ++index) {
        output[index] = static_cast<std::uint8_t>((value >> (8U * index)) & 0xffU);
    }
}

struct Event {
    std::uint64_t sequence = 0;
    std::uint8_t command = 0;
    bool data = false;
    bool lit = false;
    std::uint16_t senderPort = 0;
    int outputRequest = -1;
};

struct PendingStatus {
    libera::net::udp::endpoint destination;
    std::array<std::uint8_t, 64> packet{};
};

struct PendingAck {
    libera::net::udp::endpoint destination;
    std::array<std::uint8_t, 4> packet{};
};

class LoopbackCube final {
public:
    LoopbackCube()
        : io(libera::net::shared_io_context()), commandSocket(*io), dataSocket(*io) {
        CHECK(!commandSocket.open_v4());
        CHECK(!dataSocket.open_v4());
        std::error_code error;
        const auto loopback = libera::net::asio::ip::make_address("127.0.0.1", error);
        CHECK(!error);
        CHECK(!commandSocket.bind(loopback, 0));
        CHECK(!dataSocket.bind(loopback, 0));
        commandPort = commandSocket.raw().local_endpoint(error).port();
        CHECK(!error);
        dataPort = dataSocket.raw().local_endpoint(error).port();
        CHECK(!error);
        commandWorker = std::thread([this] { commandLoop(); });
        dataWorker = std::thread([this] { dataLoop(); });
    }

    ~LoopbackCube() {
        running.store(false);
        commandSocket.close();
        dataSocket.close();
        changed.notify_all();
        if (commandWorker.joinable()) commandWorker.join();
        if (dataWorker.joinable()) dataWorker.join();
    }

    LoopbackCube(const LoopbackCube&) = delete;
    LoopbackCube& operator=(const LoopbackCube&) = delete;

    LaserCubeNetStatus status() const {
        LaserCubeNetStatus value;
        value.payloadVersion = 0;
        value.firmwareMajor = firmwareMajor.load();
        value.firmwareMinor = firmwareMinor.load();
        value.firmwareVersion =
            std::to_string(value.firmwareMajor) + "." +
            std::to_string(value.firmwareMinor);
        value.outputEnabled = suppressOutputFlag.load() ? false : outputEnabled.load();
        value.pointRate = 30000;
        value.pointRateMax = 30000;
        value.bufferFree = bufferFree.load();
        value.bufferMax = bufferMax.load();
        value.serialNumber = "010203040506";
        value.ipAddress = "127.0.0.1";
        value.modelNumber = 10;
        value.modelName = "LaserCube Test";
        value.lastSeen = Clock::now();
        return value;
    }

    LaserCubeNetNetworkConfig network() const {
        LaserCubeNetNetworkConfig config;
        config.discoveryDestinations = {"127.0.0.1"};
        config.localBindAddress = "127.0.0.1";
        config.discoveryBindPort = reservePort();
        config.commandPort = commandPort;
        config.dataPort = dataPort;
        config.sendTimeout = 20ms;
        config.receivePollTimeout = 5ms;
        config.discoveryWindow = 20ms;
        config.discoveryInterval = 5ms;
        return config;
    }

    void forceOutput(bool enabled) { outputEnabled.store(enabled); }
    void setFirmware(std::uint8_t major, std::uint8_t minor) {
        firmwareMajor.store(major);
        firmwareMinor.store(minor);
    }
    void suppressReportedOutputFlag(bool value) { suppressOutputFlag.store(value); }
    void forceBuffer(std::uint16_t free, std::uint16_t maximum) {
        bufferMax.store(maximum);
        bufferFree.store(free);
    }
    void setIgnoreAllOff(bool value) { ignoreAllOff.store(value); }
    void dropNextOff(int count = 1) { dropOff.store(count); }
    void dropNextClear(int count = 1) { dropClear.store(count); }
    void dropNextStatus(int count = 1) { dropStatus.store(count); }
    void malformNextStatus(int count = 1) { malformedStatus.store(count); }
    void reportDisabledForNextEnableStatuses(int count) {
        staleEnableStatuses.store(count);
    }
    void requirePostEnableData(bool value) {
        requirePostEnableSamples.store(value);
    }
    void holdStatus(bool value) { holdStatuses.store(value); }
    void clearPendingStatuses() {
        std::lock_guard<std::mutex> lock(mutex);
        pendingStatuses.clear();
    }
    void setAckReorder(bool value) { reorderAcks.store(value); }
    void setAckDuplicate(bool value) { duplicateAcks.store(value); }
    void dropNextAck(int count = 1) { dropAcks.store(count); }
    void holdNextAcks(int count, std::uint16_t excludedSenderPort = 0) {
        holdAckExcludedSenderPort.store(excludedSenderPort);
        holdAcks.store(count);
    }

    std::vector<Event> events() const {
        std::lock_guard<std::mutex> lock(mutex);
        return history;
    }

    std::size_t commandCount(std::uint8_t command) const {
        const auto snapshot = events();
        return static_cast<std::size_t>(std::count_if(
            snapshot.begin(), snapshot.end(),
            [&](const Event& event) { return !event.data && event.command == command; }));
    }

    std::size_t dataCount() const {
        const auto snapshot = events();
        return static_cast<std::size_t>(std::count_if(
            snapshot.begin(), snapshot.end(), [](const Event& event) { return event.data; }));
    }

    std::optional<std::uint16_t> lastCommandSender(std::uint8_t command) const {
        std::lock_guard<std::mutex> lock(mutex);
        const auto found = std::find_if(
            history.rbegin(), history.rend(), [&](const Event& event) {
                return !event.data && event.command == command;
            });
        if (found == history.rend()) return std::nullopt;
        return found->senderPort;
    }

    std::optional<std::uint16_t> waitForCommandSenderAfter(
        std::uint8_t command,
        std::uint64_t marker,
        Clock::time_point deadline,
        std::optional<std::uint16_t> excludedPort = std::nullopt) {
        std::unique_lock<std::mutex> lock(mutex);
        const bool found = changed.wait_until(lock, deadline, [&] {
            return std::any_of(history.begin(), history.end(), [&](const Event& event) {
                return event.sequence > marker && !event.data && event.command == command &&
                       (!excludedPort || event.senderPort != *excludedPort);
            });
        });
        if (!found) return std::nullopt;
        const auto event = std::find_if(
            history.rbegin(), history.rend(), [&](const Event& value) {
                return value.sequence > marker && !value.data && value.command == command &&
                       (!excludedPort || value.senderPort != *excludedPort);
            });
        return event->senderPort;
    }

    std::uint64_t lastSequence() const {
        std::lock_guard<std::mutex> lock(mutex);
        return history.empty() ? 0 : history.back().sequence;
    }

    bool waitForCommand(std::uint8_t command, std::size_t count, Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_until(lock, deadline, [&] {
            return static_cast<std::size_t>(std::count_if(
                       history.begin(), history.end(), [&](const Event& event) {
                           return !event.data && event.command == command;
                       })) >= count;
        });
    }

    bool waitForLitPacket(Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_until(lock, deadline, [&] {
            return std::any_of(history.begin(), history.end(),
                               [](const Event& event) { return event.data && event.lit; });
        });
    }

    bool waitForData(std::size_t count, Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_until(lock, deadline, [&] {
            return static_cast<std::size_t>(std::count_if(
                       history.begin(), history.end(),
                       [](const Event& event) { return event.data; })) >= count;
        });
    }

    bool waitForDataQuiet(
        std::chrono::milliseconds quietPeriod,
        Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        auto observedCount = history.size();
        auto quietUntil = Clock::now() + quietPeriod;
        while (Clock::now() < deadline) {
            changed.wait_until(lock, std::min(quietUntil, deadline));
            if (history.size() != observedCount) {
                observedCount = history.size();
                quietUntil = Clock::now() + quietPeriod;
                continue;
            }
            if (Clock::now() >= quietUntil) return true;
        }
        return false;
    }

    std::vector<std::uint16_t> waitForPendingPorts(
        std::size_t distinctCount, Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        changed.wait_until(lock, deadline, [&] {
            std::vector<std::uint16_t> ports;
            for (const auto& pending : pendingStatuses) {
                const auto port = pending.destination.port();
                if (std::find(ports.begin(), ports.end(), port) == ports.end()) {
                    ports.push_back(port);
                }
            }
            return ports.size() >= distinctCount;
        });
        std::vector<std::uint16_t> ports;
        for (const auto& pending : pendingStatuses) {
            const auto port = pending.destination.port();
            if (std::find(ports.begin(), ports.end(), port) == ports.end()) ports.push_back(port);
        }
        return ports;
    }

    bool waitForPendingStatus(std::uint16_t port, Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_until(lock, deadline, [&] {
            return std::any_of(
                pendingStatuses.begin(), pendingStatuses.end(),
                [&](const PendingStatus& pending) {
                    return pending.destination.port() == port;
                });
        });
    }

    std::optional<std::uint16_t> waitForPendingPortDifferentFrom(
        std::uint16_t retiredPort,
        Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        const bool found = changed.wait_until(lock, deadline, [&] {
            return std::any_of(
                pendingStatuses.begin(), pendingStatuses.end(),
                [&](const PendingStatus& pending) {
                    return pending.destination.port() != retiredPort;
                });
        });
        if (!found) return std::nullopt;
        const auto pending = std::find_if(
            pendingStatuses.rbegin(), pendingStatuses.rend(),
            [&](const PendingStatus& value) {
                return value.destination.port() != retiredPort;
            });
        return pending->destination.port();
    }

    std::optional<std::uint16_t> latestPendingStatusPort() const {
        std::lock_guard<std::mutex> lock(mutex);
        if (pendingStatuses.empty()) return std::nullopt;
        return pendingStatuses.back().destination.port();
    }

    bool waitForPendingStatusAfterOutputOn(
        std::uint64_t afterSequence, Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_until(lock, deadline, [&] {
            return std::any_of(history.begin(), history.end(), [&](const Event& event) {
                if (event.sequence <= afterSequence || event.data ||
                    event.command != LaserCubeNetConfig::CMD_SET_OUTPUT ||
                    event.outputRequest != 1) {
                    return false;
                }
                return std::any_of(
                    pendingStatuses.begin(), pendingStatuses.end(),
                    [&](const PendingStatus& pending) {
                        return pending.destination.port() == event.senderPort;
                    });
            });
        });
    }

    bool releaseOnePending(std::uint16_t port) {
        PendingStatus pending;
        {
            std::lock_guard<std::mutex> lock(mutex);
            const auto found = std::find_if(
                pendingStatuses.begin(), pendingStatuses.end(),
                [&](const PendingStatus& value) { return value.destination.port() == port; });
            if (found == pendingStatuses.end()) return false;
            pending = *found;
            pendingStatuses.erase(found);
            statusResponsesToRelease.push_back(pending);
            changed.notify_all();
        }
        return true;
    }

    bool releaseOnePendingMalformed(std::uint16_t port) {
        PendingStatus pending;
        {
            std::lock_guard<std::mutex> lock(mutex);
            const auto found = std::find_if(
                pendingStatuses.begin(), pendingStatuses.end(),
                [&](const PendingStatus& value) { return value.destination.port() == port; });
            if (found == pendingStatuses.end()) return false;
            pending = *found;
        }
        pending.packet[2] = 0xff;
        {
            std::lock_guard<std::mutex> lock(mutex);
            statusResponsesToRelease.push_back(pending);
            changed.notify_all();
        }
        return true;
    }

    bool releaseOnePendingWrongSerial(std::uint16_t port) {
        PendingStatus pending;
        {
            std::lock_guard<std::mutex> lock(mutex);
            const auto found = std::find_if(
                pendingStatuses.begin(), pendingStatuses.end(),
                [&](const PendingStatus& value) { return value.destination.port() == port; });
            if (found == pendingStatuses.end()) return false;
            pending = *found;
        }
        pending.packet[26] = 0x99;
        {
            std::lock_guard<std::mutex> lock(mutex);
            statusResponsesToRelease.push_back(pending);
            changed.notify_all();
        }
        return true;
    }

    bool releaseOnePendingFromWrongSender(std::uint16_t port) {
        PendingStatus pending;
        {
            std::lock_guard<std::mutex> lock(mutex);
            const auto found = std::find_if(
                pendingStatuses.begin(), pendingStatuses.end(),
                [&](const PendingStatus& value) { return value.destination.port() == port; });
            if (found == pendingStatuses.end()) return false;
            pending = *found;
        }
        libera::net::UdpSocket wrongSender(*io);
        std::error_code error;
        const auto loopback = libera::net::asio::ip::make_address("127.0.0.1", error);
        if (error || wrongSender.open_v4(false) || wrongSender.bind(loopback, 0, false)) {
            return false;
        }
        return !wrongSender.send_to(
            pending.packet.data(), pending.packet.size(), pending.destination, 20ms, false);
    }

    bool waitForPendingAcks(std::size_t count, Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_until(lock, deadline, [&] {
            return pendingAcks.size() >= count;
        });
    }

    std::vector<std::uint16_t> pendingAckPorts() const {
        std::lock_guard<std::mutex> lock(mutex);
        std::vector<std::uint16_t> ports;
        ports.reserve(pendingAcks.size());
        for (const auto& pending : pendingAcks) {
            ports.push_back(pending.destination.port());
        }
        return ports;
    }

    std::vector<std::uint8_t> pendingAckMessageNumbers() const {
        std::lock_guard<std::mutex> lock(mutex);
        std::vector<std::uint8_t> messageNumbers;
        messageNumbers.reserve(pendingAcks.size());
        for (const auto& pending : pendingAcks) {
            messageNumbers.push_back(pending.packet[1]);
        }
        return messageNumbers;
    }

    bool aliasOldestAckToFirstCurrentAck() {
        std::lock_guard<std::mutex> lock(mutex);
        if (pendingAcks.size() < 2) return false;
        pendingAcks.front().packet[1] = pendingAcks[1].packet[1];
        return true;
    }

    bool releaseOldestPendingAck() {
        std::lock_guard<std::mutex> lock(mutex);
        if (pendingAcks.empty()) return false;
        acknowledgementsToRelease.push_back(pendingAcks.front());
        pendingAcks.erase(pendingAcks.begin());
        changed.notify_all();
        return true;
    }

private:
    static std::uint16_t reservePort() {
        auto io = libera::net::shared_io_context();
        libera::net::UdpSocket socket(*io);
        CHECK(!socket.open_v4());
        std::error_code error;
        const auto loopback = libera::net::asio::ip::make_address("127.0.0.1", error);
        CHECK(!error);
        CHECK(!socket.bind(loopback, 0));
        const auto port = socket.raw().local_endpoint(error).port();
        CHECK(!error);
        socket.close();
        return port;
    }

    static bool consume(std::atomic<int>& counter) {
        int value = counter.load();
        while (value > 0) {
            if (counter.compare_exchange_weak(value, value - 1)) return true;
        }
        return false;
    }

    std::array<std::uint8_t, 64> statusPacket(bool malformed = false) const {
        const auto current = status();
        std::array<std::uint8_t, 64> packet{};
        packet[2] = malformed ? 0xff : 0;
        packet[3] = firmwareMajor.load();
        packet[4] = firmwareMinor.load();
        packet[5] = current.outputEnabled ? 1 : 0;
        writeLe32(&packet[10], current.pointRate);
        writeLe32(&packet[14], current.pointRateMax);
        writeLe16(&packet[19], current.bufferFree);
        writeLe16(&packet[21], current.bufferMax);
        packet[23] = 255;
        packet[24] = 25;
        packet[25] = 1;
        for (std::size_t index = 0; index < 6; ++index) packet[26 + index] = index + 1;
        packet[32] = 127;
        packet[35] = 1;
        packet[37] = 10;
        constexpr char name[] = "LaserCube Test";
        std::copy(std::begin(name), std::end(name), packet.begin() + 38);
        return packet;
    }

    void record(
        std::uint8_t command,
        bool data,
        bool lit,
        std::uint16_t senderPort,
        int outputRequest = -1) {
        std::lock_guard<std::mutex> lock(mutex);
        history.push_back(Event{++sequence, command, data, lit, senderPort, outputRequest});
        changed.notify_all();
    }

    void commandLoop() {
        while (running.load()) {
            std::optional<PendingStatus> pendingRelease;
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (!statusResponsesToRelease.empty()) {
                    pendingRelease = statusResponsesToRelease.front();
                    statusResponsesToRelease.erase(statusResponsesToRelease.begin());
                }
            }
            if (pendingRelease) {
                (void)commandSocket.send_to(
                    pendingRelease->packet.data(), pendingRelease->packet.size(),
                    pendingRelease->destination, 20ms, false);
                continue;
            }
            std::array<std::uint8_t, 128> input{};
            libera::net::udp::endpoint sender;
            std::size_t received = 0;
            const auto error = commandSocket.recv_from(
                input.data(), input.size(), sender, received, 2ms, false);
            if (error || received == 0) continue;
            const int outputRequest =
                input[0] == LaserCubeNetConfig::CMD_SET_OUTPUT && received >= 2
                    ? static_cast<int>(input[1])
                    : -1;
            record(input[0], false, false, sender.port(), outputRequest);

            if (input[0] == LaserCubeNetConfig::CMD_SET_OUTPUT && received >= 2) {
                if (input[1] == 0) {
                    enableWaitingForSamples.store(false);
                    if (!consume(dropOff) && !ignoreAllOff.load()) outputEnabled.store(false);
                } else {
                    const bool waitForSamples = requirePostEnableSamples.load();
                    enableWaitingForSamples.store(waitForSamples);
                    outputEnabled.store(!waitForSamples);
                }
            } else if (input[0] == LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER) {
                if (!consume(dropClear)) bufferFree.store(bufferMax.load());
            } else if (input[0] == LaserCubeNetConfig::CMD_GET_FULL_INFO) {
                if (consume(dropStatus)) continue;
                auto packet = statusPacket(consume(malformedStatus));
                if (consume(staleEnableStatuses)) {
                    packet[5] &= static_cast<std::uint8_t>(~std::uint8_t{1});
                }
                if (holdStatuses.load()) {
                    std::lock_guard<std::mutex> lock(mutex);
                    pendingStatuses.push_back(PendingStatus{sender, packet});
                    changed.notify_all();
                } else {
                    (void)commandSocket.send_to(
                        packet.data(), packet.size(), sender, 20ms, false);
                }
                continue;
            }
            const std::array<std::uint8_t, 2> acknowledgement{input[0], 0};
            (void)commandSocket.send_to(
                acknowledgement.data(), acknowledgement.size(), sender, 20ms, false);
        }
    }

    void dataLoop() {
        std::optional<std::pair<libera::net::udp::endpoint, std::array<std::uint8_t, 4>>> heldAck;
        while (running.load()) {
            std::optional<PendingAck> pendingRelease;
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (!acknowledgementsToRelease.empty()) {
                    pendingRelease = acknowledgementsToRelease.front();
                    acknowledgementsToRelease.erase(acknowledgementsToRelease.begin());
                }
            }
            if (pendingRelease) {
                (void)dataSocket.send_to(
                    pendingRelease->packet.data(), pendingRelease->packet.size(),
                    pendingRelease->destination, 20ms, false);
                continue;
            }
            std::array<std::uint8_t, 1600> input{};
            libera::net::udp::endpoint sender;
            std::size_t received = 0;
            const auto error = dataSocket.recv_from(
                input.data(), input.size(), sender, received, 2ms, false);
            if (error || received < 4 || input[0] != LaserCubeNetConfig::CMD_SAMPLE_DATA) continue;
            bool lit = false;
            for (std::size_t offset = 4; offset + 9 < received; offset += 10) {
                lit = lit || std::any_of(input.begin() + static_cast<std::ptrdiff_t>(offset + 4),
                                         input.begin() + static_cast<std::ptrdiff_t>(offset + 10),
                                         [](std::uint8_t value) { return value != 0; });
            }
            record(input[0], true, lit, sender.port());
            if (enableWaitingForSamples.exchange(false)) {
                outputEnabled.store(true);
            }
            const auto points = static_cast<std::uint16_t>((received - 4) / 10);
            const auto free = bufferFree.load();
            bufferFree.store(points >= free ? 0 : static_cast<std::uint16_t>(free - points));
            std::array<std::uint8_t, 4> ack{
                LaserCubeNetConfig::CMD_GET_RINGBUFFER_FREE, input[2], 0, 0};
            writeLe16(&ack[2], bufferFree.load());
            if (consume(dropAcks)) continue;
            if (sender.port() != holdAckExcludedSenderPort.load() && consume(holdAcks)) {
                std::lock_guard<std::mutex> lock(mutex);
                pendingAcks.push_back(PendingAck{sender, ack});
                changed.notify_all();
                continue;
            }
            if (reorderAcks.load() && !heldAck) {
                heldAck = std::make_pair(sender, ack);
                continue;
            }
            (void)dataSocket.send_to(ack.data(), ack.size(), sender, 20ms, false);
            if (duplicateAcks.load()) {
                (void)dataSocket.send_to(ack.data(), ack.size(), sender, 20ms, false);
            }
            if (heldAck) {
                (void)dataSocket.send_to(
                    heldAck->second.data(), heldAck->second.size(), heldAck->first, 20ms, false);
                heldAck.reset();
            }
        }
    }

    std::shared_ptr<libera::net::asio::io_context> io;
    libera::net::UdpSocket commandSocket;
    libera::net::UdpSocket dataSocket;
    std::uint16_t commandPort = 0;
    std::uint16_t dataPort = 0;
    std::atomic<bool> running{true};
    std::atomic<bool> outputEnabled{true};
    std::atomic<std::uint8_t> firmwareMajor{1};
    std::atomic<std::uint8_t> firmwareMinor{13};
    std::atomic<bool> suppressOutputFlag{false};
    std::atomic<std::uint16_t> bufferFree{500};
    std::atomic<std::uint16_t> bufferMax{1000};
    std::atomic<bool> ignoreAllOff{false};
    std::atomic<int> dropOff{0};
    std::atomic<int> dropClear{0};
    std::atomic<int> dropStatus{0};
    std::atomic<int> malformedStatus{0};
    std::atomic<int> staleEnableStatuses{0};
    std::atomic<bool> requirePostEnableSamples{false};
    std::atomic<bool> enableWaitingForSamples{false};
    std::atomic<bool> holdStatuses{false};
    std::atomic<bool> reorderAcks{false};
    std::atomic<bool> duplicateAcks{false};
    std::atomic<int> dropAcks{0};
    std::atomic<int> holdAcks{0};
    std::atomic<std::uint16_t> holdAckExcludedSenderPort{0};
    mutable std::mutex mutex;
    std::condition_variable changed;
    std::uint64_t sequence = 0;
    std::vector<Event> history;
    std::vector<PendingStatus> pendingStatuses;
    std::vector<PendingStatus> statusResponsesToRelease;
    std::vector<PendingAck> pendingAcks;
    std::vector<PendingAck> acknowledgementsToRelease;
    std::thread commandWorker;
    std::thread dataWorker;
};

void installLitCallback(LaserCubeNetController& controller) {
    controller.setPointCallback([](const libera::core::PointFillRequest& request,
                                   std::vector<libera::core::LaserPoint>& points) {
        points.assign(request.minimumPointsRequired,
                      libera::core::LaserPoint{0.25f, -0.25f, 1.0f, 0.5f, 0.25f});
    });
}

std::vector<Event> after(const std::vector<Event>& events, std::uint64_t sequence) {
    std::vector<Event> result;
    std::copy_if(events.begin(), events.end(), std::back_inserter(result),
                 [&](const Event& event) { return event.sequence > sequence; });
    return result;
}

void checkExactCommandOrder(
    const std::vector<Event>& events,
    const std::vector<std::uint8_t>& expected,
    std::uint16_t senderPort = 0) {
    std::vector<std::uint8_t> actual;
    for (const auto& event : events) {
        if (!event.data && (senderPort == 0 || event.senderPort == senderPort)) {
            actual.push_back(event.command);
        }
    }
    if (actual == expected) return;
    std::cerr << "command order mismatch; expected:";
    for (const auto command : expected) std::cerr << " 0x" << std::hex << int(command);
    std::cerr << " actual:";
    for (const auto command : actual) std::cerr << " 0x" << std::hex << int(command);
    std::cerr << std::dec << '\n';
    CHECK(false);
}

void checkCommandPrefixWithRepeatedTail(
    const std::vector<Event>& events,
    const std::vector<std::uint8_t>& expected,
    std::uint16_t senderPort) {
    CHECK(!expected.empty());
    std::vector<std::uint8_t> actual;
    for (const auto& event : events) {
        if (!event.data && event.senderPort == senderPort) {
            actual.push_back(event.command);
        }
    }
    const auto prefixSize = expected.size() - 1;
    const bool prefixMatches =
        actual.size() >= expected.size() &&
        std::equal(expected.begin(), expected.begin() + static_cast<std::ptrdiff_t>(prefixSize),
                   actual.begin());
    const bool tailMatches = prefixMatches &&
        std::all_of(actual.begin() + static_cast<std::ptrdiff_t>(prefixSize), actual.end(),
                    [&](std::uint8_t command) { return command == expected.back(); });
    if (prefixMatches && tailMatches) return;
    std::cerr << "command prefix/tail mismatch; expected prefix:";
    for (std::size_t index = 0; index < prefixSize; ++index) {
        std::cerr << " 0x" << std::hex << int(expected[index]);
    }
    std::cerr << " then one-or-more 0x" << std::hex << int(expected.back()) << " actual:";
    for (const auto command : actual) std::cerr << " 0x" << std::hex << int(command);
    std::cerr << std::dec << '\n';
    CHECK(false);
}

void connectEnableDisableShutdownIsOrdered() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    const auto connected = controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms));
    CHECK(connected);
    CHECK(connected->evidence == LaserCubeNetRemoteEvidence::DeviceReportedDisabled);
    CHECK(connected->hasFreshStatus && connected->bufferConfirmedEmpty);
    const std::vector<std::uint8_t> expected{
        LaserCubeNetConfig::CMD_SET_OUTPUT,
        LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER,
        LaserCubeNetConfig::CMD_SET_OUTPUT,
        LaserCubeNetConfig::CMD_ENABLE_BUFFER_RESPONSE,
        LaserCubeNetConfig::CMD_SET_ILDA_RATE,
        LaserCubeNetConfig::CMD_GET_FULL_INFO};
    const auto initial = cube.events();
    checkCommandPrefixWithRepeatedTail(initial, expected, initial.front().senderPort);
    CHECK(std::none_of(initial.begin(), initial.end(), [](const Event& event) { return event.data; }));

    installLitCallback(controller);
    controller.startThread();
    cube.setAckReorder(true);
    cube.setAckDuplicate(true);
    const auto enabled = controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms));
    CHECK(enabled && enabled->evidence == LaserCubeNetRemoteEvidence::DeviceReportedEnabled);
    const auto onCount = cube.commandCount(LaserCubeNetConfig::CMD_SET_OUTPUT);
    CHECK(controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms)));
    CHECK(cube.commandCount(LaserCubeNetConfig::CMD_SET_OUTPUT) == onCount);
    CHECK(cube.waitForLitPacket(Clock::now() + 1s));
    const auto dataBeforeDroppedAck = cube.dataCount();
    cube.dropNextAck();
    CHECK(cube.waitForData(dataBeforeDroppedAck + 1, Clock::now() + 1s));

    const auto enabledEvents = cube.events();
    const auto on = std::find_if(enabledEvents.begin(), enabledEvents.end(), [](const Event& event) {
        return !event.data && event.command == LaserCubeNetConfig::CMD_SET_OUTPUT &&
               event.outputRequest == 1;
    });
    CHECK(on != enabledEvents.end());
    CHECK(std::any_of(enabledEvents.begin(), enabledEvents.end(), [&](const Event& event) {
        return event.data && !event.lit && event.sequence < on->sequence;
    }));
    CHECK(std::none_of(enabledEvents.begin(), enabledEvents.end(), [&](const Event& event) {
        return event.data && event.lit && event.sequence < on->sequence;
    }));

    const auto marker = cube.lastSequence();
    const auto disabled = controller.disableDark(LaserCubeNetOperation::withTimeout(500ms));
    if (!disabled) {
        std::cerr << "disable failed: " << disabled.error().message()
                  << " evidence=" << static_cast<int>(disabled->evidence)
                  << " fresh=" << disabled->hasFreshStatus
                  << " output=" << disabled->status.outputEnabled
                  << " free=" << disabled->status.bufferFree
                  << " max=" << disabled->status.bufferMax << '\n';
    }
    CHECK(disabled && !controller.isArmed());
    const auto disabledEvents = after(cube.events(), marker);
    const auto firstDisableOff = std::find_if(
        disabledEvents.begin(), disabledEvents.end(), [](const Event& event) {
            return !event.data && event.command == LaserCubeNetConfig::CMD_SET_OUTPUT;
        });
    CHECK(firstDisableOff != disabledEvents.end());
    checkCommandPrefixWithRepeatedTail(
        disabledEvents,
        {LaserCubeNetConfig::CMD_SET_OUTPUT,
         LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER,
         LaserCubeNetConfig::CMD_SET_OUTPUT,
         LaserCubeNetConfig::CMD_GET_FULL_INFO},
        firstDisableOff->senderPort);
    // Any packet observed here was already in flight from the retired output
    // generation. The blocked-callback race test below proves stale prepared
    // work cannot send after the generation changes.

    const auto closeMarker = cube.lastSequence();
    const auto shutdown = controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms));
    CHECK(shutdown && shutdown->evidence == LaserCubeNetRemoteEvidence::DeviceReportedDisabled);
    const auto closeEvents = after(cube.events(), closeMarker);
    const auto firstCloseOff = std::find_if(
        closeEvents.begin(), closeEvents.end(), [](const Event& event) {
            return !event.data && event.command == LaserCubeNetConfig::CMD_SET_OUTPUT;
        });
    CHECK(firstCloseOff != closeEvents.end());
    checkCommandPrefixWithRepeatedTail(
        closeEvents,
        {LaserCubeNetConfig::CMD_SET_OUTPUT,
         LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER,
         LaserCubeNetConfig::CMD_SET_OUTPUT,
         LaserCubeNetConfig::CMD_GET_FULL_INFO},
        firstCloseOff->senderPort);
    CHECK(std::none_of(closeEvents.begin(), closeEvents.end(),
                       [](const Event& event) { return event.data; }));
}

void enableWaitsForDeviceReportedStateToConverge() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    installLitCallback(controller);
    controller.startThread();

    const auto statusCount =
        cube.commandCount(LaserCubeNetConfig::CMD_GET_FULL_INFO);
    cube.requirePostEnableData(true);
    cube.reportDisabledForNextEnableStatuses(2);
    const auto enabled =
        controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms));
    CHECK(enabled);
    CHECK(enabled->evidence == LaserCubeNetRemoteEvidence::DeviceReportedEnabled);
    CHECK(enabled->hasFreshStatus);
    CHECK(enabled->status.outputEnabled);
    CHECK(cube.commandCount(LaserCubeNetConfig::CMD_GET_FULL_INFO) >= statusCount + 3);
    const auto events = cube.events();
    const auto outputOn = std::find_if(events.begin(), events.end(), [](const Event& event) {
        return !event.data && event.command == LaserCubeNetConfig::CMD_SET_OUTPUT &&
               event.outputRequest == 1;
    });
    CHECK(outputOn != events.end());
    CHECK(std::any_of(outputOn, events.end(), [](const Event& event) {
        return event.data && !event.lit;
    }));

    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

void ultra117UsesAcknowledgedEnableEvidence() {
    LoopbackCube cube;
    cube.setFirmware(1, 17);
    cube.suppressReportedOutputFlag(true);
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    installLitCallback(controller);
    controller.startThread();

    const auto enabled =
        controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms));
    CHECK(enabled);
    CHECK(enabled->evidence ==
          LaserCubeNetRemoteEvidence::DeviceAcknowledgedEnabled);
    CHECK(enabled->hasFreshStatus);
    CHECK(!enabled->status.outputEnabled);

    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

void bufferCapabilityAndDroppedTrafficAreHandled() {
    {
        LoopbackCube cube;
        cube.forceBuffer(0, 0);
        LaserCubeNetControllerInfo info(cube.status());
        LaserCubeNetController controller(info, cube.network());
        const auto result = controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms));
        CHECK(result && !result->bufferCapacitySupported && !result->bufferConfirmedEmpty);
    }
    {
        LoopbackCube cube;
        cube.dropNextOff();
        LaserCubeNetControllerInfo info(cube.status());
        LaserCubeNetController controller(info, cube.network());
        const auto result = controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms));
        CHECK(result);
        CHECK(cube.commandCount(LaserCubeNetConfig::CMD_SET_OUTPUT) >= 2);
    }
    {
        LoopbackCube cube;
        cube.dropNextClear();
        LaserCubeNetControllerInfo info(cube.status());
        LaserCubeNetController controller(info, cube.network());
        const auto result = controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms));
        CHECK(result);
        CHECK(cube.commandCount(LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER) >= 2);
    }
    {
        LoopbackCube cube;
        cube.dropNextStatus();
        LaserCubeNetControllerInfo info(cube.status());
        LaserCubeNetController controller(info, cube.network());
        const auto result = controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms));
        CHECK(result);
        CHECK(cube.commandCount(LaserCubeNetConfig::CMD_GET_FULL_INFO) >= 2);
    }
    {
        LoopbackCube cube;
        cube.malformNextStatus();
        LaserCubeNetControllerInfo info(cube.status());
        LaserCubeNetController controller(info, cube.network());
        const auto result = controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms));
        CHECK(result);
        CHECK(cube.commandCount(LaserCubeNetConfig::CMD_GET_FULL_INFO) >= 2);
    }
    {
        LoopbackCube cube;
        cube.setIgnoreAllOff(true);
        LaserCubeNetControllerInfo info(cube.status());
        LaserCubeNetController controller(info, cube.network());
        const auto result = controller.connectDark(info, LaserCubeNetOperation::withTimeout(80ms));
        CHECK(!result);
        CHECK(result->evidence == LaserCubeNetRemoteEvidence::HostDarkRequested);
        CHECK(!controller.isArmed());
    }
}

void rotatedSocketRejectsDelayedPriorStatus() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));

    cube.holdStatus(true);
    const auto first = controller.disableDark(LaserCubeNetOperation::withTimeout(150ms));
    CHECK(!first && first->evidence == LaserCubeNetRemoteEvidence::HostDarkRequested);
    CHECK(!cube.waitForPendingPorts(1, Clock::now() + 200ms).empty());
    const auto oldPort = cube.latestPendingStatusPort();
    CHECK(oldPort.has_value());

    cube.forceOutput(true);
    cube.setIgnoreAllOff(true);
    std::atomic<bool> cancelSecond{false};
    std::optional<LaserCubeNetLifecycleResult> second;
    std::thread operation([&] {
        second.emplace(controller.disableDark(LaserCubeNetOperation{
            Clock::now() + 2s, [&] { return cancelSecond.load(); }}));
    });
    const auto newPort = cube.waitForPendingPortDifferentFrom(
        *oldPort, Clock::now() + 500ms);
    CHECK(newPort.has_value());
    CHECK(cube.releaseOnePending(*oldPort));
    cube.holdStatus(false);
    CHECK(cube.releaseOnePending(*newPort));
    // Observe the exact public partial-evidence state rather than inferring it
    // from server command arrival order. The report cache has independent
    // synchronization so it remains observable while the lifecycle lock is
    // held by this operation.
    auto observed = controller.getLastLifecycleReport();
    const auto reportDeadline = Clock::now() + 1s;
    while ((!observed.hasFreshStatus || !observed.status.outputEnabled ||
            observed.evidence != LaserCubeNetRemoteEvidence::HostDarkRequested) &&
           Clock::now() < reportDeadline) {
        std::this_thread::yield();
        observed = controller.getLastLifecycleReport();
    }
    CHECK(observed.hasFreshStatus && observed.status.outputEnabled);
    CHECK(observed.evidence == LaserCubeNetRemoteEvidence::HostDarkRequested);
    cancelSecond.store(true);
    operation.join();
    CHECK(second.has_value() && !*second);
    CHECK(second->value().evidence == LaserCubeNetRemoteEvidence::HostDarkRequested);
    if (!second->value().hasFreshStatus || !second->value().status.outputEnabled) {
        std::cerr << "stale test report fresh=" << second->value().hasFreshStatus
                  << " output=" << second->value().status.outputEnabled
                  << " error=" << second->error().message() << '\n';
    }
    CHECK(second->value().hasFreshStatus && second->value().status.outputEnabled);
}

void cancellationPreservesPartialEvidence() {
    {
        LoopbackCube cube;
        LaserCubeNetControllerInfo info(cube.status());
        LaserCubeNetController controller(info, cube.network());
        CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
        std::atomic<int> checks{0};
        const LaserCubeNetOperation cancelAfterClear{
            Clock::now() + 1s, [&] { return checks.fetch_add(1) >= 2; }};
        const auto marker = cube.lastSequence();
        const auto offCount = cube.commandCount(LaserCubeNetConfig::CMD_SET_OUTPUT);
        const auto clearCount = cube.commandCount(LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER);
        const auto result = controller.disableDark(cancelAfterClear);
        CHECK(!result && result.error() == std::errc::operation_canceled);
        CHECK(result->evidence == LaserCubeNetRemoteEvidence::HostDarkRequested);
        CHECK(cube.waitForCommand(
            LaserCubeNetConfig::CMD_SET_OUTPUT, offCount + 1, Clock::now() + 500ms));
        CHECK(cube.waitForCommand(
            LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER, clearCount + 1, Clock::now() + 500ms));
        const auto events = after(cube.events(), marker);
        const auto firstOff = std::find_if(events.begin(), events.end(), [](const Event& event) {
            return !event.data && event.command == LaserCubeNetConfig::CMD_SET_OUTPUT;
        });
        CHECK(firstOff != events.end());
        checkExactCommandOrder(
            events,
            {LaserCubeNetConfig::CMD_SET_OUTPUT,
             LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER},
            firstOff->senderPort);
    }
    {
        LoopbackCube cube;
        LaserCubeNetControllerInfo info(cube.status());
        LaserCubeNetController controller(info, cube.network());
        CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
        installLitCallback(controller);
        cube.holdStatus(true);
        std::atomic<bool> cancelled{false};
        std::optional<LaserCubeNetLifecycleResult> result;
        const auto enableMarker = cube.lastSequence();
        std::thread operation([&] {
            result.emplace(controller.enableOutput(LaserCubeNetOperation{
                Clock::now() + 1s, [&] { return cancelled.load(); }}));
        });
        CHECK(cube.waitForPendingStatusAfterOutputOn(enableMarker, Clock::now() + 500ms));
        cancelled.store(true);
        operation.join();
        CHECK(result.has_value() && !*result);
        if (result->value().evidence != LaserCubeNetRemoteEvidence::HostEnableRequested) {
            std::cerr << "enable cancellation evidence "
                      << static_cast<int>(result->value().evidence)
                      << " error " << result->error().message() << '\n';
        }
        CHECK(result->value().evidence == LaserCubeNetRemoteEvidence::HostEnableRequested);
        CHECK(!controller.isArmed());
    }
}

void disablePreemptsBlockedEnableCommit() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    installLitCallback(controller);
    controller.startThread();

    cube.holdStatus(true);
    const auto enableMarker = cube.lastSequence();
    std::optional<LaserCubeNetLifecycleResult> enableResult;
    std::thread enable([&] {
        enableResult.emplace(
            controller.enableOutput(LaserCubeNetOperation::withTimeout(2s)));
    });
    CHECK(cube.waitForPendingStatusAfterOutputOn(enableMarker, Clock::now() + 500ms));

    const auto enableEvents = after(cube.events(), enableMarker);
    const auto outputOn = std::find_if(
        enableEvents.begin(), enableEvents.end(), [](const Event& event) {
            return !event.data && event.command == LaserCubeNetConfig::CMD_SET_OUTPUT &&
                   event.outputRequest == 1;
        });
    CHECK(outputOn != enableEvents.end());

    std::optional<LaserCubeNetLifecycleResult> disableResult;
    std::thread disable([&] {
        disableResult.emplace(
            controller.disableDark(LaserCubeNetOperation::withTimeout(2s)));
    });
    const auto preemptDeadline = Clock::now() + 500ms;
    while (controller.contentSource() !=
               libera::core::LaserController::ContentSource::None &&
           Clock::now() < preemptDeadline) {
        std::this_thread::yield();
    }
    CHECK(controller.contentSource() ==
          libera::core::LaserController::ContentSource::None);

    cube.holdStatus(false);
    CHECK(cube.releaseOnePending(outputOn->senderPort));
    enable.join();
    disable.join();

    CHECK(enableResult.has_value() && !*enableResult);
    CHECK(enableResult->error() == std::errc::operation_canceled);
    CHECK(enableResult->value().evidence ==
          LaserCubeNetRemoteEvidence::HostEnableRequested);
    CHECK(enableResult->value().hasFreshStatus &&
          enableResult->value().status.outputEnabled);
    CHECK(disableResult.has_value() && *disableResult);
    CHECK(disableResult->value().evidence ==
          LaserCubeNetRemoteEvidence::DeviceReportedDisabled);
    CHECK(!controller.isArmed());
    const auto finalEvents = cube.events();
    CHECK(std::none_of(finalEvents.begin(), finalEvents.end(), [](const Event& event) {
        return event.data && event.lit;
    }));
}

void blockedWorkerCannotLeakAcrossDisableReenable() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));

    std::mutex callbackMutex;
    std::condition_variable callbackChanged;
    bool callbackEntered = false;
    bool releaseCallback = false;
    controller.setPointCallback(
        [&](const libera::core::PointFillRequest& request,
            std::vector<libera::core::LaserPoint>& points) {
            {
                std::unique_lock<std::mutex> lock(callbackMutex);
                callbackEntered = true;
                callbackChanged.notify_all();
                callbackChanged.wait(lock, [&] { return releaseCallback; });
            }
            points.assign(request.minimumPointsRequired,
                          libera::core::LaserPoint{0.2f, 0.2f, 1.0f, 1.0f, 1.0f});
        });
    controller.startThread();
    CHECK(controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms)));
    {
        std::unique_lock<std::mutex> lock(callbackMutex);
        CHECK(callbackChanged.wait_until(
            lock, Clock::now() + 1s, [&] { return callbackEntered; }));
    }

    CHECK(controller.disableDark(LaserCubeNetOperation::withTimeout(500ms)));
    installLitCallback(controller);
    CHECK(controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms)));
    {
        std::lock_guard<std::mutex> lock(callbackMutex);
        releaseCallback = true;
    }
    callbackChanged.notify_all();
    CHECK(cube.waitForLitPacket(Clock::now() + 1s));
    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

void lateRetiredAckCannotSatisfyReenableStartup() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    installLitCallback(controller);
    controller.startThread();
    CHECK(controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms)));
    CHECK(cube.waitForLitPacket(Clock::now() + 1s));

    // Retain one ordinary streaming ACK so it arrives only after disable has
    // retired every acknowledgement slot owned by this output generation.
    cube.holdNextAcks(1);
    CHECK(cube.waitForPendingAcks(1, Clock::now() + 1s));
    const auto priorEpochPorts = cube.pendingAckPorts();
    CHECK(priorEpochPorts.size() == 1);
    CHECK(controller.disableDark(LaserCubeNetOperation::withTimeout(500ms)));
    controller.clearErrors();
    installLitCallback(controller);
    CHECK(cube.waitForDataQuiet(20ms, Clock::now() + 500ms));

    // A fresh enable sends exactly two startup blank packets. Hold both so the
    // test can prove that the orphan does not release their delivery barrier.
    cube.holdNextAcks(2, priorEpochPorts.front());
    std::mutex enableMutex;
    std::condition_variable enableChanged;
    bool enableFinished = false;
    std::optional<LaserCubeNetLifecycleResult> enableResult;
    const auto outputOnCount = cube.commandCount(LaserCubeNetConfig::CMD_SET_OUTPUT);
    std::thread enable([&] {
        auto result = controller.enableOutput(LaserCubeNetOperation::withTimeout(3s));
        {
            std::lock_guard<std::mutex> lock(enableMutex);
            enableResult.emplace(std::move(result));
            enableFinished = true;
        }
        enableChanged.notify_all();
    });
    CHECK(cube.waitForPendingAcks(3, Clock::now() + 500ms));
    const auto ackPorts = cube.pendingAckPorts();
    CHECK(ackPorts.size() == 3);
    CHECK(ackPorts[0] != ackPorts[1]);
    CHECK(ackPorts[1] == ackPorts[2]);
    // Synthetically reproduce the exact wire ambiguity after uint8 wrap: the
    // retired ACK carries the same message ID as the first current startup ACK.
    CHECK(cube.aliasOldestAckToFirstCurrentAck());
    const auto ackMessageNumbers = cube.pendingAckMessageNumbers();
    CHECK(ackMessageNumbers.size() == 3);
    CHECK(ackMessageNumbers[0] == ackMessageNumbers[1]);
    CHECK(ackMessageNumbers[1] != ackMessageNumbers[2]);

    // Release the retired ACK and one current ACK back-to-back. Even together
    // they cannot satisfy the two-ACK startup barrier for the current epoch.
    CHECK(cube.releaseOldestPendingAck());
    CHECK(cube.releaseOldestPendingAck());
    {
        std::unique_lock<std::mutex> lock(enableMutex);
        const bool finishedEarly =
            enableChanged.wait_for(lock, 20ms, [&] { return enableFinished; });
        CHECK(!finishedEarly);
    }
    CHECK(cube.commandCount(LaserCubeNetConfig::CMD_SET_OUTPUT) == outputOnCount);

    CHECK(cube.releaseOldestPendingAck());
    {
        std::unique_lock<std::mutex> lock(enableMutex);
        CHECK(enableChanged.wait_until(
            lock, Clock::now() + 500ms, [&] { return enableFinished; }));
    }
    enable.join();
    CHECK(enableResult.has_value() && *enableResult);
    CHECK(cube.commandCount(LaserCubeNetConfig::CMD_SET_OUTPUT) == outputOnCount + 1);

    const auto errors = controller.getErrors();
    CHECK(std::none_of(errors.begin(), errors.end(), [](const auto& error) {
        return error.code == "network.packet_loss";
    }));
    const auto recent = controller.getRecentEvent();
    CHECK(!recent || recent->code != "network.packet_loss");
    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

void failedStartupAckEpochCannotPoisonRetry() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    installLitCallback(controller);
    controller.startThread();

    // Deliver one startup ACK and hold the other past generic one-second slot
    // cleanup. Cleanup must not masquerade as the second delivery proof.
    cube.holdNextAcks(1);
    const auto outputOnCount = cube.commandCount(LaserCubeNetConfig::CMD_SET_OUTPUT);
    const auto first = controller.enableOutput(LaserCubeNetOperation::withTimeout(1200ms));
    CHECK(!first && first.error() == std::errc::timed_out);
    CHECK(cube.commandCount(LaserCubeNetConfig::CMD_SET_OUTPUT) == outputOnCount);
    CHECK(cube.waitForPendingAcks(1, Clock::now() + 500ms));
    const auto failedEpochPorts = cube.pendingAckPorts();
    CHECK(failedEpochPorts.size() == 1);

    controller.clearErrors();
    const auto retry = controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms));
    CHECK(retry && retry->evidence == LaserCubeNetRemoteEvidence::DeviceReportedEnabled);
    CHECK(cube.releaseOldestPendingAck());
    const auto errors = controller.getErrors();
    CHECK(std::none_of(errors.begin(), errors.end(), [](const auto& error) {
        return error.code == "network.packet_loss";
    }));
    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

void directQueryUsesExclusiveUnpublishedEpoch() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));

    cube.clearPendingStatuses();
    cube.holdStatus(true);
    const auto firstMarker = cube.lastSequence();
    const auto firstBarrier = Clock::now();
    const auto first = controller.queryFreshStatus(
        LaserCubeNetOperation::withTimeout(80ms), firstBarrier);
    CHECK(!first && first.error() == std::errc::timed_out);
    const auto firstPort = cube.waitForCommandSenderAfter(
        LaserCubeNetConfig::CMD_GET_FULL_INFO,
        firstMarker,
        Clock::now() + 200ms);
    CHECK(firstPort.has_value());

    std::mutex queryMutex;
    std::condition_variable queryChanged;
    bool queryFinished = false;
    std::optional<libera::expected<LaserCubeNetStatus>> second;
    const auto secondBarrier = Clock::now();
    const auto secondMarker = cube.lastSequence();
    std::thread query([&] {
        auto result = controller.queryFreshStatus(
            LaserCubeNetOperation::withTimeout(5s), secondBarrier);
        {
            std::lock_guard<std::mutex> lock(queryMutex);
            second.emplace(std::move(result));
            queryFinished = true;
        }
        queryChanged.notify_all();
    });
    const auto secondPort = cube.waitForCommandSenderAfter(
        LaserCubeNetConfig::CMD_GET_FULL_INFO,
        secondMarker,
        Clock::now() + 500ms,
        firstPort);
    CHECK(secondPort.has_value());
    CHECK(cube.releaseOnePending(*firstPort));
    {
        std::unique_lock<std::mutex> lock(queryMutex);
        CHECK(!queryChanged.wait_for(lock, 20ms, [&] { return queryFinished; }));
    }
    cube.holdStatus(false);
    query.join();
    CHECK(second.has_value() && *second);
    CHECK(second->value().probeEpochStartedAt > secondBarrier);
    CHECK(second->value().lastSeen > second->value().probeEpochStartedAt);

    const auto cachedBefore = controller.getLatestStatus();
    CHECK(cachedBefore && cachedBefore->bufferMax == 1000);
    const auto bufferBefore = controller.getBufferState();
    CHECK(bufferBefore && bufferBefore->totalBufferPoints == 1000);
    cube.forceBuffer(2500, 3000);
    const auto raw = controller.queryFreshStatus(
        LaserCubeNetOperation::withTimeout(500ms), Clock::now());
    CHECK(raw && raw->bufferMax == 3000 && raw->bufferFree == 2500);
    const auto stillCached = controller.getLatestStatus();
    CHECK(stillCached && stillCached->bufferMax == 1000);
    const auto bufferStillCached = controller.getBufferState();
    CHECK(bufferStillCached && bufferStillCached->totalBufferPoints == 1000);
    controller.updateDiscoveredStatus(*raw);
    const auto published = controller.getLatestStatus();
    CHECK(published && published->bufferMax == 3000);
    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

void directQueryDoesNotStarveStreamingAndSerializes() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    installLitCallback(controller);
    controller.startThread();
    CHECK(controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms)));
    CHECK(cube.waitForLitPacket(Clock::now() + 1s));

    cube.forceBuffer(1000, 1000);
    cube.holdStatus(true);
    std::optional<libera::expected<LaserCubeNetStatus>> heldResult;
    std::thread held([&] {
        heldResult.emplace(controller.queryFreshStatus(
            LaserCubeNetOperation::withTimeout(5s), Clock::now()));
    });
    const auto ports = cube.waitForPendingPorts(1, Clock::now() + 500ms);
    CHECK(!ports.empty());
    const auto dataBefore = cube.dataCount();
    CHECK(cube.waitForData(dataBefore + 1, Clock::now() + 500ms));

    const auto blocked = controller.queryFreshStatus(
        LaserCubeNetOperation::withTimeout(50ms), Clock::now());
    CHECK(!blocked && blocked.error() == std::errc::timed_out);
    cube.holdStatus(false);
    held.join();
    CHECK(heldResult.has_value() && *heldResult);

    const auto deadline = Clock::now() + 20ms;
    const auto invalid = controller.queryFreshStatus(
        LaserCubeNetOperation{deadline, [] { return false; }}, deadline);
    CHECK(!invalid && invalid.error() == std::errc::invalid_argument);
    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

void directQueryRejectsInvalidResponsesAndCancelsPromptly() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    cube.holdStatus(true);

    std::mutex queryMutex;
    std::condition_variable queryChanged;
    bool queryFinished = false;
    std::optional<libera::expected<LaserCubeNetStatus>> queryResult;
    const auto queryMarker = cube.lastSequence();
    std::thread query([&] {
        auto result = controller.queryFreshStatus(
            LaserCubeNetOperation::withTimeout(5s), Clock::now());
        {
            std::lock_guard<std::mutex> lock(queryMutex);
            queryResult.emplace(std::move(result));
            queryFinished = true;
        }
        queryChanged.notify_all();
    });
    const auto queryPort = cube.waitForCommandSenderAfter(
        LaserCubeNetConfig::CMD_GET_FULL_INFO,
        queryMarker,
        Clock::now() + 500ms);
    CHECK(queryPort.has_value());
    const auto port = *queryPort;
    CHECK(cube.waitForPendingStatus(port, Clock::now() + 500ms));
    CHECK(cube.releaseOnePendingFromWrongSender(port));
    CHECK(cube.releaseOnePendingMalformed(port));
    CHECK(cube.releaseOnePendingWrongSerial(port));
    {
        std::unique_lock<std::mutex> lock(queryMutex);
        CHECK(!queryChanged.wait_for(lock, 20ms, [&] { return queryFinished; }));
    }
    cube.holdStatus(false);
    query.join();
    CHECK(queryResult.has_value() && *queryResult);

    cube.holdStatus(true);
    std::atomic<bool> cancelled{false};
    std::optional<libera::expected<LaserCubeNetStatus>> cancelledResult;
    const auto cancelledMarker = cube.lastSequence();
    std::thread cancelledQuery([&] {
        cancelledResult.emplace(controller.queryFreshStatus(
            LaserCubeNetOperation{Clock::now() + 5s, [&] { return cancelled.load(); }},
            Clock::now()));
    });
    CHECK(cube.waitForCommandSenderAfter(
        LaserCubeNetConfig::CMD_GET_FULL_INFO,
        cancelledMarker,
        Clock::now() + 500ms,
        port));
    const auto cancelStarted = Clock::now();
    cancelled.store(true);
    cancelledQuery.join();
    CHECK(Clock::now() - cancelStarted < 100ms);
    CHECK(cancelledResult.has_value() && !*cancelledResult);
    CHECK(cancelledResult->error() == std::errc::operation_canceled);

    cube.holdStatus(false);
    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

void closeInvalidatesDirectQueryGeneration() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    LaserCubeNetController controller(info, cube.network());
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    cube.clearPendingStatuses();
    cube.holdStatus(true);

    std::optional<libera::expected<LaserCubeNetStatus>> oldResult;
    const auto oldMarker = cube.lastSequence();
    std::thread oldQuery([&] {
        oldResult.emplace(controller.queryFreshStatus(
            LaserCubeNetOperation::withTimeout(5s), Clock::now()));
    });
    const auto oldPort = cube.waitForCommandSenderAfter(
        LaserCubeNetConfig::CMD_GET_FULL_INFO,
        oldMarker,
        Clock::now() + 500ms);
    CHECK(oldPort.has_value());
    const auto closeStarted = Clock::now();
    controller.close();
    oldQuery.join();
    CHECK(Clock::now() - closeStarted < 100ms);
    CHECK(oldResult.has_value() && !*oldResult);

    cube.holdStatus(false);
    CHECK(controller.connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    cube.holdStatus(true);
    const auto newMarker = cube.lastSequence();
    std::mutex queryMutex;
    std::condition_variable queryChanged;
    bool queryFinished = false;
    std::optional<libera::expected<LaserCubeNetStatus>> newResult;
    std::thread newQuery([&] {
        auto result = controller.queryFreshStatus(
            LaserCubeNetOperation::withTimeout(5s), Clock::now());
        {
            std::lock_guard<std::mutex> lock(queryMutex);
            newResult.emplace(std::move(result));
            queryFinished = true;
        }
        queryChanged.notify_all();
    });
    const auto newPort = cube.waitForCommandSenderAfter(
        LaserCubeNetConfig::CMD_GET_FULL_INFO,
        newMarker,
        Clock::now() + 500ms,
        oldPort);
    CHECK(newPort.has_value());
    CHECK(cube.releaseOnePending(*oldPort));
    {
        std::unique_lock<std::mutex> lock(queryMutex);
        CHECK(!queryChanged.wait_for(lock, 20ms, [&] { return queryFinished; }));
    }
    // Let the next probe on the new epoch receive a freshly generated reply;
    // this avoids selecting among multiple intentionally retained retries.
    cube.holdStatus(false);
    newQuery.join();
    CHECK(newResult.has_value() && *newResult);
    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

class CountingManager final : public LaserCubeNetManager {
public:
    explicit CountingManager(LaserCubeNetNetworkConfig config)
        : LaserCubeNetManager(std::move(config)) {}

    int createCount = 0;
    std::weak_ptr<LaserCubeNetController> lastCreated;

private:
    ControllerPtr createController(const LaserCubeNetControllerInfo& info) override {
        ++createCount;
        auto controller = std::make_shared<LaserCubeNetController>(info, networkConfigCopy);
        lastCreated = controller;
        return controller;
    }

public:
    // The base does not expose its config to derived test managers, so the
    // test installs the same immutable value before connection attempts.
    LaserCubeNetNetworkConfig networkConfigCopy;
};

void managerDropsFailedInstanceAndReconnectStaysDark() {
    LoopbackCube cube;
    auto config = cube.network();
    CountingManager manager(config);
    manager.networkConfigCopy = config;
    LaserCubeNetControllerInfo info(cube.status());
    cube.holdStatus(true);
    auto first = manager.connectController(info);
    CHECK(!first && manager.createCount == 1 && manager.lastCreated.expired());
    cube.holdStatus(false);
    cube.setIgnoreAllOff(false);
    cube.forceOutput(true);
    cube.forceBuffer(500, 1000);
    auto second = manager.connectController(info);
    CHECK(second && manager.createCount == 2 && !second->isArmed());
    manager.closeAll();

    LoopbackCube reconnectCube;
    LaserCubeNetControllerInfo reconnectInfo(reconnectCube.status());
    LaserCubeNetController controller(reconnectInfo, reconnectCube.network());
    CHECK(controller.connectDark(reconnectInfo, LaserCubeNetOperation::withTimeout(500ms)));
    installLitCallback(controller);
    controller.startThread();
    CHECK(controller.enableOutput(LaserCubeNetOperation::withTimeout(500ms)));
    controller.close();
    reconnectCube.forceOutput(true);
    reconnectCube.forceBuffer(500, 1000);
    const auto marker = reconnectCube.lastSequence();
    const auto clearCount =
        reconnectCube.commandCount(LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER);
    controller.updateDiscoveredStatus(reconnectCube.status());
    CHECK(reconnectCube.waitForCommand(
        LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER,
        clearCount + 1,
        Clock::now() + 2s));
    const auto deadline = Clock::now() + 2s;
    while (!controller.hasConnection() && Clock::now() < deadline) std::this_thread::yield();
    CHECK(controller.hasConnection() && !controller.isArmed());
    const auto recovery = after(reconnectCube.events(), marker);
    CHECK(std::none_of(recovery.begin(), recovery.end(), [](const Event& event) {
        return !event.data && event.command == LaserCubeNetConfig::CMD_SET_OUTPUT &&
               event.outputRequest == 1;
    }));
    CHECK(controller.shutdownDark(LaserCubeNetOperation::withTimeout(500ms)));
}

void failedShutdownNeverClaimsConfirmation() {
    LoopbackCube cube;
    LaserCubeNetControllerInfo info(cube.status());
    auto controller = std::make_unique<LaserCubeNetController>(info, cube.network());
    CHECK(controller->connectDark(info, LaserCubeNetOperation::withTimeout(500ms)));
    cube.holdStatus(true);
    const auto result = controller->shutdownDark(LaserCubeNetOperation::withTimeout(60ms));
    CHECK(!result);
    CHECK(result->evidence == LaserCubeNetRemoteEvidence::HostDarkRequested);
    CHECK(result->evidence != LaserCubeNetRemoteEvidence::DeviceReportedDisabled);
    controller.reset();
}

} // namespace

int main() {
    connectEnableDisableShutdownIsOrdered();
    enableWaitsForDeviceReportedStateToConverge();
    ultra117UsesAcknowledgedEnableEvidence();
    bufferCapabilityAndDroppedTrafficAreHandled();
    rotatedSocketRejectsDelayedPriorStatus();
    cancellationPreservesPartialEvidence();
    disablePreemptsBlockedEnableCommit();
    blockedWorkerCannotLeakAcrossDisableReenable();
    lateRetiredAckCannotSatisfyReenableStartup();
    failedStartupAckEpochCannotPoisonRetry();
    directQueryUsesExclusiveUnpublishedEpoch();
    directQueryDoesNotStarveStreamingAndSerializes();
    directQueryRejectsInvalidResponsesAndCancelsPromptly();
    closeInvalidatesDirectQueryGeneration();
    managerDropsFailedInstanceAndReconnectStaysDark();
    failedShutdownNeverClaimsConfirmation();
    return 0;
}
