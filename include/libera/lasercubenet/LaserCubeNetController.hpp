#pragma once

#include "libera/core/Expected.hpp"
#include "libera/core/LaserController.hpp"
#include "libera/lasercubenet/LaserCubeNetConfig.hpp"
#include "libera/lasercubenet/LaserCubeNetControllerInfo.hpp"
#include "libera/net/NetService.hpp"
#include "libera/net/UdpSocket.hpp"

#include <atomic>
#include <array>
#include <cstdint>
#include <mutex>
#include <memory>
#include <string>
#include <chrono>
#include <optional>
#include <functional>

namespace libera::lasercubenet {

struct LaserCubeNetOperation {
    std::chrono::steady_clock::time_point deadline;
    std::function<bool()> cancelled;

    static LaserCubeNetOperation withTimeout(std::chrono::milliseconds timeout) {
        return LaserCubeNetOperation{
            std::chrono::steady_clock::now() + timeout,
            [] { return false; }};
    }
};

enum class LaserCubeNetRemoteEvidence {
    HostDarkRequested,
    DeviceReportedDisabled,
    HostEnableRequested,
    DeviceReportedEnabled
};

struct LaserCubeNetLifecycleReport {
    LaserCubeNetRemoteEvidence evidence = LaserCubeNetRemoteEvidence::HostDarkRequested;
    LaserCubeNetStatus status;
    bool bufferConfirmedEmpty = false;
};

class LaserCubeNetController : public core::LaserController {
public:
    explicit LaserCubeNetController(LaserCubeNetNetworkConfig networkConfig = {});
    LaserCubeNetController(LaserCubeNetControllerInfo info,
                           LaserCubeNetNetworkConfig networkConfig = {});
    ~LaserCubeNetController() override;

    libera::expected<void> connect(const LaserCubeNetControllerInfo& info);
    libera::expected<LaserCubeNetLifecycleReport> connectDark(
        const LaserCubeNetControllerInfo& info,
        const LaserCubeNetOperation& operation);
    libera::expected<LaserCubeNetLifecycleReport> enableOutput(
        const LaserCubeNetOperation& operation);
    libera::expected<LaserCubeNetLifecycleReport> disableDark(
        const LaserCubeNetOperation& operation);
    libera::expected<LaserCubeNetLifecycleReport> shutdownDark(
        const LaserCubeNetOperation& operation);
    void close();
    std::optional<core::BufferState> getBufferState() const override;
    void updateDiscoveredStatus(const LaserCubeNetStatus& status);
    std::optional<LaserCubeNetStatus> getLatestStatus() const;

protected:
    void run() override;
    void setPointRate(std::uint32_t pointRate) override;

private:
    libera::expected<void> connectToStatus(const LaserCubeNetStatus& status);
    bool reconnectToLatestStatus();

    /// Push the desired point rate to the device if it differs from the
    /// last-sent value, or if a forced re-push is pending after reconnect.
    void syncPointRate();

    bool sendPoints();
    bool sendData(const std::uint8_t* buffer, std::size_t size);
    bool sendCommand(std::uint8_t cmd, const std::uint8_t* payload, std::size_t size);
    bool sendCommandLocked(std::uint8_t cmd, const std::uint8_t* payload, std::size_t size);
    libera::expected<LaserCubeNetStatus> requestFreshStatus(
        const LaserCubeNetOperation& operation,
        std::chrono::steady_clock::time_point newerThan);
    bool operationStopped(const LaserCubeNetOperation& operation) const;
    void checkAcks();

    int getTotalBufferCapacity() const;

    std::shared_ptr<asio::io_context> io;
    std::unique_ptr<net::UdpSocket> dataSocket;
    std::unique_ptr<net::UdpSocket> commandSocket;
    net::udp::endpoint dataEndpoint;
    net::udp::endpoint commandEndpoint;

    std::string ipAddress;
    LaserCubeNetNetworkConfig networkConfig;
    std::mutex lifecycleMutex;

    std::atomic<int> pointBufferCapacity{1000};
    std::atomic<std::uint32_t> maxPointRate{60000};
    std::atomic<bool> networkConnected{false};
    std::atomic<bool> reconnectRequested{false};

    std::uint8_t messageNumber{0};
    std::uint8_t frameNumber{0};

    mutable std::mutex latestStatusMutex;
    std::optional<LaserCubeNetStatus> latestStatus;

    // Fixed-size array indexed by messageNumber (uint8_t wraps at 256).
    // A default-constructed time_point (epoch) means "slot empty".
    std::array<std::chrono::steady_clock::time_point, 256> messageTimes{};
    int pendingAckCount{0};

    // Timing helpers for buffer estimation and health tracking.
    std::chrono::steady_clock::time_point lastAckTime{};
    std::chrono::steady_clock::time_point lastAckWarningTime{};
    std::chrono::steady_clock::time_point lastUnexpectedAckSenderLogTime{};
    std::chrono::steady_clock::time_point lastDataSentTime{};
    int lastDataSentBufferSize{0};
    std::atomic<int> lastReportedBufferFullness{0};
    std::atomic<int> lastEstimatedBufferFullness{0};

    // Tracks the rate we've successfully told the device about. Worker-thread
    // only, so not atomic. Starts at 0 so the first tick always pushes.
    std::uint32_t lastSentPointRate{0};
    // Latched true on (re)connect so the next syncPointRate() tick force-sends
    // the rate even if it matches lastSentPointRate (stale after reconnect).
    bool pointRatePushNeeded{true};
};

} // namespace libera::lasercubenet
