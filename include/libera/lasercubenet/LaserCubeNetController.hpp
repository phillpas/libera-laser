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
#include <system_error>
#include <utility>

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
    None,
    HostDarkRequested,
    DeviceReportedDisabled,
    HostEnableRequested,
    DeviceReportedEnabled
};

struct LaserCubeNetLifecycleReport {
    LaserCubeNetRemoteEvidence evidence = LaserCubeNetRemoteEvidence::None;
    LaserCubeNetStatus status;
    bool hasFreshStatus = false;
    bool bufferCapacitySupported = false;
    bool bufferConfirmedEmpty = false;
};

// A lifecycle failure still carries the strongest evidence obtained before the
// error. This deliberately mirrors the small expected<T> surface used by
// existing callers while preserving HostDarkRequested/HostEnableRequested.
class LaserCubeNetLifecycleResult {
public:
    static LaserCubeNetLifecycleResult success(LaserCubeNetLifecycleReport report) {
        return LaserCubeNetLifecycleResult(std::move(report), {});
    }

    static LaserCubeNetLifecycleResult failure(
        std::error_code error,
        LaserCubeNetLifecycleReport report = {}) {
        return LaserCubeNetLifecycleResult(std::move(report), error);
    }

    explicit operator bool() const noexcept { return !errorValue; }
    const std::error_code& error() const noexcept { return errorValue; }
    LaserCubeNetLifecycleReport& value() { return reportValue; }
    const LaserCubeNetLifecycleReport& value() const { return reportValue; }
    LaserCubeNetLifecycleReport& operator*() { return reportValue; }
    const LaserCubeNetLifecycleReport& operator*() const { return reportValue; }
    LaserCubeNetLifecycleReport* operator->() { return &reportValue; }
    const LaserCubeNetLifecycleReport* operator->() const { return &reportValue; }

private:
    LaserCubeNetLifecycleResult(
        LaserCubeNetLifecycleReport report,
        std::error_code error)
        : reportValue(std::move(report)), errorValue(error) {}

    LaserCubeNetLifecycleReport reportValue;
    std::error_code errorValue;
};

class LaserCubeNetController : public core::LaserController {
public:
    explicit LaserCubeNetController(LaserCubeNetNetworkConfig networkConfig = {});
    LaserCubeNetController(LaserCubeNetControllerInfo info,
                           LaserCubeNetNetworkConfig networkConfig = {});
    ~LaserCubeNetController() override;

    libera::expected<void> connect(const LaserCubeNetControllerInfo& info);
    LaserCubeNetLifecycleResult connectDark(
        const LaserCubeNetControllerInfo& info,
        const LaserCubeNetOperation& operation);
    LaserCubeNetLifecycleResult enableOutput(
        const LaserCubeNetOperation& operation);
    LaserCubeNetLifecycleResult disableDark(
        const LaserCubeNetOperation& operation);
    LaserCubeNetLifecycleResult shutdownDark(
        const LaserCubeNetOperation& operation);
    LaserCubeNetLifecycleReport getLastLifecycleReport() const;
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
    void syncPointRateLocked();

    bool sendPoints();
    bool sendDataLocked(const std::uint8_t* buffer, std::size_t size);
    bool sendCommand(std::uint8_t cmd, const std::uint8_t* payload, std::size_t size);
    bool sendCommandLocked(std::uint8_t cmd, const std::uint8_t* payload, std::size_t size);
    bool rotateCommandSocketLocked();
    bool sendStartupBlankLocked(std::size_t packetCount = 2);
    bool confirmStartupDeliveryLocked(const LaserCubeNetOperation& operation);
    bool drainCommandResponsesLocked(const LaserCubeNetOperation& operation);
    libera::expected<LaserCubeNetStatus> requestFreshStatus(
        const LaserCubeNetOperation& operation,
        std::chrono::steady_clock::time_point newerThan);
    LaserCubeNetLifecycleResult darkSequenceLocked(
        const LaserCubeNetOperation& operation,
        bool configureTransport);
    bool operationStopped(const LaserCubeNetOperation& operation) const;
    void clearOutputIntentLocked() noexcept;
    std::uint64_t beginEnableIntentLocked() noexcept;
    bool commitEnableIntentLocked(std::uint64_t expectedGeneration) noexcept;
    void setLastLifecycleReport(const LaserCubeNetLifecycleReport& report);
    void bestEffortOffLocked() noexcept;
    bool lockLifecycle(
        const LaserCubeNetOperation& operation,
        std::unique_lock<std::timed_mutex>& lock);
    void checkAcksLocked();

    int getTotalBufferCapacity() const;

    std::shared_ptr<asio::io_context> io;
    std::unique_ptr<net::UdpSocket> dataSocket;
    std::unique_ptr<net::UdpSocket> commandSocket;
    net::udp::endpoint dataEndpoint;
    net::udp::endpoint commandEndpoint;

    std::string ipAddress;
    LaserCubeNetNetworkConfig networkConfig;
    mutable std::timed_mutex lifecycleMutex;
    std::mutex outputIntentMutex;
    mutable std::mutex lifecycleReportMutex;
    LaserCubeNetLifecycleReport lastLifecycleReport;
    bool remoteEnabledConfirmed = false;
    std::atomic<bool> streamingAllowed{false};
    std::atomic<std::uint64_t> outputGeneration{0};

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
