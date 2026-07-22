#include "libera/lasercubenet/LaserCubeNetController.hpp"

#include "libera/core/ByteRead.hpp"
#include "libera/core/ByteBuffer.hpp"
#include "libera/core/ControllerErrorTypes.hpp"
#include "libera/log/Log.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <thread>
#include <utility>

namespace libera::lasercubenet {

namespace error_types = libera::core::error_types;

constexpr auto ackDisconnectThreshold = std::chrono::milliseconds(500);
constexpr auto reconnectRetryDelay = std::chrono::milliseconds(100);

LaserCubeNetController::LaserCubeNetController(LaserCubeNetNetworkConfig networkConfigValue)
    : networkConfig(std::move(networkConfigValue)) {
    // Reuse the shared IO context so sockets share the same network thread.
    io = net::shared_io_context();
}

LaserCubeNetController::LaserCubeNetController(
    LaserCubeNetControllerInfo info,
    LaserCubeNetNetworkConfig networkConfigValue)
    : LaserCubeNetController(std::move(networkConfigValue)) {
    ipAddress = info.ipAddress();
}

LaserCubeNetController::~LaserCubeNetController() {
    stopThread();
    close();
}

void LaserCubeNetController::updateDiscoveredStatus(const LaserCubeNetStatus& status) {
    {
        std::lock_guard<std::mutex> lock(latestStatusMutex);
        latestStatus = status;
    }

    maxPointRate.store(status.pointRateMax, std::memory_order_relaxed);
    pointBufferCapacity.store(status.bufferMax, std::memory_order_relaxed);

    const auto clampedRate =
        LaserCubeNetConfig::clampPointRate(getPointRate(), status.pointRateMax);
    LaserControllerStreaming::setPointRate(clampedRate);

    if (!networkConnected.load(std::memory_order_relaxed)) {
        reconnectRequested.store(true, std::memory_order_relaxed);
    }
}

std::optional<LaserCubeNetStatus> LaserCubeNetController::getLatestStatus() const {
    std::lock_guard<std::mutex> lock(latestStatusMutex);
    return latestStatus;
}

libera::expected<void> LaserCubeNetController::connect(const LaserCubeNetControllerInfo& info) {
    const auto result = connectDark(
        info, LaserCubeNetOperation::withTimeout(std::chrono::milliseconds(750)));
    if (!result) {
        return libera::unexpected(result.error());
    }
    return {};
}

LaserCubeNetLifecycleResult LaserCubeNetController::connectDark(
    const LaserCubeNetControllerInfo& info,
    const LaserCubeNetOperation& operation) {
    std::unique_lock<std::timed_mutex> lock(lifecycleMutex, std::defer_lock);
    if (!lockLifecycle(operation, lock)) {
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(operation.cancelled && operation.cancelled()
                                     ? std::errc::operation_canceled
                                     : std::errc::timed_out));
    }

    clearOutputIntentLocked();
    clearContentSource();
    updateDiscoveredStatus(info.status());
    if (auto connected = connectToStatus(info.status()); !connected) {
        clearOutputIntentLocked();
        return LaserCubeNetLifecycleResult::failure(connected.error());
    }

    auto dark = darkSequenceLocked(operation, true);
    if (!dark) {
        clearOutputIntentLocked();
        lock.unlock();
        close();
        return dark;
    }
    return dark;
}

LaserCubeNetLifecycleResult LaserCubeNetController::enableOutput(
    const LaserCubeNetOperation& operation) {
    std::unique_lock<std::timed_mutex> lock(lifecycleMutex, std::defer_lock);
    if (!lockLifecycle(operation, lock)) {
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(operation.cancelled && operation.cancelled()
                                     ? std::errc::operation_canceled
                                     : std::errc::timed_out));
    }
    if (remoteEnabledConfirmed && isArmed()) {
        return LaserCubeNetLifecycleResult::success(getLastLifecycleReport());
    }
    if (contentSource() == ContentSource::None) {
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(std::errc::invalid_argument));
    }

    const auto enableGeneration = beginEnableIntentLocked();
    if (!networkConnected.load(std::memory_order_relaxed)) {
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(std::errc::not_connected));
    }

    // Push the configured rate and a real blank startup packet while output is
    // still remotely off. The streaming worker is gated until on is confirmed.
    if (!rotateCommandSocketLocked()) {
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(std::errc::io_error));
    }
    const auto desiredRate = getPointRate();
    core::ByteBuffer ratePayload;
    ratePayload.appendUInt32(desiredRate);
    if (!sendCommandLocked(
            LaserCubeNetConfig::CMD_SET_ILDA_RATE,
            ratePayload.data(), ratePayload.size()) ||
        !sendStartupBlankLocked() ||
        !confirmStartupDeliveryLocked(operation) ||
        !drainCommandResponsesLocked(operation)) {
        bestEffortOffLocked();
        clearOutputIntentLocked();
        return LaserCubeNetLifecycleResult::failure(
            operationStopped(operation)
                ? std::make_error_code(operation.cancelled && operation.cancelled()
                                           ? std::errc::operation_canceled
                                           : std::errc::timed_out)
                : std::make_error_code(std::errc::io_error));
    }
    lastSentPointRate = desiredRate;
    pointRatePushNeeded = false;

    const std::uint8_t enabled = 1;
    bool enableSuperseded = false;
    bool enableSent = false;
    {
        // Serialize the final epoch check with disableDark's pre-disarm. If
        // disable has already claimed priority, no output-on datagram is sent.
        std::lock_guard<std::mutex> intentLock(outputIntentMutex);
        enableSuperseded =
            outputGeneration.load(std::memory_order_acquire) != enableGeneration;
        if (!enableSuperseded) {
            enableSent = sendCommandLocked(
                LaserCubeNetConfig::CMD_SET_OUTPUT, &enabled, 1);
        }
    }
    if (enableSuperseded) {
        bestEffortOffLocked();
        clearOutputIntentLocked();
        LaserCubeNetLifecycleReport report;
        setLastLifecycleReport(report);
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(std::errc::operation_canceled), report);
    }
    if (!enableSent) {
        clearOutputIntentLocked();
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(std::errc::io_error));
    }
    LaserCubeNetLifecycleReport report;
    report.evidence = LaserCubeNetRemoteEvidence::HostEnableRequested;
    setLastLifecycleReport(report);

    const auto requestTime = std::chrono::steady_clock::now();
    auto status = requestFreshStatus(operation, requestTime);
    if (!status) {
        bestEffortOffLocked();
        clearOutputIntentLocked();
        setLastLifecycleReport(report);
        return LaserCubeNetLifecycleResult::failure(status.error(), report);
    }
    report.status = *status;
    report.hasFreshStatus = true;
    report.bufferCapacitySupported = status->bufferMax > 0;
    report.bufferConfirmedEmpty =
        report.bufferCapacitySupported && status->bufferFree == status->bufferMax;
    if (!status->outputEnabled) {
        bestEffortOffLocked();
        clearOutputIntentLocked();
        setLastLifecycleReport(report);
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(std::errc::state_not_recoverable), report);
    }

    if (!commitEnableIntentLocked(enableGeneration)) {
        bestEffortOffLocked();
        clearOutputIntentLocked();
        setLastLifecycleReport(report);
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(std::errc::operation_canceled), report);
    }
    report.evidence = LaserCubeNetRemoteEvidence::DeviceReportedEnabled;
    setLastLifecycleReport(report);
    return LaserCubeNetLifecycleResult::success(report);
}

LaserCubeNetLifecycleResult LaserCubeNetController::disableDark(
    const LaserCubeNetOperation& operation) {
    // Fail closed immediately. A worker packet that has not reached its locked
    // send point observes the generation change and is discarded.
    {
        // This short critical section linearizes disable priority against the
        // enable output-on send and final local re-arm commit.
        std::lock_guard<std::mutex> intentLock(outputIntentMutex);
        setArmed(false);
        streamingAllowed.store(false, std::memory_order_release);
        outputGeneration.fetch_add(1, std::memory_order_acq_rel);
    }
    clearContentSource();

    std::unique_lock<std::timed_mutex> lock(lifecycleMutex, std::defer_lock);
    if (!lockLifecycle(operation, lock)) {
        LaserCubeNetLifecycleReport report;
        report.evidence = LaserCubeNetRemoteEvidence::None;
        return LaserCubeNetLifecycleResult::failure(
            std::make_error_code(operation.cancelled && operation.cancelled()
                                     ? std::errc::operation_canceled
                                     : std::errc::timed_out),
            report);
    }
    clearOutputIntentLocked();
    return darkSequenceLocked(operation, false);
}

LaserCubeNetLifecycleResult LaserCubeNetController::shutdownDark(
    const LaserCubeNetOperation& operation) {
    auto report = disableDark(operation);
    stopThread();
    close();
    return report;
}

LaserCubeNetLifecycleReport LaserCubeNetController::getLastLifecycleReport() const {
    std::lock_guard<std::mutex> lock(lifecycleReportMutex);
    return lastLifecycleReport;
}

void LaserCubeNetController::setLastLifecycleReport(
    const LaserCubeNetLifecycleReport& report) {
    std::lock_guard<std::mutex> lock(lifecycleReportMutex);
    lastLifecycleReport = report;
}

libera::expected<void> LaserCubeNetController::connectToStatus(const LaserCubeNetStatus& status) {
    if (auto configError = networkConfig.validate()) {
        recordConnectionError(error_types::network::connectFailed);
        return libera::unexpected(configError);
    }

    ipAddress = status.ipAddress;
    maxPointRate.store(status.pointRateMax, std::memory_order_relaxed);
    pointBufferCapacity.store(status.bufferMax, std::memory_order_relaxed);
    const auto initialRate =
        LaserCubeNetConfig::clampPointRate(getPointRate(), status.pointRateMax);
    LaserControllerStreaming::setPointRate(initialRate);
    messageTimes.fill(std::chrono::steady_clock::time_point{});
    pendingAckCount = 0;

    lastAckTime = std::chrono::steady_clock::now();
    lastAckWarningTime = std::chrono::steady_clock::time_point{};
    lastUnexpectedAckSenderLogTime = std::chrono::steady_clock::time_point{};
    lastDataSentTime = std::chrono::steady_clock::time_point{};
    lastDataSentBufferSize = 0;
    lastReportedBufferFullness.store(0, std::memory_order_relaxed);
    lastEstimatedBufferFullness.store(0, std::memory_order_relaxed);

    if (!io) {
        io = net::shared_io_context();
    }

    if (dataSocket) {
        dataSocket->close();
    }
    if (commandSocket) {
        commandSocket->close();
    }

    // Data socket sends point packets; command socket handles control/status.
    dataSocket = std::make_unique<net::UdpSocket>(*io);
    commandSocket = std::make_unique<net::UdpSocket>(*io);

    if (auto ec = dataSocket->open_v4()) {
        recordConnectionError(error_types::network::connectFailed);
        return libera::unexpected(ec);
    }
    std::error_code bindAddressError;
    const auto bindAddress = libera::net::asio::ip::make_address(
        networkConfig.localBindAddress, bindAddressError);
    if (bindAddressError) {
        recordConnectionError(error_types::network::connectFailed);
        return libera::unexpected(bindAddressError);
    }
    if (auto ec = dataSocket->bind(bindAddress, 0)) {
        recordConnectionError(error_types::network::connectFailed);
        return libera::unexpected(ec);
    }
    if (auto ec = commandSocket->open_v4()) {
        recordConnectionError(error_types::network::connectFailed);
        return libera::unexpected(ec);
    }
    if (auto ec = commandSocket->bind(bindAddress, 0)) {
        recordConnectionError(error_types::network::connectFailed);
        return libera::unexpected(ec);
    }

    std::error_code ecAddr;
    auto address = libera::net::asio::ip::make_address(ipAddress, ecAddr);
    if (ecAddr) {
        recordConnectionError(error_types::network::connectFailed);
        return libera::unexpected(std::make_error_code(std::errc::invalid_argument));
    }

    dataEndpoint = libera::net::asio::ip::udp::endpoint(address, networkConfig.dataPort);
    commandEndpoint = libera::net::asio::ip::udp::endpoint(address, networkConfig.commandPort);

    networkConnected.store(true, std::memory_order_relaxed);
    reconnectRequested.store(false, std::memory_order_relaxed);
    setConnectionState(true);
    // Device's internal rate is unknown after a fresh connection, so force
    // the next syncPointRate() tick to push the current value.
    pointRatePushNeeded = true;
    resetStartupBlank();
    setVerbose(false);
    return {};
}

bool LaserCubeNetController::reconnectToLatestStatus() {
    std::optional<LaserCubeNetStatus> status;
    {
        std::lock_guard<std::mutex> lock(latestStatusMutex);
        status = latestStatus;
    }

    if (!status) {
        return false;
    }

    // Recovery is a fresh dark connection. Previous armed/output intent is
    // never restored as a side effect of discovery or transport recovery.
    LaserCubeNetControllerInfo info(*status);
    auto result = connectDark(
        info, LaserCubeNetOperation::withTimeout(std::chrono::milliseconds(750)));
    if (!result) {
        logError("[LaserCubeNetController] reconnect failed", result.error().message());
        return false;
    }

    return true;
}

void LaserCubeNetController::close() {
    std::lock_guard<std::timed_mutex> lock(lifecycleMutex);
    clearOutputIntentLocked();
    clearContentSource();
    networkConnected.store(false, std::memory_order_relaxed);
    reconnectRequested.store(false, std::memory_order_relaxed);
    setConnectionState(false);
    messageTimes.fill(std::chrono::steady_clock::time_point{});
    pendingAckCount = 0;
    if (dataSocket) {
        dataSocket->close();
    }
    if (commandSocket) {
        commandSocket->close();
    }
}

void LaserCubeNetController::run() {
    using namespace std::chrono_literals;

    resetStartupBlank();

    while (running.load()) {
        if (!networkConnected.load(std::memory_order_relaxed)) {
            setConnectionState(false);
            if (reconnectRequested.exchange(false, std::memory_order_relaxed)) {
                reconnectToLatestStatus();
            }
            std::this_thread::sleep_for(reconnectRetryDelay);
            continue;
        }

        setConnectionState(true);
        syncPointRate();

        if (streamingAllowed.load(std::memory_order_acquire)) {
            // Send at most one packet per loop; lifecycle generation checks
            // discard work that was prepared before a concurrent disable.
            (void)sendPoints();
            std::lock_guard<std::timed_mutex> lock(lifecycleMutex);
            checkAcksLocked();
        }

        // Adaptive sleep: wait roughly as long as the buffer excess takes to drain.
        // This avoids a busy loop when the controller doesn't need more data yet.
        {
            const int bufferFullness = lastEstimatedBufferFullness.load(std::memory_order_relaxed);
            std::uint32_t activePointRate = 0;
            {
                std::lock_guard<std::timed_mutex> lock(lifecycleMutex);
                activePointRate = lastSentPointRate;
            }
            const int targetFull = LaserCubeNetConfig::targetBufferPoints(
                activePointRate, getTotalBufferCapacity(), targetLatency());
            const int excess = bufferFullness - targetFull;
            int msToWait = 1;
            if (excess > 0 && activePointRate > 0) {
                msToWait = static_cast<int>(std::llround(
                    pointsToMillis(static_cast<std::size_t>(excess), activePointRate)));
                msToWait = std::clamp(msToWait, 1, 10);
            }
            if (msToWait > 2) {
                logInfoVerbose("[LaserCubeNet] sleep", msToWait, "ms",
                               "excess", excess,
                               "buf", bufferFullness,
                               "target", targetFull);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(msToWait));
        }
    }
}

void LaserCubeNetController::setPointRate(std::uint32_t pointRateValue) {
    pointRateValue = LaserCubeNetConfig::clampPointRate(
        pointRateValue,
        maxPointRate.load(std::memory_order_relaxed));
    core::LaserControllerStreaming::setPointRate(pointRateValue);
}

bool LaserCubeNetController::sendPoints() {
    const auto generationAtRequest = outputGeneration.load(std::memory_order_acquire);
    std::uint32_t activePointRate = 0;
    int sentBufferSizeSnapshot = 0;
    std::chrono::steady_clock::time_point dataSentTimeSnapshot{};
    std::chrono::steady_clock::time_point ackTimeSnapshot{};
    {
        std::lock_guard<std::timed_mutex> lock(lifecycleMutex);
        activePointRate = lastSentPointRate;
        sentBufferSizeSnapshot = lastDataSentBufferSize;
        dataSentTimeSnapshot = lastDataSentTime;
        ackTimeSnapshot = lastAckTime;
    }

    // Estimate how full the controller buffer is right now.
    const int minEstimatedBufferFullness =
        std::max(
            calculateBufferFullnessFromSnapshot(
                sentBufferSizeSnapshot,
                dataSentTimeSnapshot,
                activePointRate,
                0),
            calculateBufferFullnessFromSnapshot(
                lastReportedBufferFullness.load(std::memory_order_relaxed),
                ackTimeSnapshot,
                activePointRate,
                0));
    lastEstimatedBufferFullness.store(minEstimatedBufferFullness, std::memory_order_relaxed);
    // Keep a latency-derived point cushion, but leave fixed packet headroom so
    // estimated fullness + delayed acks do not push the controller into overrun.
    const int targetBufferPoints =
        LaserCubeNetConfig::targetBufferPoints(
            activePointRate,
            getTotalBufferCapacity(),
            targetLatency());
    int maxPointsToAdd = std::max(
        0,
        targetBufferPoints - minEstimatedBufferFullness);

    const int maxPointsInPacket = static_cast<int>(LaserCubeNetConfig::MAX_POINTS_PER_PACKET);

    if (maxPointsToAdd <= 0) {
        return true;
    }

    // as we are only going to process one packet's worth of points every time, if we need more 
    // we'll just take what we can send right now, but then should ask again once it's sent.
    if (maxPointsToAdd > maxPointsInPacket) {
        maxPointsToAdd = maxPointsInPacket;
    }

    core::PointFillRequest request{};
    // LaserCubeNet sends one packet per loop, so require enough points to make
    // this packet worth transmitting when we've decided a refill is needed.
    request.minimumPointsRequired = static_cast<std::size_t>(maxPointsToAdd);
    request.maximumPointsRequired = static_cast<std::size_t>(maxPointsToAdd);
    const auto renderLead = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
        std::chrono::duration<double, std::milli>(
            pointsToMillis(static_cast<std::size_t>(minEstimatedBufferFullness), activePointRate)));
    request.estimatedFirstPointRenderTime = std::chrono::steady_clock::now() + renderLead;

    logInfoVerbose("[LaserCubeNet] sendPoints",
                   "estBuf", minEstimatedBufferFullness,
                   "target", targetBufferPoints,
                   "toAdd", maxPointsToAdd,
                   "renderLeadMs", std::chrono::duration<double, std::milli>(renderLead).count());

    if (!requestPoints(request)) {
        return false;
    }

    if (pointsToSend.empty()) {
        return true;
    }

    // Safety clamp in case the callback overfilled.
    if (pointsToSend.size() > static_cast<std::size_t>(maxPointsToAdd)) {
        pointsToSend.resize(static_cast<std::size_t>(maxPointsToAdd));
    }

    // Build a single UDP packet: header + packed 12-bit points.
    //
    // Packet layout (byte offsets):
    //  0: CMD_SAMPLE_DATA (0xA9)
    //  1: Reserved, always 0x00
    //  2: messageNumber (uint8, wraps 0..255)
    //  3: frameNumber   (uint8, wraps 0..255)
    //  4..: Point data, each point is 10 bytes:
    //       x[0], x[1], y[0], y[1], r[0], r[1], g[0], g[1], b[0], b[1]
    //       All channels are unsigned 12-bit values stored little-endian in 16-bit slots.
    std::lock_guard<std::timed_mutex> lock(lifecycleMutex);
    if (!streamingAllowed.load(std::memory_order_acquire) ||
        !isArmed() ||
        generationAtRequest != outputGeneration.load(std::memory_order_acquire)) {
        return false;
    }

    // Message/frame counters and their acknowledgement slots are owned by the
    // lifecycle mutex so startup blank submission cannot race the worker.
    core::ByteBuffer packet;
    packet.appendUInt8(LaserCubeNetConfig::CMD_SAMPLE_DATA);
    packet.appendUInt8(0x00);
    packet.appendUInt8(messageNumber);
    packet.appendUInt8(frameNumber++);
    for (const auto& pt : pointsToSend) {
        packet.appendUInt16(encodeUnsigned12FromSignedUnit(-pt.x));
        packet.appendUInt16(encodeUnsigned12FromSignedUnit(pt.y));
        packet.appendUInt16(encodeUnsigned12FromUnit(pt.r));
        packet.appendUInt16(encodeUnsigned12FromUnit(pt.g));
        packet.appendUInt16(encodeUnsigned12FromUnit(pt.b));
    }

    const bool success = sendDataLocked(packet.data(), packet.size());
    if (success) {
        const auto now = std::chrono::steady_clock::now();
        if (messageTimes[messageNumber] == std::chrono::steady_clock::time_point{}) {
            ++pendingAckCount;
        }
        messageTimes[messageNumber] = now;
        lastDataSentTime = now;
        lastDataSentBufferSize = minEstimatedBufferFullness + static_cast<int>(pointsToSend.size());
    }

    messageNumber++;

    return success;
}

void LaserCubeNetController::syncPointRate() {
    std::lock_guard<std::timed_mutex> lock(lifecycleMutex);
    syncPointRateLocked();
}

void LaserCubeNetController::syncPointRateLocked() {
    const auto desired = getPointRate();
    // Short-circuit when nothing has changed and no resync is pending.
    if (!pointRatePushNeeded && desired == lastSentPointRate) {
        return;
    }
    core::ByteBuffer payload;
    payload.appendUInt32(desired);
    const bool ok = sendCommandLocked(
        LaserCubeNetConfig::CMD_SET_ILDA_RATE, payload.data(), payload.size());
    if (ok) {
        lastSentPointRate = desired;
        pointRatePushNeeded = false;
    } else {
        // Leave the latch set so the next tick retries.
        pointRatePushNeeded = true;
        clearOutputIntentLocked();
        bestEffortOffLocked();
    }
}

bool LaserCubeNetController::sendDataLocked(const std::uint8_t* buffer, std::size_t size) {
    if (!dataSocket || !buffer || size == 0) {
        recordConnectionError(error_types::network::sendFailed);
        networkConnected.store(false, std::memory_order_relaxed);
        clearOutputIntentLocked();
        bestEffortOffLocked();
        return false;
    }
    auto ec = dataSocket->send_to(buffer, size, dataEndpoint, std::chrono::milliseconds(50));
    if (ec) {
        logError("[LaserCubeNetController] Failed to send data", ec.message());
        recordConnectionError(error_types::network::sendFailed);
        networkConnected.store(false, std::memory_order_relaxed);
        clearOutputIntentLocked();
        bestEffortOffLocked();
        return false;
    }
    return true;
}

bool LaserCubeNetController::sendCommand(std::uint8_t cmd, const std::uint8_t* payload, std::size_t size) {
    std::lock_guard<std::timed_mutex> lock(lifecycleMutex);
    const bool sent = sendCommandLocked(cmd, payload, size);
    if (!sent) clearOutputIntentLocked();
    return sent;
}

bool LaserCubeNetController::sendCommandLocked(
    std::uint8_t cmd,
    const std::uint8_t* payload,
    std::size_t size) {
    if (!commandSocket) {
        recordConnectionError(error_types::network::sendFailed);
        networkConnected.store(false, std::memory_order_relaxed);
        return false;
    }

    core::ByteBuffer buffer;
    buffer.appendUInt8(cmd);
    for (std::size_t i = 0; i < size; ++i) {
        buffer.appendUInt8(payload ? payload[i] : 0);
    }

    auto ec = commandSocket->send_to(
        buffer.data(), buffer.size(), commandEndpoint, networkConfig.sendTimeout);
    if (ec) {
        logError("[LaserCubeNetController] command send failed", ec.message());
        recordConnectionError(error_types::network::sendFailed);
        networkConnected.store(false, std::memory_order_relaxed);
        return false;
    }
    return true;
}

bool LaserCubeNetController::rotateCommandSocketLocked() {
    if (commandSocket) {
        commandSocket->close();
    }

    commandSocket = std::make_unique<net::UdpSocket>(*io);
    if (auto error = commandSocket->open_v4()) {
        recordConnectionError(error_types::network::connectFailed);
        networkConnected.store(false, std::memory_order_relaxed);
        clearOutputIntentLocked();
        return false;
    }

    std::error_code addressError;
    const auto bindAddress = libera::net::asio::ip::make_address(
        networkConfig.localBindAddress, addressError);
    if (addressError || commandSocket->bind(bindAddress, 0)) {
        recordConnectionError(error_types::network::connectFailed);
        networkConnected.store(false, std::memory_order_relaxed);
        clearOutputIntentLocked();
        return false;
    }
    return true;
}

bool LaserCubeNetController::sendStartupBlankLocked(const std::size_t packetCount) {
    const auto blankPointCount = static_cast<std::size_t>(std::clamp(
        millisToPoints(1.0),
        1,
        static_cast<int>(LaserCubeNetConfig::MAX_POINTS_PER_PACKET)));

    for (std::size_t packetIndex = 0; packetIndex < packetCount; ++packetIndex) {
        core::ByteBuffer packet;
        packet.appendUInt8(LaserCubeNetConfig::CMD_SAMPLE_DATA);
        packet.appendUInt8(0x00);
        packet.appendUInt8(messageNumber);
        packet.appendUInt8(frameNumber++);
        for (std::size_t index = 0; index < blankPointCount; ++index) {
            packet.appendUInt16(encodeUnsigned12FromSignedUnit(0.0f));
            packet.appendUInt16(encodeUnsigned12FromSignedUnit(0.0f));
            packet.appendUInt16(encodeUnsigned12FromUnit(0.0f));
            packet.appendUInt16(encodeUnsigned12FromUnit(0.0f));
            packet.appendUInt16(encodeUnsigned12FromUnit(0.0f));
        }

        if (!sendDataLocked(packet.data(), packet.size())) {
            return false;
        }

        const auto now = std::chrono::steady_clock::now();
        if (messageTimes[messageNumber] == std::chrono::steady_clock::time_point{}) {
            ++pendingAckCount;
        }
        messageTimes[messageNumber] = now;
        lastDataSentTime = now;
        lastDataSentBufferSize += static_cast<int>(blankPointCount);
        ++messageNumber;
    }
    return true;
}

bool LaserCubeNetController::drainCommandResponsesLocked(
    const LaserCubeNetOperation& operation) {
    if (!commandSocket) {
        return false;
    }

    std::array<std::uint8_t, 64> discard{};
    while (!operationStopped(operation)) {
        const auto now = std::chrono::steady_clock::now();
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            operation.deadline - now);
        const auto timeout = std::max(
            std::chrono::milliseconds(1),
            std::min(std::chrono::milliseconds(2), remaining));
        libera::net::asio::ip::udp::endpoint sender;
        std::size_t received = 0;
        const auto error = commandSocket->recv_from(
            discard.data(), discard.size(), sender, received, timeout, false);
        if (error == libera::net::asio::error::timed_out) {
            return true;
        }
        if (error == libera::net::asio::error::operation_aborted) {
            return false;
        }
        if (error) {
            return false;
        }
    }
    return false;
}

bool LaserCubeNetController::confirmStartupDeliveryLocked(
    const LaserCubeNetOperation& operation) {
    // This barrier proves only that the all-black startup content was accepted
    // before output-on. It is never used as output or clear evidence.
    while (pendingAckCount > 0 && !operationStopped(operation)) {
        checkAcksLocked();
        if (pendingAckCount > 0) std::this_thread::yield();
    }
    return pendingAckCount == 0;
}

LaserCubeNetLifecycleResult LaserCubeNetController::darkSequenceLocked(
    const LaserCubeNetOperation& operation,
    const bool configureTransport) {
    LaserCubeNetLifecycleReport report;
    const std::uint8_t disabled = 0;

    while (!operationStopped(operation)) {
        // Retire sample acknowledgements from the prior output generation.
        // They never gate the first off or contribute to dark confirmation.
        messageTimes.fill(std::chrono::steady_clock::time_point{});
        pendingAckCount = 0;
        // Each attempt owns a fresh receive endpoint. A response delayed from
        // an older lifecycle epoch targets the retired port and is ineligible.
        if (!rotateCommandSocketLocked()) {
            return LaserCubeNetLifecycleResult::failure(
                std::make_error_code(std::errc::io_error), report);
        }
        if (!sendCommandLocked(LaserCubeNetConfig::CMD_SET_OUTPUT, &disabled, 1)) {
            clearOutputIntentLocked();
            return LaserCubeNetLifecycleResult::failure(
                std::make_error_code(std::errc::io_error), report);
        }
        report.evidence = LaserCubeNetRemoteEvidence::HostDarkRequested;
        setLastLifecycleReport(report);

        if (!sendCommandLocked(LaserCubeNetConfig::CMD_CLEAR_RINGBUFFER, nullptr, 0) ||
            !drainCommandResponsesLocked(operation) ||
            !sendCommandLocked(LaserCubeNetConfig::CMD_SET_OUTPUT, &disabled, 1)) {
            clearOutputIntentLocked();
            setLastLifecycleReport(report);
            return LaserCubeNetLifecycleResult::failure(
                operationStopped(operation)
                    ? std::make_error_code(operation.cancelled && operation.cancelled()
                                               ? std::errc::operation_canceled
                                               : std::errc::timed_out)
                    : std::make_error_code(std::errc::io_error),
                report);
        }
        const auto finalOffRequestTime = std::chrono::steady_clock::now();

        if (configureTransport) {
            core::ByteBuffer ratePayload;
            ratePayload.appendUInt32(getPointRate());
            if (!sendCommandLocked(
                    LaserCubeNetConfig::CMD_ENABLE_BUFFER_RESPONSE, nullptr, 0) ||
                !sendCommandLocked(
                    LaserCubeNetConfig::CMD_SET_ILDA_RATE,
                    ratePayload.data(), ratePayload.size())) {
                clearOutputIntentLocked();
                setLastLifecycleReport(report);
                return LaserCubeNetLifecycleResult::failure(
                    std::make_error_code(std::errc::io_error), report);
            }
            lastSentPointRate = getPointRate();
            pointRatePushNeeded = false;
        }

        auto status = requestFreshStatus(operation, finalOffRequestTime);
        if (!status) {
            clearOutputIntentLocked();
            setLastLifecycleReport(report);
            return LaserCubeNetLifecycleResult::failure(status.error(), report);
        }

        report.status = *status;
        report.hasFreshStatus = true;
        report.bufferCapacitySupported = status->bufferMax > 0;
        report.bufferConfirmedEmpty =
            report.bufferCapacitySupported && status->bufferFree == status->bufferMax;
        setLastLifecycleReport(report);

        const bool bufferAcceptable =
            !report.bufferCapacitySupported || report.bufferConfirmedEmpty;
        if (!status->outputEnabled && bufferAcceptable) {
            report.evidence = LaserCubeNetRemoteEvidence::DeviceReportedDisabled;
            setLastLifecycleReport(report);
            remoteEnabledConfirmed = false;
            lastReportedBufferFullness.store(0, std::memory_order_relaxed);
            lastEstimatedBufferFullness.store(0, std::memory_order_relaxed);
            lastDataSentBufferSize = 0;
            lastDataSentTime = std::chrono::steady_clock::now();
            lastAckTime = lastDataSentTime;
            messageTimes.fill(std::chrono::steady_clock::time_point{});
            pendingAckCount = 0;
            return LaserCubeNetLifecycleResult::success(report);
        }

        // State did not reflect this attempt. Rotate the receive epoch and
        // retry the complete off-clear-off sequence while budget remains.
        report.evidence = LaserCubeNetRemoteEvidence::HostDarkRequested;
        clearOutputIntentLocked();
        setLastLifecycleReport(report);
    }

    return LaserCubeNetLifecycleResult::failure(
        std::make_error_code(operation.cancelled && operation.cancelled()
                                 ? std::errc::operation_canceled
                                 : std::errc::timed_out),
        report);
}

void LaserCubeNetController::clearOutputIntentLocked() noexcept {
    std::lock_guard<std::mutex> intentLock(outputIntentMutex);
    streamingAllowed.store(false, std::memory_order_release);
    remoteEnabledConfirmed = false;
    setArmed(false);
    outputGeneration.fetch_add(1, std::memory_order_acq_rel);
}

std::uint64_t LaserCubeNetController::beginEnableIntentLocked() noexcept {
    std::lock_guard<std::mutex> intentLock(outputIntentMutex);
    streamingAllowed.store(false, std::memory_order_release);
    remoteEnabledConfirmed = false;
    setArmed(false);
    return outputGeneration.fetch_add(1, std::memory_order_acq_rel) + 1;
}

bool LaserCubeNetController::commitEnableIntentLocked(
    const std::uint64_t expectedGeneration) noexcept {
    std::lock_guard<std::mutex> intentLock(outputIntentMutex);
    if (outputGeneration.load(std::memory_order_acquire) != expectedGeneration) {
        return false;
    }
    setArmed(true);
    streamingAllowed.store(true, std::memory_order_release);
    remoteEnabledConfirmed = true;
    outputGeneration.fetch_add(1, std::memory_order_acq_rel);
    return true;
}

void LaserCubeNetController::bestEffortOffLocked() noexcept {
    if (!commandSocket) {
        return;
    }
    const std::uint8_t disabled = 0;
    (void)sendCommandLocked(LaserCubeNetConfig::CMD_SET_OUTPUT, &disabled, 1);
}

bool LaserCubeNetController::lockLifecycle(
    const LaserCubeNetOperation& operation,
    std::unique_lock<std::timed_mutex>& lock) {
    while (!operationStopped(operation)) {
        const auto now = std::chrono::steady_clock::now();
        const auto nextPoll = std::min(
            operation.deadline, now + std::chrono::milliseconds(2));
        if (lock.try_lock_until(nextPoll)) {
            return true;
        }
    }
    return false;
}

bool LaserCubeNetController::operationStopped(const LaserCubeNetOperation& operation) const {
    return std::chrono::steady_clock::now() >= operation.deadline ||
           (operation.cancelled && operation.cancelled());
}

libera::expected<LaserCubeNetStatus> LaserCubeNetController::requestFreshStatus(
    const LaserCubeNetOperation& operation,
    const std::chrono::steady_clock::time_point newerThan) {
    if (!commandSocket) {
        return libera::unexpected(std::make_error_code(std::errc::not_connected));
    }

    std::array<std::uint8_t, 64> buffer{};
    while (!operationStopped(operation)) {
        if (!sendCommandLocked(LaserCubeNetConfig::CMD_GET_FULL_INFO, nullptr, 0)) {
            return libera::unexpected(std::make_error_code(std::errc::io_error));
        }
        const auto probeSentAt = std::chrono::steady_clock::now();

        const auto now = std::chrono::steady_clock::now();
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            operation.deadline - now);
        const auto timeout = std::max(
            std::chrono::milliseconds(1),
            std::min(networkConfig.receivePollTimeout, remaining));
        libera::net::asio::ip::udp::endpoint sender;
        std::size_t received = 0;
        auto ec = commandSocket->recv_from(
            buffer.data(), buffer.size(), sender, received, timeout, false);
        if (ec == libera::net::asio::error::timed_out ||
            ec == libera::net::asio::error::operation_aborted) {
            continue;
        }
        if (ec) {
            return libera::unexpected(ec);
        }
        const auto receivedAt = std::chrono::steady_clock::now();
        const auto epochBarrier = std::max(newerThan, probeSentAt);
        if (sender.address() != commandEndpoint.address() ||
            sender.port() != commandEndpoint.port() || receivedAt <= epochBarrier) {
            continue;
        }
        auto status = LaserCubeNetStatus::parse(buffer.data(), received);
        if (!status || status->serialNumber.empty()) {
            continue;
        }
        const auto expectedStatus = getLatestStatus();
        if (expectedStatus && status->serialNumber != expectedStatus->serialNumber) {
            continue;
        }
        status->ipAddress = sender.address().to_string();
        status->lastSeen = receivedAt;
        updateDiscoveredStatus(*status);
        return *status;
    }
    return libera::unexpected(
        std::make_error_code(operation.cancelled && operation.cancelled()
                                 ? std::errc::operation_canceled
                                 : std::errc::timed_out));
}

void LaserCubeNetController::checkAcksLocked() {
    if (!dataSocket) {
        return;
    }

    constexpr std::size_t maxAckPollsPerCall = 64;
    std::size_t polls = 0;
    const auto now = std::chrono::steady_clock::now();

    while (polls < maxAckPollsPerCall) {
        std::array<std::uint8_t, 4> buffer{};
        libera::net::asio::ip::udp::endpoint sender;
        std::size_t received = 0;
        auto ec = dataSocket->recv_from(buffer.data(), buffer.size(), sender,
                                        received, std::chrono::milliseconds(1), false);

        if (ec == libera::net::asio::error::timed_out ||
            ec == libera::net::asio::error::operation_aborted) {
            break;
        }
        if (ec) {
            logInfo("[LaserCubeNetController] ack receive failed", ec.message());
            recordIntermittentError(error_types::network::receiveFailed);
            break;
        }
        ++polls;

        if (sender.address() != dataEndpoint.address() || sender.port() != dataEndpoint.port()) {
            if (lastUnexpectedAckSenderLogTime == std::chrono::steady_clock::time_point{} ||
                (now - lastUnexpectedAckSenderLogTime) > std::chrono::seconds(1)) {
                logInfo("[LaserCubeNetController] ignoring ack from unexpected sender",
                        sender.address().to_string(), sender.port(),
                        "expected", dataEndpoint.address().to_string(), dataEndpoint.port());
                recordIntermittentError(error_types::network::packetLoss);
                lastUnexpectedAckSenderLogTime = now;
            }
            continue;
        }

        if (received != 4) {
            if (received > 0) {
                logInfo("[LaserCubeNetController] ack packet unexpected size", received);
                recordIntermittentError(error_types::network::protocolError);
            }
            continue;
        }

        if (buffer[0] != LaserCubeNetConfig::CMD_GET_RINGBUFFER_FREE) {
            logInfo("[LaserCubeNetController] DIFFERENT RESPONSE", static_cast<int>(buffer[0]));
            recordIntermittentError(error_types::network::protocolError);
            continue;
        }

        const std::uint8_t receivedMessageNumber = buffer[1];
        const auto& sentTime = messageTimes[receivedMessageNumber];
        if (sentTime == std::chrono::steady_clock::time_point{}) {
            recordIntermittentError(error_types::network::packetLoss);
            continue;
        }

        recordLatencySample(now - sentTime);
        lastAckTime = now;
        lastAckWarningTime = std::chrono::steady_clock::time_point{};

        const std::uint16_t bufferSpace = core::bytes::readLe16(&buffer[2]);
        const int bufferFullness = getTotalBufferCapacity() - static_cast<int>(bufferSpace);
        logInfoVerbose("[LaserCubeNet] ack",
                       "buf", bufferFullness,
                       "space", bufferSpace,
                       "rttMs", std::chrono::duration<double, std::milli>(now - sentTime).count());
        lastReportedBufferFullness.store(bufferFullness, std::memory_order_relaxed);
        lastEstimatedBufferFullness.store(bufferFullness, std::memory_order_relaxed);

        // Always reset the sent-data snapshot to ack ground truth.
        // The previous guard (sentTime >= lastDataSentTime) was never
        // satisfied because sendPoints() updates lastDataSentTime to now
        // on every packet, so the ack's sentTime was always older.  This
        // caused the sent-based buffer estimate to self-reinforce and
        // drift above the real hardware buffer level.
        lastDataSentTime = now;
        lastDataSentBufferSize = bufferFullness;

        if (bufferSpace == 0) {
            logInfo("[LaserCubeNetController] BUFFER OVERRUN ------------------");
            recordIntermittentError(error_types::network::bufferOverrun);
        }

        messageTimes[receivedMessageNumber] = std::chrono::steady_clock::time_point{};
        if (pendingAckCount > 0) --pendingAckCount;
    }

    if (pendingAckCount > 0) {
        // Clean up entries that have been pending for more than 1 second.
        for (auto& slot : messageTimes) {
            if (slot == std::chrono::steady_clock::time_point{}) continue;
            if ((now - slot) > std::chrono::seconds(1)) {
                slot = std::chrono::steady_clock::time_point{};
                if (pendingAckCount > 0) --pendingAckCount;
            }
        }

        if (pendingAckCount > 0) {
            const bool staleAcks = (now - lastAckTime) > std::chrono::seconds(1);
            const bool shouldLogWait =
                staleAcks &&
                (lastAckWarningTime == std::chrono::steady_clock::time_point{} ||
                 (now - lastAckWarningTime) > std::chrono::seconds(1));
            if (shouldLogWait) {
                // Find the oldest pending entry for diagnostics.
                auto oldestTime = now;
                for (const auto& slot : messageTimes) {
                    if (slot != std::chrono::steady_clock::time_point{} && slot < oldestTime) {
                        oldestTime = slot;
                    }
                }
                const auto oldestMs =
                    std::chrono::duration_cast<std::chrono::milliseconds>(now - oldestTime).count();
                logInfo("[LaserCubeNetController] waiting for ack",
                        "pending", pendingAckCount,
                        "oldest_ms", oldestMs);
                recordIntermittentError(error_types::network::packetLoss);
                lastAckWarningTime = now;
            }

            // When the ack ring is saturated and the oldest pending packet has
            // been stuck for long enough, the controller is no longer making
            // forward progress. Treat this as a real connection loss so the UI
            // goes red instead of staying on a soft warning indefinitely.
            auto oldestTime = now;
            for (const auto& slot : messageTimes) {
                if (slot != std::chrono::steady_clock::time_point{} && slot < oldestTime) {
                    oldestTime = slot;
                }
            }
            const bool ackQueueSaturated =
                pendingAckCount >= static_cast<int>(messageTimes.size());
            const auto ackStallDuration = now - oldestTime;
            if (ackQueueSaturated && ackStallDuration >= ackDisconnectThreshold) {
                logError("[LaserCubeNetController] ack stream stalled, marking connection lost",
                         "pending", pendingAckCount,
                         "oldest_ms",
                         std::chrono::duration_cast<std::chrono::milliseconds>(ackStallDuration).count());
                recordConnectionError(error_types::network::connectionLost);
                networkConnected.store(false, std::memory_order_relaxed);
                setConnectionState(false);
                clearOutputIntentLocked();
                bestEffortOffLocked();
                return;
            }
        }
    }
}

int LaserCubeNetController::getTotalBufferCapacity() const {
    return pointBufferCapacity.load(std::memory_order_relaxed);
}

std::optional<core::BufferState> LaserCubeNetController::getBufferState() const {
    return buildBufferState(
        pointBufferCapacity.load(std::memory_order_relaxed),
        lastEstimatedBufferFullness.load(std::memory_order_relaxed));
}

} // namespace libera::lasercubenet
