#include "libera/lasercubenet/LaserCubeNetManager.hpp"
#include "libera/lasercubenet/LaserCubeNetController.hpp"

#include "libera/log/Log.hpp"

#include <array>
#include <exception>
#include <utility>

namespace libera::lasercubenet {

LaserCubeNetManager::LaserCubeNetManager(LaserCubeNetNetworkConfig networkConfigValue)
    : networkConfig(std::move(networkConfigValue)) {
    networkError = networkConfig.validate();
    if (networkError) {
        logError("[LaserCubeNetManager] invalid network configuration");
        listenerFinished.store(true, std::memory_order_release);
        return;
    }

    io = net::shared_io_context();
    socket = std::make_unique<net::UdpSocket>(*io);
    networkError = socket->open_v4();
    if (networkError) {
        listenerFinished.store(true, std::memory_order_release);
        return;
    }
    networkError = socket->enable_broadcast(true);
    if (networkError) {
        socket->close();
        listenerFinished.store(true, std::memory_order_release);
        return;
    }

    std::error_code addressError;
    const auto bindAddress = asio::ip::make_address(
        networkConfig.localBindAddress, addressError);
    if (addressError) {
        networkError = std::make_error_code(std::errc::invalid_argument);
    } else {
        networkError = socket->bind(bindAddress, networkConfig.discoveryBindPort);
    }
    if (networkError) {
        socket->close();
        listenerFinished.store(true, std::memory_order_release);
        return;
    }
    running.store(true);
    listenerFinished.store(false, std::memory_order_relaxed);
    // Dedicated discovery thread so controller scanning never blocks the caller.
    listener = std::thread([this]{
        try {
            discoveryThread();
        } catch (const std::exception& e) {
            logError("[LaserCubeNetManager] uncaught exception in discovery thread", e.what());
            running.store(false);
        } catch (...) {
            logError("[LaserCubeNetManager] uncaught unknown exception in discovery thread");
            running.store(false);
        }
        listenerFinished.store(true, std::memory_order_release);
    });
}

LaserCubeNetManager::~LaserCubeNetManager() {
    closeAll();
}

void LaserCubeNetManager::discoveryThread() {
    if (!socket) {
        return;
    }

    std::array<std::uint8_t, 64> buffer{};

    while (running.load()) {
        // Broadcast a GET_FULL_INFO probe to discover controllers.
        sendProbe();

        const auto windowStart = Clock::now();
        while (running.load() && Clock::now() - windowStart < networkConfig.discoveryWindow) {
            asio::ip::udp::endpoint sender;
            std::size_t received = 0;
            auto ec = socket->recv_from(buffer.data(), buffer.size(), sender, received,
                                        networkConfig.receivePollTimeout, false);
            if (ec) {
                if (ec == asio::error::operation_aborted || !running.load()) {
                    break;
                }
                continue;
            }

            if (!networkConfig.acceptsDiscoveryResponse(
                    sender.address().to_string(), sender.port())) {
                logInfo("[LaserCubeNetManager] ignored discovery response from unexpected sender",
                        sender.address().to_string(), sender.port());
                continue;
            }

            // Parse and stash the most recent status for each controller.
            if (auto status = LaserCubeNetStatus::parse(buffer.data(), received)) {
                status->ipAddress = sender.address().to_string();
                status->lastSeen = Clock::now();
                std::shared_ptr<LaserCubeNetController> activeController;
                bool isNew = false;
                {
                    std::lock_guard lock(controllersMutex);
                    isNew = controllers.find(status->serialNumber) == controllers.end();
                    controllers[status->serialNumber] = ControllerEntry{*status, status->lastSeen};
                }
                {
                    activeController = findLiveController(status->serialNumber);
                }
                if (activeController) {
                    activeController->updateDiscoveredStatus(*status);
                }
                if (isNew) {
                    logInfo("[LaserCubeNetManager] discovery ok",
                            status->ipAddress,
                            sender.port(),
                            "serial",
                            status->serialNumber,
                            "model",
                            status->modelName,
                            "fw",
                            status->firmwareVersion,
                            "buffer",
                            status->bufferFree,
                            "/",
                            status->bufferMax,
                            "pps",
                            status->pointRate,
                            "/",
                            status->pointRateMax);
                }
            }
        }

        // Prune stale entries.
        {
            std::lock_guard lock(controllersMutex);
            for (auto it = controllers.begin(); it != controllers.end(); ) {
                if (Clock::now() - it->second.lastSeen > std::chrono::seconds(3)) {
                    it = controllers.erase(it);
                } else {
                    ++it;
                }
            }
        }

        // Shutdown wakes this wait immediately; discoveryInterval is only the
        // normal production cadence, never an uninterruptible close delay.
        std::unique_lock<std::mutex> waitLock(listenerWaitMutex);
        listenerWaitChanged.wait_for(
            waitLock,
            networkConfig.discoveryInterval,
            [this] { return !running.load(); });
    }
}

void LaserCubeNetManager::sendProbe() {
    if (!socket) return;
    // One-byte command broadcast; controllers reply with a 64-byte status payload.
    const std::uint8_t cmd = LaserCubeNetConfig::CMD_GET_FULL_INFO;
    for (const auto& destination : networkConfig.discoveryDestinations) {
        std::error_code addressError;
        const auto address = asio::ip::make_address(destination, addressError);
        if (addressError) {
            continue;
        }
        asio::ip::udp::endpoint endpoint(address, networkConfig.commandPort);
        socket->send_to(&cmd, 1, endpoint, networkConfig.sendTimeout);
    }
}

std::vector<std::unique_ptr<core::ControllerInfo>> LaserCubeNetManager::discover() {
    std::vector<std::unique_ptr<core::ControllerInfo>> out;
    std::lock_guard lock(controllersMutex);
    out.reserve(controllers.size());
    for (const auto& [id, entry] : controllers) {
        out.emplace_back(std::make_unique<LaserCubeNetControllerInfo>(entry.status));
    }
    return out;
}

std::shared_ptr<LaserCubeNetController>
LaserCubeNetManager::createController(const LaserCubeNetControllerInfo& info) {
    return std::make_shared<LaserCubeNetController>(info, networkConfig);
}

LaserCubeNetManager::NewControllerDisposition
LaserCubeNetManager::prepareNewController(LaserCubeNetController& controller,
                                          const LaserCubeNetControllerInfo& info) {
    controller.updateDiscoveredStatus(info.status());

    // A failed dark handshake is never cached. A later retry constructs a
    // fresh locally disarmed controller instance.
    const auto operation = LaserCubeNetOperation::withTimeout(std::chrono::milliseconds(750));
    if (auto result = controller.connectDark(info, operation); !result) {
        logError("[LaserCubeNetManager] initial connect failed", result.error().message());
        return NewControllerDisposition::DropController;
    }
    controller.startThread();
    return NewControllerDisposition::KeepController;
}

void LaserCubeNetManager::prepareExistingController(LaserCubeNetController& controller,
                                                    const LaserCubeNetControllerInfo& info) {
    controller.updateDiscoveredStatus(info.status());
}

void LaserCubeNetManager::beforeCloseControllers() {
    running.store(false);
    listenerWaitChanged.notify_all();
    if (socket) {
        socket->close();
    }
    // All listener waits above are now deadline-bounded and wakeable. Joining
    // preserves object lifetime and cannot leave a detached thread using this.
    if (listener.joinable()) {
        listener.join();
    }
}

void LaserCubeNetManager::afterCloseControllers() {
    std::lock_guard lock(controllersMutex);
    controllers.clear();
}

void LaserCubeNetManager::closeController(const std::string& key,
                                          LaserCubeNetController& controller) {
    (void)key;
    const auto operation = LaserCubeNetOperation::withTimeout(std::chrono::milliseconds(750));
    if (auto result = controller.shutdownDark(operation); !result) {
        logError("[LaserCubeNetManager] shutdown-dark failed", result.error().message());
    }
}

} // namespace libera::lasercubenet
