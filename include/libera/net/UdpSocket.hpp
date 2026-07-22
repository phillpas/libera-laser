#pragma once
#include "libera/net/NetConfig.hpp"
#include "libera/log/Log.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace libera::net {

/**
 * UdpSocket
 *
 * Small helper for UDP use-cases like controller discovery or broadcast.
 *
 * LaserCubeNet calls this synchronous facade from its own worker threads. The
 * underlying socket is nonblocking, so each operation is bounded by the
 * caller's monotonic deadline without relying on the shared Asio executor.
 * close() sets a wake flag before waiting for the serialized socket operation,
 * giving shutdown a strict polling bound even if that executor is stopped.
 * - Enable broadcast on macOS/Linux by setting the socket option when needed.
 */
class UdpSocket {
public:
    explicit UdpSocket(asio::io_context& io) : sock(io) {}

    std::error_code open_v4(bool logFailure = true) {
        std::lock_guard<std::mutex> lock(socketMutex);
        closeRequested.store(false, std::memory_order_release);
        std::error_code ec;
        sock.open(udp::v4(), ec);
        if (!ec) {
            sock.non_blocking(true, ec);
            if (ec) {
                std::error_code ignored;
                sock.close(ignored);
            }
        }
        if (ec && logFailure) {
            logError("[UdpSocket] open_v4 failed", ec.message());
        }
        return ec;
    }

    std::error_code bind_any(uint16_t port, bool logFailure = true) {
        std::lock_guard<std::mutex> lock(socketMutex);
        std::error_code ec;
        sock.bind(udp::endpoint(udp::v4(), port), ec);
        if (ec && logFailure) {
            logError("[UdpSocket] bind_any failed on port", port, ec.message());
        }
        return ec;
    }

    std::error_code bind(const asio::ip::address& address,
                         uint16_t port,
                         bool logFailure = true) {
        std::lock_guard<std::mutex> lock(socketMutex);
        std::error_code ec;
        sock.bind(udp::endpoint(address, port), ec);
        if (ec && logFailure) {
            logError("[UdpSocket] bind failed", address.to_string(), port, ec.message());
        }
        return ec;
    }

    std::error_code enable_broadcast(bool on=true) {
        std::lock_guard<std::mutex> lock(socketMutex);
        std::error_code ec;
        sock.set_option(asio::socket_base::broadcast(on), ec);
        return ec;
    }

    // Send a datagram or return timed_out at the supplied monotonic deadline.
    std::error_code send_to(const void* data, std::size_t n,
                            const udp::endpoint& ep,
                            std::chrono::milliseconds timeout,
                            bool logTimeout = true) {
        if (!data || n == 0 || timeout <= std::chrono::milliseconds::zero()) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::unique_lock<std::mutex> socketLock(socketMutex);
        const auto deadline = Clock::now() + timeout;
        for (;;) {
            if (closeRequested.load(std::memory_order_acquire)) {
                return asio::error::operation_aborted;
            }

            std::error_code ec;
            const auto sent = sock.send_to(asio::buffer(data, n), ep, 0, ec);
            if (!ec) {
                return sent == n ? std::error_code{} : asio::error::message_size;
            }
            if (!wouldBlock(ec)) {
                return ec;
            }
            if (!waitForRetry(deadline)) {
                if (logTimeout) {
                    logInfo("[UdpSocket] send deadline expired after", timeout.count(), "ms");
                }
                return asio::error::timed_out;
            }
        }
    }

    // Receive one datagram, with timeout. Returns ec + fills out_ep + out_n.
    std::error_code recv_from(void* data, std::size_t max,
                                        udp::endpoint& out_ep, std::size_t& out_n,
                                        std::chrono::milliseconds timeout,
                                        bool logTimeout = true) {
        out_n = 0;
        out_ep = {};
        if (!data || max == 0 || timeout <= std::chrono::milliseconds::zero()) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::unique_lock<std::mutex> socketLock(socketMutex);
        const auto deadline = Clock::now() + timeout;
        for (;;) {
            if (closeRequested.load(std::memory_order_acquire)) {
                return asio::error::operation_aborted;
            }

            std::error_code ec;
            out_n = sock.receive_from(asio::buffer(data, max), out_ep, 0, ec);
            if (!ec) {
                return {};
            }
            out_n = 0;
            out_ep = {};
            if (!wouldBlock(ec)) {
                return ec;
            }
            if (!waitForRetry(deadline)) {
                if (logTimeout) {
                    logInfo("[UdpSocket] receive deadline expired after", timeout.count(), "ms");
                }
                return asio::error::timed_out;
            }
        }
    }

    udp::socket& raw() { return sock; }
    void close() noexcept {
        // Signalling does not need the socket lock, so a blocked receive wakes
        // before close waits to serialize access to the Asio socket object.
        closeRequested.store(true, std::memory_order_release);
        retryChanged.notify_all();

        std::lock_guard<std::mutex> lock(socketMutex);
        std::error_code ignored;
        sock.close(ignored);
    }

private:
    using Clock = std::chrono::steady_clock;
    static constexpr auto IO_POLL_INTERVAL = std::chrono::milliseconds(2);

    static bool wouldBlock(const std::error_code& ec) {
        return ec == asio::error::would_block || ec == asio::error::try_again;
    }

    bool waitForRetry(Clock::time_point deadline) {
        const auto now = Clock::now();
        if (now >= deadline) {
            return false;
        }

        // UDP readiness is polled at a documented two-millisecond maximum.
        // close() wakes this condition variable immediately instead of waiting
        // for the next poll, satisfying priority-shutdown cancellation needs.
        std::unique_lock<std::mutex> waitLock(retryMutex);
        retryChanged.wait_until(
            waitLock,
            std::min(deadline, now + IO_POLL_INTERVAL),
            [this] { return closeRequested.load(std::memory_order_acquire); });
        return Clock::now() < deadline;
    }

    udp::socket sock;
    std::mutex socketMutex;
    std::mutex retryMutex;
    std::condition_variable retryChanged;
    std::atomic<bool> closeRequested{false};
};

} // namespace libera::net
