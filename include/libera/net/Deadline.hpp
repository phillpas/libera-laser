#pragma once
#include "libera/net/NetConfig.hpp"
#include "libera/log/Log.hpp"

#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <memory>
#include <system_error>

/**
 * @brief Run an async operation with a deadline enforced by an Asio timer.
 *
 * This is the established TCP helper. LaserCubeNet UDP intentionally does not
 * use it: UdpSocket applies executor-independent monotonic deadlines directly.
 */
namespace libera::net {

template<typename StartAsync, typename Cancel>
std::error_code with_deadline(
    asio::any_io_executor ex,
    std::chrono::milliseconds timeout,
    StartAsync start_async,
    Cancel cancel,
    const char* label = "",
    bool logTimeout = false)
{
    struct State {
        std::mutex m;
        std::condition_variable cv;
        bool done = false;
        std::error_code ec = asio::error::would_block;
    };

    auto st = std::make_shared<State>();
    auto timer = std::make_shared<asio::steady_timer>(ex);

    auto op_handler = [st, timer](const std::error_code& op_ec, auto&&... /*ignored*/) {
        {
            std::lock_guard<std::mutex> lk(st->m);
            if (st->done) return;
            st->ec = op_ec;
            st->done = true;
        }
        st->cv.notify_one();
        try {
            timer->cancel();
        } catch (const std::exception& e) {
            logError("[with_deadline] timer cancel failed", e.what());
        } catch (...) {
            logError("[with_deadline] timer cancel failed", "unknown exception");
        }
    };

    try {
        start_async(op_handler);
    } catch (const std::system_error& e) {
        logError("[with_deadline] async start failed", label, e.what());
        return e.code();
    } catch (const std::exception& e) {
        logError("[with_deadline] async start failed", label, e.what());
        return asio::error::operation_aborted;
    } catch (...) {
        logError("[with_deadline] async start failed", label, "unknown exception");
        return asio::error::operation_aborted;
    }

    timer->expires_after(timeout);
    timer->async_wait([st, cancel, timer, timeout, label, logTimeout](const std::error_code& tec){
        if (tec == asio::error::operation_aborted) {
            return;
        }
        bool notify = false;
        {
            std::lock_guard<std::mutex> lk(st->m);
            if (st->done) {
                return;
            }
            st->ec = asio::error::timed_out;
            st->done = true;
            notify = true;
        }
        if (notify) {
            if (logTimeout) {
                logInfo("[with_deadline] timeout fired after", timeout.count(), "ms", label);
            }
            try {
                cancel();
            } catch (const std::exception& e) {
                logError("[with_deadline] cancel failed", label, e.what());
            } catch (...) {
                logError("[with_deadline] cancel failed", label, "unknown exception");
            }
            st->cv.notify_one();
        }
    });

    std::unique_lock<std::mutex> lk(st->m);
    st->cv.wait(lk, [&]{ return st->done; });
    return st->ec;
}

}
