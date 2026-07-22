#pragma once
#include "libera/net/NetConfig.hpp"
#include "libera/log/Log.hpp"

#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <memory>
#include <system_error>
#include <utility>

/**
 * @brief Run an async operation with a deadline enforced by an Asio timer.
 *
 * Pattern:
 * - Post creation and initiation of the async operation and timer to the same
 *   executor.
 * - Whichever completes first cancels the other and signals a condition
 *   variable so this call can return synchronously with a timeout.
 *
 * Why it is useful:
 * - Blocking APIs with timeouts are common in openFrameworks; in Asio the
 *   equivalent pattern is "async operation + timer + cancel". This helper wraps
 *   that flow and surfaces the resulting `std::error_code`.
 *
 * Safety notes:
 * - Every operation on the Asio timer and socket is initiated from the I/O
 *   executor. This matters because a socket's synchronous setup thread and its
 *   completion thread must not concurrently initiate/cancel async operations.
 * - Completion handlers capture a `shared_ptr<State>` so they cannot access
 *   destroyed synchronisation primitives even if they run after this function
 *   returns. This avoids a common use-after-free race in naive implementations.
 * - The `cancel()` functor must cancel the same socket or timer that launched
 *   the operation. It is invoked behind a catch boundary so a cancellation
 *   failure cannot escape the Asio handler thread.
 *
 * Requirements:
 * - The associated `asio::io_context` must already be running while we block.
 *   Otherwise the wait would never complete.
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
    //std::cout << "[with_deadline] start timeout=" << timeout.count() << "ms\n";
    struct State {
        std::mutex m;
        std::condition_variable cv;
        bool done = false;
        bool timeoutRequested = false;
        std::error_code ec = asio::error::would_block;
    };

    auto st = std::make_shared<State>();

    // Asio permits handlers to run on another thread, but shared socket/timer
    // objects are not generally safe for concurrent initiation and cancellation.
    // Post the whole setup so timer construction, async initiation and cancel all
    // happen on the executor thread. The caller only waits on State.
    try {
        asio::post(ex,
            [st,
             ex,
             timeout,
             start_async = std::move(start_async),
             cancel = std::move(cancel),
             label,
             logTimeout]() mutable {
                auto timer = std::make_shared<asio::steady_timer>(ex);

                auto complete = [st](const std::error_code& ec) {
                    {
                        std::lock_guard<std::mutex> lk(st->m);
                        if (st->done) return;
                        st->ec = ec;
                        st->done = true;
                    }
                    st->cv.notify_one();
                };

                auto op_handler = [st, timer, complete](
                                      const std::error_code& op_ec,
                                      auto&&... /*ignored*/) {
                    bool timedOut = false;
                    {
                        std::lock_guard<std::mutex> lk(st->m);
                        if (st->done) return;
                        timedOut = st->timeoutRequested;
                    }

                    try {
                        timer->cancel();
                    } catch (...) {
                        // The operation result is authoritative. A timer cancel
                        // failure must not prevent the waiter from completing.
                    }
                    complete(timedOut ? std::error_code(asio::error::timed_out)
                                      : op_ec);
                };

                timer->expires_after(timeout);
                timer->async_wait(
                    [st, cancel, complete, timeout, label, logTimeout](
                        const std::error_code& timerEc) mutable {
                        if (timerEc == asio::error::operation_aborted) return;

                        {
                            std::lock_guard<std::mutex> lk(st->m);
                            if (st->done) return;
                            st->timeoutRequested = true;
                        }

                        if (logTimeout) {
                            logInfo("[with_deadline] timeout fired after",
                                    timeout.count(), "ms", label);
                        }

                        try {
                            cancel();
                            // The operation's cancellation handler completes the
                            // synchronous call, ensuring no old handler remains
                            // live when the caller starts another operation.
                        } catch (const std::exception& e) {
                            logError("[with_deadline] cancel failed", label, e.what());
                            complete(asio::error::timed_out);
                        } catch (...) {
                            logError("[with_deadline] cancel failed", label,
                                     "unknown exception");
                            complete(asio::error::timed_out);
                        }
                    });

                try {
                    start_async(std::move(op_handler));
                } catch (const std::system_error& e) {
                    logError("[with_deadline] async start failed", label, e.what());
                    try { timer->cancel(); } catch (...) {}
                    complete(e.code());
                } catch (const std::exception& e) {
                    logError("[with_deadline] async start failed", label, e.what());
                    try { timer->cancel(); } catch (...) {}
                    complete(asio::error::operation_aborted);
                } catch (...) {
                    logError("[with_deadline] async start failed", label,
                             "unknown exception");
                    try { timer->cancel(); } catch (...) {}
                    complete(asio::error::operation_aborted);
                }
            });
    } catch (const std::system_error& e) {
        logError("[with_deadline] executor post failed", label, e.what());
        return e.code();
    } catch (const std::exception& e) {
        logError("[with_deadline] executor post failed", label, e.what());
        return asio::error::operation_aborted;
    } catch (...) {
        logError("[with_deadline] executor post failed", label,
                 "unknown exception");
        return asio::error::operation_aborted;
    }

    // Wait until either branch completes
    std::unique_lock<std::mutex> lk(st->m);
    st->cv.wait(lk, [&]{ return st->done; });
    return st->ec;
}
}
