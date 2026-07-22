# LaserCubeNet confirmed lifecycle API

`LaserCubeNetController` keeps the legacy `connect()` entry point, but it now
implements that call through a bounded `connectDark()` operation. New code
should call the explicit lifecycle methods and supply a monotonic deadline plus
an optional cancellation predicate:

- `connectDark(info, operation)`
- `enableOutput(operation)`
- `disableDark(operation)`
- `shutdownDark(operation)`

The methods return `LaserCubeNetLifecycleResult`. Its boolean, `error()`,
`value()`, dereference, and arrow operations intentionally match the small
`expected<T>` surface used by existing callers. Unlike `expected<T>`, a failed
result retains a report. `None` means no remote request was sent;
`HostDarkRequested` and `HostEnableRequested` mean UDP transmission was
requested but do not claim a device acknowledgement. Only a qualifying full
status produces `DeviceReportedDisabled` or `DeviceReportedEnabled`.

## Dark sequence and correlation

Connect, reconnect, disable, and result-bearing shutdown locally disarm and
gate streaming before the remote lifecycle. Sample acknowledgement state from
the retired output generation is abandoned and cannot delay the first off or
contribute to dark evidence. If a previously submitted data datagram arrives
late and refills the FIFO, the fresh nonempty buffer status forces another
complete clear attempt. The remote command order is:

```text
output off -> clear buffer -> output off -> full-status probe
```

Connect additionally configures buffer responses and the selected point rate
between the final off and the status probe.

Every lifecycle attempt binds a new ephemeral command/status socket. Delayed
full-status datagrams from an older attempt therefore target a retired receive
port and cannot confirm the current attempt. Within one socket epoch, an
eligible status must be received from the configured device command endpoint,
match the selected serial, follow the final control command and a subsequent
full-status probe, parse successfully, and report the requested state. The
LaserCubeNet wire format has no request nonce, so this design retains the
explicit assumption that a response delivered within the new endpoint epoch
corresponds to a probe in that epoch. Hardware verification must validate that
assumption; receive timestamp alone is not treated as correlation.

When `bufferMax > 0`, dark confirmation also requires
`bufferFree == bufferMax`. A zero `bufferMax` means the capability is unknown:
output-disabled status may still confirm dark, while the report keeps
`bufferCapacitySupported == false` and `bufferConfirmedEmpty == false`.

## Enable, failure, and cleanup

Enable requires an installed content source. It first rotates to a new
command/status socket epoch, then sends the selected rate and a pair of real
all-black startup sample packets while remote output remains off. Their sample
acknowledgements are used only as startup-content delivery barriers, including
under acknowledgement reordering; they are not output or clear evidence.
After draining unexpected command responses, enable requests output on and
accepts only status received after that request and a subsequent full-status
probe. Ordinary streaming remains gated until the qualifying status reports
enabled. Repeated confirmed enable is idempotent.

A concurrent disable has priority over an in-flight enable. Disable advances a
local output-intent generation before waiting for the lifecycle lock; enable
checks that generation before sending output-on and atomically again before
arming local streaming. If disable has preempted it, enable remains disarmed,
sends a best-effort off, and returns cancellation with
`HostEnableRequested` retained when output-on had already been sent.

Connection, send, malformed/stale status, buffer, and cancellation failures
clear local output intent. Data-path failures also make a best-effort off
request. Reconnect always runs `connectDark()` and never restores prior armed
intent. A manager drops a controller whose first dark handshake fails, so a
retry constructs a fresh disarmed instance.

`shutdownDark()` returns the strongest evidence obtained before closing its
sockets. Destructor cleanup remains non-throwing and performs no operation that
can upgrade partial evidence to confirmed dark.

## Clear-buffer provenance

`CMD_CLEAR_RINGBUFFER` (`0x8d`, zero payload) was not present in the audited
permissive Libera baseline. It is implemented here only under the downstream
synthetic/unverified protocol record and must not be described as manufacturer
documentation or permissive-source evidence. Its payload, empty-buffer effect,
and physical-device behavior remain subject to the named hardware-verification
gate.
