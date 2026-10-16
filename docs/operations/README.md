# Operations

`ObservabilityLifecycle` coordinates only providers explicitly passed to its constructor. It does
not install instrumentation, discover exporters, choose telemetry backends, or mutate a global SDK
provider. The application owns provider creation and transfers flush/shutdown responsibility to
the lifecycle coordinator.

## Application lifecycle

Create configured providers after their dependencies are ready, then construct one lifecycle
owner. Register an application shutdown hook that calls `await lifecycle.shutdown()` before
stopping resources the exporters need, such as their event loop or network clients. Shutdown first
flushes each provider and then calls every provider's shutdown method, even when a flush fails.
Providers should release their own queues, threads, clients, and transports and make shutdown
idempotent.

Use `force_flush()` for an explicit checkpoint when buffered telemetry must be pushed before
continuing. Flush calls are serialized with shutdown. Do not invoke this API from synchronous signal
handlers; schedule shutdown on the running event loop. Shutdown is shielded from waiter cancellation:
the retained cleanup task continues, and concurrent callers can await it. A later call retries
provider stages that failed previously; completed shutdown stages are not repeated. A flush failure
is reported even if provider shutdown succeeds, since telemetry may already have been lost.

## Timeouts, concurrency, and failures

The coordinator accepts 1–32 providers and runs at most `max_concurrency` callbacks at once. The
per-provider timeout defaults to 10 seconds and must be finite, greater than zero, and no more than
300 seconds. Provider implementations receive the same timeout. At the deadline, the coordinator
cancels the callback and reports a timeout even if the callback suppresses cancellation and returns
later. Cancellation is cooperative: a callback that suppresses it can delay completion of the
coordinator itself. Provider adapters must pass the timeout to the SDK or transport and propagate
cancellation; the coordinator cannot forcibly stop arbitrary Python or native work. Ensure provider
callbacks clean up correctly when cancelled.

Every eligible provider is attempted even if another provider fails or times out. Failures are
aggregated as `LifecycleFailure(provider, operation, code)`. The only codes are `timeout` and
`provider_failure`; vendor exception text is deliberately discarded. Catch
`ObservabilityLifecycleError` at the application boundary, report the safe failure fields, and
decide whether telemetry loss should affect shutdown or readiness. Do not infer the underlying
cause from the category alone.

Cancellation of a normal `force_flush()` propagates to active provider operations. Shutdown uses a
retained task so cancellation of its waiter does not abort cleanup. Avoid starting multiple
independent coordinators for the same exporter because each would believe it owns that provider.

## Telemetry and release validation

The lifecycle only bounds flush and shutdown. It does not filter, redact, encrypt, or authenticate
telemetry. Validate provider behavior for flush deadlines, cancellation, repeated shutdown,
shutdown ordering, exporter outages, sensitive attribute handling, and event-loop termination.
Run integration checks using the actual selected exporter versions and a non-production collector;
unit tests cannot certify delivery to a backend.
