# Architecture and lifecycle contract

## Responsibility

`orbit-observability` coordinates lifecycle operations for explicitly configured telemetry
providers. It does not own logging, metrics, or tracing APIs, create instrumentation, select
exporters, configure a collector, or install global SDK state. Those concerns remain in their
capability and provider packages.

```text
Application chooses providers and policy
  └── ObservabilityLifecycle
      ├── bounds concurrent callbacks
      ├── enforces per-provider deadlines
      ├── attempts flush/shutdown for each registered provider
      └── reports sanitized aggregate failures
            └── TelemetryProvider adapter owns exporter resources
```

The application must pass the configured providers, ensure one lifecycle owner per provider, and
call shutdown while dependencies required by exporters are still running.

## Provider contract

`TelemetryProvider` declares a unique lowercase slug, a non-empty set of known
`TelemetrySignal` values, and async `force_flush` and `shutdown` methods. Providers receive a
per-operation timeout and should honor it internally. Shutdown should release owned resources and
be idempotent. The coordinator does not validate that a provider truly exports each declared
signal.

The registry contains 1–32 providers. Operations use bounded concurrency; each callback has a
finite deadline greater than zero and at most 300 seconds. Each provider is attempted even if a
different callback fails. Timeouts cancel a callback. Ordinary flush is cancellable; shutdown runs
in a retained task so a cancelled waiter can call again and observe completion.

## Error contract

The coordinator reports `timeout` or `provider_failure` using the registered provider slug and
operation. It discards original exception details. `LifecycleFailure` validates these fields even
when constructed directly, preserving the safety of diagnostic rendering. Cancellation is not
converted into a provider failure.

## Extension boundaries

Do not add exporter SDKs or vendor settings here. Keep instrumentation and signal APIs in the
logging, metrics, or tracing capability; keep remote clients and vendor error mapping in adapters;
keep application readiness, telemetry filtering, and shutdown ordering in the application. This
package's lifecycle does not certify delivery, privacy, or backend availability.
