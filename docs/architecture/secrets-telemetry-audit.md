# Secrets and OpenTelemetry coverage audit

**Audit date:** 2026-10-10  
**Scope:** checked-out Orbit repositories and the implementation in this workspace. A package name,
roadmap entry, README promise, or protocol sketch is not treated as implemented behavior.

## Coverage before this change

| Area | Implemented before this change | Planned or missing before this change |
| --- | --- | --- |
| Secrets | Existing cloud packages enumerate resources; configuration examples can represent secret-like values with Pydantic `SecretStr`. | No provider-neutral secret resolution package, versioned secret value contract, or AWS/GCP/Azure secrets adapter. Resource inventory is not secret management. |
| Configuration | `orbit-config-server` defines a versioned remote configuration contract and snapshot behavior. | No centralized secret server, secret backend, automatic credential injection, or secret rotation coordinator. |
| Cloud | `orbit-cloud` defines read-only resource inventory and separate AWS, GCP, Azure inventory adapters. | Those adapters do not resolve cloud secret values. |
| Logging | `orbit-logging` provides opt-in structured JSON formatting and bounded Orbit application/request/correlation/trace/span context. | No OpenTelemetry Logs SDK export package, OTLP logs transport, or log/trace correlation bridge. |
| Metrics | `orbit-core` owns bounded in-process counters, gauges, histograms, and immutable snapshots. `orbit-metrics` defines provider-neutral export. `orbit-metrics-prometheus` adds optional Prometheus text exposition and a route. Grafana package provides dashboards. | No OpenTelemetry Metrics exporter for Core snapshots and no trace-linked metric exemplar contract. Core snapshots are local to one process. |
| Tracing | `orbit-tracing` adapts Core tracing to OpenTelemetry SDK spans and supports explicit W3C trace-context extraction/injection. | No automatic HTTP client/server instrumentation; no shared OTel resource/correlation policy across logs and metrics. |

## Added in this alpha candidate

- `orbit-secrets` defines bounded async resolve/refresh/close contracts, opaque resource references,
  immutable versioned values, fixed safe exception messages, retryability categories, and explicit
  reveal methods. Values are not cached or automatically rotated.
- `orbit-secrets-aws`, `orbit-secrets-gcp`, and `orbit-secrets-azure` map provider version semantics,
  lazily acquire provider clients/identity, bound request time and concurrency, sanitize provider
  errors, propagate cancellation, drain active reads, and close owned clients. SDK imports are lazy.
- `orbit-logging` exposes `current_log_context()` for detached, bounded identifiers.
  `orbit-logging-opentelemetry` exports through an isolated OTel LoggerProvider and a bounded batch
  processor. It does not mutate source records, bounds message bodies, excludes exception
  message/stack data, and does not alter the global logger provider.
- Core histogram snapshots now retain exact minimum and maximum. Core captures at most four recent
  trace exemplars per series for counter increments and histogram observations made inside valid
  Orbit trace context. IDs remain separate from labels.
- `orbit-metrics-opentelemetry` maps Core snapshot counters, gauges, histograms, and exemplars to the
  OTel SDK data model and exports through an injected SDK exporter. It uses one sequential bounded
  collection loop and does not register a global MeterProvider.

## Cross-signal use

See [`../../examples/cloud_native.py`](../../examples/cloud_native.py) for a local integration
scenario. It resolves a fake AWS Secrets Manager response through `orbit-secrets`, runs an Orbit
request span, emits a redacted log, and exports a Core metric with a trace exemplar. All three
signals use the same `service.name`; the example's secret value is never sent to telemetry.

## Validation and release gates

Offline unit tests use fake provider clients, so they verify request mapping, bounds, safe failures,
version extraction, cancellation, and cleanup behavior only. They do not establish live IAM,
provider SDK/API compatibility, cloud throttling behavior, or key-rotation propagation. Validate
each adapter in a dedicated non-production account before calling it cloud-certified.

The workspace's Python 3.11.14 interpreter hangs while shutting down the default executor after
`asyncio.to_thread`, even in a standalone one-line check. The cloud adapter fake-client tests and
the end-to-end example pass locally; the example uses immediate in-memory exporters and runs their
offload inline so it can verify all three signals despite this interpreter issue. Both OpenTelemetry
exporter lifecycle test assertions also pass with that same test-only inline offload. The normal
default-executor shutdown path still needs a clean hosted run on Python 3.11–3.14. Also run live
OTLP collector tests, provider SDK dependency installation, package builds, vulnerability audit,
and release provenance before publication. No hosted workflow, publication, or cloud account
validation is asserted by this local audit.
