# Cloud native secrets and telemetry example

[`cloud_native.py`](cloud_native.py) shows the cross-package composition:

```text
orbit-secrets → orbit-secrets-aws (fake client for local execution)
orbit-tracing ─┐
orbit-logging-opentelemetry ─┼→ one service identity and correlated telemetry
orbit-metrics-opentelemetry ─┘
```

The example verifies that the resolved value is absent from logs, metric attributes, and spans;
logs inherit the active trace context; and Core counter/histogram observations export trace-linked
exemplars. The fake client exercises Orbit's adapter boundary only, not AWS IAM, AWS service
behavior, or network transport.

Install the Orbit packages from their workspace checkouts plus the OpenTelemetry SDK, `orbit-testing`,
and `orbit-secrets-aws`. Run `python examples/cloud_native.py` from this repository after installing
those dependencies. OTLP exporters can replace the in-memory exporters for a separately configured
collector test.
