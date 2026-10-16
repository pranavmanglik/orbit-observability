# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Exercise secret resolution and correlated OTel signals without cloud credentials.

Run this example from an environment with the local Orbit packages and OpenTelemetry SDK extras
installed. Fake provider transport keeps the sample deterministic; it does not validate cloud IAM.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any

from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.metrics.export import MetricExporter, MetricExportResult, MetricsData
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from orbit import Application, ApplicationConfig
from orbit.asgi import Response
from orbit.runtime import Runtime
from orbit_logging_opentelemetry import OpenTelemetryLoggingConfig, OpenTelemetryLoggingPlugin
from orbit_metrics_opentelemetry import OpenTelemetryMetricsConfig, OpenTelemetryMetricsPlugin
from orbit_secrets import SecretReference
from orbit_secrets_aws import AWSSecretsProvider
from orbit_testing import TestClient
from orbit_tracing import OpenTelemetryConfig, OpenTelemetryPlugin

SERVICE_NAME = "orbit-cloud-native-example"
SECRET_TEXT = "example-secret-never-export-this"


class _FakeSecretsClient:
    """Return a test-only AWS-shaped secret response."""

    async def get_secret_value(self, **kwargs: Any) -> Mapping[str, Any]:
        assert kwargs["SecretId"] == "examples/database/password"
        return {"SecretString": SECRET_TEXT, "VersionId": "v-test-1"}


@asynccontextmanager
async def _fake_client_context():
    """Inject an in-memory client while preserving the AWS adapter's ownership lifecycle."""
    yield _FakeSecretsClient()


class _MemoryMetricExporter(MetricExporter):
    """Keep exported SDK data in memory for exact correlation assertions."""

    def __init__(self) -> None:
        super().__init__()
        self.items: list[MetricsData] = []

    def export(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 10_000,
        **kwargs: Any,
    ) -> MetricExportResult:
        self.items.append(metrics_data)
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        return None


async def verify_example() -> None:
    """Resolve a secret in a request and assert correlated spans, logs, and metric exemplars."""
    span_exporter = InMemorySpanExporter()
    log_exporter = InMemoryLogRecordExporter()
    metric_exporter = _MemoryMetricExporter()
    tracing = OpenTelemetryPlugin(
        OpenTelemetryConfig(
            service_name=SERVICE_NAME,
            schedule_delay_millis=50,
            export_timeout_millis=1_000,
            max_queue_size=64,
            max_export_batch_size=32,
        ),
        exporter=span_exporter,
    )
    logger = logging.getLogger("orbit.example.orders")
    logging_plugin = OpenTelemetryLoggingPlugin(
        OpenTelemetryLoggingConfig(
            service_name=SERVICE_NAME,
            logger_name=logger.name,
            schedule_delay_millis=50,
            export_timeout_millis=1_000,
            max_queue_size=64,
            max_export_batch_size=32,
        ),
        exporter=log_exporter,
        logger=logger,
    )
    metrics_plugin = OpenTelemetryMetricsPlugin(
        OpenTelemetryMetricsConfig(
            service_name=SERVICE_NAME,
            export_interval_millis=60_000,
            export_timeout_millis=10_000,
        ),
        exporter=metric_exporter,
    )
    app = Application(ApplicationConfig(name=SERVICE_NAME))
    app.register_plugin(tracing)
    app.register_plugin(logging_plugin)
    app.register_plugin(metrics_plugin)
    request_count = app.diagnostics.metrics.counter("example_secret_reads_total")
    read_duration = app.diagnostics.metrics.histogram(
        "example_secret_read_seconds", buckets=(0.01, 0.1, 1.0)
    )
    secret_provider = AWSSecretsProvider(client_factory=_fake_client_context)

    @app.router.route("/ready", name="secret-backed-readiness")
    async def secret_backed_readiness(request):
        """Resolve without logging the secret and emit low-cardinality measurements."""
        value = await secret_provider.resolve(SecretReference(name="examples/database/password"))
        assert value.reveal_text() == SECRET_TEXT
        request_count.inc()
        read_duration.observe(0.002)
        logger.info("secret-backed dependency check succeeded", extra={"result": "ok"})
        return Response.text("ready")

    runtime = Runtime(app, tracer=tracing)
    original_level = logger.level
    logger.setLevel(logging.INFO)
    try:
        async with TestClient(runtime) as client:
            response = await client.request("GET", "/ready")
            print("Secret-backed request returned; waiting for application shutdown.")
        assert response.status == 200
        print("Secret-backed request completed; checking exported signals.")
    finally:
        logger.setLevel(original_level)
        await secret_provider.aclose()

    spans = span_exporter.get_finished_spans()
    logs = log_exporter.get_finished_logs()
    assert spans and logs
    span = next(item for item in spans if item.name == "http.get")
    log_record = logs[-1].log_record
    assert log_record.trace_id == span.context.trace_id
    assert log_record.span_id != 0

    exported_metrics = [item for batch in metric_exporter.items for item in batch.resource_metrics]
    datapoints = [
        point
        for resource_metrics in exported_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
        for point in metric.data.data_points
    ]
    exemplars = [exemplar for point in datapoints for exemplar in point.exemplars]
    assert any(item.trace_id == span.context.trace_id for item in exemplars)
    all_telemetry = f"{spans!r}{logs!r}{metric_exporter.items!r}"
    assert SECRET_TEXT not in all_telemetry
    print("Verified secret resolution and correlated OpenTelemetry logs, metrics, and traces.")


if __name__ == "__main__":
    asyncio.run(verify_example())
