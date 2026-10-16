# Security

## Telemetry content

Logs, metrics, and traces can contain credentials, user identifiers, request bodies, internal
hostnames, and business data. This package coordinates provider lifecycle only; it does not inspect
or sanitize telemetry. Each instrumentation and exporter configuration must apply an explicit
data-minimization policy before export. Avoid high-cardinality labels and never put secrets in
metric attributes, span names, resource attributes, or log fields.

## Safe failure reporting

`LifecycleFailure` allows only a bounded lowercase provider slug, a known operation enum, and the
stable categories `timeout` or `provider_failure`. `ObservabilityLifecycleError` omits raw vendor
exceptions because those exceptions may include request data, exporter URLs, credentials, or
telemetry values. Preserve this safe representation in application logs and metrics; avoid logging
the original exception inside provider implementations unless they sanitize it first.

Provider names should be static identifiers, not endpoint URLs, tenant names, user input, or
credentials. Names appear in lifecycle errors and may become metric labels; use a small, fixed set
to keep cardinality bounded.

## Provider and transport responsibilities

Adapters own transport security, authentication, credential rotation, authorization, and
exporter-specific redaction. Use least-privilege credentials and verified TLS for remote
collectors. Do not disable certificate verification as a workaround. Configure timeouts and avoid
exporting telemetry to an untrusted collector. Review retention, access controls, and data region
for the selected backend.

## Disclosure

Follow the repository's [security policy](../../SECURITY.md) for private vulnerability reporting.
