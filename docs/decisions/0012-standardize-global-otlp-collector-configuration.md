# ADR 0012: Standardize Global OTLP Collector Configuration

Status: accepted

Date: 2026-08-14

## Context

Inherited Helm charts configured an OpenTelemetry collector independently for
each service. The duplicated `otelcollector` values drifted: some services
constructed a bearer authorization header from a token Secret, some treated
the Secret value as the complete header, and newer NVIDIA Recovery workloads had no
OTLP exporter environment variables at all.

Deployments can place one OTLP collector beside the NVIDIA Recovery release.
Collector endpoint, protocol, and authentication are release-wide deployment
properties, while application SDK initialization and telemetry consumption
remain service-owned behavior.

## Decision

The `nvidia-recovery` umbrella chart owns one `global.otelcollector` configuration and
one shared Helm helper that injects OTLP exporter environment variables into
every NVIDIA Recovery application container.

When enabled, the helper injects the standard
`OTEL_EXPORTER_OTLP_PROTOCOL` and `OTEL_EXPORTER_OTLP_ENDPOINT` variables. If
bearer authentication is enabled, it reads the token from an existing
namespace-local Secret into `OTEL_EXPORTER_OTLP_TOKEN` and constructs
`OTEL_EXPORTER_OTLP_HEADERS` from that token. Required endpoint, protocol, and
Secret-reference fields fail Helm rendering when absent.

The integration covers NVIDIA Recovery-owned application containers, including both
Slurm advisor controller workloads and the optional dependency checker. It
does not inject application exporter settings into third-party infrastructure
or helper containers. The configuration only supplies environment variables;
it does not implicitly enable or implement a service's OpenTelemetry SDK.

## Consequences

- Operators configure one collector contract for the complete umbrella
  release instead of repeating values under every service.
- Existing per-service `otelcollector` values are removed; deployment overlays
  must move those settings to `global.otelcollector`.
- Secret contents remain outside rendered manifests. The referenced key stores
  only the bearer token, and Kubernetes constructs the standard header value
  through environment-variable expansion.
- Services without OTLP consumption logic can receive the same environment
  contract now and adopt it independently later.
- OTLP injection is an umbrella-chart capability. Standalone service charts
  retain their default rendering, but the shared global integration requires
  rendering them as dependencies of the umbrella chart.

## Alternatives Rejected

- Keep per-service collector values and templates. This preserves duplicated
  configuration and the existing authentication drift.
- Make each service own collector activation and Secret parsing. Collector
  connectivity is a deployment concern and does not need to be coupled to
  each service's SDK lifecycle.
- Inject the variables into every container in the release. Infrastructure and
  helper containers are not NVIDIA Recovery telemetry producers and should not
  receive application credentials without a defined consumer contract.
