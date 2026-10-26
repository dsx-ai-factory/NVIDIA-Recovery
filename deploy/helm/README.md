# Helm Charts

## Dashboard session keys

The dashboard stores `NEXTAUTH_SECRET` and `API_GATEWAY_SESSION_SECRET` in a
release-scoped Kubernetes Secret and loads them through `secretKeyRef`. When
omitted on a fresh install, each key gets a random 32-character secret. Helm
reuses the stored keys on upgrades. If a key cannot be recovered during an
upgrade, or is missing or empty in an existing chart-managed Secret, rendering
fails. Restore the Secret or explicitly supply the missing key to rotate it.

For operator-supplied keys, set `auth.NEXTAUTH_SECRET` and
`auth.API_GATEWAY_SESSION_SECRET` in the dashboard chart. Non-empty supplied
values are used as provided, without chart-level length validation. Runtime
requirements are enforced by the session libraries. On a fresh install without
a stored Secret, a missing key is generated independently.
Explicit values override stored keys and trigger a dashboard rollout when
changed. Supply values through your secret-management workflow, not a committed
values file.

Alternatively, set `auth.existingSessionSecret` to an operator-managed Secret in
the release namespace containing both environment-variable names as keys. Do
not combine it with explicit key values. The chart does not create or modify
that Secret; restart dashboard Pods after rotating its keys. For the umbrella
chart, prefix all these settings with `dashboard.`.

Offline `helm template` and client-side dry runs cannot look up stored keys.
When the chart manages the Secret, install-mode rendering preserves explicitly
supplied keys and generates fresh values for missing keys on each render;
upgrade-mode rendering requires both keys to be explicitly supplied. When
`auth.existingSessionSecret` is set, rendering references that Secret without
looking up its contents or requiring explicit key values. The referenced Secret
must contain both keys when the Pods run. Use `--dry-run=server` to validate an
upgrade against a stored chart-managed Secret. For GitOps, use
`auth.existingSessionSecret` or supply both stable keys to avoid rotating sessions
on each reconciliation.

Upgrading from a chart with fixed keys requires explicitly supplying two new
random keys or selecting `auth.existingSessionSecret`. Do not copy the old public
defaults. This rotation rolls out the dashboard, invalidates existing dashboard
sessions/cookies, and requires users to sign in again. Enabling the dashboard for
the first time in an existing umbrella release also requires supplied keys or an
existing Secret because Helm treats it as an upgrade. The legacy ConfigMap keys
are never imported. Uninstalling deletes the chart-managed Secret; reinstalling
then generates new keys. Rolling back to a vulnerable chart restores its public
defaults and must be avoided.

## Retired audit enable values

Audit production and ingestion are always active. The following retired enable
values are no longer accepted by the chart schemas:

| Chart | Retired values |
| --- | --- |
| slurm-advisor-controller | `controller.auditTrail.enabled`, `watchdog.auditTrail.enabled` |
| job-manager | `service_config.audit_trail.enabled` |
| watcher | `config.service.audit_trail.enabled` |
| anomaly-detection-isolator | `audit_trail.enabled` |
| notifications | `app.auditLogEnabled` |

Remove these keys from values files and retained release values before upgrading.
The temporary compatibility settings and their deprecation warnings have been
removed.

Go producers support logging or gateway delivery, and Python producers log when
their rendered service configuration has no destination URL. The Python producer
charts resolve omitted audit destinations as follows:

| Chart | Destination value | Default when omitted |
| --- | --- | --- |
| job-manager | `service_config.audit_trail.destination` | `service_config.notification_service_url` plus `/v1/audit/events` |
| watcher | `config.service.audit_trail.destination` | The same-release Notifications Service on port `8080`, path `/v1/audit/events` |
| anomaly-detection-isolator | `audit_trail.destination` | The same-release Notifications Service on port `8080`, path `/v1/audit/events` |

An explicit destination URL overrides the default in each chart; set
`destination: ""` for logging only. Watcher and ADI also preserve an explicit
`destination: null` as logging only. Job Manager's `notification_service_url`
must identify the installed Notifications Service or an operator-supplied endpoint.
Job Manager removes all trailing slashes from that URL before appending the audit route.
Watcher and ADI use the Notifications chart's default Service name
(`<release>-notifications`, or the release name if it already contains
`notifications`). For standalone installations with Notifications in another
release, or custom Notifications names or ports, configure their complete audit
destination URLs explicitly.
Retained `audit_trail.destination` values remain explicit overrides; remove a
stale destination from retained release values to use the derived default.

## Audit UI visibility

`dashboard.app.AUDIT_EVENTS_ENABLED` remains an active flag and defaults to
`true`. For logging-only deployments, set it to `false` to hide Audit Events and
the job Timeline. The dashboard environment variable `AUDIT_EVENTS_ENABLED` also
defaults to on when omitted. This controls UI visibility only; producer delivery
and ingestion remain active.

## Shared OTLP collector integration

The `nvidia-recovery` umbrella chart injects one release-wide OTLP exporter
configuration into every NVIDIA Recovery application container. Configure it once
under `global.otelcollector`; service-specific `otelcollector` sections are not
used.

```yaml
global:
  otelcollector:
    enabled: true
    endpoint: http://otel-collector:4317
    protocol: grpc
    auth:
      enabled: true
      secretName: otel-collector-token
      secretKey: token
```

For the namespace, Secret-creation, and quick-start values steps, see
[Optional external OTLP collector](../../docs/operators/operations.md#optional-external-otlp-collector).

When enabled, every application receives `OTEL_EXPORTER_OTLP_PROTOCOL` and
`OTEL_EXPORTER_OTLP_ENDPOINT`. Set `protocol` to `grpc` or `http/protobuf`
(`http` is accepted as an alias). The endpoint is the collector base URL shared
by every signal: gRPC uses it as-is, while HTTP exporters send to its
`/v1/traces` and `/v1/logs` paths, so do not include a signal path. With authentication enabled, the referenced
Secret key is exposed as `OTEL_EXPORTER_OTLP_TOKEN` and used to construct the
standard `OTEL_EXPORTER_OTLP_HEADERS` value. The Secret must already exist in
the release namespace and its key must contain only the bearer token, not a
complete HTTP header.

The standalone `slurm-advisor-controller` chart accepts the same
`global.otelcollector` values through its own copy of the helper, which
`deploy/helm/scripts/check-otel-env-parity.sh` keeps identical in rendered
output.

This setting standardizes environment injection only. A service still needs
OTLP-aware runtime logic, and any service-specific SDK enablement flag remains
independent (for example, `api-gateway.otel.enabled`).

The shared settings cover job-manager, anomaly-detection-isolator,
attribution-adapter, dashboard, watcher, notifications, api-gateway, both
Slurm advisor controller workloads, and the optional dependency checker. They
do not configure third-party infrastructure containers such as databases,
node-exporter, sackd, or the collector itself.

## Bundled Loki (optional centralized log backend)

The `nvidia-recovery` umbrella chart can deploy Grafana Loki as an optional sub-chart
(single-binary mode, per the runbook `docs/ops/loki-slurm-runai-logging.md`
§4). It ships disabled; enable it with the global flag:

```bash
helm dependency build deploy/helm/nvidia-recovery
helm install heimdall deploy/helm/nvidia-recovery \
  --set global.loki.bundled.enabled=true
```

Query consumers require explicit provider connections even when the bundled
Loki is enabled. For the default gateway in the same namespace, configure:

```yaml
global:
  log_query:
    backend: loki
    providers:
      loki:
        application:
          url: http://loki-gateway
          tenants: ""
          org_id: ""
        system:
          url: http://loki-gateway
          tenants: ""
          org_id: ""
```

Log-query selection is independent of that provisioning choice. The sole
release-wide selector is `global.log_query.backend`, which defaults to `loki`
and may be set to `victorialogs`. Selecting VictoriaLogs does not deploy it or
redirect Alloy or syslog ingestion: the deployment must also supply native,
validated LogsQL variants, explicit provider credentials and tenant settings,
and qualify the production ingestion path. VictoriaLogs 1.39.0 or newer is
required. See [Log-Query Backends](../../docs/architecture/log-query-backends.md).
The operator-facing values, credentials, native-query variants, and switch
checklist are documented in
[Configure Log-Query Backends](../../docs/ops/configure-log-query-backends.md).

All deployments, including standalone charts and test deployments, use
`global.log_query.providers.<backend>.<domain>` for connection and tenant
identity. Native templates live under that provider's `queries.watcher`,
`queries.attribution`, and `queries.health` namespaces. Workflow policy and
credentials stay with their consumers. Legacy query connection and template
locations are rejected; provisioning values
under `global.loki` cannot provide or override a query URL. Application and
system tenant settings never fall back to one another.

Log *collection* (Grafana Alloy on Slurm nodes and as a Kubernetes DaemonSet)
is not part of the umbrella chart — follow the runbook §5–§6 to point
collectors at the bundled gateway. In-cluster collectors can push to
`http://loki-gateway.<ns>.svc.cluster.local/loki/api/v1/push`;
bare-metal Slurm nodes need Loki exposed via NodePort/LoadBalancer/ingress
(runbook §4 caveat).

## Feature Changes

### Extended Label Selectors

A new feature flag `enableExtendedLabelSelector` provides support for new label
selector that include the application instance. This change will provide a path
forward to support multiple deployments of the same Helm Chart, supporting
development and debugging in real development clusters.

**Important Notes**

A deployments label selectors are immutable and cannot be changed. In order to
use this new feature deployments will have to be recreated.

#### Installation

**Reinstall helm chart**

The process to make this changes involves first uninstalling the helm chart and
then reinstalling the Helm Chart.

An additional uninstall step could be added to CI temporarily for this step.
