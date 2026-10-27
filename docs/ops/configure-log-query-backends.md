# Configure Log-Query Backends

NVIDIA Recovery can query either Loki or VictoriaLogs without changing the
Watcher event contract or the Attribution Adapter's FACT payload. This guide
describes the Helm values that select and configure that query path.

The setting is deliberately query-only. It does not deploy VictoriaLogs,
redirect Grafana Alloy, change syslog collection, or disable the optional
bundled Loki chart. Qualify ingestion separately before changing providers.

For the interface and result semantics, see
[Log-Query Backends](../architecture/log-query-backends.md). For the decision
status and rejected alternatives, see
[ADR 0029](../decisions/0029-configure-log-query-backends.md).

## Choose a backend

| Backend | Selection | Query language | Deployment requirements |
| --- | --- | --- | --- |
| Loki | `global.log_query.backend: loki` | LogQL | Default. The chart supplies the existing production query variants and can use either an external Loki or the optional bundled Loki. |
| VictoriaLogs | `global.log_query.backend: victorialogs` | LogsQL | Opt-in. Supply an external VictoriaLogs 1.39.0 or newer, every selected native query variant, credentials and tenant settings, and a separately qualified ingestion path. |

Only `global.log_query.backend` selects the implementation. `event_source`
selects Watcher's polling or RabbitMQ input mode and must remain `log_query` for
backend polling. `global.loki.bundled.enabled` controls provisioning only.
The unselected provider block may be omitted.

### RabbitMQ input

With `event_source: rabbitmq`, Watcher consumes messages instead of polling a
log-query backend. Its runtime configuration uses
`datasources.rabbitmq.subscriber` for the connection, exchange, queue, and
routing key. `is_payload_protobuf` selects protobuf logs when true and JSON when
false (the default).

Optional subscriber settings are `exchange_type` (defaults to `direct`),
`queue_full_timeout_sec` (defaults to 15 seconds), and `queue_max_size_mb`
(defaults to 0, an unlimited broker queue). A positive queue size sets a limit
in MiB with the shared listener's reject-publish overflow policy. An explicit
zero timeout makes delivery to Watcher's local queue nonblocking.

Queue declaration arguments must match an existing queue. Enabling or changing
`queue_max_size_mb` on an existing durable queue can cause RabbitMQ to reject
startup with `PRECONDITION_FAILED`. This also applies when upgrading a limited
queue declared by older versions with `overflow` instead of `x-overflow`.

For limits that must change on an existing unlimited queue, keep
`queue_max_size_mb: 0` and have the broker administrator apply a RabbitMQ policy
with `max-length-bytes` and `overflow: reject-publish`. Inspect existing arguments
and policies first, and ensure the backlog fits the proposed limit before
applying it. Publisher confirms are required for publishers to receive rejection
notifications. See [RabbitMQ queue limits](https://www.rabbitmq.com/docs/maxlength).

If declaration arguments must change, coordinate a maintenance window: pause
publishers, drain the old queue with its current consumer, verify it is empty,
then stop the consumer. Have the broker administrator recreate the empty queue
with the new arguments, or use a new queue name and update its bindings and
Watcher configuration. Resume consumption before publishers. Deleting a queue
with pending messages loses those messages; do not delete an undrained queue.

## Configure the provider connection

Watcher queries the application domain. Attribution and the dependency checker
query both application and system domains. Configure the domains independently
even when they share an endpoint and credentials.

Provider URLs are base URLs; the clients append `/loki/api/v1/query_range` or
`/select/logsql/query`. Attribution requires both application and system URLs.
Watcher consumes only the application connection.

### Loki

All deployments, including existing installations and test deployments, must
place connection and tenant identity in the provider block. Loki is the default
backend selection only; it does not enable legacy connection fallback:

```yaml
global:
  log_query:
    backend: loki
    providers:
      loki:
        application:
          url: https://loki.example.com
          tenants: tenant-a
          org_id: tenant-a
        system:
          url: https://loki.example.com
          tenants: tenant-a
          org_id: tenant-a
        queries:
          watcher:
            query_prefix: '{cluster="slurm", appName="slurm-app-log"}'
```

`tenants` is the existing Basic-auth username when a password/token is present;
`org_id` becomes `X-Scope-OrgID`. Each domain has its own identity; system queries
do not inherit application tenant settings. Legacy query URL and tenant fields
are rejected with migration guidance. `global.loki` controls provisioning and
ingestion only and cannot supply or override query connection settings.

Bundled Loki also requires explicit provider URLs. With the default gateway
name in the same namespace, set both domain URLs to `http://loki-gateway`.
For another namespace or gateway name, specify its actual reachable address.
Enabling `global.loki.bundled.enabled` alone does not configure a query client.

### VictoriaLogs

VictoriaLogs uses native application and system tenant pairs:

```yaml
global:
  log_query:
    backend: victorialogs
    providers:
      victorialogs:
        application:
          url: https://victorialogs-app.example.com
          account_id: 1
          project_id: 0
          require_tenant: true
        system:
          url: https://victorialogs-system.example.com
          account_id: 2
          project_id: 0
          require_tenant: true
        queries:
          watcher:
            query_prefix: '<validated LogsQL application-stream filter>'
```

`account_id` and `project_id` must either both be present or both be absent.
Set `require_tenant: true` in a multi-tenant deployment so an omitted pair
cannot silently select tenant `(0,0)`. VictoriaLogs itself does not authorize
tenant headers; put an enforcing proxy such as `vmauth` in front when tenant
isolation is required. A configured proxy must preserve the `AccountID` and
`ProjectID` response headers because the client verifies their echoed values.

## Configure credentials

Credentials are existing Kubernetes Secrets. The chart mounts or exposes only
the selected provider's Secrets.

| Consumer | Shared bearer Secret | Domain-specific bearer Secrets | Basic-auth configuration |
| --- | --- | --- | --- |
| Watcher | `watcher.credentials.<backend>.api_token_secret_name` | Not applicable; Watcher queries application logs only | Loki: selected token Secret plus application `tenants`; VictoriaLogs: `username_secret_name` plus `password_secret_name` |
| Attribution Adapter | `attribution-adapter.credentials.<backend>.api_token_secret_name` | `app_api_token_secret_name` and `sys_api_token_secret_name` | Loki: selected token Secret plus each domain's `tenants`; VictoriaLogs: shared or matching `app_` and `sys_` username/password pairs |
| Dependency checker | `dependency_checker.log_query.<backend>.tokenSecretRef` | `appTokenSecretRef` and `sysTokenSecretRef` | Loki: selected token SecretRef plus each domain's `tenants`; VictoriaLogs: shared or matching application/system username/password SecretRefs |

Bearer and Basic authentication are mutually exclusive for each effective
domain. For Attribution bearer authentication, choose either the shared Secret
or domain-specific Secrets; the chart rejects configuring both. Attribution
Basic values and dependency-checker SecretRefs use their domain-specific value
when present and otherwise fall back to the shared value. Optional CA and paired
client-certificate/key Secret fields are available in the Watcher and
Attribution chart values; the dependency checker exposes the equivalent
VictoriaLogs SecretRefs.

Certificate Secrets are read from the standard `kubernetes.io/tls` keys:
`ca.crt` for the CA, `tls.crt` for the client certificate, and `tls.key` for
its key. `kubectl create secret tls` writes `tls.crt` and `tls.key` but not
`ca.crt`, and cert-manager adds `ca.crt` only when the issuer reports a CA. When
you configure a CA, point it at a Secret that contains the CA certificate. If a
Secret stores any of these under other keys, set the matching
`*_secret_key_name` value next to each `*_secret_name`, for example:

```yaml
attribution-adapter:
  credentials:
    loki:
      app_ca_cert_secret_name: loki-client-tls
      app_ca_cert_secret_key_name: ca.pem
```

For Loki, `tenants` is the Basic-auth username and the selected token is the
password. Without `tenants`, the token is passed as the raw `Authorization`
header value, preserving the existing Loki behavior.

For example, one bearer Secret can be shared by the VictoriaLogs consumers:

```yaml
watcher:
  credentials:
    victorialogs:
      api_token_secret_name: victorialogs-query
      api_token_secret_key_name: token

attribution-adapter:
  credentials:
    victorialogs:
      api_token_secret_name: victorialogs-query
      api_token_secret_key_name: token

dependency_checker:
  log_query:
    victorialogs:
      tokenSecretRef:
        name: victorialogs-query
        key: token
```

The VictoriaLogs Secret value contains only the token. The client adds the
`Bearer` authorization scheme.

## Supply provider-native queries

NVIDIA Recovery does not translate LogQL to LogsQL. A query variant is a
row-producing provider-native query that must preserve the same predicate,
message body, and canonical fields as its peer.

All native query text belongs under
`global.log_query.providers.<backend>.queries`. The consumer namespaces are
`watcher`, `attribution`, and `health`. Credentials and consumer policy remain
in their existing sections. Old component-level native query locations are
rejected, including prefixed VictoriaLogs template fields.

### Watcher workflows

The exact Helm paths are:

- base prefix:
  `global.log_query.providers.<backend>.queries.watcher.query_prefix`;
- workflow variants:
  `global.log_query.providers.<backend>.queries.watcher.workflows.<name>.query`;
- no-output validation:
  `global.log_query.providers.<backend>.queries.watcher.workflows.<name>.nooutput_validation_query`.

In a direct Watcher configuration file, the `queries.watcher` subtree becomes
`datasources.log_query.providers.<backend>.queries`. The name in the provider
workflow map must match the corresponding name in `rules[].workflow[]`.

Each selected `log_query` or `nooutput` workflow must have a nonempty variant
for the selected backend. For example, the two providers can define the same
logical workflow independently:

```yaml
global:
  log_query:
    providers:
      loki:
        queries:
          watcher:
            workflows:
              crash:
                query: ['<LogQL pipeline fragment>']
      victorialogs:
        queries:
          watcher:
            workflows:
              crash:
                query: ['<equivalent LogsQL filter fragment>']
```

An enabled `nooutput` workflow also needs a validation template in the same
provider workflow entry. The query list preserves ordering and may contain
phase-specific maps, for example in the Loki provider:

```yaml
workflows:
  noprogress:
    query:
      - PRE_TRAINING: '<LogQL pre-training fragment>'
      - TRAINING_LOOP: '<LogQL training-loop fragment>'
    nooutput_validation_query: '| job_id = "{job_id}"'
```

Provider workflow queries are a map, so changing a template no longer requires
replacing the workflow catalog. Workflow policy remains under
`watcher.config.anomaly_workflow_definitions` and enablement/timeout overrides
under `watcher.config.anomaly_workflows`. Local `attributes.query.regex` is
still used for downstream record processing; it is not provider query text.
When changing policy lists, remember that Helm replaces arrays rather than
merging individual entries. Each provider query list is also replaced as a
whole, so preserve any required ordered fragments when overriding it.

Watcher concatenates the configured prefix and each workflow fragment. The
VictoriaLogs driver then appends its required `_time` ordering and page limit.
Do not put a terminal `sort`, `limit`, aggregation, or statistics pipe in a
VictoriaLogs Watcher variant. Returned rows must retain `_time`, `_msg`, and
the canonical fields used by the workflow.

### Attribution Adapter

Both providers use the same three template names. A VictoriaLogs example:

```yaml
global:
  log_query:
    providers:
      victorialogs:
        queries:
          attribution:
            application: '<LogsQL application query containing literal {job_id}>'
            system: '<LogsQL system query>'
            epilogue: '<LogsQL epilogue query containing literal {job_id}>'
```

VictoriaLogs application and epilogue variants must contain the literal `{job_id}` token.
The adapter replaces only that token, so ordinary LogsQL stream-filter braces
remain untouched. The system variant is a static query evaluated over the
job-scoped time window; it receives no `{job_id}` substitution. Encode the
cluster, node, or other deployment-specific scoping that the query needs
directly in the template.

Attribution uses the same VictoriaLogs driver, which appends the required
ordering and page limit. Its row queries must likewise omit terminal sort,
limit, aggregation, or statistics pipes.

Loki templates live under `queries.attribution` in the `loki` provider. They
retain positional job-ID formatting: use `{}` for the job ID and double outer
selector braces when needed, for example
`{{cluster='slurm', appName='slurm-app-log', job_id='{}'}}`. Relocating a template
does not translate its syntax. The system query remains static.

Helm projects the selected provider's Attribution templates into the JSON
`LOG_QUERY_PROVIDERS` setting with the shape
`{"loki":{"queries":{"application":"...","system":"...","epilogue":"..."}}}`.
Standalone deployments use the same nested setting. Helm escapes literal dollar
signs as JSON `\u0024` to prevent dotenv interpolation from changing query text.
Use that encoding too when hand-authoring a dotenv value containing dollar signs;
direct environment variables do not undergo dotenv interpolation. The adapter
converts fetched records into the provider's V2 `entry_timeseries` entries.

### Dependency health queries

When the dependency checker is enabled with VictoriaLogs, supply executable
health queries that do not contain workload placeholders:

```yaml
dependency_checker:
  enabled: true
global:
  log_query:
    providers:
      victorialogs:
        queries:
          health:
            application: '<bounded LogsQL application query>'
            system: '<bounded LogsQL system query>'
```

The checker treats a clean empty response as query health and reports recent
matching data separately. A successful health check does not qualify
VictoriaLogs version, ingestion, retention, or canonical fields.
Application readiness and liveness probes remain shallow process checks; enable
the dependency checker when an authenticated provider-query check is required.

## Python metric names

Watcher exports provider-independent polling, record-count, ingestion-delay,
workflow-failure, and query-duration metrics with `log_query` in their names.
Update dashboard and alert queries that used the corresponding `loki` names;
obsolete Python aliases and duplicate legacy series are removed. For example,
`watcher_loki_poller_backlog_seconds` becomes
`watcher_log_query_poller_backlog_seconds`, and
`watcher_workflow_loki_query_failures_total` becomes
`watcher_workflow_log_query_failures_total`.
Loki-specific diagnostics and shell dependency-checker metrics are unchanged.

Attribution exports `adi_downstream_log_query_time`,
`adi_downstream_log_query_errors_total`, and
`adi_log_query_lines_returned_total` instead of duplicate Loki-named series.
Query timing and errors use `backend`, `query_id`, and (for failures)
`error_kind` labels rather than the old raw `query` and `loki_exception` labels.
Line counts retain their workload and log-type labels alongside `backend`.
Bundled dashboards use the neutral series; external dashboards need the same
migration.

## Validate before switching

Render the exact values file before deployment:

```bash
helm dependency build deploy/helm/nvidia-recovery
helm lint deploy/helm/nvidia-recovery --values nvidia-recovery-values.yaml
helm template nvidia-recovery deploy/helm/nvidia-recovery \
  --values nvidia-recovery-values.yaml >/tmp/nvidia-recovery-rendered.yaml
```

Then verify all of the following:

1. Only the selected provider's credential Secrets are mounted.
2. Watcher has a nonempty selected prefix and every enabled workflow has its
   selected native query variant.
3. Attribution has all three selected variants and the selected dialect's
   job-ID substitution syntax (literal `{job_id}` for VictoriaLogs).
4. Application and system tenant IDs are paired and match the intended domains.
5. Representative stored records preserve nanosecond timestamps, exact message
   bodies, and the canonical fields from ADR 0006.
6. Query failures, incomplete pages, and authentication errors are observable;
   a clean empty response is not mistaken for a backend failure.
7. The causal E2E scenarios pass through the selected production ingestion and
   query paths before action mode is enabled.

Watcher keeps a processing cursor in memory, namespaced by backend and query
revision. It advances only after a query chunk has been successfully processed,
including downstream publication. The cursor survives polling and query-client
recreation within the same Watcher process, but is neither persisted nor restored
after a process or Pod restart.

On startup, restart, or a query with no cursor in the current process, polling
begins at the configured lookback window:

$$
\begin{aligned}
t_{\mathrm{end}} &= t_{\mathrm{now}} - \mathtt{query\_delay} \\
t_{\mathrm{start}} &= t_{\mathrm{end}} - \mathtt{query\_max\_lookback}
\end{aligned}
$$

Here, $t_{\mathrm{now}}$ is the current time. Both settings live under
`watcher.config.datasources.log_query_policy` in Helm values. Polling can
revisit records within this window; records older than the window are not
automatically replayed. Changing the backend or query revision selects a
different cursor key, not a migration of the previous processing position.
A backend page token is only for pagination and must not be used as an
application processing cursor.

## Current limitations

- VictoriaLogs must be 1.39.0 or newer. Version
  [1.36.0](https://docs.victoriametrics.com/victorialogs/changelog/#v1360)
  provides the required end-exclusive query window, while
  [1.39.0](https://docs.victoriametrics.com/victorialogs/changelog/#v1390)
  adds the tenant response headers that the client validates. The client cannot
  reliably discover the server version from a query response.
- Neither Loki nor VictoriaLogs offers a stable query snapshot or event cursor.
  An ambiguous same-timestamp page boundary is reported as incomplete.
- Equal-timestamp records have no portable secondary order.
- Loki query-performance diagnostics remain provider-specific and are not
  fabricated for VictoriaLogs.
- Lepton credential refresh currently requires the Loki backend.
- Selecting VictoriaLogs does not configure or validate its ingestion path.

## Local Tilt and E2E

The shared local/CI selector `E2E_LOG_BACKEND` defaults to `loki`. Select
`victorialogs` to deploy the pinned v1.52.0 single-node store in `monitoring`
and render the local provider overlay after the optional attribution overlay.
Alloy continues collecting the same application log links and node syslog;
its VictoriaLogs destination disables automatic JSON message parsing to preserve
exact bodies and prevent payload fields from overwriting source labels.

```bash
E2E_LOG_BACKEND=victorialogs make dev
E2E_LOG_BACKEND=victorialogs DEPLOY_INFRA=1 DEPLOY_STACK=1 make e2e
```

Direct Tilt enables attribution by default and requires upstream source access;
use `E2E_ATTRIBUTION_MODE=unavailable` when that optional service is unnecessary.
The E2E harness's standard lane keeps attribution unavailable by default.
Backend choice does not change the Advisor profile or attribution availability.

Tilt forwards port 9428 for the VictoriaLogs UI at
`http://localhost:9428/select/vmui/`. Query collected application records with:

```bash
curl -fsSG http://localhost:9428/select/logsql/query \
  --data-urlencode 'query=cluster:="slurm" appName:="slurm-app-log" | sort by (_time) desc' \
  --data-urlencode 'limit=20'
```

In the active cluster, inspect the selected store and collector using
`kubectl logs -n monitoring deployment/victorialogs` and
`kubectl logs -n slurm daemonset/alloy`. Check application fields `cluster`,
`appName`, `job_id`, and `filename`; system records require `component=journal`
and the exact Slurm `node_name`, without workload identity fields. A healthy
`/health` response alone does not establish successful collection. The E2E
`02-log-ingestion` scenario checks deployment selection and real-file ingestion
before the recovery scenarios assert their causal evidence.

The local VictoriaLogs data volume is disposable and retention is one day.
Stop and recreate the named disposable cluster when switching backends; logs,
Alloy positions, and Watcher cursor state are not migrated or shared. Run the
same command with `E2E_LOG_BACKEND=loki` to return to the default backend.
