# Log-Query Backends

NVIDIA Recovery queries logs through a small in-process backend contract shared
by Watcher and the Attribution Adapter. Loki is the default implementation;
VictoriaLogs is an opt-in implementation. The selector does not provision a
backend, configure collection, or change an ingestion destination.

VictoriaLogs 1.39.0 or newer is the minimum supported version. The adapter's
`[start,end)` contract depends on the end-exclusive behavior introduced in
[1.36.0](https://docs.victoriametrics.com/victorialogs/changelog/#v1360), and
its tenant validation depends on the echoed `AccountID` and `ProjectID`
response headers introduced in
[1.39.0](https://docs.victoriametrics.com/victorialogs/changelog/#v1390). The
query response has no reliable version field to probe.

```text
provider-native query variant
           |
Watcher / Attribution Adapter
           |
  normalized query contract
           |
   static backend factory
       /          \
    Loki       VictoriaLogs
```

## Configuration contract

This section defines the architectural configuration boundary. Operators should
use [Configure Log-Query Backends](../ops/configure-log-query-backends.md) for
the exact Helm paths, credential choices, native-query requirements, rendering
checks, and backend-switch checklist.

`global.log_query.backend` is the only Helm-level selector:

```yaml
global:
  log_query:
    backend: victorialogs
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
      victorialogs:
        application:
          url: https://vl-app.example.com
          account_id: 1
          project_id: 0
          require_tenant: true
        system:
          url: https://vl-system.example.com
          account_id: 2
          project_id: 0
          require_tenant: true

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

`account_id` and `project_id` form a pair. Both are required when that domain's
`require_tenant` is true, and both may be omitted for a deliberately anonymous
single-tenant endpoint when it is false. Application and system domains are
configured independently.

Every deployment, including test deployments and bundled Loki installations,
must declare these connections in the provider blocks. Backend defaults select
an implementation only. Query URLs and tenant identity never fall back to
`global.loki`, component-local legacy connection fields, or another domain.

Provider-native text lives under
`global.log_query.providers.<backend>.queries`, grouped by consumer:
`watcher`, `attribution`, and `health`. Each consumer receives its own query
namespace; it does not interpret another consumer's templates.

Watcher receives the application connection as `datasources.log_query` and
the provider's `queries.watcher` subtree as its runtime provider `queries`.
Workflow names look up `queries.workflows.<name>`; workflow policy and local
regex processing stay with the workflow. The chart ships LogQL defaults;
a deployment selecting VictoriaLogs supplies the corresponding LogsQL entries,
including each enabled no-output workflow's validation query.

Watcher uses `event_source: log_query`, `type: log_query`,
`datasources.log_query_policy`, `LogQueryBatcher`, `LogQueryPoller`, and
`LogRecord` as its canonical consumer vocabulary. The old `loki` event source,
`loki_query` workflow type, and `datasources.loki` remain accepted configuration
inputs for polling policy only; connections and native queries belong in the provider
block. They do not select the active backend. Python callers use only the
neutral module, types, constants, and metrics; obsolete Loki import aliases
have been removed. Loki names remain for actual provider-specific behavior.

Attribution reads `queries.attribution.application`, `.system`, and `.epilogue`
with the same key names for either backend. The checker reads
`queries.health.application` and `.system`. Retired component-level template
locations are rejected rather than used as fallbacks. Missing selected
variants fail configuration validation. Loki retains positional job-ID
substitution in its application template; VictoriaLogs application and epilogue
templates use literal `{job_id}` substitution. Native query languages remain
independent; NVIDIA Recovery does not translate LogQL into LogsQL.

Credentials remain Kubernetes Secret references rendered as environment or
mounted-file inputs, and only the selected provider's Secrets are attached to a
workload. Loki's existing credential fields remain compatible. All three query
consumers support anonymous, bearer, or Basic proxy authentication; the
Attribution Adapter additionally allows application/system overrides of shared
credentials. The dependency checker likewise supports application/system token
or Basic SecretRefs with legacy shared fallback. Each effective domain uses
only one authentication mode. Optional CA and paired client certificate/key
Secrets configure TLS. Tenant headers are connection settings, not
caller-controlled arbitrary headers.

## Result semantics

The common contract uses integer UTC epoch nanoseconds and `[start,end)`
windows. It returns globally timestamp-ordered records containing an exact
provider-returned message body and normalized string fields. A clean empty
result is success; authentication, tenant, rate-limit, timeout, unavailable,
malformed, incomplete, and unsupported results are distinct failures.

Neither supported backend exposes a stable result snapshot or event cursor.
The implementation therefore refuses to claim completeness when pagination
cannot make unambiguous progress, including an oversized equal-timestamp
cohort. Equal-timestamp records have no portable secondary order. Provider
drivers preserve occurrences; Watcher's existing consumer deduplication remains
outside the backend.

Only client-observed duration, attempts, response bytes, and returned-record
count are portable statistics. Loki scan, queue, ingester, and store statistics
remain Loki-specific diagnostics and are not synthesized for VictoriaLogs.

See the authoritative [Loki range-query API](https://grafana.com/docs/loki/latest/reference/loki-http-api/)
and [VictoriaLogs query API](https://docs.victoriametrics.com/victorialogs/querying/)
for provider wire details.

## Ingestion qualification

`global.loki.bundled`, Loki write Secrets/routes, Alloy destinations, and
VictoriaLogs ingestion are independent settings. Before selecting
VictoriaLogs, verify the deployment runs version 1.39.0 or newer and qualify
that the production collector preserves:

- the exact message body and nanosecond timestamp;
- the application/system fields required by ADR 0006;
- tenant isolation, retention, and bounded ingestion delay; and
- the causal E2E evidence required by ADR 0005.

VictoriaLogs' Loki ingestion parses JSON messages by default. A deployment
requiring Loki-equivalent bodies must use its documented message-preservation
setting or an explicitly tested field policy. An adapter test that writes
fixtures directly to storage does not qualify the production ingestion path.

## Dependency health

The umbrella dependency checker runs a bounded, authenticated provider-native
query against the selected backend. It reports query protocol health separately
from recent matching data. A clean empty result is healthy but indicates no
recent data; process `/health` alone does not prove query, tenant, storage, or
field availability.

The relevant metrics are:

- `dependency_checker_log_query_health{backend}`;
- `dependency_checker_log_query_query_health{backend,source}`;
- `dependency_checker_log_query_recent_data{backend,source}`; and
- `dependency_checker_log_query_http_code{backend,source}`.

Legacy `dependency_checker_loki_*` series remain while Loki is selected for one
compatibility window.
