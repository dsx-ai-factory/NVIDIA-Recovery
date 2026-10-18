# ADR 0029: Configure Log-Query Backends

Status: proposed

Date: 2026-09-08

> **Implementation note (2026-09-23):** The [providers](../../recovery_libs/recovery_libs/log_query) are implemented, but the [filename-correlation E2E](https://github.com/NVIDIA/NVIDIA-Recovery/blob/47a8fdc9/tests/e2e/43-slurm-log-filename-correlation/test_slurm_log_filename_correlation.py) still requires Loki at `47a8fdc9`. Acceptance and VictoriaLogs ingestion/recovery qualification remain unresolved.

Related:
[ADR 0005](0005-require-causal-evidence-in-slurm-recovery-e2e-tests.md)
requires evidence through the backend used by the deployed component, and
[ADR 0006](0006-standardize-log-labels-and-syslog-ingestion.md) defines the
application- and system-log field and ingestion contracts. This record
would, if accepted, supersede only wording in those decisions that assumes Loki
is the sole query backend. Their causal-evidence, domain-separation, identity,
and ingestion requirements remain active.

## Context

Watcher and the Attribution Adapter currently depend on Loki at several layers:
LogQL configuration, `/query_range`, Loki response streams, authentication,
pagination, statistics, deployment checks, and tests. Supporting another store
by returning synthetic Loki responses would preserve Loki as the architecture
and require every future provider to emulate it.

VictoriaLogs has a native LogsQL query API and a different flat JSONL response.
Its Loki-compatible API covers ingestion, not querying. LogQL and LogsQL also
differ in filtering, parsing, field, ordering, and statistics semantics, so
query text cannot be translated safely as a general operation.

VictoriaLogs 1.39.0 or newer is required. The common `[start,end)` window
depends on the end-exclusive query behavior introduced in 1.36.0, and tenant
validation depends on the echoed `AccountID` and `ProjectID` response headers
introduced in 1.39.0. The query response has no reliable server-version signal,
so this is a deployment qualification gate, not a runtime capability probe.

The storage selected for queries is related to, but distinct from, log
collection. Equivalent query outcomes require qualified ingestion of the same
message body, nanosecond timestamp, and canonical fields. Query selection does
not itself provision a store or redirect Alloy, syslog, or application-log
writes.

## Decision

### Use one small in-process provider boundary

`recovery-libs` owns backend-neutral immutable query, record, page, statistics,
and typed-failure objects; synchronous and asynchronous query protocols; Loki
and VictoriaLogs implementations; and a fixed factory. The only supported
backend names are statically registered in source.

The shared boundary owns HTTP transport, provider authentication and tenancy,
provider response validation, timestamp conversion, ordering, and pagination.
Watcher retains polling windows, durable cursors, backoff, record validation,
deduplication, and post-publication cursor commits. The Attribution Adapter
retains application/system separation, FACT page sizing, credential refresh,
and the compatibility serializer required by FACT's `RAW_LOKI_STREAMS` input.

There is no dynamic plugin loading, Python entry-point discovery, query gateway,
or network service.

### Select one backend and use native query variants

`global.log_query.backend` is the single deployment selector and defaults to
`loki`. Provider connection settings remain in explicit `loki` and
`victorialogs` blocks. All deployments, including tests and bundled storage
profiles, must explicitly configure provider/domain connections and tenant
identity. There is no legacy URL/tenant fallback or application-to-system
identity inheritance. Storage provisioning does not derive query endpoints.

Consumer-facing Watcher names are backend-neutral: `event_source: log_query`,
workflow `type: log_query`, `datasources.log_query_policy`, and the
`watcher.lib.plugins.log_query` module. Former Loki-named configuration values
remain accepted inputs, but Python callers use the neutral API directly.
Compatibility-only Python aliases and duplicate legacy metrics are removed.

Each logical query has independently maintained provider-native variants under
`global.log_query.providers.<backend>.queries`. The explicit `watcher`,
`attribution`, and `health` namespaces contain the respective consumer's native
text. Watcher workflows look up their native queries by existing workflow name;
polling, anomaly policy, and local regex processing remain consumer-owned.
Attribution uses `application`, `system`, and `epilogue` template names for
either backend. The checker uses `application` and `system` health queries.
Retired component-level native query locations are rejected, with no fallback
precedence. A missing selected variant is a configuration failure. The
implementation does not send LogQL to VictoriaLogs or translate query text.

Adding a future Splunk backend requires one implementation, one static factory
entry, explicit connection configuration, native SPL query variants, and the
same contract tests. It must not require Watcher, Attribution, FACT, or Advisor
API changes.

### Define the portable result and failure semantics

Every request uses an inclusive start and exclusive end, expressed as integer
UTC epoch nanoseconds. Results preserve the provider-returned message body and
all normalized string fields. Application and system records continue to obey
ADR 0006. A successful empty result is distinct from invalid query,
authentication or tenant failure, rate limiting, timeout, unavailable backend,
malformed response, incomplete result, and unsupported semantics.

Forward and backward results are globally monotonic by timestamp. No
cross-provider order is promised among equal timestamps unless a provider
supplies a stable per-event identifier. Provider drivers preserve occurrences;
consumer-owned deduplication remains outside the boundary.

Neither Loki nor VictoriaLogs provides a stable snapshot cursor. A full page
therefore requires another request, and the implementation must report
`incomplete` when it cannot prove progress or when an indistinguishable
same-timestamp cohort exceeds a safe page. It must not return a plausible empty
or partial success.

Only client-observable duration, attempts, response bytes, and returned-record
count are portable statistics. Loki scan, queue, ingester, and store statistics
remain Loki diagnostics. VictoriaLogs-specific data remains provider
diagnostics and cannot be fabricated to drive common correctness behavior.

### Keep authentication and health provider-specific behind explicit policy

Credentials, TLS, and tenant identity are provider connection configuration;
callers cannot inject arbitrary request headers. Loki preserves its existing
Bearer/Basic, `X-Scope-OrgID`, and credential-file precedence. VictoriaLogs
uses its configured proxy authentication and explicit `AccountID` and
`ProjectID`; a declared multi-tenant deployment may not silently fall back to
tenant zero. Only the selected provider's Secrets are exposed to each workload.
For VictoriaLogs, each domain's tenant IDs are a pair required when
`require_tenant` is true; an explicitly anonymous single-tenant profile may set
it false and omit both IDs.

A backend dependency check issues a short authenticated and tenant-scoped
native query. HTTP/query health and recent-data presence are separate results:
a clean empty response proves query availability but not ingestion freshness.
Shallow process readiness is not evidence that provider queries or canonical
fields work.

### Qualify ingestion independently

Changing `global.log_query.backend` does not enable or disable bundled Loki,
change Loki write routes, or reconfigure collectors. A VictoriaLogs deployment
must use version 1.39.0 or newer and separately prove exact message-body
preservation, nanosecond timestamps, canonical application/system fields,
tenant isolation, retention, and bounded ingestion delay before it can claim
equivalent production results.

E2E scenarios governed by ADR 0005 query the backend selected by the deployed
component and still prove the same causal chain. Direct backend fixture writes
may test an adapter, but they do not prove collection or ingestion parity.

## Consequences

Loki remains the default while its transport is moved behind a reusable
contract. Watcher and Attribution can select VictoriaLogs without making their
downstream contracts provider-shaped. FACT continues to receive its existing
Loki-stream compatibility format.

Configuration requires explicit provider blocks and native query variants. This
duplicates some query intent, but the duplication is visible, testable, and
smaller than maintaining a portable query compiler.

Common tests compare normalized record multisets, boundaries, body and field
preservation, empty results, typed failures, and downstream outcomes. Each
provider also needs protocol-specific tests. Production VictoriaLogs enablement
remains gated on a separately qualified ingestion path.

Some semantics cannot be guaranteed by either backend: stable order among
equal timestamps, exact pagination across arbitrarily large indistinguishable
timestamp cohorts, or a snapshot that excludes concurrent late ingestion.
Those limitations are explicit failures or rollout constraints rather than
silent data loss.

## Alternatives Rejected

### Make VictoriaLogs return Loki-shaped responses

This minimizes the first caller diff but leaves Loki as the shared contract,
duplicates normalization across components, and forces future providers such
as Splunk to emulate Loki streams and pagination.

### Translate LogQL into LogsQL or a portable query model

The repository contains arbitrary prefix-plus-fragment LogQL pipelines. Query
languages differ in filters, regular expressions, parsing, field projection,
aggregation, and formatting. A useful portable model would become a new query
engine and still require provider-specific escape hatches.

### Add a dynamic plugin system

Two in-process providers do not justify runtime discovery, entry points,
untrusted extension loading, or compatibility governance. A fixed registry is
explicit and sufficient.

### Add a log-query gateway service

A gateway would introduce another API, deployment, authentication boundary,
availability dependency, and rollout protocol without an independent consumer
or policy need.

### Couple query selection to ingestion provisioning

Query selection cannot prove that collectors wrote equivalent records, and a
deployment may query an external provider while retaining unrelated bundled or
write-only resources. Coupling these settings would make migration less safe
and obscure ownership.
