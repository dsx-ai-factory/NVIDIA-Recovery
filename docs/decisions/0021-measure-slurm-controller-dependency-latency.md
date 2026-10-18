# ADR 0021: Measure Slurm Controller Dependency Latency

Status: proposed

Date: 2026-08-27

> **Implementation note (2026-09-23):** Only part of this proposal is implemented, with different metric names and buckets; reconcile the general operation/pool metrics and series budget against [registrations](../../slurm-advisor-controller/internal/telemetry/telemetry.go) at `47a8fdc9` before accepting.

Related:
[ADR 0020](0020-bound-slurm-controller-publication-and-accounting-concurrency.md)
uses bounded concurrency for independent NVIDIA Recovery publication and Slurm
accounting work. Those limits require dependency-specific evidence to tune
safely.

## Context

The controller exposes total reconciliation duration and durations for the
job-status-polling, job-reconciliation, failed-job-reconciliation, and
Advice-reconciliation stages. It also records source outcomes and item counts.

Those metrics locate a slow stage but do not identify the dependency or
operation responsible. Job-status polling includes the `squeue` snapshot,
NVIDIA Recovery job pagination, NVIDIA Recovery Advice pagination, the local seen store,
active log-metadata enrichment, and targeted finalized-job accounting.
Job reconciliation includes seen-store changes and individual NVIDIA Recovery
upserts. A multi-minute stage therefore cannot currently be attributed to a
specific RPC, command, page count, work queue, or small set of outliers.

Logs alone are not a reliable latency product, and adding job or attempt IDs to
Prometheus labels would create unbounded cardinality. Traces can explain one
sampled execution but cannot replace aggregate saturation and capacity
measurements.

## Decision

### Retain stage metrics and add source duration

The existing stage histograms remain the top-level controller view. Add the
`slurm_advisor_controller_source_duration_seconds` classic histogram for the
bounded logical operations within a tick, with stable `source` and `outcome`
labels. The initial source vocabulary is:

- `slurm_active`;
- `slurm_accounting`;
- `slurm_log_metadata`;
- `heimdall_jobs`;
- `heimdall_advice`; and
- `seen_store`.

The source vocabulary is a closed allowlist. The only duration outcomes are
`success`, `error`, `timeout`, and `canceled`. An implementation must map all
other failures to `error`; it must not derive either label from an error,
endpoint, command, or other runtime value.

Outcome classification is shared by source and operation duration: no error is
`success`; an error matching `context.DeadlineExceeded` is `timeout`; otherwise
an error matching `context.Canceled` is `canceled`; every other error is
`error`. This order also defines the result for joined errors.

The source timer covers the complete logical operation visible to
reconciliation, including pagination or a complete accounting worker-pool run.
Concurrent base-snapshot sources record their own elapsed times rather than
inheriting the duration of the slowest source.

### Measure outbound dependency operations

Add the `slurm_advisor_controller_operation_duration_seconds` classic
histogram at the boundary that owns each actual outbound call. Labels are
limited to `operation` and `outcome`. The dependency is part of each closed
operation value, avoiding a redundant independently variable label:

- `slurm_squeue_active_jobs`;
- `slurm_sacct_finalized_jobs`;
- `slurm_sacct_active_log_metadata`;
- `slurm_sacct_finalized_log_metadata`;
- `slurm_scontrol_log_metadata`;
- `heimdall_get_jobs_page`;
- `heimdall_list_advice_page`; and
- `heimdall_upsert_job`.

The operation outcome vocabulary is the same closed `success`, `error`,
`timeout`, and `canceled` set used by source duration. The histogram's generated
`_count` series is the operation counter; the controller does not register a
redundant operation-call counter.

Retries are recorded as individual outbound operations, while the source timer
records the total logical duration seen by the controller. Timeouts and
cancellations are explicit outcomes rather than being folded into a generic
error when the boundary can distinguish them.

The new duration histograms use these 28 finite buckets, in seconds:
`0.1`, `0.25`, `0.5`, `1`, `2.5`, `5`, `7.5`, `10`, `15`, `20`, `25`, `30`,
`40`, `50`, `60`, `75`, `90`, `120`, `150`, `180`, `240`, `300`, `360`,
`450`, `600`, `720`, `900`, and `1200`. They cover sub-second calls, the
configured command and reconciliation deadlines, and observed overruns without
collapsing every value above ten minutes into the terminal `+Inf` bucket.

### Record workload and concurrency without identifiers

Add these worker-pool metrics:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `slurm_advisor_controller_work_items_submitted_total` | counter | `pool` | Items accepted into a worker pool. |
| `slurm_advisor_controller_work_items_completed_total` | counter | `pool`, `outcome` | Submitted items that reached a terminal outcome. |
| `slurm_advisor_controller_work_queue_depth` | gauge | `pool` | Submitted items not yet claimed by a worker. |
| `slurm_advisor_controller_workers_in_flight` | gauge | `pool` | Workers currently executing external I/O. |
| `slurm_advisor_controller_workers_max_in_flight` | gauge | `pool` | Peak concurrency in the current or most recently completed pool run. |
| `slurm_advisor_controller_worker_limit` | gauge | `pool` | Configured worker limit. |

The closed `pool` vocabulary is `heimdall_upsert` and `slurm_accounting`.
Completed work uses the same closed `success`, `error`, `timeout`, and
`canceled` outcome vocabulary as the duration histograms.

An item increments the submitted counter and queue-depth gauge when the pool
accepts it. Claiming an item decrements queue depth and increments in-flight
workers. When claimed work reaches a terminal outcome, it decrements in-flight
workers and increments the matching completed counter. On cancellation, each
unclaimed item instead decrements queue depth and increments the `canceled`
completed counter. A pool run does not finish until every submitted item is
accounted for and both gauges return to zero. Subject to scrape races, each pool
maintains this invariant within one process lifetime:

```text
work_items_submitted_total
  = sum_outcomes(work_items_completed_total)
  + work_queue_depth
  + workers_in_flight
```

The maximum-in-flight gauge resets to zero at the start of each pool run and is
set to the highest concurrency observed so far. After the run, it retains that
run's peak until the next run starts. The worker-limit gauge is set from the
validated configuration. Gauge values are overwritten rather than accumulated;
Prometheus retains their samples over time in the same way as every metric.

Metrics must not contain job IDs, attempt indexes, workflow IDs, usernames,
paths, endpoints, error strings, or other unbounded or customer-identifying
values. Detailed identity remains in appropriately scoped logs and future
traces. Every label and allowed value is declared in code and validated by
tests.

### Enforce a per-target series budget

The initial metrics added by this decision must remain below 1,800 time series
per scraped controller target. This is a ceiling rather than an expected
steady-state count because Prometheus creates only label combinations that are
observed.

With 28 finite buckets, a classic histogram produces 31 series per observed
label combination: 29 bucket series including `+Inf`, plus `_sum` and `_count`.
The closed vocabularies therefore permit at most 744 source-duration series
(`6 sources * 4 outcomes * 31`) and 992 operation-duration series (`8
operations * 4 outcomes * 31`). The two pools add 18 series across the six
worker-pool metric families. The complete initial ceiling is therefore 1,754
new series per scraped controller target.

Adding or changing a source, operation, outcome, bucket, pool, metric family, or
label requires recalculating this ceiling during review. Tests exercise the
complete allowlists and fail when the declared initial budget is exceeded.

### Keep measurement behavior-neutral

Instrumentation observes existing contexts and outcomes but does not add
retries, change deadlines, suppress failures, or authorize scheduler actions.
Internal helpers return contextual errors; the existing reconcile or process
boundary remains responsible for logging the outcome once.

Concurrent workers return measurements to a concurrency-safe recorder or to
the coordinating goroutine. They do not append directly to the tick's
unsynchronized observation slices.

Tests verify metric registration, label vocabularies, outcome classification,
counter aggregation, gauge transitions and reset behavior, histogram
observations, the pool-accounting invariant, the series-budget calculation, and
concurrent recording. The metrics must remain usable when optional Prometheus
serving is disabled.

## Consequences

Operators can distinguish an expensive Slurm snapshot from accounting fan-out,
metadata enrichment, NVIDIA Recovery pagination, slow upserts, and local bookkeeping.
They can compare operation latency with work counts and in-flight limits before
tuning ADR 0020's pools.

The controller exports more time series and performs additional clock reads
and metric updates. The initial worst case is significant but bounded below
1,800 series per controller target. Fixed label vocabularies, valid
operation and pool values, and the review-time budget calculation prevent
cardinality from growing with jobs or errors.
Fleet-wide storage and ingestion still scale linearly with the number of
scraped controller targets, so operators must apply the per-target ceiling to
their deployed target count.

Source and operation metrics intentionally overlap: source duration answers
what delayed the reconciliation phase, while operation duration answers which
dependency calls and retries produced that delay. Dashboards should use the
total and stage metrics for service-level views and descend into source and
operation metrics for diagnosis.

Metric names and finite label values become an operational compatibility
surface. New operations may extend the vocabulary, but identifiers and raw
errors must never be promoted into labels.

## Alternatives Rejected

### Put outcomes only on counters

Removing `outcome` from the duration histograms would reduce their series count
by a factor of four, but it would also hide whether long calls completed,
failed, timed out, or were canceled. The closed four-value vocabulary and the
explicit per-target budget retain that diagnostic split without allowing
workload-dependent cardinality.

### Keep only total and stage duration

Stage timing detects a regression but cannot distinguish source latency,
pagination, command fan-out, queueing, or per-call outliers. It is insufficient
for safely choosing concurrency limits.

### Derive latency from logs

Log queries are more expensive, depend on delivery and retention, and require
parsing a diagnostic format that is not a stable metric contract. Logs remain
useful for identity-specific investigation, not aggregate capacity analysis.

### Use traces as the only detailed signal

Sampling and exporter availability make traces unsuitable for authoritative
rates, percentiles, and saturation. Traces complement metrics by explaining
individual executions.

### Label metrics by job or attempt

Per-job labels would grow with workload history and expose customer-specific
identity in the metric system. Aggregate counts plus logs and traces provide
diagnostic value without unbounded cardinality.

### Measure only successful calls

Slow failures, timeouts, cancellations, and retries are often the cause of
controller saturation. Excluding them would systematically hide the incidents
these metrics are intended to explain.
