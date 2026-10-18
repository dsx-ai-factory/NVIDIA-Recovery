# Slurm Advisor Controller And Watchdog Harness

This harness runs the Go Slurm advisor controller against a local Slurm
docker-compose cluster and a stateful mock `HeimdallService`. It also runs the
standalone `slurm-watchdog` directly against Slurm without an NVIDIA Recovery backend.

It is the Phase 2 seed for the advice-based Slurm controller plan. It is
separate from `workflow_controller/slurm/tests_harness` so the replacement path
does not depend on RabbitMQ or the inherited Python WFC package.

## Services

- `mysql`: Slurm accounting database.
- `slurmdbd`: Slurm database daemon.
- `slurmctld`: Slurm controller.
- `c1`, `c2`, `c3`: simulated compute nodes.
- `mock-heimdall`: public `HeimdallService` gRPC mock plus debug HTTP.
- `controller`: Go `slurm-advisor-controller` in observe or action mode.
- `mock-heimdall-peer` and `controller-peer`: a second independent backend and
  controller enabled by the `environment-isolation` Compose profile. The two
  controllers share Slurm while using distinct APS environments.
- `watchdog`: independent Go `slurm-watchdog` in observe or action mode; it has
  no dependency on `controller` or `mock-heimdall`.
- `watchdog-peer`: a second watchdog enabled by the `environment-isolation`
  profile for proving that differently scoped action-capable processes sharing
  one Slurm job set release only their own jobs.

The Slurm topology matches the inherited harness: `normal` uses `c1-c2`, and
`lowmem` uses `c3`.

## Images

The harness builds three local images and does not depend on a registry-pushed
image:

- `slurm-advisor-controller-slurm-base:${IMAGE_TAG:-dev}`: Rocky Linux 9,
  Slurm 25.05, Munge, Slurm configs, and sample jobs.
- `slurm-advisor-controller-controller:${IMAGE_TAG:-dev}`: controller and mock
  NVIDIA Recovery binaries layered onto the Slurm base image.
- `slurm-watchdog:${IMAGE_TAG:-dev}`: only the source-built standalone watchdog
  binary layered onto the Slurm base image.

Build them directly with:

```bash
make harness-build
```

## Commands

From the repository root:

```bash
make test-slurm-advisor-harness
```

Or from `slurm-advisor-controller/`:

```bash
make harness-smoke
make harness-down
```

The smoke script builds the harness images, starts the stack, registers the
Slurm cluster with `slurmdbd`, and runs the tagged Go scenario suite in
`harness/scenarios`.

The scenario suite covers:

- `JOB_STATUS_PENDING` upsert for the held job.
- `JOB_STATUS_RUNNING` upsert for the running job.
- `JOB_STATUS_COMPLETED` upsert after the running job finishes and is finalized
  through `sacct`.
- expanded parent and numeric custom-step stdout paths in
  `scheduler_metadata.slurm_log_file_paths`, with synthetic `batch` and
  `extern` steps excluded.
- controller default and job-level `ignore` omit jobs from NVIDIA Recovery upserts.
- valid APS comments without `mode` inherit each controller default, including
  passive `ignore` and `observe` behavior.
- observe-only restart advice does not mutate Slurm.
- missing and invalid comments never authorize action, and an unacknowledged
  job-level `act` request is capped to observe.
- two acknowledged action-mode controllers configured for `blue` and `green`
  publish only their matching APS jobs to independent backends; cross-routed
  advice cannot mutate Slurm, while matching advice can.
- action-mode restart advice converges through `scontrol requeuehold`, then
  releases the requeued job from Slurm hold.
- action-mode release advice opens an exact held DAG attempt-0 gate without a
  legacy workflow-controller action message.
- failed jobs remain failed without creating a successor while waiting for
  Advice; delayed restart Advice recovers them, while delayed termination
  Advice preserves the failed source.
- `SLURM_ADVISOR_ENABLE_EAGER_REQUEUE=true` explicitly restores eager
  `requeuehold` without Advice; the harness defaults this flag to false.
- action-mode terminate advice converges through `scancel`.
- restart plus node-exclusion advice requeues into hold, then updates
  constraints before the job can run again.
- API outage does not trigger scheduler mutation.
- the standalone watchdog releases a managed retry and a normal managed job
  first observed held while the controller and mock NVIDIA Recovery backend are stopped.
- unmanaged jobs and initial DAG children remain held during the standalone
  watchdog scenario.
- two action-capable watchdogs using `blue/*` and `green/*` release only their
  matching held jobs; an unmatched held job remains untouched and per-process
  logs prove which watchdog produced each release candidate.

The mock gateway debug endpoint is exposed on
`http://127.0.0.1:${MOCK_HEIMDALL_DEBUG_PORT:-18081}`.
The environment-isolation peer endpoint is exposed on
`http://127.0.0.1:${MOCK_HEIMDALL_PEER_DEBUG_PORT:-18082}` while that profile
is active.

## Observability

The controller writes zap JSON logs to stdout. Set
`SLURM_ADVISOR_LOG_LEVEL=debug` for per-loop debug summaries; the default is
`info`. Every successful poll emits a bounded APS environment-admission summary
with the controller environment and the fixed count fields `matched`,
`controller_unspecified`, `prod_default`, `environment_mismatch`,
`not_onboarded`, and `invalid_environment`. Other non-debug logs cover startup,
metrics listener startup, failed syncs, scheduler action outcomes, and
shutdown/cleanup problems.

Prometheus metrics are enabled by default inside the harness and exposed on:

```text
http://127.0.0.1:${SLURM_ADVISOR_METRICS_PORT:-19090}/metrics
```

Runtime controls:

- `SLURM_ADVISOR_LOG_METADATA_REFRESH_INTERVAL=5m` controls the
  attempt-scoped stdout metadata cache expiration.
- `SLURM_ADVISOR_ENVIRONMENT=unspecified` controls APS job admission for the
  primary controller. The isolation scenario overrides it with an exact value
  and uses `SLURM_ADVISOR_PEER_ENVIRONMENT` for the peer.
- `SLURM_ADVISOR_LOG_METADATA_ENRICHMENT_TIMEOUT=2s` bounds each active-job or
  finalized-job pass querying supplemental `sacct` and `scontrol` metadata; a
  controller sync may run both passes independently.
- `SLURM_ADVISOR_JOB_UPSERT_WORKER_POOL_SIZE=8` and
  `SLURM_ADVISOR_ACCOUNTING_WORKER_POOL_SIZE=8` independently bound publication and
  targeted single-job `sacct` workers.
- `SLURM_ADVISOR_ACCOUNTING_TIMEOUT=5m` bounds the total duration of one
  targeted accounting phase.
- `SLURM_ADVISOR_METRICS_ENABLED=true` enables the controller metrics listener.
- `SLURM_ADVISOR_METRICS_ADDR=:9090` controls the in-container listener.
- `SLURM_ADVISOR_METRICS_PORT=19090` controls the host port used by the
  harness.

Controller metrics include:

- `slurm_advisor_controller_reconcile_loops_total`
- `slurm_advisor_controller_reconcile_duration_seconds`
- `slurm_advisor_controller_reconcile_stage_duration_seconds`
- `slurm_advisor_controller_source_reads_total`
- `slurm_advisor_controller_source_read_duration_seconds`
- `slurm_advisor_controller_source_items_total`
- `slurm_advisor_controller_snapshot_jobs`
- `slurm_advisor_controller_snapshot_advice`
- `slurm_advisor_controller_job_upserts_total`
- `slurm_advisor_controller_advice_skips_total`
- `slurm_advisor_controller_advice_actions_total`
- `slurm_advisor_controller_failed_requeues_total`
- `slurm_advisor_controller_job_mode_decisions_total`
- `slurm_advisor_controller_job_environment_admissions_total`

The reconcile duration histograms use production-oriented buckets from one
second through ten minutes, followed by Prometheus's implicit `+Inf` bucket.
The stage-duration histogram has bounded `stage` and `outcome` labels.
Its `stage` values are `job_status_polling`, `job_reconciliation`,
`failed_job_reconciliation`, and `advice_reconciliation`.
The source-read duration histogram has bounded `source` and `outcome` labels.
It records each Slurm, NVIDIA Recovery, and local seen-store read independently, so
the concurrent base-snapshot read durations overlap rather than summing to the
`job_status_polling` stage duration. Slurm log-metadata enrichment is reported
as `slurm_log_metadata`, separately from the initial `slurm_active` read. The
`heimdall_jobs` source is observed only while the process is still bootstrapping
its local discovery set.

Metric labels are intentionally bounded. Do not add labels for job IDs, advice
IDs, workflow IDs, user names, job names, node names, raw Slurm reasons,
command paths, or partition names.

Runtime advice fixtures can be replaced through the mock debug endpoint after a
Slurm job ID is known:

```bash
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  -d '[{"advice_id":"restart-12345","job_id":"12345","attempt_index":0,"state":"ADVICE_STATE_RESTARTED","scheduler_metadata":{"scheduler":"slurm","managed_by":"slurm-advisor-controller"}}]' \
  "http://127.0.0.1:${MOCK_HEIMDALL_DEBUG_PORT:-18081}/advice"
```

## Watchdog Fail-Open

The watchdog is a separate process with no NVIDIA Recovery endpoint or Advisor API
configuration. On each `squeue` poll it keys by exact Slurm job ID and derives
eligibility from a valid `APS` or `HEIMDALL` comment, DAG identity, current
state, hold bit, and `restart_cnt`. Normal managed jobs and DAG attempts greater
than zero are monitored. A held DAG attempt 0 is reserved for exact release
advice. Held duration is process-local, so a watchdog restart resets only the
release timer.

Dedicated runtime variables:

- `SLURM_WATCHDOG_MODE=observe`; use `act` to enable mutations.
- `SLURM_WATCHDOG_ACKNOWLEDGE_SCHEDULER_ACTIONS=false`; act mode requires
  explicit `true`.
- `SLURM_WATCHDOG_ENVIRONMENT_PATTERN=unspecified`; literal values follow the
  controller's exact legacy-compatible environment rules. `*` matches zero or
  more characters, including `/`, and every other character is literal.
- `SLURM_WATCHDOG_POLL_INTERVAL=15s`.
- `SLURM_WATCHDOG_RELEASE_TIMEOUT=10m`.
- `SLURM_WATCHDOG_CLEANUP_GRACE_PERIOD=10m`.
- `SLURM_WATCHDOG_SLURM_EXEC_TIMEOUT=5s`.
- `SLURM_WATCHDOG_PARTITION_ALLOWLIST=`; optional comma-separated partitions.
- `SLURM_WATCHDOG_VERSIONED_SQUEUE_JSON=false`.
- `SLURM_WATCHDOG_LOG_LEVEL=info`.
- `SLURM_WATCHDOG_METRICS_ENABLED=true`.
- `SLURM_WATCHDOG_METRICS_ADDR=:9091`.

The harness exposes watchdog metrics at
`http://127.0.0.1:${SLURM_WATCHDOG_METRICS_PORT:-19091}/metrics`. Metric names
are `slurm_watchdog_polls_total`,
`slurm_watchdog_poll_duration_seconds`, `slurm_watchdog_observed_jobs`,
`slurm_watchdog_managed_jobs`, `slurm_watchdog_tracked_jobs`,
`slurm_watchdog_environment_admissions_total`, and
`slurm_watchdog_release_candidates_total`. Their labels are bounded; job IDs,
names, partitions, users, and environment pattern values appear only in
structured logs.

Missing, malformed, and unrelated comments are unmanaged. A non-empty
`dag_dependencies` list identifies a DAG child. Observe mode reports candidates
without calling `scontrol release`.
Slack and PagerDuty routing from the inherited Python watchdog is not part of
this slice.

## Scope

This harness covers the Phase 2 reference smoke safety matrix for metadata
upsert, final-state capture, restart, terminate, node exclusion, observe-only
no-mutation, and API-outage no-mutation against a local Slurm cluster and mock
`HeimdallService`. The watchdog scenarios prove real Slurm observation,
`requeuehold`, exact-job release without the controller or backend, and
two-process environment isolation through production interfaces. Watchdog
action mode requires explicit scheduler-action acknowledgement before it can
mutate Slurm.
