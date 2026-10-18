# Port Inventory

Use this file for inherited behavior or code that can move into OSS with
minimal semantic change.

| Behavior | Source Evidence | Requirement | Target Location | Status |
| --- | --- | --- | --- | --- |
| Symptom-pattern detection catalog | `watcher/`, `watcher/docs/ANOMALY_DETECTION.md` | Public detection-event behavior | detection service | open |
| Unproductive-restart guardrail | `job_manager/`, `anomaly_remediation_manager/` | Public recovery guardrail | advice or policy layer | open |
| Requeue-sequence early-stop detection | `job_manager/job_manager/service.py`, `job_manager/tests/test_job_manager.py` | Detect stopped requeue sequences after the configured threshold while preserving the public metric | Job Manager periodic task | ported |
| Watchdog fail-open behavior | `workflow_controller/slurm/workflow_controller/watchdog.py`, `workflow_controller/slurm/tests/unit/test_watchdog.py`, `workflow_controller/slurm/workflow_controller/slurm/actions.py`, `workflow_controller/slurm/tests/unit/test_slurm_actions.py` | Public fail-open behavior | standalone Slurm watchdog and Slurm adapter | adapted-in-progress |

## Watchdog Fail-Open Behavior

Retained Slurm watchdog behavior is defined by the inherited
`workflow_controller/slurm/workflow_controller/watchdog.py` implementation and
`workflow_controller/slurm/tests/unit/test_watchdog.py` coverage. The watchdog
polls active Slurm jobs, groups queued and running jobs by job name, tracks when
each group first becomes held, and releases the next eligible held job only
after the configured timeout has elapsed. A group observed as held on process
cold start starts its timeout when it is first seen.

The retained held reasons are `JobHeldUser`, `JobHeldAdmin`, `(JobHeldUser)`,
`(JobHeldAdmin)`, `job requeued in held state`, and
`(job requeued in held state)`. A group is treated as running, and its held
timer is reset, when any job in the group is `RUNNING` or when a `PENDING` job
has a reason outside that held set. When more than one held job is eligible, the
watchdog chooses the smallest Slurm job ID.

Job IDs accepted by the inherited watchdog include plain IDs, concrete array
element IDs, and array range IDs. Before calling `scontrol release`, array range
suffixes such as `_[0-9]` or `_[0-99%4]` are stripped to the base job ID, while
plain IDs and concrete element IDs such as `12345_2` are preserved. Unsupported
or malformed IDs are skipped before release selection.

Watchdog eligibility defaults open. Missing comments, non-JSON comments,
comments without `APS` or `HEIMDALL`, and invalid APS specs are treated as
eligible. A job is skipped only when its APS or HEIMDALL spec explicitly sets
`watchdog_eligible` to false.

The inherited watchdog sends Slack and PagerDuty notifications around a release.
That routing is not retained in this OSS slice.

The accepted OSS adaptation deliberately tightens the inherited behavior. The
standalone `slurm-watchdog` never groups by job name. It keys state by exact
Slurm job ID and derives eligibility from every fresh `squeue` snapshot. Normal
managed jobs are protected on every attempt. A DAG child, identified by a
non-empty dependency list, is protected on attempts greater than zero while
its held attempt 0 remains reserved for exact Advisor release. Missing,
malformed, and unrelated comments are unmanaged. The held timer is in memory
and restarts with the watchdog process.

The Advisor controller does not configure, run, log, or export metrics for the
watchdog. Observe/action mode, scheduler-action acknowledgement, in-memory
history, retry behavior, Zap logs, and Prometheus metrics are owned by the
standalone process. Notification/export behavior remains a separate design.
