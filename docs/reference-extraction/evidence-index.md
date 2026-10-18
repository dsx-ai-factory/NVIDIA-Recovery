# Evidence Index

This file records public, repository-local evidence used to preserve, adapt,
quarantine, or drop inherited behavior.

## Evidence Rules

- Link behavior to current architecture, an accepted architecture decision, or
  an executable public contract.
- Prefer concrete code paths, tests, fixtures, and public documentation over
  historical planning records.
- Record whether evidence supports end-user behavior, implementation detail, or
  a deployment habit.
- Do not preserve behavior only because inherited code already implements it.
- Do not record internal issue trackers, private runbooks, personal workspaces,
  cluster identities, or private infrastructure links.

## Current Evidence

| Evidence | Path | Supports | Notes |
| --- | --- | --- | --- |
| Project overview and supported deployment path | `README.md`, `docs/getting-started.md` | Public scope, prerequisites, and operator workflow | User-facing source of truth |
| Architecture decisions | `docs/decisions/` | Durable cross-cutting decisions | Accepted decisions supersede historical planning notes |
| Slurm Advisor architecture | `docs/architecture/slurm-advisor-controller.md` | Advisor API and Slurm/Slinky target | Current component boundary |
| Symptom-pattern detection | `watcher/` | Loki-compatible anomaly detection | Validate through production interfaces |
| Workload state and recovery audit | `job_manager/`, `recovery_libs/` | Persistence, guardrails, and recovery state | Retained public behavior |
| Requeue-sequence early-stop detection | `job_manager/job_manager/service.py`, `job_manager/tests/test_job_manager.py` | Public recovery guardrail and monitoring behavior | Threshold-aligned scan window prevents eligible stopped sequences from falling outside the database query |
| Optional attribution | `anomaly_detection_isolator/`, `attribution_adapter/` | Standalone detection with optional attribution | Private FACT artifacts remain quarantined |
| Slurm controller | `slurm-advisor-controller/`, `deploy/helm/slurm-advisor-controller/` | Scheduler observation and approved actions | Legacy Python controller is retired |
| Slurm log-link compatibility | `docs/decisions/0004-materialize-slurm-log-links.md` | Ownership-scoped flat-directory compatibility | Advisor-owned adaptation |
| CI and release configuration | `.gitlab/`, `.github/`, `scripts/ci/` | Build, verification, and provider-scoped publication | Review private surfaces before public release |

Add evidence rows before porting inherited behavior into the OSS core.
