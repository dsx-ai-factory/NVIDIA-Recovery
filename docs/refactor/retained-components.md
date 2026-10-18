# Retained Components

This file records inherited components that remain active in the OSS
fork and why they are retained.

## Retention Rules

Retain inherited code only when it satisfies at least one condition:

- It is required for Slurm/Slinky parity.
- It implements a documented public requirement.
- It supports the agentic harness or verification workflow.
- It is temporarily needed as a compatibility shim with a named removal
  checkpoint.

## Checkpoint 1 Component Map

| Component | Retention Rationale | Target Role | Refactor Disposition | Removal Condition |
| --- | --- | --- | --- | --- |
| `api-gateway/` | Required gateway material for HTTP/gRPC access and future Advisor API exposure | Scheduler-neutral Advisor API and service facade | adapt | Remove private GitLab module/import paths and regenerate public bindings |
| `job_manager/` | Owns inherited job state, persistence, orchestration, and audit-related data needed for recovery parity | Workload, attempt, advice, action-result, and audit store | adapt | Replace Slurm-shaped models only after Advisor API contracts and fixtures exist |
| `watcher/` | Contains the production-validated Loki polling path and symptom-pattern detection catalog required by the public detection contract | Normalized detection event producer | adapt | Keep until detection events are contract-tested through production ingestion paths |
| `anomaly_detection_isolator/` | Carries ADI and attribution flow that must become a discrete component for public scope | Recommendation/ADI service with optional attribution consumption | adapt | Replace inherited FACT/Slurm coupling after the ADI interface is frozen |
| `attribution_adapter/` | Retains production attribution integration source without distributing its proprietary client dependency | Quarantined compatibility source for a future public attribution provider | quarantine | Restore workspace, build, and deployment ownership only after a publicly distributable client and service contract are available |
| `anomaly_remediation_manager/` | Encodes current recovery policy and scheduler mutation behavior, including guardrail material | Candidate policy/advice component; scheduler actions move to adapters | quarantine | Remove direct scheduler-action ownership once Advisor action-result flow is implemented |
| `slurm-advisor-controller/` | Implements the supported Slurm/Slinky Advisor API adapter and watchdog | Slurm job-state observer, advice reconciler, and fail-open watchdog | port | Retain behind production API and local Slurm CLI interfaces |
| `dashboard/` | Cockpit UI is in OSS scope for audit trail and workload state | Operator UI with OSS-safe dependencies and configurable observability links | quarantine | Retain only after private packages/icons and Datadog defaults are removed or configured |
| `kpis_service/` and `metrics_service/` | The inherited productivity, efficiency, and metric-query capability is outside the retained recovery product | None | drop | Remove service code, API/UI consumers, persistence, deployment, CI, and live documentation surfaces |
| `recovery_libs/` | Shared schema, storage, constants, and migration material are required by multiple retained services | Shared OSS contracts and persistence helpers | adapt | Remove inherited/private naming, deleted-service helpers, generated notification clients, Casper/Lustre support, and private dependency assumptions after contracts stabilize |
| Root workspace metadata | `pyproject.toml`, `uv.lock`, `Makefile`, `.pre-commit-config.yaml`, `.mcp.json`, `AGENTS.md`, `CLAUDE.md`, `README.md`, `LICENSE.txt` | Needed to run, test, lint, and guide the OSS repository | adapt | Remove tracked local env files, IDE metadata, private package indexes, deleted service members, and inherited internal CI/deploy hooks |
| `deploy/helm/` | Helm installability is required for bare-metal Slurm and Slinky deployments | OSS-safe reference charts and examples | adapt | Remove private image defaults and internal environment coupling before public release |
| `agent-capabilities/`, `.agents/`, `.claude/`, `.codex/` | Required for the team's agentic-driven development workflow and generated client-specific projections | Agentic engineering harness and portable workflow assets | port | Retain generated projections; edit `agent-capabilities/` first and regenerate rather than editing projections directly |

Update this table before each deletion or deep refactor.
