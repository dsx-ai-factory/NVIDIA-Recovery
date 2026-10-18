# Adapt Inventory

Use this file for inherited behavior that should remain, but only after it is
reshaped around OSS contracts.

| Behavior | Source Evidence | Required Adaptation | Target Contract | Status |
| --- | --- | --- | --- | --- |
| Slurm job state ingestion | `slurm-advisor-controller/`, `job_manager/` | map into scheduler-neutral workload and attempt updates | Advisor API workload update | adapted; legacy controller removed |
| Slurm log-link materialization | retired `workflow_controller/slurm/workflow_controller/agent.py`, `workflow_controller/slurm/workflow_controller/misc/actions.py`, and `workflow_controller/slurm/docs/EXTRA_APP_LOG_SYMLINKS.md` | replace WFC polling, SSH writes, unsafe overwrite, and unscoped age cleanup with an optional validated Advisor collaborator and manifest-owned lifecycle | Slurm Advisor deployment and collector shared-filesystem contract in ADR 0004 | decision proposed; implementation candidate in progress |
| Legacy anomaly payloads | `watcher/`, `recovery_libs/schema/` | emit normalized detection events | `DetectionEvent` | open |
| Remediation decisions | `anomaly_remediation_manager/` | split policy, advice, and scheduler action ownership | advice plus action result | open |
| KPI and metric-query reporting | `kpis_service/`, `metrics_service/`, `recovery_libs/kpis_service/`, `dashboard/` | retire the inherited productivity and efficiency capability without rehoming it | none | dropped |
| Notification audit behavior | `job_manager/`, future OSS-safe notification/audit path | preserve user-facing audit and optional notification semantics after deleting inherited `notifications/` service | audit event and operator-configured notification routing | open |
