# Drop Inventory

Use this file for inherited behavior that is intentionally excluded from the
OSS v1.x release.

| Behavior or Surface | Reason | Evidence | Status |
| --- | --- | --- | --- |
| Hard-coded corporate identity routing | External operators need configurable identity mapping | Public operator-configuration requirement | open |
| Internal package and container registry assumptions | Not usable by external adopters | Public distribution requirement | open |
| Source-only incompatible release paths | Public distribution excludes unsupported pre-built containers | Release-scope decision | open |
| Product-unscoped support services | `casper/`, `lepton/`, `zdt_service/`, `otelc/`, and `munge/` are outside the user-confirmed OSS release scope | User disposition decision, 2026-06-04 | deleted in `chore/strict-easy-cleanup` |
| Inherited notifications service | The directory-level implementation is out of release scope even though notification/audit behavior remains a requirement for replacement design | User disposition decision and public audit requirement | planned |
| Legacy CI helper templates | `ci/` is tied to inherited internal CI/Vault workflows and will not be part of the OSS release surface | User disposition decision, Checkpoint 1A | deleted in `chore/strict-easy-cleanup` |
| Vault CI config directory | `vault-ci/` contains internal Vault agent configuration and is outside the OSS release surface | User disposition decision, 2026-06-04 | deleted in `chore/strict-easy-cleanup` |
| Inherited deployment environment values | `deploy/environments/*.yaml` contains fork-local internal cluster and example deployment values with no live deployment target in this OSS cleanup branch | User clarification, 2026-06-04 | deleted in `chore/strict-easy-cleanup` |
| Workflow-controller deployment artifacts | `workflow_controller/slurm/deploy/`, `workflow_controller/slurm/systemd/`, and `workflow_controller/slurm/workflow_controller_service/` contain per-cluster launch scripts, private deployment defaults, and systemd deployment surfaces that are outside the supported OSS deployment direction | User disposition decision, 2026-06-08 | deleted in `chore/scrub-wfc-deploy-artifacts` |
| Legacy Workflow Controller runtime and chart | The standalone Slurm Advisor Controller replaces the Python runtime, chart, image, dashboard, and deployment profiles; adjacent Job Manager compatibility behavior is retained pending a separate parity decision | User disposition decision, 2026-07-16 | removed 2026-07-16 |
| Legacy fake-job publisher | The tool publishes to the removed controller's RabbitMQ exchange rather than a retained production interface | Production-interface testing invariant; user disposition decision, 2026-07-16 | removed 2026-07-16 |
| Helm/deploy validation helpers | `scripts/helm_validation.py`, `scripts/helm_validation_utils.py`, `scripts/helm-values-config.yaml`, `scripts/helm-diff-ignore.txt`, and `tools/variables_replace.sh` depend on deleted inherited environment values and internal GitLab Pages assumptions | Active reference trace from Helm workflow and helper configs | deleted in `chore/strict-easy-cleanup` |
| Shared Casper/Lustre quota support | `recovery_libs/storage/casper` and `recovery_libs/schema/lustre_user_quota_*` only serve the deleted Casper/ZDT quota path | Import trace limited to deleted services and local schema files | deleted in `chore/strict-easy-cleanup` |
| Generated notification clients | Generated Python and dashboard clients target the inherited `notifications/` service | Consumer trace shows follow-on ARM/job-manager/dashboard refactor required before deletion | planned |
| Slack helper library | `recovery_libs/slack` is consumed by retained notification paths and deleted ZDT paths, and encodes Slack-only behavior | Import trace and identity-mapping requirement | planned |
| Internal sample clients and local metadata | `recovery_libs/event_bus/sample_client`, tracked `.env*`, `*.iml`, `recovery_libs/poetry.lock` | Internal hostnames, local state, IDE files, and obsolete package-management artifacts are not OSS behavior | deleted in `chore/strict-easy-cleanup` |
| SSA/Vault schedule helpers | `scripts/create-ssa-rotation-schedule.py`, `scripts/README-ssa-rotation.md` | Internal GitLab/Vault automation has no OSS release role | deleted in `chore/strict-easy-cleanup` |
| Inherited Docker compose reference bundle | `docker/` held local Loki/Prometheus/Vector compose support with stale references to deleted OTELC source-build paths | Review found no active retained consumer after the cleanup removed compose-based local dependency startup | deleted in MR `!9` |
