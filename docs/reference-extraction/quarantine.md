# Quarantine Inventory

Use this file for inherited behavior that needs a product, security, or
architecture decision before it remains active in the OSS fork.

| Behavior or Component | Reason | Needed Decision | Status |
| --- | --- | --- | --- |
| Auto-RCA and internal agent endpoints | Not listed as v1.x OSS core scope | Agent UI, analysis client, automatic RCA, and conversation endpoints were dropped; optional attribution remains separate retained architecture. | resolved — drop; see `docs/refactor/deletion-inventory.md` |
| Break/fix hooks | Internal operational surface | drop or replace with OSS-safe extension point | open |
| Dashboard private observability links | Not valid for external adopters | remove or make configurable | open |
| Internal deployment environments | Private deployment surface | remove or replace with OSS examples | deleted in `chore/strict-easy-cleanup` |
| Root CI and workspace configuration | Current `.gitlab-ci*`, `.gitlab/`, root `pyproject.toml`, `Makefile`, and `.pre-commit-config.yaml` still reference private registries, deleted services, or inherited deploy flows | replace with OSS-safe CI/workspace metadata | open |
| Notification consumers in retained services | ARM, job-manager, and dashboard still call generated clients for the inherited `notifications/` service | remove behavior, replace with audit-only events, or design OSS-safe notification provider | open |
| Helm/deploy validation helpers | Validation scripts are tied to deleted `deploy/environments/*.yaml`, internal GitLab Pages, and inherited Helm layout | adapt to future OSS examples only if needed | deleted in `chore/strict-easy-cleanup` |
| Local environment reference bundles | Private deployment assumptions with no supported public contract | replace or refactor away | removed from the release tree |
| `tools/variables_replace.sh` | Ad hoc deploy example placeholder replacement includes private observability and Slack placeholders | fold into OSS example generation only if needed later | deleted in `chore/strict-easy-cleanup` |
