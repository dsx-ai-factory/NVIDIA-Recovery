# Migration Checkpoints

## Baseline

Fork baseline:

```text
Repository: NVIDIA Recovery fork of the upstream project
Branch at bootstrap: main
Observed baseline commit: cfafd2fff6e4f8c43beac3ab6fd4eee2ee10828f
Bootstrap branch: docs/harness-bootstrap
```

## Checkpoint 0: Harness Bootstrap

Goal:

- Add root agent guidance.
- Capture public architecture and decision inputs under `docs/`.
- Add agentic engineering charter.
- Add a concise repository workflow and document bootstrap CI limitations.
- Add refactor and reference-extraction inventory templates.
- Add GitLab merge request template.

Exit criteria:

- Focused checks for the changed repository guidance pass.
- The MR template captures summary, risk, and verification.
- Private surfaces are reviewed without blocking bootstrap work.
- GitLab/GitHub workflow boundaries are documented.

## Checkpoint 1: Inventory And Scrub Planning

Goal:

- Produce service and package inventory.
- Assign inherited services and major modules a refactor disposition: port,
  adapt, quarantine, or drop.

Exit criteria:

- `docs/refactor/deletion-inventory.md` contains the first deletion set.
- `docs/refactor/retained-components.md` explains retained services.

## Checkpoint 1A: CI Pipeline Planning

Goal:

- Define the replacement GitLab CI pipeline for internal development.
- Define the GitHub Actions pipeline for GitHub-specific CI/CD validation in the
  mirrored private OSS repository and eventual public GitHub repository.
- Decide which checks run in GitLab, which GitHub-specific checks run in
  GitHub, and whether any checks genuinely need to run in both places.

Exit criteria:

- GitLab CI replacement plan exists.
- GitHub Actions workflow plan exists.
- Existing inherited CI assumptions have a refactor disposition of drop,
  adapt, or replacement.
- Mirror-push behavior is validated only as needed for GitHub-specific CI/CD
  work against `NVIDIA-dev/Heimdall-OSS`.

## Checkpoint 1B: Directory Deletion And Root Cleanup

Goal:

- Delete the user-confirmed out-of-scope directories: `casper/`, `lepton/`,
  `zdt_service/`, `otelc/`, `munge/`, `ci/`, `vault-ci/`, and
  `notifications/`.
- Delete orphaned shared code that only serves those directories, including
  Casper/Lustre quota storage and schemas, Slack-only helper code, generated
  notification clients after consumers are refactored, and internal sample
  clients.
- Clean root and tooling metadata so workspace membership, Docker targets,
  pre-commit hooks, scripts, and generated-client paths no longer reference
  deleted services.
- Keep `.agents/` and `.claude/` according to the current retention
  decisions; remove the retired KPI and metrics services and private
  development overlays.

Exit criteria:

- Deleted-service directories and their orphaned `recovery_libs/` support paths
  are gone or have an explicit blocked-by-consumer note.
- Root workspace metadata and script inventories no longer point at deleted
  services except in reference-only docs.
- Notification behavior is either disabled through retained services or routed
  to a documented OSS-safe audit/notification follow-up.
- Targeted affected-service tests pass or have documented expected failures.

## Checkpoint 2: Advisor API Contract Freeze

Goal:

- Define workload identity, attempt identity, advice lifecycle, action result,
  detection event, recommendation result, and audit event contracts.

Exit criteria:

- Contract docs exist under `docs/architecture/contracts/`.
- Replay fixture shape is documented.
- Slurm/Slinky adapter contract is reviewed.

## Checkpoint 3: Slurm/Slinky Advisor API Slice

Goal:

- Route Slurm/Slinky parity behavior through the Advisor API integration shape.

Exit criteria:

- Fake Slurm/Slinky adapter passes contract tests.
- Initial replay fixtures pass.
- Real Slurm/Slinky adapter has an approved implementation plan.
