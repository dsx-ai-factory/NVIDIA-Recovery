# Repository Workflow

This document records the small amount of delivery policy that is specific to
the NVIDIA Recovery fork. General source-control mechanics belong to the active
developer tooling rather than repository instructions.

## Change Scope And Isolation

- Start substantial work from the current canonical `main` state.
- Keep one reviewable outcome per change and preserve unrelated workspace edits.
- Use an isolated workspace only when concurrent or long-running work would
  otherwise interfere with another task.
- Do not encode agent or tool provenance in change names or commit metadata.

## Review And Verification

Before marking a change ready for review:

1. Review the scoped diff against the requested outcome.
2. Run the smallest owning formatter, lint, test, or `make verify-<area>` target.
3. Check the relevant pipeline when a pull request exists.
4. Broaden verification and request independent review only for the risk
   boundaries defined in `AGENTS.md`.

There is no repository-wide preflight or branch/PR metadata script.

## GitHub Authority

[GitHub](https://github.com/dsx-ai-factory/NVIDIA-Recovery) is the sole development,
review and CI authority. Use `gh` for repository operations. The old GitLab
mirror and schedule retirement remain pending administrator action.
NVIDIA copy-pr-bot's `pull-request/*` branches remain the approved CI entry
point. Follow [contribution and CI approval policy](../../CONTRIBUTING.md).

## CI Scope

External actions and reusable workflows must use full commit SHAs, with a
version comment for readability. Container actions must use SHA-256 digests;
local references remain relative paths. The `action-sha-pins` pre-commit hook
checks workflows and composite actions and runs in the main CI workflow.
Dependabot maintains the pinned action versions. Run the check locally with
`uv run --locked pre-commit run action-sha-pins --all-files --hook-stage manual`.

Run focused component verification locally. Required GitHub checks must report
on the current PR head. Nightly suites cover the additional expensive scenarios.
Public PR checks receive no private Attribution credentials. Optional private
qualification uses the protected `attribution-qualification` environment on main.
