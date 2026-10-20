# NVIDIA Recovery Agent Guide

NVIDIA Recovery is adapting inherited recovery behavior to the
Kubernetes-style Advisor API. The first product target is Slurm and
Slurm-on-Kubernetes through Slinky.

## Outcomes And Invariants

- Treat the inherited tree as a starting artifact, not an architecture
  endorsement. Preserve end-user recovery behavior while moving it behind the
  Advisor API; Slurm and Slinky are adapters to that contract.
- Test through production interfaces. Do not add hidden test-only ingress.
- Keep changes scoped to the requested outcome and avoid unrelated refactors.
- Do not include internal issue or ticket tracker links in pull request
  descriptions or repository documentation.
- Do not silently preserve internal hostnames, package indexes, registries,
  identity assumptions, credentials, customer data, or deployment-only paths.

Important system design invariants and rules.

1. An instance of NVIDIA Recovery currently operates on a single cluster at a time. Code may assume all jobs are from the same cluster.
2. `heimdall_job_id` is always
   `uuid.uuid5(uuid.NAMESPACE_URL, f"{job_id}-{job_attempt_index}")`, as defined
   by `generate_heimdall_job_id` in
   `recovery_libs/recovery_libs/schema/job_manager/job_identity.py`.

## Action Boundaries

- For requests to answer, explain, review, diagnose, or plan, inspect the
  relevant material and report the result. Do not implement changes unless the
  request asks for them.
- For requests to change, build, or fix, make the requested in-scope local
  changes and run focused non-destructive validation without pausing for routine
  approval.
- Ask before external writes, destructive or costly actions, or a material
  expansion of scope.
- Before requesting approval for a sensitive external action, use the target
  system's safe read-only inspection interfaces to record current state,
  affected scope, preconditions, rollback path, proposed action, and pre-action
  validation; stop before mutation.
- Respect the VCS already active in the workspace and avoid mixing VCS clients.

## Task Routing

- For product scope or architecture decisions, read the current public
  [architecture documentation](docs/architecture/slurm-advisor-controller.md),
  then consult the [ADR index](docs/decisions/README.md) and relevant accepted
  decisions.
- Add or supersede an ADR for a human-approved choice that is durable,
  cross-cutting, or expensive to reverse. Do not require ADR ceremony for
  routine or reversible work, and do not silently rewrite an accepted decision.
- Use `adr-maintenance` when authoring or reviewing ADRs, implementing a
  recorded decision, or auditing decision drift. Follow the lifecycle and
  implementation-note rules in [the ADR guide](docs/decisions/README.md).
  Before finishing related work, reconcile the affected ADR's status and index
  with approval already given, and make material implementation differences
  visible in its top-of-file note. This is scoped upkeep, not a repository-wide
  ADR audit on every change.
- For migration, disposition, or checkpoint work, use the relevant records in
  `docs/refactor/` and `docs/reference-extraction/`.
- For Go work, use `go-engineering`, read
  `docs/quality/go-engineering.md`, and follow the nearest nested
  `AGENTS.md`.
- For portable agent-capability changes, edit `agent-capabilities/` and run
  `python3 scripts/sync-agent-capabilities.py --write`; do not edit generated
  projections directly.
- For GitHub operations, follow
  `docs/governance/repository-workflow.md`.

## Code Documentation

- Document public APIs, including public functions, methods, types, structs or
  classes, interfaces, fields, and constants, using the language's standard
  docstring or documentation-comment format.
- Document long functions and functions that coordinate concurrent work. For
  concurrency, capture responsibilities, ordering constraints, ownership,
  cancellation, and synchronization decisions that are not apparent from the
  code.
- Use comments to explain why the implementation is necessary, not merely what
  each statement does. When code is not straightforward, record the invariant,
  tradeoff, or failure mode that requires its shape.

## CI And Automated Test Quality

Tests should establish meaningful behavior or invariants, not merely confirm
that an implementation contains the edits just made. TDD is welcome when the
failing test expresses an independently justified requirement.

Before adding a test or CI gate:

- **State what it proves.** Identify the user-visible behavior, system
  invariant, or realistic failure it detects. “The generated file contains the
  value I added” is insufficient unless that value establishes a meaningful
  external contract.
- **Make the proof reviewable.** A reviewer should readily understand the
  setup, action, expected outcome, and why failure matters. Prefer small,
  explicit cases over opaque shell pipelines, large assertion scripts, or
  elaborate test infrastructure.
- **Test behavior through appropriate interfaces.** Prefer public APIs,
  observable outcomes, and structured assertions. Avoid checking source text,
  exact diagnostic wording, YAML formatting, ordering, or implementation
  details unless those are explicitly part of the contract.
- **Require robustness to valid evolution.** Refactoring, reformatting,
  renaming internal symbols, or upgrading a tool without changing supported
  behavior should not break the test. A test that fails should point to a
  meaningful regression.
- **Use independent expectations.** Derive expected results from requirements
  or independently specified examples—not by copying implementation logic or
  snapshotting whatever the implementation currently emits.
- **Match cost to confidence.** Consider maintenance, runtime, flakiness, and
  review burden. Do not add tests solely to increase test count, satisfy a
  test-first ritual, or demonstrate that an agent completed an edit.

For Helm and deployment configuration, use native lint/schema validation for
structural checks. When additional coverage is warranted, use structured
assertions on rendered resources for meaningful deployment invariants, or
focused installation/integration tests for behavior that rendering cannot
establish. Do not build shell scripts that grep rendered manifests or source
files to duplicate schema checks or assert incidental text.

CI gates must be reproducible, actionable, and worth blocking a change over.
Prefer focused checks with clear failure diagnostics. Fix or remove brittle,
redundant tests rather than weakening assertions until they pass.

During review, ask: **What realistic defect would this test catch, and what
harmless change might break it?** If the answers do not justify the test,
redesign it or omit it.

## Verification And Review

- Run the smallest formatter, lint, test, or `make verify-<area>` target that
  proves the changed behavior. Use file-scoped pre-commit checks when
  applicable; repository-wide preflight is not a default gate.
- Review the scoped diff against the requested outcome before marking work
  ready.
- Broaden verification and request independent review for cross-component
  changes, public contracts, security-sensitive behavior, hard-to-reverse
  changes, or validation and release machinery whose failure could mask
  defects.
- Human review is required for authentication, authorization, scheduler
  actions, secret loading, migrations, release automation, and behavior that
  can kill, release, stop, or modify a workload.
- Use the instructing human's configured author identity. Do not add AI
  coauthor or generated-by metadata.
- Every agent-created or amended commit must include a DCO sign-off: use
  `git commit -s` (or `--signoff`). Keep configured cryptographic signing
  enabled; `-S` and `commit.gpgsign` do not replace `-s`. Before pushing, verify
  that the commit message contains a `Signed-off-by` trailer with the
  instructing human's configured identity.
