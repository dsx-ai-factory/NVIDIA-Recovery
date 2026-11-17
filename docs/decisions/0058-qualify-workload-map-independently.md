# ADR 0058: Qualify Workload-Map Independently of Recovery Services

Status: accepted

Date: 2026-11-16

> **Implementation note (2026-11-16):** The [qualification suite](../../tests/workload_map/README.md)
> now records CPU lifecycle/retry evidence across all four combinations, beyond
> the initial exploratory coverage described below, and read-only inspection
> additionally distinguishes inconclusive observations.
> Qualification targets Recovery's CRD operations without a PodGroup-topology
> requirement or a generic SDK serialization validator; node-affinity replacement
> is checked offline, while live placement and full recovery remain unqualified; Pod association qualification is
> narrowed to template-label propagation, with unused SDK component inference
> removed and job-controller Pod/node publishing left to integration work.

## Context

Direct Kubernetes recovery depends on Workload-Map interpreting workload
objects and locating their Pod templates and lifecycle controls correctly.
Field mutation alone does not establish that a workload controller drains and
recreates Pods, preserves workload identity, or exposes usable attempt evidence.

The local Kubernetes foundation in [ADR 0056](0056-establish-kubernetes-local-development-foundation.md)
provides isolated workloads and staged scenarios. Qualification should use that
foundation without requiring Advice Operator, a mock gateway, or the Recovery
backend. The same checks should eventually help operators assess their own
clusters.

## Decision

Maintain a focused qualification suite that exercises the actual Workload-Map
library and explicitly pinned definitions against NVIDIA Recovery requirements.
Start with JobSet and PyTorchJob under default and KAI scheduling. Qualify exact
library, definition, controller, scheduler, and Kubernetes versions and relevant
settings; an API version alone is not a tested controller release.

Compare extracted identity, components, replicas, templates, state, and Pod
associations with independently specified expectations and Kubernetes evidence.
Exercise correlation-label and placement-constraint instrumentation while
preserving user metadata and constraints. Persist suspend/resume mutations via
the Kubernetes API, then verify complete Pod drain, replacement execution, and
unchanged parent name and UID. Record native restart behavior, terminal-state
boundaries, and gaps in attempt and restart-budget information without inventing
normalized fields or treating Pod readiness as training progress.

Separate execution into three modes with shared checks and evidence contracts:

- Cluster-free checks exercise extraction and mutation on representative objects.
- Active qualification runs owned workloads on a disposable or explicitly
  selected test cluster, reusing the staged harness and launcher boundaries from
  [ADR 0052](0052-run-e2e-tests-as-isolated-staged-scenarios.md).
- Read-only inspection evaluates available definitions and selected cluster
  resources. It reports prerequisites and observable compatibility, not a passed
  lifecycle qualification. Active probes are a separate explicit selection.

Keep cluster provisioning outside scenarios. Require explicit cluster and
namespace selection for live access, retain UID-based ownership for cleanup,
and never adopt or modify unrelated workloads. Disruptive scheduler experiments
require separately isolated test capacity; a namespace alone is insufficient.

Reports distinguish pass, fail, unsupported, and not tested, retain exact
versions and definition provenance, and capture enough observations to explain
failures. Missing prerequisites cannot count as a passed capability. A locally
modified definition is qualified as an override, never as the upstream default.

Build this suite independently of Advice Operator integration. Environment
wiring may share the Kubernetes development profile, but workload-only checks
must remain runnable without Advice services. Full Recovery-path tests,
checkpoint restoration, training-progress verification, and recovery-time
benchmarks are subsequent qualification work.

## Consequences

- Upstream definition and controller gaps can be discovered before the full
  Recovery stack is implemented, with reproducible evidence for follow-up work.
- Local and CI execution share requirements and assertions; an existing-cluster
  inspection can reuse them without provisioning or reconfiguring that cluster.
- Support claims remain specific to observed capabilities and pinned versions.
  Initial exploratory coverage does not qualify the entire four-combination
  matrix or promise a production-ready cluster-validation tool.
- Maintaining independent Kubernetes expectations adds work, but avoids a
  self-confirming test that only repeats Workload-Map's own interpretation.

## Alternatives Rejected

- Depend on Advice Operator and the full backend for every check: this couples
  library qualification to unrelated services and delays useful feedback.
- Verify only manifests or mocked controllers: this cannot establish actual
  suspend/drain/resume behavior or scheduler interaction.
- Ship separate local, CI, and cluster-validator implementations: their checks
  and support claims would diverge.
- Treat prototype lifecycle results as Workload-Map qualification: direct
  controller mutations and customized definitions do not prove upstream
  library and definition behavior.
