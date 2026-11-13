# ADR 0056: Establish a Kubernetes Local Development Foundation

Status: accepted

Date: 2026-11-02

> **Implementation note (2026-11-11):** The [direct Kubernetes profile](../development/kubernetes.md) now includes Advice Operator and scoped mock-gateway scenarios; the standalone chart supports JobSet owner validation, with ownership and lifecycle to be revisited alongside Advice routing. Real backend integration and full workload recovery qualification remain subsequent work.

## Context

Direct Kubernetes workload recovery needs a repeatable environment in which
components can be developed and tested incrementally. The existing Tilt
environment targets Slurm through Slinky; requiring that infrastructure for
direct Kubernetes development adds unrelated dependencies.

The staged scenario harness already separates workload launch, evidence, and
stack setup. [ADR 0052](0052-run-e2e-tests-as-isolated-staged-scenarios.md)
defines its isolation and execution contracts.

## Decision

Establish a minimal Tilt-based local Kubernetes development profile, independent
of Slurm and Slinky, while preserving the existing Slurm workflow. Reuse common
build and deployment infrastructure and add Recovery components incrementally
as their implementation lands.

Start with Kind, JobSet, the default Kubernetes scheduler, and small CPU-only
workloads. Allow the environment to grow to PyTorchJob, optional KAI scheduling,
and multiple worker nodes for placement tests. Full workload and scheduler
qualification is separate from establishing this foundation.

Extend the existing staged scenario harness through its launcher boundary for
Kubernetes workload submission, observation, and owned-resource cleanup.
Preserve namespace and UID identity and keep Kubernetes access out of individual
scenario bodies. Reuse deadlines, timelines, and evidence clients; do not invent
Kubernetes attempt semantics merely to populate Slurm-derived fields.

Keep cluster setup and deployment outside pytest. Provide repeatable start,
test, and teardown entry points, with the same test entry point usable by later
CI integration. Failure diagnostics include workload and Pod state, Kubernetes
Events, logs, and the scenario timeline.

The initial acceptance scenario submits a workload, observes its expected Pods
running, and cleans up only its own resources. Repeated and concurrent runs
must remain isolated. This establishes the environment and test harness; it
does not demonstrate recovery correctness.

The Advice operator, workload discovery, admission, telemetry, and recovery
controllers are subsequent component work. Their implementation is not a
prerequisite for this foundation. Lifecycle qualification, full recovery tests,
checkpoint-progress verification, and recovery benchmarks build on it later.

## Consequences

- Developers can begin component tests without first implementing the recovery
  pipeline or provisioning GPUs and Slurm.
- Shared harness contracts and deployment machinery reduce duplication while
  workload-specific launch and observation remain behind explicit boundaries.
- A passing foundation smoke test proves workload execution and test isolation,
  not supported recovery behavior or release qualification.
- Later components must add their real dependencies and behavioral scenarios;
  the minimal environment is a starting point, not a substitute for full-stack
  validation.

## Alternatives Rejected

- **Require the existing Slinky environment.** It introduces scheduler and
  storage dependencies unrelated to direct Kubernetes workloads.
- **Bring up every Recovery component first.** This makes the development
  foundation depend on the components it is intended to help build.
- **Create a separate Kubernetes test framework.** The existing staged harness
  already supplies isolation, lifecycle, and diagnostic contracts that can be
  extended through its launcher boundary.
