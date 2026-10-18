# Slurm Advisor Controller Agent Guide

This file applies to `slurm-advisor-controller/`. Follow root `AGENTS.md`, invoke
the `go-engineering` skill, and apply `docs/quality/go-engineering.md` first.

## Component Rules

- Preserve level-based reconciliation from fresh observed Slurm state. Local
  action history must not become the source of truth.
- Keep scheduler-neutral contracts in `internal/cluster`, Slurm commands in
  `internal/slurm`, API mapping in `internal/heimdall`, orchestration in
  `internal/reconcile`, and process wiring in `cmd/slurm-advisor-controller`.
- Use `github.com/kelseyhightower/envconfig` with exact existing environment
  names and preserved absent-versus-explicit-value behavior.
- Use typed Zap fields and log returned errors once at the process, reconcile,
  or scheduler-action boundary that owns the outcome.
- Use `github.com/cockroachdb/errors` for production construction and wrapping;
  preserve sentinel, `Is`, `As`, join, and cleanup-error behavior.
- Use MockGen for call expectations on interfaces. Keep behavioral fakes for
  stateful NVIDIA Recovery, Slurm, clock, and store simulations.
- Keep `//go:generate` beside the owning interface. Never edit generated mocks.
- Observe mode must remain non-mutating with respect to scheduler actions.
  Independently configured filesystem collaborators must not enable or bypass
  scheduler actions. Action mode and watchdog release remain behind explicit
  acknowledgement and require human scheduler-action review.
- Do not add hidden test-only ingress. Exercise production interfaces through
  unit, fixture, contract, or harness layers.

## Verification

Prefer root targets:

```bash
make GO_PACKAGES=slurm-advisor-controller verify-go
```

Run `make test-slurm-advisor-harness` when scheduler commands, action policy,
controller wiring, or harness behavior changes. Direct Go commands are allowed
only when no owning Make target exists or for focused diagnosis of a target
failure, and must be followed by the owning Make target.
