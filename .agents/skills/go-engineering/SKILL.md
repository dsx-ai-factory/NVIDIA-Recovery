---
name: go-engineering
description: Use when writing, reviewing, testing, debugging, or changing Go code, Go modules, generated Go artifacts, configuration, structured logging, errors, or interaction mocks in NVIDIA Recovery.
---

# Go Engineering

## Goal

Produce idiomatic NVIDIA Recovery Go changes that preserve configuration and error
semantics, use the repository's established libraries, and pass the owning Make
verification target.

Read `contract.json`, `docs/quality/go-engineering.md`, and the nearest
nested `AGENTS.md` for the affected component.

## Project Contracts

| Concern | Standard |
| --- | --- |
| Configuration | `github.com/kelseyhightower/envconfig`; explicit environment-name tags; separate dependent defaults, normalization, and validation. |
| Logging | `go.uber.org/zap.Logger` with typed fields and `zap.Error`; log once at the boundary that owns the outcome. |
| Errors | `github.com/cockroachdb/errors` for production construction, wrapping, `Is`/`As`, and aggregation. |
| Interaction mocks | MockGen beside the interface owner; behavioral fakes remain valid for stateful simulations. |

## Decision Rules

- Inspect existing component patterns and tests before changing behavior.
  Retrieve current external documentation when correctness depends on an exact
  library version or API detail.
- Add or update focused tests for changed behavior. Preserve production
  interfaces and avoid test-only ingress.
- Preserve existing environment names and explicit false, zero, or empty-value
  semantics.
- Preserve causal error identity through standard-library-compatible
  `errors.Is` and `errors.As` chains. Do not hand-edit generated mocks or add
  a separate verbose stack field beside `zap.Error`.
- Use root Make targets as the completion interface. Direct Go or generator
  commands are suitable for focused diagnosis or when no owning target exists;
  finish with the owning Make target and do not bypass a failure.

## Validation And Stop Rules

For one module, run `make GO_PACKAGES=<module> verify-go`. Use
`make verify-go` for shared tooling, cross-module contracts, or changes
consumed by multiple services. Scheduler-action changes also require the owning
harness target and explicit human review.

If required generation, dependencies, or validation cannot run, report the
specific blocker and the best available evidence instead of claiming the Go
change is ready.
