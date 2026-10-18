# Proportional Quality Gates

Verification should prove the requested change without turning every edit into a
repository-wide audit. Use the smallest owning formatter, lint, test, or
`make verify-<area>` target, then broaden only when risk or blast radius requires
it.

## Routine Changes

For documentation, tests, and localized configuration or code changes:

1. Run file-scoped pre-commit checks when applicable.
2. Run the smallest component target that exercises the changed behavior.
3. Review the scoped diff against the requested outcome.

There is no repository-wide preflight or branch/MR metadata script.

## Human-Readable Business Logic

New or materially changed public functions, classes, and complex business
logic must have docstrings. The docstring must explain the business contract
and the non-obvious behavior a maintainer must preserve, such as invariants,
state transitions, idempotency, failure handling, or deliberate degradation.
Every function or method docstring governed by this rule must document each
explicit parameter and its meaning, excluding only the implicit `self` or
`cls`, and must document its return value or explicitly state that it returns no
value. For multiple return values, document each value in declared order and
explain the meaning of status booleans or sentinel values. This documentation
must explain semantics rather than merely restating the function name,
parameter names, or return type.

Generated code, trivial accessors, and obvious one-line adapters are exempt.
When logic is complex enough to need several implementation comments, add a
function or class docstring that first gives readers the coherent high-level
contract, then use local comments only for decisions specific to individual
branches.

## Focused Repository Policy Checks

Run these checks only when their owning artifacts or concerns change:

| Change | Verification |
| --- | --- |
| Portable skills, agent prompts, or repository agent guidance | Regenerate projections, run the capability sync self-test and check, then review `agent-capabilities/evals/prompt-routing.md` |

## Helm Charts

Helm chart edits use the existing `helm-chart-check` pre-commit hook. To check
every chart locally with the same commands used by CI, run:

```bash
helm dependency build deploy/helm/nvidia-recovery
deploy/helm/scripts/check-charts.sh deploy/helm/*/Chart.yaml
```

## Prompt And Capability Changes

Change one prompt or capability group at a time when practical. Review the same
representative cases in `agent-capabilities/evals/prompt-routing.md` after each
group, recording any unavailable behavioral run as a validation gap. The matrix
is review guidance, not an online CI dependency.

## Broader Verification

Broaden verification and request independent review when a change crosses
components, modifies a public contract, is difficult to reverse, or changes
validation or release machinery in a way that could mask defects. Shared Go
contracts and tooling use unscoped `make verify-go`; a single Go module uses
`make GO_PACKAGES=<module> verify-go`.

## Human Review

Explicit human review is required for authentication, authorization, secret
loading, migrations, release automation, scheduler actions, private-surface
exceptions, and behavior that can kill, release, stop, requeue, exclude, or
otherwise modify a workload.

## Environment Limitations

If a relevant check cannot run because credentials, services, hardware, or
network access are unavailable, report the attempted command and the concrete
blocker. Do not replace an owning check with an unrelated broader ritual.
