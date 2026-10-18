# ADR 0016: Expose DAG Dependency Evaluation Reasons

Status: accepted

Date: 2026-08-18

Related:
[ADR 0007](0007-resolve-dag-release-through-advice-state.md) assigns DAG
release intent to Job Manager and scheduler mutation to the adapter.
[ADR 0008](0008-baseline-oss-database-schemas.md) requires normal follow-on
migrations for future shared-storage changes.
[ADR 0011](0011-cancel-dag-dependents-after-parent-abort.md) defines terminal
parent propagation. This record supersedes only ADR 0011's placement of DAG
parent-termination causality in a synthetic child `Issue`.

## Context

A dependency-blocked child is intentionally pending and held until Job Manager
resolves every dependency. The durable dependency row currently exposes only
the requested action, its lifecycle status, and the last action time. A user
can see that the gate is `not_eligible`, but cannot distinguish a parent that
has not registered, a registered parent that is still running, an ambiguous
workflow-scoped name, or one satisfied dependency waiting for other parents.

`Advice` answers what the scheduler adapter should do. `Issue` records an
actual job anomaly. Normal dependency waiting is neither a scheduler action nor
an anomaly, and parent-termination causality describes why the dependency gate
was aborted rather than an anomaly detected on the held child.

## Decision

Job Manager stores the latest dependency evaluation explanation on each
`dag_dependency` row using four nullable fields:

- `reason_code`: stable machine-readable code;
- `reason_message`: human-readable text that clients must not parse;
- `reason_details`: structured identity and state context;
- `last_evaluated_at`: the UTC time of the latest evaluation.

The first supported codes are:

- `WAITING_FOR_PARENT_REGISTRATION`;
- `WAITING_FOR_PARENT_COMPLETION`;
- `DEPENDENCY_SATISFIED_WAITING_FOR_OTHERS`;
- `PARENT_NAME_AMBIGUOUS`;
- `PARENT_TERMINATED`.

Job Manager writes action status and evaluation explanation atomically. A
transition to `in_progress` or `successful` clears a stale waiting reason. An
`aborted` gate remains absorbing and records `PARENT_TERMINATED` with the
causal parent job, attempt, scheduler state, terminal Advice reason, and
workflow ID.

Parent resolution returns a diagnostic outcome internally so an unresolved
name and an ambiguous name are observable without changing the existing
workflow scoping, exact-name matching, latest-attempt selection, or fail-closed
release behavior. Evaluation is event-driven when a child is registered, a
potential parent is first observed, a parent completes, or terminal Advice is
propagated.

The dependency listing API exposes the four fields. Existing rows remain valid
and return null until evaluated. Shared storage adds a normal Alembic revision
after the OSS baseline.

`TERMINATED` Advice remains the cancellation command consumed by SAC. Job
Manager no longer creates a synthetic `DAG_PARENT_TERMINATED` child `Issue`;
existing unrelated Issues are preserved. This changes observability placement,
not descendant cancellation, release eligibility, requeue precedence, or the
JM/SAC ownership boundary.

This decision does not emit `WORKFLOW_ID_REQUIRED` until JM can distinguish an
explicit workflow ID from SAC's fallback. It does not emit `PARENT_NOT_FOUND`
without a lifecycle deadline that distinguishes "not registered yet" from
"will never register." It adds no arbitrary timeout, singleton-chain
detection, graph-pattern inference, or behavior for a child released in
violation of the established held-child invariant.

## Consequences

Users and automation can query a held dependency and receive a durable,
structured explanation. Normal waiting no longer pollutes job anomaly data,
while SAC continues to receive the same exact-attempt termination intent.

The public API, ORM, migration history, Python clients, Go bindings, and
dashboard contracts must evolve together. Evaluation adds bounded database
writes at meaningful dependency events. Nullable fields preserve compatibility
for existing rows and clients that ignore unknown response properties.

Reason details are operational context, not identity authority. Consumers must
branch on `reason_code`, not parse `reason_message`, and must tolerate new
detail keys and future reason codes.

## Alternatives Rejected

Logs and metrics alone were rejected because they do not provide durable,
per-dependency state through the supported API.

Keeping DAG provenance in `Issue` was rejected because it conflates a gate
decision with an anomaly on the child and cannot explain ordinary waiting.

Deriving an explanation only when the API is read was rejected because the
answer could differ from the decision JM actually made and would not preserve
the evaluation timestamp or causal terminal context.
