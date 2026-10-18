# ADR 0027: Route Each Advice Generation to One Reconciler

Status: accepted

Date: 2026-09-04

Amended: 2026-09-04 to expose only the current generation's optional status on
Advice rather than a list of retained generation history.

Related:

- [ADR 0025](0025-filter-and-terminally-reconcile-actionable-advice.md)
  defines Advice generations, `only_active`, and terminal reconciliation
  status. This record supersedes ADR 0025 where status and active selection are
  partitioned by a logical controller scope.
- [ADR 0026](0026-classify-advice-expiration-in-scheduler-controller.md)
  assigns expiration to the scheduler controller. This record supersedes ADR
  0026 only where the resulting status is scoped to a controller.

## Context

The initial actionable-Advice contract allowed multiple logical consumers to
reconcile the same Advice generation independently. Each active query and
status row therefore carried a `controller_scope`. That field did not route
Advice; it only partitioned terminal observations after every consumer had
already received the record.

Advice already carries scheduler metadata used to select its owner, and the
system will have a separate routing mechanism for assigning Advice. Once
routing selects exactly one authoritative reconciler, independent per-consumer
terminal states are contradictory: a non-owner must not reconcile the Advice,
and replicas of the selected owner are cooperating instances of the same
reconciler.

Keeping `controller_scope` for a hypothetical multi-consumer model adds a
required query parameter, public status field, database column, indexes, page-
token context, and client vocabulary. It also permits two consumers to record
different terminal outcomes for one desired-state generation, leaving no
single answer to whether that Advice is active.

## Decision

Each Advice generation has exactly one authoritative reconciler, selected by
the Advice routing contract. Defining or changing that routing mechanism is
outside this decision, but exclusive ownership is a precondition for writing
reconciliation status.

Reconciliation status is identified by:

```text
(advice_id, observed_generation)
```

There is at most one durable status row for that identity. Replicas of the
authoritative reconciler share it; status writes remain idempotent and terminal
transitions remain monotonic.

`only_active=true` does not accept a controller identifier. A current-
generation `CONVERGED` or `ABANDONED` status excludes the Advice globally from
active reads. Advice responses expose at most one optional
`reconciliation_status`, and only when its observed generation equals the
Advice's current generation. An older generation's status remains internal
history and does not appear as the current lifecycle state.

Routing metadata that can change the authoritative reconciler is action-
affecting desired state. Job Manager advances the Advice generation before the
new owner acts, so terminal status from the previous routing assignment cannot
suppress newly routed work. The Advice target remains immutable.

The `controller_scope` field and query parameter are removed from Job Manager,
API Gateway, SAC, and the reconciliation-status table. Because the scoped
contract never merged or shipped, its draft protobuf numbers and names are not
reserved.

## Consequences

The active query and status contract are smaller, and one Advice generation has
one unambiguous lifecycle outcome. Persisted terminal status still bounds a
controller restart to unresolved generations plus work accumulated during
downtime.

Consumers no longer receive an ever-growing status array or need to select the
entry matching `advice.generation`. Persisting prior generation rows still
supports monotonic writes and operational diagnosis, but that history is not
part of the Advice response contract.

The routing boundary now carries more correctness weight. A misrouted writer
could close work globally, so API authorization and the reconciler's ownership
checks must continue to prevent non-owners from publishing terminal status.
This decision does not make server filtering authorization for scheduler
mutation.

The model intentionally does not support independent reconciliation of one
generation by multiple controller types. A future product requirement for
fan-out reconciliation would need a new parent/child work model or another
explicit ownership design rather than restoring an unverified string scope.

## Alternatives Rejected

### Keep controller scope for future flexibility

The current routing model requires one owner, while the additional field makes
every consumer understand a topology the system does not support. It also
allows divergent terminal truth without defining how those outcomes compose.

### Use a controller instance identity

Pod or process identity would reopen completed work on restarts and replicas,
defeating durable convergence.

### Infer a scope in Job Manager

Job Manager does not own scheduler routing. Inferring consumer identity during
reads would duplicate routing policy and retain the ambiguous multi-owner
status model.
