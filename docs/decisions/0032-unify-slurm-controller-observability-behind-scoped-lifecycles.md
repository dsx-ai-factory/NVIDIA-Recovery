# ADR 0032: Unify Slurm Controller Observability Behind Scoped Lifecycles

Status: superseded

Date: 2026-09-02

Discussion: [PR #75](https://github.com/NVIDIA/NVIDIA-Recovery/pull/75)

Superseded by [ADR 0048](0048-start-recovery-traces-in-backend-and-share-observations.md).
Maintainer confirmed this disposition on 2026-10-16: retain this record as
historical rationale, adopt ADR 0048's narrower recovery lifecycle, and retire
the broader controller-wide proposal.

Related:
[ADR 0013](0013-correlate-recovery-with-durable-trace-context.md) defines
durable recovery identity, W3C correlation, recovery spans, and completion
semantics across services. This decision preserves those contracts while
changing how the Slurm Advisor Controller describes observable work to its
in-process metrics, tracing, and audit implementations.

## Context

The Slurm Advisor Controller currently exposes reconciliation through several
independent observability contracts:

- a per-`SyncOnce` observation accumulated throughout reconciliation and
  projected into Prometheus metrics
- a recovery-telemetry contract that exposes trace-parent restoration, span
  starts, span ends, and recovery-specific metrics
- audit observers and decorators that translate selected recovery decisions
  and scheduler actions into best-effort audit events

These products consume many of the same facts: the sync mode and result, source
reads, job publication, Advice identity and outcome, direct failed-job recovery,
scheduler actions, skip reasons, errors, and recovery correlation. Expressing
those facts through separate interfaces duplicates lifecycle classification at
the scheduler-action call sites and allows the products to drift.

Making recovery telemetry the primary contract does not solve the broader
problem. Trace-oriented operations such as restoring a parent, starting a
span, and ending a particular span type are not controller-domain operations.
Constraint-only Advice and the standalone watchdog do not naturally fit a
recovery-span interface. Audit decoration of that interface also requires
tracing vocabulary and incidental mechanisms, such as context values, to
associate child scheduler actions with their containing Advice evaluation.

Observability is auxiliary to reconciliation. Metrics export, trace export,
audit buffering, and audit delivery must not authorize a scheduler action,
change the context used to execute it, block it on an external dependency, or
change its result.

ADR 0013 still requires SAC to continue a durable per-recovery W3C context and
create correctly parented recovery spans. That requirement does not require
the controller-facing interface itself to expose `context.Context`. A tracing
implementation can retain its OpenTelemetry context privately inside the scope
objects that represent nested observable operations.

## Decision

### Describe controller work through one scoped domain interface

The reconciliation package will own one top-level `Observability` interface.
The controller will begin one observability scope for each `SyncOnce` call and
will describe nested work through typed child scopes and point-in-time events.

The conceptual contract is:

```go
type Observability interface {
	BeginSync(SyncStarted) SyncScope
}

type SyncScope interface {
	BeginSourceRead(SourceReadStarted) SourceReadScope
	BeginJobUpsert(JobUpsertStarted) JobUpsertScope
	BeginAdviceApplication(AdviceApplicationStarted) AdviceApplicationScope
	BeginFailedJobRecovery(FailedJobRecoveryStarted) FailedJobRecoveryScope

	SnapshotBuilt(SnapshotBuilt)
	ModeResolved(ModeResolved)
	LogLinksReconciled(LogLinksReconciled)
	RecoveryContextResolved(RecoveryContextResolved)
	RecoveryMilestoneReported(RecoveryMilestoneReported)

	End(SyncFinished)
}

type SchedulerActionOwner interface {
	BeginSchedulerAction(SchedulerActionStarted) SchedulerActionScope
}

type AdviceApplicationScope interface {
	SchedulerActionOwner
	End(AdviceApplicationFinished)
}

type FailedJobRecoveryScope interface {
	SchedulerActionOwner
	End(FailedJobRecoveryFinished)
}

type SourceReadScope interface {
	End(SourceReadFinished)
}

type JobUpsertScope interface {
	End(JobUpsertFinished)
}

type SchedulerActionScope interface {
	End(SchedulerActionFinished)
}
```

The names are illustrative of the selected structure; implementation may make
small naming refinements without changing the decision. It must preserve these
relationships:

```text
Sync
|-- source read
|-- job upsert
|-- snapshot, mode, and log-link events
|-- Advice application
|   `-- scheduler action
`-- direct failed-job recovery
    `-- scheduler action
```

Every successful `Begin` returns a non-nil scope. Every returned scope is ended
exactly once, child scopes end before their parent, and a no-op implementation
is always available. Begin and end methods do not return observability errors.

### Keep lifecycle values typed and domain-oriented

A `Started` value contains stable identity and intent known when an operation
begins. A `Finished` value contains its logical outcome, skip reason, and error.
The scope implementation owns its start time, duration, completion timestamp,
and exporter-specific state.

Point-in-time facts use past-tense event values instead of artificial scopes.
One method call represents one occurrence, so domain values do not carry
aggregate `Count` fields. The contract uses typed outcomes, scheduler actions,
sources, operation modes, job-attempt identity, and recovery correlation rather
than metric labels, audit envelopes, span attributes, or `map[string]any`.

The containing Advice or failed-job scope owns job, attempt, Advice, operation,
and correlation identity. A nested scheduler-action start therefore carries
only action-specific intent such as the canonical scheduler action and
effective mode. Projections inherit the containing identity from their parent
scope rather than receiving duplicate fields at every call.

Application outcomes and scheduler-action outcomes remain distinct. Advice and
failed-job application outcomes are `applied`, `planned`, `skipped`, or
`failed`. Scheduler-action outcomes are `planned`, `success`, or `error`.
This prevents a planned observe-mode action from being reported as an executed
command and preserves the existing observe-mode safety boundary.

### Fan out each scope to independent projections

The configured implementation is a composite. Each composite scope contains a
per-sync metrics scope, a recovery-telemetry scope, and an audit scope.
Beginning a child operation begins the corresponding child in each projection
and returns another composite scope. Ending a composite scope ends each
projection with the same typed result.

The projections interpret the shared lifecycle independently:

- the per-sync metrics projection accumulates the existing `SyncObservation`
  facts and preserves their Prometheus metric names, labels, and update timing
- the recovery-telemetry projection creates OpenTelemetry recovery reconcile
  and scheduler-action spans and records its recovery-specific context,
  reconcile, action, duration, and milestone metrics
- the audit projection turns selected completed applications and scheduler
  actions into typed, asynchronously delivered audit events

A projection may ignore a signal. For example, audit does not need a sync-end
event, and tracing need not create a span for every snapshot count. The shared
interface is a source of domain facts, not a requirement that all facts appear
in every output.

During migration, the metrics projection may accumulate the same records as
the existing `SyncObservation` and invoke the existing recorder at sync end.
This adapter permits the controller call sites to move to scopes without
renaming metrics or changing their behavior.

### Examples of how the three projections work

These examples illustrate the selected ownership and call shape. They are not
intended to freeze exact Go names or implementation details.

#### Per-sync source observations

Reconciliation begins the outer scope, brackets a source read, records the
resulting snapshot, and ends the scope on every return path:

```go
syncScope := c.observability().BeginSync(SyncStarted{
	Mode: c.modePolicy().DefaultMode,
})
defer func() {
	syncScope.End(SyncFinished{Err: err})
}()

sourceScope := syncScope.BeginSourceRead(SourceReadStarted{
	Source: SourceReadSlurmActive,
})
jobs, readErr := c.JobReader.ActiveJobs(ctx)
sourceScope.End(SourceReadFinished{
	Items: len(jobs),
	Err:   readErr,
})

syncScope.SnapshotBuilt(SnapshotBuilt{
	ActiveJobs: len(jobs),
	// Other source counts omitted.
})
```

The projections handle those calls differently:

- per-sync metrics retains the source result and snapshot counts in its private
  sync scope, then updates the existing source, item, snapshot, loop, and
  duration metrics when the sync ends
- recovery telemetry ignores these calls because a polling source read is not
  part of an individual recovery lifecycle
- audit ignores these calls because ordinary polling and inventory counts are
  not workload audit events

The metrics scope can initially adapt this lifecycle back into the existing
`SyncObservation`:

```go
type metricsSyncScope struct {
	started     time.Time
	observation SyncObservation
	recorder    Observer
}

func (s *metricsSyncScope) End(finished SyncFinished) {
	s.observation.Duration = time.Since(s.started)
	s.observation.Outcome = outcomeFromError(finished.Err)
	s.recorder.ObserveSync(s.observation)
}
```

#### One successful Advice release

Reconciliation describes the Advice and scheduler-action lifecycle while
retaining its application context solely for command execution:

```go
application := syncScope.BeginAdviceApplication(AdviceApplicationStarted{
	AdviceID:          advice.GetHeimdallJobId(),
	Job:               JobAttempt{JobID: job.JobID, AttemptIndex: advice.GetAttemptIndex()},
	TargetAttempt:     job.AttemptIndex,
	DesiredState:      "released",
	Mode:              mode,
	RecoveryOperation: RecoveryDAGRelease,
	Correlation:       recoveryCorrelation,
})

action := application.BeginSchedulerAction(SchedulerActionStarted{
	Action: SchedulerActionRelease,
	Mode:   mode,
})
actionErr := c.ActionExecutor.JobRelease(ctx, job.JobID)
// Successful-path example: actionErr is nil.
action.End(SchedulerActionFinished{
	Outcome: SchedulerActionSuccess,
	Err:     actionErr,
})

application.End(AdviceApplicationFinished{
	Outcome:  ApplicationApplied,
	Terminal: true,
	Err:      actionErr,
})
```

The composite application scope holds one child from each projection:

```go
type compositeAdviceScope struct {
	syncMetrics AdviceApplicationScope
	recovery    AdviceApplicationScope
	audit       AdviceApplicationScope
}

func (s *compositeAdviceScope) BeginSchedulerAction(
	started SchedulerActionStarted,
) SchedulerActionScope {
	return &compositeActionScope{
		syncMetrics: s.syncMetrics.BeginSchedulerAction(started),
		recovery:    s.recovery.BeginSchedulerAction(started),
		audit:       s.audit.BeginSchedulerAction(started),
	}
}

func (s *compositeAdviceScope) End(finished AdviceApplicationFinished) {
	s.syncMetrics.End(finished)
	s.recovery.End(finished)
	s.audit.End(finished)
}
```

For this occurrence:

- per-sync metrics records one successful Advice release action with its
  effective mode
- recovery telemetry starts `job.recovery.reconcile` from the durable remote
  parent, starts `job.recovery.scheduler_action` as its child, ends both spans
  with successful, terminal attributes, and updates its recovery reconcile,
  action, and duration metrics
- audit emits an `advice_scheduler_action_completed` event for the returned
  command and an `advice_application_applied` event for the containing Advice

The OpenTelemetry scope retains its parent context privately:

```go
type otelAdviceScope struct {
	ctx    context.Context
	span   trace.Span
	tracer trace.Tracer
}

func (s *otelAdviceScope) BeginSchedulerAction(
	started SchedulerActionStarted,
) SchedulerActionScope {
	ctx, span := s.tracer.Start(
		s.ctx,
		"job.recovery.scheduler_action",
		trace.WithAttributes(actionAttributes(started)...),
	)
	return &otelActionScope{ctx: ctx, span: span}
}
```

The audit scope retains event construction state rather than an OTel context:

```go
type auditAdviceScope struct {
	started        AdviceApplicationStarted
	idempotencyKey string
	plannedActions []SchedulerAction
	sink           audit.Sink
}

type auditActionScope struct {
	parent         *auditAdviceScope
	action         SchedulerAction
	idempotencyKey string
}

func (s *auditActionScope) End(finished SchedulerActionFinished) {
	if finished.Outcome == SchedulerActionPlanned {
		s.parent.plannedActions = append(s.parent.plannedActions, s.action)
		return
	}

	s.parent.sink.Add(completedActionEvent(
		s.parent.started,
		s.action,
		finished,
		s.idempotencyKey,
	))
}
```

The audit implementation never receives the application `ctx`, and the OTel
implementation never receives the audit sink or controls the scheduler call.

#### Constraint-only Advice in observe mode

Constraint-only Advice uses the same application scope with no recovery
operation or durable recovery correlation. Its scheduler action ends as
`planned`, and the application ends as `planned`:

```go
application := syncScope.BeginAdviceApplication(AdviceApplicationStarted{
	AdviceID:      advice.GetHeimdallJobId(),
	Job:           JobAttempt{JobID: job.JobID, AttemptIndex: job.AttemptIndex},
	DesiredState:  "update_constraints",
	Mode:          ObserveMode,
})

action := application.BeginSchedulerAction(SchedulerActionStarted{
	Action: SchedulerActionUpdateConstraints,
	Mode:   ObserveMode,
})
action.End(SchedulerActionFinished{Outcome: SchedulerActionPlanned})
application.End(AdviceApplicationFinished{Outcome: ApplicationPlanned})
```

Here per-sync metrics records a planned node-update action, recovery telemetry
no-ops because this is not a recovery operation, and audit emits an
`advice_application_evaluated` event containing the planned action. No
scheduler-command completion event is emitted because no command ran.

### Keep OpenTelemetry context private to its scopes

The controller-facing observability interface does not accept or return
`context.Context`. Application contexts remain owned by reconciliation for
cancellation, deadlines, and execution of Slurm, Heimdall, state-store, and
filesystem operations.

The OpenTelemetry implementation may use `context.Context` internally. An
Advice scope validates and extracts explicit durable `traceparent` and
`tracestate` correlation data into a private OpenTelemetry context and stores
that context with its reconcile span. A scheduler-action scope starts its span
from the private Advice-scope context and stores the resulting child context
until it ends. The private context is never returned to reconciliation and is
never substituted for the context used to execute a scheduler command.

Per ADR 0013, a recovery's durable remote parent, not the polling sync, is the
parent of its reconcile span. An optional sync span may link to recovery work
but does not become its parent merely because the work occurred during that
poll.

This design intentionally does not provide automatic propagation of a private
scope context through arbitrary HTTP or gRPC calls made by application code.
Where transport propagation is required, the owning client or transport must
instrument it using explicit correlation data. Observability must not gain
control of application execution merely to propagate a trace context.

### Keep durable recovery operations outside observability

Beginning or finding a durable recovery sequence and recording a recovery
milestone are application/API operations. They remain owned by reconciliation
and the Heimdall client. The observability scope may record the result of those
operations, but it does not perform them and does not treat their success as
authorization for scheduler work.

Failure to acquire, validate, export, or report recovery telemetry remains
observable and fail-open with respect to the existing scheduler decision. It
must not cause a command to be added, suppressed, or repeated.

### Keep audit selection and delivery independent

Audit event definitions and the asynchronous producer remain separate from the
domain lifecycle contract. Audit scopes own audit timestamps, event
idempotency keys, planned-action accumulation, payload conversion, and
non-blocking submission to the audit sink.

Not every lifecycle occurrence is necessarily an audit event. In particular,
level-triggered Advice can be evaluated or skipped on every poll. The audit
projection must make an explicit event-selection and semantic-deduplication
choice rather than converting all metric observations into durable records.
Actual scheduler-command attempts remain distinct occurrences and are eligible
for distinct completion events.

Audit history is not derived from exported spans. Trace sampling, export, and
retention remain independent from best-effort workload audit delivery.

### Keep standalone process contracts consumer-owned

The standalone Slurm watchdog is not nested under a controller `SyncScope`.
It will own a correspondingly narrow scoped observability interface for its
sync and release lifecycle. It may reuse canonical action and result types and
the same metrics, tracing, or audit projection machinery, but the controller
interface will not become a process-generic event bus to accommodate it.

## Consequences

- Reconciliation receives one observability dependency instead of separate
  per-sync metrics, recovery-telemetry, Advice-audit, and action-audit hooks.
- Metrics, tracing, and audit classify the same completed domain operation and
  cannot silently disagree because separate call sites were missed.
- Constraint-only Advice and recovery Advice use the same application scope;
  tracing and audit can independently choose how to project either form.
- Parent scopes naturally retain child identity and planned actions, removing
  the need to place observability-owned mutable state in application contexts.
- OpenTelemetry implementation details, including local span objects and
  contexts, remain private to the tracing projection.
- Existing metric contracts can be preserved through an adapter while call
  sites migrate incrementally.
- Existing typed audit-event definitions and non-blocking delivery can be
  retained while their producer changes from a recovery-telemetry decorator to
  an audit scope projection.
- The implementation adds small scope objects and fan-out boilerplate. Internal
  projections must remain cheap, non-blocking, and safe when disabled.
- A broad domain lifecycle interface means adding a new observable touch point
  changes the composite implementation. New touch points should be added only
  for stable domain operations, not exporter-specific wishes.
- Private OTel contexts cannot automatically parent arbitrary instrumented
  outbound calls. Such propagation must be designed at the relevant client or
  transport boundary.
- Instrumenting scheduler-action call sites still requires explicit human
  review even though observability is not permitted to change action policy or
  execution semantics.

This decision does not supersede ADR 0013. Durable recovery identity, W3C
correlation, span names and outcomes, milestone persistence, and recovery
completion semantics remain authoritative. It replaces only the proposed
choice to make a trace-specific `RecoveryTelemetry` interface the controller's
primary in-process fan-out boundary.

## Alternatives Rejected

### Retain separate metrics, recovery-telemetry, and audit interfaces

Separate interfaces appear narrower, but they require scheduler-action and
Advice call sites to classify the same work multiple times. They have already
created coverage gaps for Advice that does not enter the recovery trace and
make future changes prone to metrics, trace, and audit drift.

### Make `RecoveryTelemetry` the primary lifecycle interface

A trace-oriented interface exposes parent restoration, span names, and span
end methods to reconciliation. Audit then becomes a decorator around tracing
and still needs companion observers for operations outside the recovery model.
Recovery tracing is one projection of controller work, not the domain boundary
that defines all observable work.

### Pass `context.Context` through every observability method

Returning modified application contexts would make tracing mechanics visible
to reconciliation and could accidentally change the context used for command
execution. Nested OTel spans can instead retain their private contexts in the
returned scope objects. The loss of automatic arbitrary transport propagation
is accepted and assigned to explicit client or transport instrumentation.

### Let observability execute callbacks around application work

A callback API could run work beneath a private tracing context, but it would
give an auxiliary concern control over scheduler and network execution. It
would also complicate error, cancellation, panic, and result ownership.
Reconciliation remains responsible for executing work between `Begin` and
`End`.

### Publish generic events to an in-process event bus

A generic event name plus untyped attribute map would reduce interface method
count but move required-field validation and legal outcome combinations to
runtime. Typed scopes make operation nesting and result ownership explicit and
keep exporter-specific schemas out of controller code.

### Derive audit events from OpenTelemetry spans

Span-derived audit would make workload history depend on sampling, exporter and
Collector availability, attribute limits, and trace retention. Audit and trace
projections may consume the same lifecycle facts, but neither is the source of
truth for the other.
