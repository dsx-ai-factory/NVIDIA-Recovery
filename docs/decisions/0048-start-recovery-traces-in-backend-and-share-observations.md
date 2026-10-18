# ADR 0048: Start Recovery Traces in the Backend and Share Observations

Status: accepted

Date: 2026-10-07

Discussion: [PR #245](https://github.com/NVIDIA/NVIDIA-Recovery/pull/245)

Accepted following PR review and maintainer confirmation on 2026-10-08.
This decision partially supersedes
[ADR 0013](0013-correlate-recovery-with-durable-trace-context.md), replacing
SAC-initiated sequence creation and direct-path tracing, and defines how
recovery instrumentation shares facts with the audit trail. ADR 0013's
remaining correlation, propagation, and completion contracts remain in force.

Related decisions:
[ADR 0032](0032-unify-slurm-controller-observability-behind-scoped-lifecycles.md)
is superseded by this decision: retain its recovery-lifecycle approach and
retire the broader controller-wide scope proposal, as confirmed by the
maintainer on 2026-10-16.

[ADR 0053](0053-fanout-audit-and-recovery-trace.md), originally proposed in
[PR #39](https://github.com/NVIDIA/NVIDIA-Recovery/pull/39), supplies the fanout
principle adopted here: overlapping recovery activity feeds both tracing and
audit while keeping their responsibilities and delivery independent. This
decision supersedes that proposal by specifying shared operation observations
and a separate milestone projection, as confirmed by the maintainer on
2026-10-16.

[ADR 0045](0045-require-advice-before-slurm-requeue.md) establishes Advice-driven
requeue; [ADR 0038](0038-catalog-audit-event-payloads-and-identifiers.md) adopts
the audit persistence/delivery boundary from
[ADR 0017](0017-persist-best-effort-workload-audit-events-in-notifications.md)
and defines event identity; [ADR 0031](0031-expose-notifications-http-api-through-authenticated-gateway.md)
and [ADR 0036](0036-always-produce-and-expose-audit-events.md) govern transport
and always-on audit production.

The [design study and stack changes](../architecture/recovery-observability-revision.md)
record current implementation evidence and the alternatives considered.

## Context

ADR 0013 accommodated SAC requeueing failed jobs before receiving Advice.
Its public `BeginOrGetRecoverySequence` operation let SAC obtain durable
correlation before that command. With recovery and DAG actions initiated by
backend Advice, that consumer-side creation path is unnecessary. The legacy
eager feature is outside the revised tracing coverage target.

The merged persistence work in [PR #20](https://github.com/NVIDIA/NVIDIA-Recovery/pull/20)
remains useful: backend triggers can still race, attribution precedes visible
Advice, and action reports still need durable deduplication. Removing an
external creator does not remove those requirements.

Audit producers now capture many of the same operations as the pending tracing
stack. They also cover activity without a recovery sequence. Audit delivery is
best effort and independent of trace sampling. Neither product can serve as
the other's complete source of truth.

## Decision

### Backend creation; read-only public lookup

Job Manager alone creates or obtains a sequence as part of processing a
recovery or DAG trigger, before attribution or Advice work that belongs in
that sequence. It retains the internal atomic create-or-get operation and the
scoped natural key from ADR 0013. Concurrent backend triggers return the first
committed row, including its immutable trigger timestamp and W3C parent.

Expose `GetRecoverySequence` in place of the unmerged public
`BeginOrGetRecoverySequence` API. Its request identifies the exact scheduler
system, scheduler scope, job, source attempt, and operation. It accepts no
trigger timestamp, trigger type, or creation intent; a missing sequence returns
not found and causes no write or recovery-root span. The response carries the
existing `RecoveryContext`. Keep the same gateway authentication boundary.

Normal SAC reconciliation obtains context directly from each Advice item;
lookup is for explicit readers and diagnostics, not a required pre-command
round trip or a missing-context creation fallback. Older or uncorrelated
Advice still reconciles normally and reports an observability coverage gap.

Keep sequences independent of Advice existence: attribution and backend
decision work may precede visible Advice. Keep the current natural key rather
than adding Advice generation to it. Same-operation generations reuse the
sequence; a change of operation attaches the sequence for that operation.
Include Advice ID and generation in observations so historical activity is
not mistaken for the current generation's reconciliation state.

Sequence ownership beginning at the backend does not move the latency start
to Advice publication. Preserve the trustworthy trigger observation timestamp
from ADR 0013, with backend acceptance time as an explicit fallback. DAG
release starts at the backend dependency decision, not job submission or the
start of its wait for dependencies. Preserve the first committed trigger when
additional evidence arrives. Incoming producer traces may be linked; they do
not choose or replace the canonical recovery parent.

### Capture one operation lifecycle and project it independently

Use a narrow, domain-oriented observation boundary for Advice application and
its scheduler actions. Extend the existing SAC observations with start/end
lifecycle support and the correlation needed by tracing; do not put audit on
top of a trace exporter or nest two command-execution wrappers.

The scheduler adapter remains the only command executor. For each actual
command attempt it captures once:

- a fresh UUIDv4 occurrence ID, reused for delivery retries of that occurrence;
- exact job/source/target attempt, Advice ID and generation, and available
  recovery identity;
- the namespaced action, effective mode, start time, completion time, result,
  and error;
- explicit `completes_sequence` intent, computed by the adapter after the
  command result is known.

Capture completion immediately on command return, before reporting or export.
The action span end, audit `event_produced_at`, and milestone `completed_at`
use that one timestamp. Local duration uses a monotonic clock; persisted times
use UTC with microsecond precision. Distinct command attempts get distinct
occurrence IDs. An Advice-application event is a different occurrence and gets
its own ID.

The same local facts feed three independent projections:

1. OpenTelemetry spans and recovery metrics.
2. Existing typed audit events submitted to the audit buffer.
3. Scheduler-action milestones submitted to Job Manager.

The third projection is an explicit recovery-reporting collaborator, not an
OTel exporter or a Notification Service side effect. Keep separate bounded
delivery and failure accounting for audit and milestones; remote reporting
must not delay the next scheduler action. Enqueue immutable results locally,
preserve order per sequence where possible, and bound retries and shutdown
flushes. No durable outbox or exactly-once delivery is introduced. Reporting
retries never execute the scheduler command again.

Start real spans around the operations they measure. A finish callback alone
must not manufacture retrospective operation spans. The tracing projection
keeps its local span/context state; the controller retains execution deadlines
and cancellation. The recovery parent comes from the Advice item, not the
polling RPC. This change does not require refactoring all `SyncOnce` metrics,
source reads, publication workers, or watchdog scopes.

Apply the same principle at overlapping Python boundaries: use the committed
Advice transition for its audit snapshot and Advice span, and the actual
attribution request/result for their observations. Preserve existing snapshot
allowlists and event selection. A Watcher observation before sequence creation
can be audited and linked as trigger evidence without inventing a sequence.

### Share correlation without collapsing identities

Keep `heimdall_job_id` as the required audit workload-attempt key. Add optional
`recovery_sequence_id`, `trace_id`, and `span_id` to overlapping event payloads
when known. A trace ID comes from validated W3C/SDK context; a span ID identifies
the actual corresponding operation, not an unrelated transport span.

Do not reinterpret legacy `correlation_uuid` as a guaranteed sequence ID: it
currently falls back to Advice ID. Preserve that field for compatibility while
new consumers use explicitly named identifiers. Audit events without a valid
trace or recovery sequence remain valid. Unsampled or expired traces can leave
valid audit correlation without an available trace in the backend.

TODO: Remove legacy `correlation_uuid` after producers and consumers adopt
explicit recovery and trace identifiers; retain readability of historical
audit events. This follow-up was agreed in the
[PR discussion](https://github.com/NVIDIA/NVIDIA-Recovery/pull/245#issuecomment-6051129419).

Use the shared scheduler-action occurrence UUID as both that audit event's
producer idempotency key and that action's milestone idempotency key. Retain
each store's existing uniqueness rules. The audit row's internal UUIDv7 remains
owned by Notifications and is not a recovery or occurrence ID. No additional
common event store or database join is required.

### Preserve storage ownership and measurement semantics

Job Manager owns `RecoverySequence`, canonical context, accepted milestones,
and the persisted trigger-to-completion interval. Notifications owns the
append-only, best-effort workload audit history. OTLP remains the trace delivery
path. Keep the existing audit ingestion/query endpoints and
`RecordRecoveryMilestone`; the common gateway connection can carry both RPCs,
but one service's acknowledgement does not acknowledge the other's write.

Keep phase-one tables and migrations. Do not reconstruct milestones from audit
rows, send every audit event to Job Manager, or derive audit events from
exported spans. Audit destination selection, sampling, retention, queue loss,
and service outages must not control the other product.

The adapter marks only successful final release as completing a restart
sequence. Job Manager validates sequence/operation identity, timestamp validity,
idempotency, and that completion intent accompanies a successful action; it
does not infer completion from Slurm action names. Preserve phase one's first
accepted completing milestone and its immutable timestamp. Delayed delivery
uses command-return time, never receipt time. Clock inconsistencies produce a
coverage gap instead of a fabricated non-negative duration.

Restart latency still ends when release succeeds, excluding subsequent queue
wait and confirmation polling. DAG release and termination remain separately
observable and outside the failed-job restart SLO. Planned actions, retryable
failures, and already-satisfied observations do not supply command-attributed
restart completion. Preserve real spans, per-item context, bounded metric
labels, and the remaining ADR 0013 contracts.

Keep command completion separate from Advice convergence. Reuse current
generation-scoped convergence/abandonment classification under
[ADR 0025](0025-filter-and-terminally-reconcile-actionable-advice.md),
[ADR 0026](0026-classify-advice-expiration-in-scheduler-controller.md), and
[ADR 0027](0027-route-each-advice-generation-to-one-reconciler.md). Observability
must not implement a second skip/expiry policy. A later state observation does
not rewrite release time; terminal reconciliation without a received completion
report has unknown command timing. Phase-one `active`/`completed` sequence
status alone is not proof that recovery is still executing: age diagnostics
must distinguish missing completion evidence from the owning Advice
generation's terminal status and from missing generation linkage.

An audit `advice_application_applied` event means the application performed its
applicable work; it is not by itself a completed restart. The shared result
must retain explicit completion intent separately from that application
outcome. Later convergence must not generate another command-completion sample
or be counted as a second successful recovery.

### Preserve audit coverage and enable UI correlation

Audit remains broader than recovery tracing. Keep operational events,
constraint-only Advice, and standalone watchdog events without requiring a
sequence. Repeated non-terminal poll skips need not become audit rows merely
because tracing observes them. Preserve generation-terminal audit events.

The watchdog remains independent of backend availability. Its release audit
does not create a sequence or substitute for a SAC release milestone; such
releases have no SAC command-attributed latency unless a future design supplies
trustworthy correlation. Retained eager-path audit behavior similarly does not
require extending the revised recovery tracing coverage.

The existing audit UI can present an optional trace link from explicit
correlation fields using an operator-configured trace destination. It continues
to query Notifications for events; trace detail stays in the trace backend.
Missing links or traces do not hide events. Do not fetch each event's mutable
current Advice to reconstruct historical context, persist deployment URLs in
events, or add a new combined timeline/trace API as a prerequisite.

## Consequences

- SAC loses sequence creation, eager-path context acquisition, and duplicate
  command wrappers. Attribution still contributes to end-to-end latency.
- Existing durable storage and audit transport are retained. Sharing occurs
  at the operation boundary, not through a distributed transaction.
- One captured result prevents different timestamps, IDs, or outcomes for
  the same command. Independent delivery can still leave different coverage
  in audit, milestones, and traces; expose those gaps separately.
- Typed observation and payload changes require coordinated generated-contract
  updates and behavioral validation in the implementation stack. Scheduler
  instrumentation retains human review requirements.
- A durable completion row does not guarantee exactly-once metric export:
  crashes between commit and histogram observation can lose a sample. Neither
  an audit acknowledgement nor a span count resolves that limitation.

## Alternatives Rejected

- **Keep the public begin-or-get API for compatibility.** Its runtime API is
  unmerged, and its SAC creation use case is retired. Internal create-or-get
  remains necessary for backend concurrency.
- **Use audit ingestion as the sole completion channel.** Makes measurement
  depend on audit destination, event selection, and delivery, and requires
  cross-service projection or moving completion ownership.
- **Generate audit from spans or milestones.** Sampling and limited milestone
  coverage omit valid workload history and couple independent products.
- **Keep nested audit and tracing command wrappers.** Duplicates operation
  boundaries and can measure reporting time as scheduler time.
- **Unify all controller observability immediately.** A broader scoped
  lifecycle can follow later; recovery overlap does not justify changing
  unrelated per-sync metrics and worker instrumentation in this stack.
