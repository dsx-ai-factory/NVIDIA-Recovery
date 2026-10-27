# ADR 0054: Persist Application-Requested Job Termination

Status: accepted

Date: 2026-10-27

## Context

An application can determine that its own failure is unrecoverable before
NVIDIA Recovery reaches the same conclusion. Retrying that Slurm job wastes
resources and can create a crash loop. The application can reliably identify
its scheduler job ID, but it does not know NVIDIA Recovery's attempt index or
canonical workflow ID.

Advice is intentionally scoped to one exact attempt. A request can race with
Job Manager producing `RESTARTED` Advice or with SAC applying that Advice. An
exact-attempt record alone therefore cannot preserve the application decision
for a successor created by the race. The decision must also survive Job
Manager restarts and may arrive before the first attempt is registered.

NVIDIA Recovery currently operates one instance per scheduler cluster, so a
scheduler job ID is unambiguous within one Job Manager database. Existing
Gateway authentication remains the ingress trust boundary; a job ID identifies
the target but is not itself an authentication credential.

[ADR 0045](0045-require-advice-before-slurm-requeue.md) assigns recovery-policy
decisions and Advice production to Job Manager and scheduler mutation to SAC.
[ADR 0011](0011-cancel-dag-dependents-after-parent-abort.md) makes terminal
parent Advice an absorbing DAG abort.

## Decision

Expose an authenticated application-intention operation that accepts a
scheduler job ID, the single supported desired state `CANCELLED`, and an
optional bounded reason. The operation records a durable, idempotent
application intention keyed by scheduler job ID. The first accepted intention
is absorbing; this phase does not provide a release or revocation operation.

Store the intention in a dedicated `application_intentions` table rather than
an execution-attempt or Advice row. Job execution and Advice rows remain scoped
to exact attempts, while the intention applies to the current attempt and any
successor sharing the scheduler job ID. The table has no foreign key to a job
attempt so an intention can be accepted before Job Manager registers the job.

Job Manager acknowledges the request as soon as the intention is durably
stored. HTTP 200 means "accepted and persisted"; it does not mean that Advice
has been created or that Slurm has cancelled the job. After sending the
response, Job Manager performs a best-effort background reconciliation of the
latest known attempt. For a non-final attempt it publishes exact-attempt
`TERMINATED` Advice, which SAC translates to cancellation subject to its
existing mode, ownership, and action-acknowledgement safeguards. For an
already-final attempt the same Advice records the terminal decision and SAC
observes it as already satisfied.

The first durable request also emits a best-effort
`application_termination_requested` audit event tagged to the latest known
attempt. If the request arrives before registration, Job Manager defers that
event until the first attempt is observed. Later Advice and scheduler-action
events remain scoped to the exact attempts they affect. A repeated idempotent
request does not emit another request event.

Job Manager also checks the durable intention after every attempt upsert and
again when a late attribution result is about to produce recovery Advice for
an exact attempt. This second check covers an attempt that was already observed
when the application request arrived but whose recovery decision was still in
flight. These level-triggered checks also recover if Job Manager stops after
acknowledging the application but before its background task creates Advice.
Every applicable attempt gets `TERMINATED` Advice with
`application_requested_cancellation` provenance. Existing storage rules keep
`TERMINATED` Advice sticky, so late attribution or another policy writer cannot
replace it with `RESTARTED`.

If SAC has already executed older `RESTARTED` Advice, the scheduler may create
a successor before Job Manager observes the new terminal generation. The
durable job-level intention applies when that successor is registered, causing
it to receive `TERMINATED` Advice. This design guarantees convergence on
termination; it does not claim atomic revocation of a scheduler command that
has already executed.

Application-requested terminal Advice participates in existing DAG propagation.
If the target is a resolved DAG parent, Job Manager terminates its reachable
blocked descendants under ADR 0011.

The public Gateway contract is scheduler-neutral. Job Manager continues to own
the decision and SAC remains the only component that calls the scheduler. No
new SAC policy or command path is introduced.

## Consequences

Applications need only their scheduler job ID. Repeated requests are
idempotent, and the decision remains effective across Job Manager and SAC
restarts. Applications do not block on Job Manager policy evaluation, DAG
propagation, SAC polling, or the Slurm command. A job that races through requeue
can briefly produce another attempt, but that attempt cannot continue once the
normal Advice loop observes its terminal recommendation.

The shared Job Manager schema gains one additive table and therefore requires
an Alembic migration and human-reviewed rollout. Existing job and Advice table
schemas remain unchanged.

The intention is permanent for the scheduler job ID within the retained Job
Manager database. Operators must not use this phase as a pause/resume control.
Any future support for revocation, additional desired states, multiple
schedulers per instance, or scheduler job-ID reuse across retained logical
jobs requires a successor decision with a stronger identity and authorization
contract.

Observe mode and disabled scheduler-action acknowledgement still prevent
mutation. The API can record an intention in those modes, but only action-mode
SAC can make Slurm cancel a non-final attempt.

## Alternatives Rejected

- **Store the flag on `job_execution_records`.** That table has one row per
  attempt, cannot represent an intention received before registration, and
  provides no single authoritative value for later attempts.
- **Use only exact-attempt Advice.** This terminates the currently known attempt
  but loses the decision when a requeue race creates a successor.
- **Let Job Manager call Slurm directly.** This duplicates credentials and
  scheduler safety policy and violates the established JM/SAC boundary.
- **Scope the first version to workflows.** The application does not reliably
  know the canonical workflow ID; scheduler job ID is the available target and
  matches Slurm requeue identity.
