# ADR 0059: Restore Attribution Results and Exclusion Outcomes

Status: accepted

Date: 2026-11-23

## Context

Dedicated attribution results are a consumer contract distinct from workload
state, desired recovery Advice, and the audit timeline. The Gateway import
omitted attribution reads even though Job Manager retained their implementation.
[ADR 0051](0051-retire-embedded-anomaly-attributions.md) retired expensive
attribution enrichment inside anomaly responses and explicitly retained the
dedicated reads; it did not select their retirement at the Gateway.

Consumers also use the per-node exclusion application outcome to distinguish a
diagnosis from an effective recovery constraint. Advice now converges through
SAC's fresh scheduler observations, replacing the inherited controller response
path that supplied this outcome. Audit delivery is best effort and its timeline
payloads do not replace this persisted result contract.

The maintainer selected restoration of the existing consumer contract, with
minimal consumer changes, and rejected additional attribution-to-Advice database
relationships and historical recommendation snapshots for the rare case of
multiple FAST results for one attempt.

## Decision

### Restore dedicated attribution reads

Expose `GetErrorAttributions` and `GetJobErrorAttributions` on the existing
`HeimdallService`, mapped to `GET /v1/error-attributions` and
`GET /v1/job-error-attributions`. Preserve the inherited protobuf JSON contract:
the `attributions` envelope, node and global events, summary and insight,
camelCase field names, and `ExclusionApplicationStatus` enum values. Preserve
job-filtered batch reads with repeatable `jobIds`, limit/offset pagination,
and support for a page size of 200.

Return the effective and original exclusion classifications separately.
`exclusionType` is ADI's decision after allocation and isolation-safety filtering;
`originalExclusionType` must not authorize an exclusion suppressed by that
filtering. This does not restore embedded anomaly enrichment or change
authentication.

### Persist outcomes from confirmed constraints

Restore the per-node `exclusionApplied` field. `APPLIED` means that the requested
constraints are confirmed satisfied; it does not assert which actor executed
the scheduler mutation. SAC remains responsible for comparing fresh scheduler
state with desired Advice and reports the generation-scoped reconciliation
disposition through the Gateway, as selected in
[ADR 0025](0025-filter-and-terminally-reconcile-actionable-advice.md) and
[ADR 0027](0027-route-each-advice-generation-to-one-reconciler.md). Job Manager
projects an accepted applicable convergence report into attribution outcomes;
it does not reproduce the scheduler comparison.

Only effective `TEMPORARY` or `CONFIRMED` FAST attribution rows for the source
job ID, source attempt, and nodes in current-generation converged `Restarted`
Advice are eligible for `APPLIED`. SLOW results never cause exclusions. With
multiple anomalies for an attempt, select the newest FAST anomaly
deterministically rather than marking every historical attribution. Keep the
association in existing identity and classification fields; do not introduce a
database relationship or retain full
recommendation history for this projection.

Termination, DAG release, a planned operation, a transient scheduler error, or
an abandoned generation does not by itself establish an applied exclusion.
Preserve `UNKNOWN`, `NOT_APPLICABLE`, and `NOT_APPLIED` as distinct outcomes;
missing historical evidence must not be converted into `APPLIED`.

### Keep populated constraints stable

Allow initial population and identical retries of node constraints. Once
populated, reject and log an unexpected replacement of those constraints while
preserving the stored constraints. Legitimate Advice lifecycle transitions,
including termination, remain possible. A future demonstrated need for revised
constraints can introduce reconciliation of a new recommendation deliberately;
it is not required for this restoration.

### Separate contract restoration from consistency work

Deliver this decision in two reviewable parts. The first contains this ADR,
the restored public contract, persistence and normal convergence projection
in the same transaction as status acceptance under the existing Advice lock,
and the populated-constraint guard. The second handles retries, ordering,
late attribution arrival, and consistency repair. Both are required before
claiming production migration readiness. Outcome persistence must be idempotent
and recoverable when completion and result persistence are interrupted or
observed out of order; audit delivery is not the repair mechanism.

## Consequences

Existing consumers retain their attribution parsing and finding rules. New
consumers can query diagnosis and exclusion outcome directly without joining
the audit timeline or interpreting scheduler-specific commands. Public API
bindings and specifications must be regenerated with the restored fields.

Schema changes must retain historical attribution rows and represent unknown
application history honestly. Deploy the additive schema and compatible backend
before consumers rely on restored outcomes; migration execution requires human
review under the repository's deployment boundaries.

The lookup uses existing job-attempt, node, and anomaly identity. Its deliberately
limited treatment of multiple FAST results avoids a new relationship and
recommendation-history subsystem. Persisted status and application projection
must still respect the reported generation rather than interpreting a stale
report as confirmation of a different recommendation.

This extends dedicated read availability under ADR 0051 and adds a narrow
durable outcome projection permitted by ADR 0025. It does not supersede their
decisions or make audit events the attribution source of truth.

## Alternatives Rejected

- Reconstruct attribution results and application decisions in each consumer
  from audits: moves recovery interpretation downstream and relies on best-effort
  timeline delivery.
- Restore only Gateway forwarding: leaves consumers without the application
  outcome on which their findings depend.
- Treat every converged Advice as applied exclusion evidence: confuses
  termination and DAG release with exclusion-bearing recovery.
- Add attribution foreign keys and historical recommendation snapshots:
  unnecessary for the selected one-result-per-attempt operating assumption.
- Accept arbitrary changes to populated constraints: expands reconciliation and
  historical-association requirements for an exceptionally rare producer case.
