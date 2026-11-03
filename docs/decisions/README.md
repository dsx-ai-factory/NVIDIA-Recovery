# Architecture Decision Records

Architecture Decision Records (ADRs) preserve durable architectural rationale
that would otherwise be lost in a chat, execution plan, handoff, or code review.
They explain why a consequential choice was made and what constraints follow
from it. They are not a substitute for current implementation documentation.

This directory records NVIDIA Recovery decisions from the adoption of this process
onward. Earlier decisions are not included.

## Decision Index

| ADR | Status | Decision |
| --- | --- | --- |
| [0001](0001-use-repository-local-architecture-decision-records.md) | accepted | Record durable architecture decisions in an indexed repository-local ADR collection. |
| [0002](0002-carry-slurm-log-paths-in-scheduler-metadata.md) | accepted | Carry Slurm log file paths as Slurm scheduler metadata and translate them at the legacy Job Manager boundary. |
| [0003](0003-discover-slurm-job-log-paths.md) | proposed | Discover parent and custom-step Slurm stdout paths through bounded, best-effort scheduler metadata enrichment. |
| [0004](0004-materialize-slurm-log-links.md) | proposed | Materialize optional, ownership-scoped Slurm log links in the Advisor Controller for flat-directory collector compatibility. |
| [0005](0005-require-causal-evidence-in-slurm-recovery-e2e-tests.md) | accepted | Require source-specific causal evidence in Slurm recovery E2E tests and validate log collection against a shared-filesystem reference topology. |
| [0006](0006-standardize-log-labels-and-syslog-ingestion.md) | accepted | Separate job-scoped application-log labels from node-scoped system-log labels and inject E2E fixtures through the node syslog file and Alloy. |
| [0007](0007-resolve-dag-release-through-advice-state.md) | accepted | Resolve exact-attempt DAG release through `AdviceState` and enable watchdog monitoring for DAG attempts 1 and later. |
| [0008](0008-baseline-oss-database-schemas.md) | accepted | Replace inherited database histories with two ORM-authoritative, fresh-install Alembic baselines. |
| [0009](0009-derive-watchdog-release-from-slurm-state.md) | accepted | Derive watchdog release eligibility from current Slurm state and attempt index without a per-job watchdog flag. |
| [0010](0010-resolve-squeue-final-state-during-completing.md) | accepted | Parse squeue with final-state selection so failures are detected during the COMPLETING epilog window, superseding the squeue first-token policy. |
| [0011](0011-cancel-dag-dependents-after-parent-abort.md) | accepted | Treat a parent's final `TERMINATED` Advice as an absorbing DAG workflow abort and terminate every descendant. |
| [0012](0012-standardize-global-otlp-collector-configuration.md) | accepted | Configure OTLP exporter environment injection once per umbrella release and apply it to every NVIDIA Recovery application container. |
| [0013](0013-correlate-recovery-with-durable-trace-context.md) | accepted | Correlate exact-attempt recovery across services with durable W3C context and measure restart completion at successful scheduler release; SAC-initiated sequence creation and direct-path tracing are superseded by ADR 0048. |
| [0014](0014-route-slurm-jobs-by-aps-environment.md) | accepted | Route Slurm jobs by the inherited APS environment convention before mode resolution or publication. |
| [0015](0015-scope-slurm-watchdog-by-aps-environment-pattern.md) | accepted | Scope watchdog tracking and release by a controller-compatible APS environment selector with an explicit wildcard; job-mode independence is superseded by ADR 0042. |
| [0016](0016-expose-dag-dependency-evaluation-reasons.md) | accepted | Store and expose durable structured reasons for held and aborted DAG dependency gates. |
| [0017](0017-persist-best-effort-workload-audit-events-in-notifications.md) | proposed | Persist structured audit events in a dedicated Notification Service table through non-blocking, best-effort delivery. |
| [0020](0020-bound-slurm-controller-publication-and-accounting-concurrency.md) | accepted | Bound independent NVIDIA Recovery job publication and necessary Slurm accounting work with separate concurrency limits; all-or-nothing accounting publication is superseded by ADR 0046. |
| [0021](0021-measure-slurm-controller-dependency-latency.md) | proposed | Measure controller source and dependency-operation latency plus bounded worker-pool backlog, throughput, and saturation. |
| [0022](0022-bootstrap-sac-job-discovery-once.md) | accepted | Bootstrap SAC job discovery once from a filtered JM query, then reconcile from fresh Slurm state, seen attempts, and Advice. |
| [0025](0025-filter-and-terminally-reconcile-actionable-advice.md) | accepted | Define `only_active` Advice selection using stale-attempt filtering and converged or abandoned reconciliation dispositions. |
| [0026](0026-classify-advice-expiration-in-scheduler-controller.md) | accepted | Make SAC the sole owner of Advice expiration and persist expired generations as abandoned. |
| [0027](0027-route-each-advice-generation-to-one-reconciler.md) | accepted | Route each Advice generation to one authoritative reconciler and key status by Advice ID and generation. |
| [0028](0028-project-slurm-job-fields-from-scheduler-metadata.md) | accepted | Carry dashboard-required Slurm job fields through scheduler metadata and project them at the Job Manager persistence boundary. |
| [0029](0029-configure-log-query-backends.md) | proposed | Select Loki or VictoriaLogs through one in-process log-query contract while keeping provider-native queries and ingestion configuration separate. |
| [0030](0030-route-audit-ingestion-through-internal-gateway-grpc.md) | superseded | Route producer audit ingestion through an internal-only API Gateway gRPC contract; superseded by ADR 0031. |
| [0031](0031-expose-notifications-http-api-through-authenticated-gateway.md) | accepted | Expose Audit Trail ingestion through the shared generated HTTP/gRPC facade; ADR 0049 extends this boundary to authenticated reads. |
| [0032](0032-unify-slurm-controller-observability-behind-scoped-lifecycles.md) | superseded | Proposed controller-wide typed scopes for metrics, tracing, and audit; superseded by ADR 0048's narrower recovery lifecycle, with the broader proposal retired. |
| [0033](0033-deliver-watchdog-audit-events-through-gateway.md) | accepted | Deliver optional watchdog audits through gateway gRPC while keeping scheduler decisions independent of remote services. |
| [0034](0034-publish-source-and-charts-with-external-contributions.md) | accepted | Publish source and Helm charts with external contributions and GitHub authority; Attribution defaults are partially superseded by ADR 0048. |
| [0035](0035-enrich-active-slurm-log-metadata-asynchronously.md) | accepted | Move supplemental active Slurm log-path discovery off the serialized reconciliation tick into a bounded, exact-attempt background cache. |
| [0036](0036-always-produce-and-expose-audit-events.md) | accepted | Always produce and expose audit events while retaining configurable destinations; supersedes the feature gates in ADRs 0031 and 0033. |
| [0037](0037-filter-sac-bootstrap-by-aps-environment.md) | accepted | Filter SAC backend bootstrap candidates by stored APS environment before retaining seen keys or querying accounting, refining ADR 0022. |
| [0038](0038-catalog-audit-event-payloads-and-identifiers.md) | accepted | Catalog the release-coupled audit event payloads and distinguish internal event identity from producer idempotency. |
| [0039](0039-retire-on-demand-job-updates.md) | accepted | Retire on-demand job refresh requests and their tracking while retaining stale-job detection. |
| [0040](0040-retire-job-anomaly-relay.md) | accepted | Retire the JM-to-ARM anomaly relay while preserving anomaly persistence, Advice, and direct ADI requests. |
| [0041](0041-retire-arm-attribution-result-queue.md) | accepted | Remove ADI’s obsolete ARM result queue declaration; Job Manager owns the normal attribution result queue. |
| [0042](0042-align-sac-and-watchdog-job-action-policy.md) | accepted | Share job-mode precedence and temporary username action selection between SAC and watchdog while preserving monitoring and independent safety gates. |
| [0043](0043-bound-failed-job-requeue-retries.md) | accepted | Bound direct failed-job requeue retries and defer durable give-up status until Advice-driven recovery. |
| [0044](0044-budget-failed-job-recovery-phase.md) | accepted | Bound the failed-job phase, preserve retry allowances across ticks, and expose scheduler command timeout counts. |
| [0045](0045-require-advice-before-slurm-requeue.md) | accepted | Require restart Advice before recovery, with an eager-recovery compatibility path for rollout and rollback; partially supersedes ADRs 0013 and 0022. |
| [0046](0046-preserve-successful-slurm-accounting-results.md) | accepted | Preserve and publish successful targeted accounting results after partial failures while retaining the complete-snapshot requirement for recovery and Advice actions. |
| [0047](0047-bound-sac-bootstrap-discovery.md) | accepted | Bound SAC bootstrap to non-final attempts, unresolved Advice, and recent failures for eager recovery. |
| [0048](0048-start-recovery-traces-in-backend-and-share-observations.md) | accepted | Start recovery sequences only in the backend and share operation facts across tracing, audit, and milestone reporting; partially supersedes ADR 0013. |
| [0049](0049-enable-attribution-by-default.md) | accepted | Include Attribution by default, retain explicit degraded operation, and require public compatible inputs and platform qualification; partially supersedes ADR 0034. |
| [0050](0050-expose-authenticated-whole-job-audit-reads.md) | accepted | Add authenticated whole-job audit reads with bounded responses and additive identity backfill; partially supersedes ADR 0031. |
| [0051](0051-retire-embedded-anomaly-attributions.md) | accepted | Retire embedded anomaly attribution queries, public fields, configuration, and dashboard attribution-count badges. |
| [0052](0052-run-e2e-tests-as-isolated-staged-scenarios.md) | accepted | Run each E2E test as a staged scenario that launches its own jobs through a scheduler-neutral launcher and verifies evidence over HTTP, isolated from other tests; narrows ADR 0005's service-proxy transport allowance. |
| [0053](0053-fanout-audit-and-recovery-trace.md) | superseded | Fan out overlapping recovery activity to trace spans and audit events without making recovery-sequence data the audit source of truth; superseded by ADR 0048. |
| [0054](0054-persist-application-requested-job-termination.md) | accepted | Persist application-requested cancellation by scheduler job ID and translate it into sticky exact-attempt terminal Advice. |

ADR numbers 0018, 0019, 0023, and 0024 are retired; their external-service
decisions were removed with maintainer approval. Do not reuse these numbers.

## Relationship To Other Sources

- Public architecture and operations documentation define current product
  intent and supported behavior.
- Accepted ADRs define durable implementation architecture within those
  requirements.
- Current architecture and operations documentation describe what is actually
  implemented and deployed.
- Code, tests, schemas, and generated contracts define executable behavior.

If these sources disagree, investigate the drift. An ADR must not silently
override a public contract, and implementation must not silently violate an
accepted ADR.

## When To Write One

Write an ADR for a choice that is durable, cross-cutting, expensive to reverse,
or establishes one of these boundaries:

- public API, scheduler-neutral, identity, or persistence contracts
- security, authorization, trust, or secret-handling policy
- component ownership, dependency direction, or provider boundaries
- operational failure and recovery semantics
- major runtime, framework, storage, or infrastructure dependencies

Routine implementation choices, localized refactors, and reversible experiments
do not need ADRs. Record temporary execution choices and experimental evidence
in the change description.

## Lifecycle And Human Authority

- `proposed`: under discussion and not authoritative
- `accepted`: explicitly selected and an active architectural constraint
- `superseded`: replaced by a later ADR
- `rejected`: considered and deliberately not adopted

Humans own NVIDIA Recovery product intent and architecture decisions. Agents may
research alternatives and draft a `proposed` ADR. Mark a record `accepted` only
when the instructing human explicitly selects the decision or asks to record a
decision they have already selected.

Status records the decision, not implementation completeness. Update the ADR
and index together when approval is given; use approval already established in
the task or linked review rather than asking again. Code existence alone is
not approval. If an older proposed record has an implementation but its
acceptance is unclear, note that uncertainty rather than inventing an approval
date or silently accepting every clause.

Initially report opportunities for editorial fixes, link repairs, and corrections
to inaccurate or misleading explanations as suggested edits. Apply them only
when the user explicitly requests or authorizes that kind of editing and the
chosen decision is preserved; an audit or upkeep request alone does not authorize
rewriting the record.
If a misconception undermines the rationale or calls the decision into question,
flag it in the brief note below rather than rewriting the historical reasoning.
A deliberate change to the decision needs a successor ADR, links in both
directions, and an index update.

## Keeping Records Useful

When implementing or changing behavior governed by an ADR, check the affected
record before finishing. Keep its status, index entry, and successor links
consistent. Review only relevant records unless a broader audit is requested.
The `adr-maintenance` repository skill provides the authoring, implementation,
and upkeep workflow; this guide is the policy source.

For a material implementation difference or incomplete requirement, add one
short blockquote after the header metadata, before the original context:

```markdown
> **Implementation note (YYYY-MM-DD):** Describe the specific difference or
> gap and whether it is unresolved or covered by a successor. Evidence:
> [PR](URL), or [source](relative/path) at `commit-sha`.
```

This is a current notice, not a running log or a second status system. Keep it
to one sentence where possible, two sentences maximum including references.
Do not squeeze a report into long compound sentences; link larger investigations
or follow-up work instead. A PR reference or source path plus commit SHA is sufficient.
Update or remove the notice when the gap is resolved; version control preserves
its history. Notes describe observations and do not authorize deviations.

- Flag editorial and factual errors first. With explicit authorization for that
  editing, correct them in place when the decision still means the same thing;
  a routine correction does not need an implementation note.
- Record a useful implementation nuance in the note; do not document every
  incidental code detail.
- For an unimplemented requirement or defect, identify the gap and link a fix
  or follow-up when available. Do not silently remove the requirement.
- For a deliberate architectural change, record the approved successor and
  link both ways. For partial supersession, name the affected rule and keep the
  original accepted status while the rest remains active. Use `superseded`
  when the decision as a whole is replaced.
- Preserve historical records and identifiers. Keep current implementation and
  operational detail in architecture/operations docs, linked from the ADR.

Keep new ADRs focused on the durable choice, rationale, and consequences.
Link detailed designs, rollout plans, and test inventories instead of copying
them into the decision. Add a brief confirmation pointer to a relevant review
or behavioral test when useful; a separate evidence ledger is not required.

This practice draws on [Nygard's decision history guidance](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
and [MADR's confirmation guidance](https://adr.github.io/madr/#full-template),
with proportionate corrections and linked discussions informed by
[Oxide's RFD process](https://rfd.shared.oxide.computer/rfd/0001).

## Format

Use the next four-digit sequence number and a short kebab-case name:

```text
docs/decisions/NNNN-short-decision-name.md
```

Every ADR must be added to the index above and contain:

```markdown
# ADR NNNN: Decision Title

Status: proposed | accepted | superseded | rejected

Date: YYYY-MM-DD

## Context

## Decision

## Consequences

## Alternatives Rejected
```

When available, add `Discussion: [PR](URL)` after the date to retain the original
review context. This is optional; do not invent a link or create a separate PR
solely to populate it. Link substantial follow-up discussion rather than copying
it into the ADR. During upkeep, read linked discussions and review outcomes for
approval and decision changes; report inaccessible links as an evidence gap.
