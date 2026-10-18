# ADR 0001: Use Repository-Local Architecture Decision Records

Status: accepted

Date: 2026-07-27

## Context

NVIDIA Recovery already has distinct artifacts for product requirements, software
requirements, current architecture, migration evidence, and long-running
execution plans. Execution plans contain decision logs, but those logs are
scoped to completing one body of work and do not form a durable, discoverable
architecture history. Without an ADR mechanism, new architecture decisions are
vulnerable to being lost in chats, plans, handoffs, and merge-request
discussion.

The repository agent harness also needs a concise rule for distinguishing a
durable architecture decision from a routine implementation choice. That rule
must preserve human ownership of product intent without preventing agents from
drafting and maintaining the resulting repository artifacts.

## Decision

NVIDIA Recovery will keep new architecture decisions under `docs/decisions/`.
`docs/decisions/README.md` is the canonical index, lifecycle policy, and format
guide.

ADRs are required for durable, cross-cutting, expensive-to-reverse choices and
for public contracts, persistence and identity, security and trust boundaries,
component ownership, operational failure semantics, and major infrastructure or
framework dependencies. Routine implementation details, localized refactors,
and reversible experiments do not require an ADR.

Humans retain authority to select architecture decisions. Agents may research
alternatives and draft proposed records, but a record becomes accepted only
after an instructing human explicitly selects the decision or directs the agent
to record a decision already selected.

Accepted ADRs are append-only in substance. A changed decision is recorded in a
new ADR that supersedes the earlier record and links both directions. Current
architecture and operations docs must still be updated as implementation lands;
an ADR records rationale rather than claiming that planned behavior is live.

The root repository guide and portable agent capability harness will route
architecture work through the ADR index. The initial mechanism remains
lightweight Markdown and review guidance. A custom ADR generator or verifier
will be added only if repeated drift demonstrates that mechanical enforcement is
worth its maintenance cost.

## Consequences

Durable design constraints become discoverable without expanding `AGENTS.md`
into a design manual or relying on task-specific plans. Architecture agents can
identify accepted constraints before proposing changes, and reviewers can
distinguish an implementation defect from a deliberate supersession.

Contributors must maintain the decision index, status, and supersession links.
Some changes now require a short architecture artifact in addition to code and
tests. The human-acceptance boundary also means an agent must leave an unresolved
choice proposed rather than silently choosing policy.

## Alternatives Rejected

### Keep durable decisions only in execution plans

Execution plans optimize for progress, recovery, and handoff of one task. Their
decision logs are useful evidence but are not an indexed architecture history,
and many durable choices do not require a long-running plan.

### Put all architecture rationale in `AGENTS.md`

The root guide should remain a concise map and set of invariants. Embedding full
decision context there would consume agent context on every task and make
supersession difficult to track.

### Add a custom ADR linter immediately

The reference repositories succeed with a lightweight Markdown process, and
NVIDIA Recovery has no observed ADR drift yet. A verifier would add validation code and
maintenance before there is evidence that review guidance is insufficient.
