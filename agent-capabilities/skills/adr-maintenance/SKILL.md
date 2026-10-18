---
name: adr-maintenance
description: Author, reconcile, or audit repository ADRs when drafting decisions, implementing or reviewing recorded decisions, recording human approval, or investigating implementation drift.
---

# ADR Maintenance

Read `docs/decisions/README.md` for the index, lifecycle, and compact
implementation-note policy. Read the relevant ADRs and current architecture
documentation; keep work scoped to the requested outcome. This README is the
single canonical ADR index, not a place for audit findings, branch inventories,
or one-off exceptions.

## Authoring

Capture the durable choice and rationale using the guide's format. Link detailed
designs rather than embedding execution plans. Check existing decisions for
overlap and identify any full or partial supersession. Use `proposed` unless
the human has already selected the decision.

## Implementation And Approval

Before finishing work governed by an ADR, compare the resulting behavior with
its architectural constraints. Update status and index together when approval
is already established; implementation completeness is a separate concern.
Use the guide's top-of-file note for material gaps or nuances: prefer one
sentence, never more than two including references; link detail elsewhere. An observed
deviation is not approval to change the decision or expand implementation scope.
Link an approved successor in both directions when the decision changes.

## Upkeep

For an audit, compare the requested records with code, behavioral tests, and
successor ADRs. When discussion or review links exist, open them and read the
relevant discussion and review outcome for approval, clarifications, or later
decisions; checking that the URL resolves is not enough. Report inaccessible or
broken links and any resulting uncertainty rather than inferring their contents.
Distinguish status/index errors, implementation refinements,
missing requirements, and changed decisions. Report evidence and recommended
disposition; edit records only when the task authorizes maintenance. Do not
infer historical approval from code, delete decisions merely because they are
old, or turn a scoped implementation task into a full ADR audit.

### Propose Concrete Edits

An audit must produce a reviewable edit list, not just open-ended findings.
For each proposed edit, give the target file and location, lifecycle category,
exact proposed wording or before/after status, evidence, and any unresolved
human decision. Use these categories:

- **Status/index reconciliation:** update the record and canonical index
  together, citing established acceptance or supersession evidence.
- **Implementation note:** give the exact brief addendum for a material gap or
  refinement; keep implementation work separate from the documentation edit.
- **Successor/reference maintenance:** identify the exact replaced rule and
  both links, or specify which dangling reference will be corrected or removed.
- **Decision change:** identify the choice requiring human selection and the
  proposed successor; do not present it as routine maintenance.

Distinguish edits ready within existing authorization from unresolved choices
and findings that require no edit. If an existing note already captures a gap,
say so instead of adding another. After authorized maintenance, report the
concrete edits made and remaining decisions separately. Do not ask again for
approval already supplied in the conversation.

### Preserve The Decision Record

Preserve decision bodies and historical rationale. Missing template sections,
stylistic inconsistency, and other formal defects are not reasons to retrofit
old ADRs. Do not add empty sections or prose saying a section was undocumented,
invent rejected alternatives, or rewrite an ADR to match current code.

If a body correction is materially useful, present it in a separate, clearly
labeled **Proposed ADR body edit — explicit authorization required** callout.
Show the exact before/after text, the reader-facing problem, and why the
decision remains unchanged. Do not bury it among implementation notes or index
updates. Apply it only when that kind of editing is explicitly authorized;
audit or general maintenance authorization alone is insufficient. A changed
decision requires the successor lifecycle instead.

When note edits are authorized, flag misconceptions that undermine the rationale
using the brief note rather than rewriting history; retain an available
discussion PR link.

### Keep One Canonical Index And Resolve References

For authoring, implementation, review, and maintenance, every ADR relied on by
the change must have a record and entry in `docs/decisions/README.md` in the
same deliverable. Check references from affected ADRs, code, and documentation;
do not leave dangling ADRs or unindexed records.

A proposal on another branch is not part of the canonical collection. Do not
reserve its number, add a branch-history exception to the README, or create a
parallel index or evidence ledger. If the change needs that decision, include
the record and index entry through the normal review process, resolving number
collisions and updating references together. Otherwise remove the dependency
or relink to the actual indexed decision that governs it; never substitute an
unrelated ADR merely to make a link resolve. If the intended decision is
unclear, report the exact reference and required resolution rather than
declaring the work ready.

Before completing authorized edits, check index/status agreement, reference
targets, and that notes preserve the original rationale. Review the README for
unrelated notes and exceptions. No custom validator, new tests, or new ADR is
needed solely to record an observation.
