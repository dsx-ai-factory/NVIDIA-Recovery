# Documentation Gardening

Documentation gardening is the recurring process for keeping repository-local
knowledge aligned with code, plans, and migration state. It turns review
feedback and stale-doc findings into small, verifiable MRs instead of letting
guidance drift.

## When To Run It

Run a doc-gardening pass when:

- a reviewer finds stale or contradictory guidance
- code, scripts, or migration checkpoints move ahead of the docs
- private-surface disposition changes
- an MR leaves known documentation gaps for follow-up

## Review, Repair, Validate

Use this loop:

1. Review: inspect the affected docs and source files without editing.
2. Repair: make the smallest documentation change that restores accuracy.
3. Validate: run the relevant docs or harness checks and record the evidence.

## What To Record

Doc-gardening MRs should record:

- the stale or missing guidance that triggered the pass
- changed docs and why those docs are the right source of truth
- validation commands and results
- any follow-up gaps that remain

If the fix changes durable agent behavior, update the closest appropriate
guidance surface. Prefer a focused doc or skill over expanding root
`AGENTS.md` unless the rule must apply to every task in the repository.
