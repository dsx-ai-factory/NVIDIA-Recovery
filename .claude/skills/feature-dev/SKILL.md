---
name: feature-dev
description: Use when developing a feature or substantial behavior change that requires codebase discovery, architecture decisions, implementation, and review.
---

# Feature Development

## Goal

Deliver the requested behavior through the repository's production interfaces
with the smallest maintainable change and evidence that the outcome works.

## Success Criteria

- The implementation satisfies the user-visible outcome and preserves relevant
  existing behavior.
- Material design choices are grounded in repository evidence or resolved with
  the user.
- Relevant accepted ADRs are preserved, and explicitly human-approved durable
  decisions are recorded without imposing ADR ceremony on routine work.
- Focused tests or harness checks cover changed behavior.
- The final diff is scoped, reviewable, and consistent with local conventions.

## Decision Rules

1. Inspect the relevant entry points, interfaces, configuration, and tests before
   choosing an implementation.
2. For durable, cross-cutting, or expensive-to-reverse choices, read
   `docs/decisions/README.md` and the relevant accepted ADRs before selecting an
   architecture. Record an explicitly human-approved choice in a new or
   superseding ADR; otherwise leave it proposed or surface the unresolved choice.
   Do not require an ADR for a routine implementation detail, localized
   refactor, or reversible experiment.
3. Ask only when missing information would materially change the result and
   cannot be discovered from the repository.
4. Prefer existing patterns and the smallest design that keeps ownership and
   validation clear.
5. Parallelize independent discovery or implementation areas when useful;
   synthesize their results before editing shared surfaces.
6. Review the completed diff for correctness, simplicity, and validation gaps.

Do not add hidden test-only ingress, unrelated refactors, or process artifacts
that do not improve the requested outcome. Follow the repository's action and
review boundaries.
