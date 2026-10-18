---
name: code-simplifier
description: Use when recently modified code should be made clearer, simpler, more maintainable, or more consistent without changing behavior.
---

# Code Simplifier

Refine recently modified code for clarity while preserving exact behavior.

## Rules

- Work on code touched in the current session unless the user gives a broader
  scope.
- Preserve public APIs, runtime behavior, data shapes, and error behavior.
- Prefer explicit readable code over clever one-liners or dense abstractions.
- Remove duplication, accidental complexity, unnecessary nesting, and comments
  that only restate obvious code.
- Keep useful abstractions that clarify ownership, testing, or future changes.
- Follow `AGENTS.md`, nested `AGENTS.md` files, and local service conventions.

## Verification

Run the smallest checks that prove behavior was preserved. If no targeted check
exists, report that gap, run file-scoped pre-commit when applicable, and rely on
scoped diff and code review.
