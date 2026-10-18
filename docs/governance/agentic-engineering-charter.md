# Agentic Engineering Charter

## Purpose

This charter defines how the NVIDIA Recovery fork is developed with an
AI-agent-first workflow. The goal is to make the repository safe and legible for
agentic work while preserving human ownership of product intent and architecture
decisions.

## Human-Owned Project Inputs

Public project documentation and accepted architecture decisions define
product intent, requirements, scope, and architectural direction.

Bootstrap inputs:

- `README.md`
- `SECURITY.md`
- `docs/architecture/`
- `docs/decisions/`
- `docs/governance/repository-workflow.md`

## Agent-Executed Repository Changes

After the agentic transition commit is declared, repository artifact changes
must be produced by agents. Humans may:

- write prompts and acceptance criteria
- review diffs and test evidence
- approve, reject, or request changes
- merge reviewed branches
- define priorities and architecture decisions

Humans should not directly edit repository files after the transition commit.
If an emergency exception occurs, record it in this file with owner, reason,
scope, and cleanup action, then link that record in the pull request.

## Commit And Attribution Policy

Commits must use the instructing human's configured author identity. Do not
hard-code one contributor as the repository-wide author default. If the
instructing human's author identity is unclear, ask before committing.

Do not add AI coauthor footers, generated-by footers, or any commit metadata
indicating that an agentic tool authored or coauthored the change. Process
provenance belongs in pull request descriptions, not in
commit attribution metadata.

## Required PR Evidence

Every pull request should include:

- what changed and why
- focused verification commands and results
- relevant risk, security, and private-surface impact
- known gaps and any required independent or human review

## GitHub Delivery

GitHub is the sole source of truth for development, pull requests, reviews and
CI. Use `gh` and follow [repository workflow](repository-workflow.md).
Keep unrelated changes isolated in worktrees. All commits require the human
contributor's DCO sign-off; retain configured cryptographic signing.

External contributors may submit pull requests. CODEOWNER maintainers authorize
untrusted CI with `/ok to test <full-current-head-sha>` through NVIDIA copy-pr-bot.
Its copied branches are retained independently of the GitLab mirror, whose retirement is pending administrator action.
See [contribution policy](../../CONTRIBUTING.md) for the review boundary.

GitLab configuration is removed. Remote mirror and schedule retirement remains
an administrator operation described in the readiness plan. Public qualification
and private Attribution qualification have separate credential boundaries.

## Review Rules

Security-sensitive changes require explicit human review. This includes:

- authentication and authorization
- scheduler actions
- secret loading
- database migrations
- release automation
- private-surface scrub exceptions
- behavior that can kill, release, stop, requeue, or mutate a workload

## Transition Record

Agentic transition commit:
`7ac5e8ba87ecf9683a3c8f78507b50a19df21a56`

Declared by: Rick Izzo

Date: 2026-06-02

Scope: repository artifact changes after this harness bootstrap PR. Human
authorship remains at the prompt, review, prioritization, approval, and merge
layers. Emergency manual edits require an exception record below.

Exceptions:

None.

Exception template:

```text
Owner:
Reason:
Scope:
Cleanup action:
Linked PR or commit:
```
