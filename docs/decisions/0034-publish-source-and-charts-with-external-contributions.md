# ADR 0034: Publish source and charts with external contributions

Status: accepted

Date: 2026-09-09

The default-off Attribution rule below is superseded by
[ADR 0049](0049-enable-attribution-by-default.md). The remaining release and
contribution decisions continue to apply.

## Context

The instructing maintainer selected the public release and contribution scope
while reviewing DSX readiness. Inherited closed-contribution and GitLab mirror
policies conflict with that scope.

## Decision

GitHub is the sole development, review and CI authority. Retire GitLab
configuration and mirroring. Accept external code and documentation PRs.
CODEOWNER maintainers authorize non-maintainer CI through NVIDIA copy-pr-bot
with `/ok to test <full-current-head-sha>`; retain the bot's copied PR branches.

Publish source and Helm charts. Operators build images; the project does not
publish binary executables or container images. Advice Operator is excluded.
NVIDIA Attribution is optional and disabled by default: public builds and
deployments must work without its source, dependencies or credentials.

The public API consists of API Gateway HTTP/gRPC and Attribution Adapter.
Use Fern for maintained public documentation and docs.nvidia.com for hosting
once allocation and access are available.

## Consequences

Update contribution policy, CI, build/deployment defaults, documentation and
release packaging to these boundaries. Keep maintainer review and protected
release authority. Record missing deployment and qualification evidence
explicitly; these decisions do not claim a completed DSX score or public launch.

## Alternatives Rejected

Maintainer-only contributions, GitLab dual authority, a replacement approval
bot, mandatory private Attribution, Advice Operator distribution and binary
publication do not match the selected scope.
