# NVIDIA Recovery Governance

NVIDIA Recovery is an NVIDIA-led open source project accepting external code
and documentation contributions. Maintainers are accountable for technical
direction, security, release quality and compliance obligations. Community
members may open issues and pull requests; maintainers retain decision authority.

## Roles

### Community Participants

Anyone may participate in project discussions or submit issues. Community
participants must follow the [Code of Conduct](CODE_OF_CONDUCT.md). Contributing
does not grant merge, release or project decision rights.

### Maintainers

The `@dsx-ai-factory/nvidia-recovery-developers` team maintains NVIDIA Recovery. Maintainers:

- Triage issues and review pull requests
- Protect the supported product scope and public interfaces
- Apply security, license, quality, and release gates
- Record durable architectural decisions
- Prepare and approve releases
- Enforce the Code of Conduct

The current maintainer record and escalation paths are in
[MAINTAINERS.md](MAINTAINERS.md).

### NVIDIA Recovery Team Developers

Members of `@dsx-ai-factory/nvidia-recovery-developers` may author branches and open
pull requests. Team membership does not grant merge or release authority;
maintainer review and repository protections still apply.

## Decision Making

Maintainers consider written technical discussion and community feedback but
retain final decision authority. Community feedback is advisory. Maintainers
resolve conflicting requirements and decisions involving security, legal,
product, architecture, or release obligations.

Routine and reversible decisions may be resolved in an issue or pull request.
Durable, cross-cutting, or expensive-to-reverse choices require a maintainer-
approved architecture decision record under `docs/decisions/`. Accepted
decisions are superseded through a new record rather than silently rewritten.

## Review and Merge

Anyone may submit pull requests. All changes require review. Non-maintainer
CI requires a maintainer approval of the current SHA through NVIDIA copy-pr-bot;
see [CONTRIBUTING.md](CONTRIBUTING.md). A maintainer may request subject-matter, security,
legal, or human review based on risk. Changes involving
authentication, authorization, scheduler actions, secret loading, migrations,
release automation, or behavior that can kill, release, stop, or modify a
workload require human review.

Maintainers may close or decline work that is unsafe,
unsupported, out of scope, insufficiently tested, incompatible with the
project license, or costly to maintain. The reason should be recorded in the
issue or pull request.

## Releases

Maintainers own release scope, qualification, versioning, and publication.
Tagged releases require the configured CI gates and explicit maintainer
approval. A passing pipeline does not replace security, legal, license, or
release-readiness review.

Release notes are recorded in [CHANGELOG.md](CHANGELOG.md). Until the project
publishes a stable release, all interfaces and deployment guidance should be
treated as experimental unless explicitly documented otherwise.

## Becoming or Leaving a Maintainer

Maintainers are nominated by the NVIDIA Recovery team and approved by the
NVIDIA Recovery maintainer team.

## Changes to Governance

Governance changes require review and approval from the maintainer team.
Changes that materially alter project control, the contribution policy,
security handling, or release authority require the applicable NVIDIA legal,
security, or open source program review.
