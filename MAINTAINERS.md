# NVIDIA Recovery Maintainers

NVIDIA Recovery is maintained by the
[`@dsx-ai-factory/nvidia-recovery-developers`](https://github.com/orgs/dsx-ai-factory/teams/nvidia-recovery-developers)
GitHub team.

## Responsibilities

The maintainer team owns issue triage, pull request review, technical direction,
repository access, security coordination, release readiness, and enforcement of
the project's governance and Code of Conduct.

Anyone may open a pull request. Only CODEOWNER maintainers may approve untrusted
CI with `/ok to test <full-current-head-sha>`. See
[bot configuration maintenance](docs/governance/ci-approval.md).

Update the bot's explicit account lists in the same change as maintainer access
changes; team membership alone does not update these lists. Nomination, removal
and final merge/release permissions remain NVIDIA-managed.

## Escalation

- Usage questions, bug reports, feature requests, and design proposals:
  [GitHub issues](https://github.com/dsx-ai-factory/NVIDIA-Recovery/issues)
- Suspected security vulnerabilities: follow [SECURITY.md](SECURITY.md)
- Code of Conduct reports: email `GitHub_Conduct@nvidia.com`

Community channels must not be used for credentials, private
hostnames, customer data, suspected vulnerabilities, or other sensitive
information.

Maintainer membership and repository permissions are managed by NVIDIA. This
file intentionally identifies the accountable team rather than publishing a
potentially stale roster of individuals.

## Membership review

The team reviews inactive maintainers and access changes with the affected
person. When access ends, remove the account from both bot approval lists in
the same change; former membership does not retain CI approval authority.
