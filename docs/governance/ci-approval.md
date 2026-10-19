# CI approval and maintainer roster

NVIDIA copy-pr-bot is the CI entry point. The repository config is
[`.github/copy-pr-bot.yaml`](../../.github/copy-pr-bot.yaml); CODEOWNERS names
`@dsx-ai-factory/nvidia-recovery-developers`. GitHub organization membership or unrelated
write access does not grant approval authority.

The maintained implementation in `nv-gha-runners/github-apps`, inspected at
`28c02d3f98140e5bf347efe38b79f5864186404b`, accepts individual account logins in
`trustees_override` and `vetters_override`. The lists must match the CODEOWNER
team. It checks PR authors and signed committers against trustees; untrusted
heads require a listed vetter and a matching SHA. Repository policy requires
the **full current head SHA**, although that bot version accepts SHA prefixes.

## Maintaining access

An authenticated team maintainer can compare the account lists with:

```bash
gh api orgs/dsx-ai-factory/teams/nvidia-recovery-developers/members --paginate --jq '.[].login'
```

Update both lists together whenever membership changes. Do not empty either
list: the bot falls back to broader organization/permission trust when an
override is empty. Its deployed enterprise-membership filter can also empty a
list; confirm the effective nonempty roster with the bot owner before opening
external CI. The inspected implementation also returns an empty enterprise roster on a fetch
or parse failure, which can empty both overrides and enable fallback trust.
**Public-launch blocker:** the bot owner must confirm deployed fail-closed
handling, including roster-read failures; a successful one-time roster check is
insufficient. Repository configuration cannot repair this upstream behavior.
No automatic roster synchronization or replacement bot is introduced.

## Administrator setup and acceptance

Apply this checklist to `dsx-ai-factory/NVIDIA-Recovery`. The
[2026-09-09 ruleset runbook](../readiness/2026-09-09-github-ruleset-commands.md)
is historical evidence for `NVIDIA/NVIDIA-Recovery`; do not reuse its repository,
team or ruleset IDs. Before any settings write, inspect and save the destination's
current settings, resolve its team and installed app IDs, review the intended
scope and rollback, and obtain administrator approval.

Read-only inspection on 2026-10-14 found a private destination repository with
only `org-admins` assigned as a team, no environments, and inherited rules named
`Protect default branches`, `Restrict transfers`, and `Restrict visibility`.
The repository runners API returned no runners; this does not establish whether
the shared runner group is available. The bot installation endpoint was
unauthorized, so installation remains unverified. These observations do not
establish that the controls below are configured.

- Grant the visible `@dsx-ai-factory/nvidia-recovery-developers` team explicit
  repository write access and confirm GitHub recognizes it in CODEOWNERS.
  Individual members' access through `org-admins` is insufficient for team
  ownership. See [GitHub's CODEOWNERS requirements](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners).
- Confirm NVIDIA copy-pr-bot is installed for the destination and reads its
  configuration. Both override lists must match the destination team and remain
  nonempty after enterprise filtering. Obtain the bot owner's confirmation of
  fail-closed behavior on roster-read and parse failures; the checked-in roster
  alone does not establish the effective trust boundary.
- Authorize the destination to use the existing `nv-cpu-general` runner group,
  including any organization and workflow restrictions. Confirm runner isolation
  for approved copied-PR jobs before enabling them. Keep the existing service;
  runner group access is an administrator setting, not a workflow label change.
- Inspect effective main protection, including inherited rules. Require reviewed
  pull requests, CODEOWNER approval, stale-review dismissal, approval of the latest
  push, resolved conversations, and current-head checks. Resolve required check
  names and their source apps from actual destination runs of
  `pull-request-and-main.yml`, `docs.yml` and `release-check.yml`; retain the
  language/package checks, `SPDX Source Headers`, `Helm Lint`,
  `Fern documentation check and render`, and `Release Packaging`, bound to GitHub
  Actions. Verify the installed DCO and CodeRabbit apps before requiring their
  checks. Do not copy old app bindings or select missing check
  names. GitHub supports [binding required checks to their source apps](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets#require-status-checks-to-pass-before-merging).
- Protect `pull-request/*` so only the installed copy-pr-bot app can create,
  update, force-update or delete those branches, without granting it main bypass.
  Restrict `v*` tag creation to authorized maintainers and prevent updates and
  deletion for every actor with a separate no-bypass rule. Resolve all bypass
  actors from destination evidence. Do not create a tag as a settings test:
  `tag-release.yml` triggers publication.

Configure the following environments before their workflows can publish or use
private credentials. Require maintainer review, prevent self-review and verify
the destination's GitHub plan supports these protections while it is private.
An environment name in YAML can create an unprotected environment; it does not
configure reviewers or ref restrictions. See [GitHub environment configuration](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments).

| Environment | Workflow and permitted refs |
| --- | --- |
| `docs-publish` | `docs.yml` (`Documentation`): protected `main` only. |
| `helm-release` | `tag-release.yml` (`Tagged Release`): protected `v*` release tags only. |
| `attribution-qualification` | `e2e-attribution.yml` (`Attribution qualification`): `main` only. |
| Preview environment, if configured | `fern-docs-preview-comment.yml` (`Preview Fern Docs: Comment`): restrict the trusted workflow's default-branch execution and require it to reference the environment. The current workflow declares none. |

Restore access to existing secrets and variables only for their intended jobs:

| Setting | Required scope or use |
| --- | --- |
| Secret `DOCS_FERN_TOKEN` | Existing `nvidia` Fern organization token for docs publication and the trusted preview workflow; review repository/organization exposure because the current preview job has no environment. |
| Variables `DOCS_PUBLISH_ENABLED`, `DOCS_NVIDIA_URL` | Leave publication disabled until hosting is approved. Set `DOCS_NVIDIA_URL` to `https://docs.nvidia.com/recovery` for `docs.yml`; the Fern organization, theme and host remain unchanged. |
| Secret `ATTRIBUTION_SOURCE_DEPLOY_KEY` | Only the protected `attribution-qualification` environment; never copied-PR jobs. |
| Secrets `BITNAMI_OCI_REGISTRY_MIRROR`, `DOCKER_HUB_REGISTRY_MIRROR` | Optional existing public CI mirrors, as described below. |
| Secret `SLACK_WEBHOOK_URL` | Existing E2E notifications in `e2e-main.yml`, `nightly.yml` and `tag-release.yml`. |

Confirm organization package policy and repository Actions access permit the
existing `GITHUB_TOKEN` with `packages: write` to publish
`ghcr.io/dsx-ai-factory/nvidia-recovery/charts/warthog` from `nightly.yml` and
`tag-release.yml`. Existing packages may need the destination repository granted
write access under [Manage Actions access](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility).
Verify package visibility separately before claiming anonymous availability;
repository or package access alone does not establish public distribution.

Before public launch, use an agreed disposable PR to verify: an outside-team
NVIDIA author and an external author cannot auto-run CI; an unauthorized
commenter, absent SHA or stale SHA cannot authorize it; a maintainer's
`/ok to test <full-current-head-sha>` runs that head; a later push needs fresh
approval. No such live bot test has been performed by this implementation.

Public E2E receives no private provider secrets. Optional Attribution E2E is
manual, main-only and uses `attribution-qualification`. Configure that environment
with maintainer reviewers, prevent self-review and allow only main before use.
Provide the private source key there. Ensure repository/organization-wide keys
are not exposed to copied-branch jobs; this environment restriction requires
administrator verification. Credential cleanup and rotation are deferred.
Runner isolation and environment controls cannot be established by YAML alone.

## Optional CI registry mirrors

Approved public CI may use the repository's existing `BITNAMI_OCI_REGISTRY_MIRROR`
and `DOCKER_HUB_REGISTRY_MIRROR` settings, as main does, to avoid shared-runner
Docker Hub limits. The reusable workflow accepts only those two optional mirror
inputs; it does not receive the private Attribution source key. Unset mirrors
retain upstream access. Public operator builds and installations do not require
these CI settings. Chart lint and source/chart packaging use hosted Ubuntu with
upstream registries.
