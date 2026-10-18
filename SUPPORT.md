# NVIDIA Recovery Community Support

We welcome questions, bug reports, ideas, and contributions. NVIDIA Recovery is maintained by NVIDIA with help from the community, and support is provided on a best-effort basis.

> **Note:** The project is currently experimental and is not yet ready for production use. APIs, configuration, and behavior may change as development continues.

## Where to Get Help

| Channel | Use it for |
| --- | --- |
| [GitHub Issues](../../issues) | Usage questions, reproducible bugs, feature requests, and documentation feedback |
| [GitHub Pull Requests](../../pulls) | Code and documentation contributions |
| [Security reporting](SECURITY.md) | Privately reporting a suspected vulnerability |

Search existing issues before opening a new one, and use the available issue templates to help us understand your request.

Please keep project support requests in the repository so others can learn from the discussion. Direct messages to individual engineers are not a reliable support channel.

## What to Expect

We aim to:

- Review new issues within a week.
- Give pull requests an initial response within about four days of being marked ready for review.
- Explain when we need more information, identify a duplicate, or decide that a request falls outside the project's scope.

These are goals, not guaranteed response times or service-level agreements (SLAs). An initial response may be a question or an assessment of next steps; it does not necessarily mean a full review or completed fix.

We use the same response goals for NVIDIA and community contributors. Timing depends on maintainer availability, the information provided, and the complexity of the request.

If your issue or pull request has not received a response after the target window, a friendly follow-up in the same thread is welcome.

## Reporting a Bug

A small, reproducible example is the quickest way to help us investigate. Please include:

- The NVIDIA Recovery version or commit.
- Your deployment method and relevant scheduler, platform, and dependency versions.
- Steps to reproduce the problem.
- What you expected and what happened instead.
- Relevant logs and configuration, with sensitive information removed.
- The impact on your workloads and any workaround you have found.

For unexpected recovery behavior, include the relevant job states and a timeline of detection and recovery actions where possible.

> **Never post** credentials, access tokens, private hostnames, customer data, or other confidential information.

## How We Prioritize Work

We consider user impact, reproducibility, available workarounds, and whether the problem affects a documented configuration. Problems involving unsafe recovery actions or failures that prevent core functionality receive particular attention.

A priority label helps organize work; it is not a promise of a fix by a particular date. Resolution may involve a code change, a workaround, documentation, or an explanation of a limitation.

Feature requests help us understand community needs. Acceptance for discussion does not guarantee implementation or a release date.

## Contributing

Code and documentation contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md), keep each pull request focused, and discuss substantial changes with maintainers before starting.

Mark your pull request ready when it is ready for review. Maintainer approval is required before CI runs for contributors outside the maintainer team.

All contributions require review and the applicable checks. A passing build does not guarantee that a change will be accepted.

## Support Lifecycle

NVIDIA Attribution components use [Semantic Versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`):

- **Major** versions may introduce breaking changes to a component's supported interfaces.
- **Minor** versions add backward-compatible functionality to a supported interface.
- **Patch** versions provide backward-compatible fixes.

## Supported Versions and Environments

Start with the [README.md](README.md), installation documentation, and release notes for the configurations and limitations relevant to your version.

Maintainers focus on reproducible problems in current upstream code and documented configurations. We may ask you to check a newer release or the current default branch.

Help with private forks, custom integrations, older versions, and environment-specific operations may be limited. When possible, reproduce the problem without local modifications.

Release notes are the reference for shipped features, compatibility changes, and upgrade guidance. Roadmap items and milestone dates may change.

## Keeping Discussions Useful

We may close duplicate, out-of-scope, or inactive issues and pull requests. If a problem still occurs, add current reproduction details and ask for it to be reconsidered.

## Security and Production Incidents

Report suspected vulnerabilities privately by following [SECURITY.md](SECURITY.md). Do not disclose vulnerability details in public issues or pull requests.

Community channels do not provide around-the-clock incident response or guaranteed resolution times. If you have an applicable commercial support agreement, use the support route specified in that agreement; its terms determine coverage and response commitments.
