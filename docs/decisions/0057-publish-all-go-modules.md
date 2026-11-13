# ADR 0057: Publish All Go Modules and Include Advice Operator in Releases

Status: accepted

Date: 2026-11-11

> **Implementation note (2026-11-11):** Advice Operator build/CI wiring and source archive inclusion are implemented in the [component workflow](../../advice-operator/README.md#development); Go module publication, published sibling dependencies, and external-consumer qualification remain pending.

## Context

[ADR 0034](0034-publish-source-and-charts-with-external-contributions.md)
excluded Advice Operator distribution. The operator and Advice API types now
live in this repository, and downstream integrations need consumable Go packages
from all repository Go modules. Existing root release tags do not version these
subdirectory modules for Go consumers.

## Decision

Every repository Go module, including future modules, is independently versioned,
tagged, and consumable without a repository checkout or consumer-supplied local
`replace` directives. Each `go.mod` defines a release boundary; packages within
that module share its version, and Go's `internal/` visibility rules still apply.

Use SemVer tags prefixed by the module's repository-relative directory:
`<module-directory>/vMAJOR.MINOR.PATCH`. The current modules are:

| Module directory | Example first release tag |
| --- | --- |
| `api-gateway` | `api-gateway/v0.1.0` |
| `advice-operator` | `advice-operator/v0.1.0` |
| `slurm-advisor-controller` | `slurm-advisor-controller/v0.1.0` |

Module versions are independent of one another and of the root `vMAJOR.MINOR.PATCH`
product/source/Helm release tags. Start new module release lines at `v0.1.0`
after checking published version history; use prerelease suffixes such as
`-rc.1` when needed. During `v0`, preserve compatibility in patch releases and
use minor releases for breaking changes. Adopt `v1.0.0` when committing to a
stable public Go API; subsequent breaking major releases require Go's matching
module-path suffix, such as `/v2`. Published tags are immutable.

Include Advice Operator source and its standalone Helm chart in release source
archives. This supersedes the Advice Operator distribution exclusion in
[ADR 0034](0034-publish-source-and-charts-with-external-contributions.md) and
extends its public API scope to exported Go packages in all repository modules.

## Consequences

Release packaging must retain the operator and chart, and archive verification
must require their presence. The standalone chart remains separate from the
umbrella chart.

Sibling dependencies must reference published module versions. Release API
Gateway before publishing controllers that require its generated gRPC packages;
replace their placeholder `v0.0.0` requirements with the released version.
Optional local workspaces may support coordinated development, but must not be
required by consumers.

Qualify each module from a separate consumer module with workspace mode disabled
and no local replacements, resolving published dependencies and compiling the
exported packages intended for consumers. Module tagging, dependency
reconciliation, and build/CI qualification must be completed before claiming a
qualified Go module release. Publishing a module does not expose its `internal/`
packages; reusable APIs must be deliberately exported.

Module publication requires protected, immutable tags and module-specific
qualification; the existing root-tag source/chart workflow does not qualify
module releases. Existing maintainer publication controls continue to apply.

## Alternatives Rejected

- Retain the distribution exclusion: prevents downstream consumers from using
  the operator and Advice API through the repository's release distribution.
- Publish only selected Go modules: leaves other modules without a supported
  consumer release path.
- Couple every module version to the product release: ties Go API compatibility
  commitments to unrelated application and chart changes.
