# ADR 0049: Enable NVIDIA Attribution by default

Status: accepted

Date: 2026-10-06

> **Implementation note (2026-10-12):** Default-on packaging uses the renamed
> upstream SDK and its V2 API, including `entry_timeseries` ingestion, as documented in the
> [source prerequisites](../public-api/optional-attribution.md); public provider/client
> publication, a clean public amd64 build, and deployed diagnosis qualification
> remain release gates.

## Context

The Virtual Developer Review evaluated NVIDIA Recovery with Attribution
disabled. Several reported diagnosis and node-exclusion gaps are capabilities
of the Attribution integration. The maintainer selected default-on deployment
with the concurrent public release of NVIDIA Attribution, while preserving an
explicit reduced-capability mode.

This decision partially supersedes the optional, default-off Attribution rule
in [ADR 0034](0034-publish-source-and-charts-with-external-contributions.md).
Its source-and-chart distribution and contribution rules remain unchanged.

## Decision

- Include and enable the Attribution service, Adapter, and ADI integration in
  supported default build, development, and installation paths. The provider
  Deployment and Service are owned by the Helm release.
- Explicitly disabling Attribution selects a documented degraded mode. It
  must not fabricate diagnoses or Attribution-derived node exclusions.
- An enabled provider with unavailable source, invalid configuration, or an
  unhealthy endpoint must report failure. It must not silently become an
  apparently successful disabled-provider configuration.
- Pin compatible provider and generated-client inputs. Default-on public
  release requires that these inputs and their build dependencies are publicly
  available and reproducible without private credentials.
- Qualify Linux/amd64 first. Do not claim default-on arm64 support before
  provider qualification on that platform.
- Retain protected qualification while upstream inputs require authorized
  access. Ordinary untrusted PR execution receives no provider credentials;
  an explicitly provider-off CI lane is not default-on release qualification.

## Consequences

Deployment, build defaults, operator documentation, and qualification must
describe the same provider graph. Release evidence distinguishes on, explicitly
off, and enabled-but-unavailable states. Provider health checks alone do not
establish diagnosis or node exclusion.

This changes default availability, not the detection policy: Attribution still
requires applicable anomaly and diagnostic evidence. Enabling it does not
promise continuous hardware monitoring or classification of every scheduler
terminal state. Missing public inputs or native-platform qualification remain
release blockers, rather than reasons to claim those requirements passed.

## Alternatives Rejected

- Keeping public installations default-off contradicts the selected deployment
  contract and reproduces the VDR's capability mismatch.
- Silently disabling an unavailable enabled provider hides an installation
  failure and confuses degraded operation with completed qualification.
- Exposing private credentials to ordinary PR execution violates the existing
  contribution trust boundary.
