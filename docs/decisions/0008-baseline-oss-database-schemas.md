# ADR 0008: Baseline the OSS Database Schemas

Status: accepted

Date: 2026-08-07

## Context

NVIDIA Recovery inherited two Alembic revision histories from the upstream project. Shared
storage owns eight SQLAlchemy tables and records its revision in
`alembic_version`. Notifications owns two SQLModel tables in the same physical
MySQL database and records its revision in `alembic_versions__notifications`.

The inherited histories contain migrations for removed services and
intermediate schemas that are not part of the OSS release. The OSS release has
not established an upgrade contract from those unpublished revision IDs. It
needs a small, reviewable persistence baseline whose DDL matches the retained
ORM models.

## Decision

Keep the two existing schema owners and give each one Alembic baseline revision
with no parent:

- shared storage owns `advice`, `dag_dependency`, `global_events`,
  `job_events`, `job_execution_errors`, `job_execution_records`,
  `key_value_cache`, and `node_events`;
- Notifications owns `Notifications` and `UserPreferences`.

The retained SQLAlchemy and SQLModel metadata are authoritative. Each baseline
contains explicit Alembic DDL generated from and reviewed against that metadata.
Alembic is the only production schema-creation path. A baseline downgrade drops
only its owner's tables in dependency-safe order.

Compatibility begins with these baselines. Databases stamped with an inherited
revision, or unversioned databases that already contain managed tables, are not
upgraded or restamped. Startup fails with an actionable requirement to provide
a fresh empty database. Future schema changes add normal revisions on top of
the appropriate baseline.

## Consequences

- Fresh installations have one base and one head for each schema owner.
- Shared storage and Notifications can continue to share one MySQL database
  without either owner treating the other's tables as migration targets.
- The legacy schema-upgrade bypass and model-driven production `create_all()`
  path are removed.
- Operators must provision a new empty database when moving from any
  pre-baseline build. Data conversion and in-place upgrade tooling are outside
  the OSS compatibility contract.
- Migration DDL and the fresh-install compatibility break require human review.

## Alternatives Rejected

- One cross-service migration owner was rejected because it would invert or
  entangle the existing package ownership boundary.
- Removing Alembic in favor of runtime `create_all()` was rejected because it
  would eliminate version checks and a controlled path for future changes.
- Preserving or bridging inherited revisions was rejected because the requested
  cleanup explicitly establishes a fresh OSS baseline without retaining the old
  upgrade graph.
