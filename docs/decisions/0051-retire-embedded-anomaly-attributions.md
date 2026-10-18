# ADR 0051: Retire Embedded Anomaly Attributions

Status: accepted

Date: 2026-10-08

## Context

Job Manager populated attribution records inside each public `JobAnomaly` by
querying node and global attribution storage for every anomaly. This added
runtime query cost and delayed dashboard loading. The disabling flag was
accidentally enabled in a testing environment; the Helm default also enabled
it, and the anomaly-list endpoint bypassed the flag entirely.

## Decision

Remove embedded anomaly attribution enrichment and its
`populate_attribution_for_anomalies` setting. Remove `node_events` and
`global_events` from `JobAnomaly` and generated contracts. Job status, job list,
and anomaly list reads do not query attribution storage.

Dashboard alert badges retain anomaly counts and scheduler reasons, and remove
the attribution count and icon derived from embedded events. Attribution details
remain available through the dedicated attribution endpoints and detail view.
This does not change the Attribution integration selected in
[ADR 0049](0049-enable-attribution-by-default.md).

## Consequences

Old service configuration cannot reactivate enrichment. Remove the obsolete
setting from custom service configuration and Helm overrides. Clients must use
`/job_error_attributions` or `/error_attributions` for attribution details rather
than embedded anomaly fields. Deploy the updated dashboard before or alongside
the backend. Stored attribution data and its processing are unchanged; no
database migration is required.

## Alternatives Rejected

- Keep the flag disabled by default: configuration can accidentally restore the
  expensive queries.
- Add batched enrichment or replacement count queries: retains a legacy read
  dependency that is unnecessary for the supported detail view.
