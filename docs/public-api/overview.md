# Public API overview

The product overview now lives in the [repository README](../../README.md),
which also supplies the Fern overview page. Use the
[documentation hub](../README.md) to find guides for users, operators, and
developers.

## Public interfaces

- [API Gateway HTTP and gRPC](api-gateway.md): workload visibility, ingestion,
  workflow lookup, advice, generation-scoped reconciliation status, and producer
  audit ingestion.
- [Attribution Adapter HTTP](attribution-adapter.md): a public request/response
  contract for an optional provider. Importing its schema and calling health
  do not qualify the private provider.

Job Manager's generated client specification is an internal Gateway input;
it is not a third public API. Components do not execute Slurm commands through
the Gateway API itself. Scheduler actions belong to the controller and watchdog.

Match client definitions, service versions, and Helm values to the same reviewed
source revision. See the [architecture](../../ARCHITECTURE.md) for component
boundaries and [optional Attribution](optional-attribution.md) for provider
requirements.
