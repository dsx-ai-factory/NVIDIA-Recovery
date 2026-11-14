# ADR 0055: Retire Job Status Event Consumer

Status: accepted

Date: 2026-11-08

## Context

The Advisor Controller publishes job status through API Gateway to Job Manager's
HTTP API. The inherited RabbitMQ job-update consumer has no production sender
and retains a separate persistence and recovery path.

## Decision

Remove the legacy job-status consumer, queue configuration, synthetic publishers,
bus-only metrics, and storage helpers used exclusively by that path. Job status
ingestion uses the HTTP API. Preserve Watcher anomaly/checkpoint ingestion and
ADI attribution messaging, together with the shared job storage model and API
upsert and partial-update operations.

## Consequences

Old configuration cannot reactivate job-status ingestion over RabbitMQ. No
database schema change is needed. Existing broker objects require operator
cleanup after confirming that they are unused; chart changes do not delete
them. See the [rollout notes](../architecture/slurm-advisor-controller.md#retired-job-status-event-consumer).

## Alternatives Rejected

- Disable the consumer with a flag: retains unsupported ingestion and storage
  behavior that configuration could reactivate.
- Keep the consumer for synthetic tests: exercises an ingress that production
  job updates do not use.
