# ADR 0041: Retire the ARM Attribution Result Queue

Status: accepted

Date: 2026-09-29

## Context

ADI still declares and binds `queue_bad_nodes_isolation` when publishing normal
attribution results. Job Manager consumes a separate `anomaly_attribution_queue`
bound to the same exchange and routing key. Every result therefore also enters
an unconsumed queue left over from the retired ARM path.

## Decision

Remove ADI's default result queue declaration and binding, and remove the
`isolator_completed_queue` setting from runtime and deployment configuration.
Job Manager owns the normal result queue and binding. ADI continues publishing
fast and slow attribution results to `isolator_completed_exchange` with the
existing `anomaly.isolation.bad_nodes` routing key and trace headers.

Preserve the explicit `output_queue` path: requests choosing a custom output
queue still declare and bind that queue with its name as the routing key.
This path does not create the default legacy queue.

## Consequences

ADI no longer creates an unconsumed duplicate queue during normal publication.
Result payloads, Job Manager attribution persistence, and Advice generation
remain unchanged. Shared legacy schema types still referenced by Advice code
are not broker declarations and remain outside this queue cleanup.

Job Manager must establish its durable queue and binding before ADI starts
publishing results on a fresh broker. A disconnected Job Manager consumer can
resume from its existing queue; no additional fallback queue is introduced.
This change does not add publisher confirms or an unroutable-message policy.

Existing legacy queues and bindings require operator retirement after updating
all ADI publishers; merely removing the declaration does not remove broker
objects. See the [rollout notes](../architecture/slurm-advisor-controller.md#retired-arm-attribution-result-queue).

## Alternatives Rejected

- Point ADI's default queue setting at JM's queue: avoids duplication only
  while two independently maintained queue names stay aligned and leaves an
  unnecessary producer-owned default queue setting.
- Remove attribution result publication: breaks the active ADI-to-JM result
  path and the recovery Advice derived from it.
