# Job manager

This directory contains the NVIDIA Recovery Job Manager.

Job Manager's responsibilities:

- Receive job updates from the Slurm Advisor Controller (SAC) through API Gateway
  and persist them into the database.
- Receive other job events (e.g. anomaly, checkpoint, etc) from watcher from the event bus and persist them into the
  database.
- Expose HTTP endpoints for dependant services to expose job and workflow data.

## Anomaly attribution reads

Job status, job list, and anomaly list responses no longer embed `node_events`
or `global_events` in `JobAnomaly`. Attribution details remain available through
`/job_error_attributions` (filterable by job, attempt, and anomaly UUID) and
`/error_attributions`. The dashboard loads these details on demand; alert badges
show anomaly counts and scheduler reasons without attribution counts.

Remove `service.populate_attribution_for_anomalies` from custom Job Manager
configuration and `service_config.populate_attribution_for_anomalies` from Helm
overrides. The setting has been retired and cannot enable enrichment. Deploy the
updated dashboard before or alongside the backend. No data migration is needed.
See [ADR 0051](../docs/decisions/0051-retire-embedded-anomaly-attributions.md).

## Developer Guide

### Install dependencies

```
cd job_manager

uv sync
```

The installation will create a python virtual environment at the directory where
`uv sync` was run.

### Unit tests

Unit tests are in `tests/` directory. Run them with `uv run --locked pytest`.

### Local e2e tests

All file paths below are relative to the `job_manager` directory.

- Properly change the service configuration file located at `job_manager/job_manager_config.yaml`.

- Start the dependent services using docker compose, specifically rabbitmq, mysql and etcd. To start them all at once,
  run `docker compose up`, or they can be started individually with `docker compose <target-service-name> up`. \* Note that you need to manually create the database, user and when the mysql local deployment is set up for the
  first time from scratch since currently there's no database creation logic in the code. You can achieve it by
  either `docker exec` into the container or through some utility CLI tools like [mysqlsh](https://dev.mysql.com/downloads/shell/).

- Start the job manager: `uv run --locked python3 job_manager/main.py`.

Verifications:

- To verify HTTP server is up and running: Go to the Swagger doc page at `localhost:8080/docs`, the list of endpoints
  should be available and ready for manual testing.

- To verify event bus consumption, there are some scripts that can generate fake data and send them to the local
  rabbitmq instance, located in `tests/scripts/` directory.
  _ `job_update_event_bus_publish.py`: publishes job update events to the event bus.
  _ `anomaly_event_bus_publish.py`: publishes job anomaly events to the event bus.
  _ `checkpoint_event_bus_publish.py`: publishes checkpoint events to the event bus.
  _ `insert_workflow_records.py`: directly inserts workflow and job records to the local mysql database.

      To run each of them, make sure you are in a python virtual env that contains all the packages.
      ```
      uv run --locked ./tests/scripts/<script>.py --help
      ```

## Deployment Guide

Use the public chart under `deploy/helm/job-manager/` or enable the Job
Manager subchart through `deploy/helm/nvidia-recovery/`. Supply database and event-bus
credentials with Kubernetes Secrets.

### Application-cancellation retry configuration

When `service.advice_management_enabled` is true, Job Manager requires a
positive `service.event_bus.anomaly_queue_delivery_retry_delay_secs` and both
`anomaly_dead_letter_routing_key` and
`anomaly_dead_letter_redelivery_routing_key` under `service.event_bus`. The two
routing keys must differ. Omitting the delay uses the consumer's 30-second
default; an explicit zero or negative delay fails startup.

Before upgrading a custom deployment, check these values in its effective
configuration. In particular, change an explicit zero delay to a positive
value and provide distinct retry routing keys before enabling Advice
management. The supplied Job Manager configuration already has a positive
delay and distinct keys. Disabling Advice management does not require a
delayed attribution retry route; a positive configured delay still requires
valid routing keys.

This validation protects late attribution results when cancellation state
cannot be read or terminal Advice cannot be persisted. Those results must be
redelivered rather than lost or allowed to restart a cancelled job. A zero
delay does not create the timer queue, so accepting it with only a warning
would not provide a working retry path. Top-level service retry settings do
not configure the consumers; use the `service.event_bus` settings above.
