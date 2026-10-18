# Watcher Configuration Guide

This document describes the configuration structure for the Watcher service, including all available options and their default values.

## Table of Contents

- [Overview](#overview)
- [Top-Level Configuration (WatcherConfig)](#top-level-configuration-watcherconfig)
- [Service Configuration (WatcherServiceConfig)](#service-configuration-watcherserviceconfig)
- [Datasources Configuration](#datasources-configuration)
- [Rules Configuration](#rules-configuration)
- [Environment Variables](#environment-variables)

## Overview

The Watcher service uses YAML configuration files to define its behavior.

## Top-Level Configuration (WatcherConfig)

The root configuration object that contains all service settings.

### Structure

```yaml
# Global settings
metrics_cleanup_threshold_secs: 300  # Cleanup interval for metrics gauges
event_source: log_query              # Primary event source: log_query or rabbitmq

# Service configuration
service:
  host: "0.0.0.0"
  port: 8080
  # ... other service settings

# Data source configurations
datasources:
  log_query:
    backend: loki
    providers:
      loki:
        url: "https://loki.example.com"
        tenants: "tenant-a"
        org_id: "tenant-a"
        queries:
          query_prefix: '{cluster="slurm", appName="slurm-app-log"}'
          workflows:
            straggler_detection:
              query: ['|= `throughput per GPU`']
  log_query_policy: { ... }
  rabbitmq: { ... }
  job_manager: { ... }

# Anomaly detection rules
rules:
  - name: "rule_name"
    workflow: [ ... ]
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `metrics_cleanup_threshold_secs` | `int` | `300` | Interval in seconds to cleanup metrics gauges |
| `event_source` | `string` | `"log_query"` | Polling source mode. `"log_query"` uses the backend selected by `datasources.log_query`; legacy `"loki"` is accepted as an alias; `"rabbitmq"` selects RabbitMQ. |
| `service` | `WatcherServiceConfig` | Default service config | Service-level configuration settings |
| `datasources` | `DatasourcesConfig` | Required | External data source configurations |
| `rules` | `List[RuleConfig]` | Required | List of anomaly detection rules |

## Service Configuration (WatcherServiceConfig)

Service-level settings that control how the Watcher service operates.

### Structure

```yaml
service:
  host: "0.0.0.0"                          # Bind address
  port: 8080                               # HTTP server port
  http_log_level: "WARNING"                # HTTP request log level
  max_queue_size: 1024                     # Maximum message queue size
  maintenance_tasks_interval_secs: 30      # Maintenance tasks interval
  num_threads: 5                           # Number of worker threads
  notification_dedup_ttl_secs: 300         # Notification deduplication TTL
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | `string` | `"0.0.0.0"` | IP address to bind the HTTP server to. Use `"0.0.0.0"` for all interfaces |
| `port` | `int` | `8080` | Port number for the HTTP server |
| `http_log_level` | `string` | `"WARNING"` | Log level for HTTP requests. Valid values: `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`, `"CRITICAL"` |
| `max_queue_size` | `int` | `1024` | Maximum number of messages in the internal processing queue |
| `maintenance_tasks_interval_secs` | `int` | `30` | Interval in seconds between maintenance task executions |
| `num_threads` | `int` | `5` | Number of worker threads for processing messages |
| `notification_dedup_ttl_secs` | `int` | `300` | Time-to-live in seconds for notification deduplication cache |

## Datasources Configuration

Configuration for external data sources that the Watcher service connects to.

### Structure

```yaml
datasources:
  log_query:
    backend: victorialogs
    providers:
      victorialogs:
        url: "https://victorialogs.example.com"
        queries:
          query_prefix: '<deployment-validated LogsQL stream filter>'
          workflows:
            straggler_detection:
              query: ['<deployment-validated LogsQL filter>']
        account_id: 1
        project_id: 0
        require_tenant: true

  # Common polling and field-remapping policy is independent of the provider.
  log_query_policy:
    query_delay: 45
    query_limit: 5000
    # ... other polling settings

  rabbitmq:
    publisher:
      host: "rabbitmq.example.com"
      username: "watcher"
      password: ""
      # ... other publisher settings
    subscriber:
      # ... subscriber settings

  job_manager:
    endpoint: "http://job-manager.example.com:8080"
```

### Log-Query Configuration

`datasources.log_query.backend` accepts `loki` or `victorialogs`. It selects one
statically registered in-process implementation; it does not select an
ingestion destination. The Helm chart derives it from the sole release-wide
`global.log_query.backend` value and maps the selected application connection
into `providers`.

Every deployment, including bundled, local, and test deployments, must supply
the selected application connection under
`global.log_query.providers.<backend>.application`. Standalone Watcher
processes supply it directly under `datasources.log_query.providers`.
The Loki default selects an implementation, not a default connection.

Each provider requires `url` and defines native text in `queries.query_prefix`
and `queries.workflows.<workflow-name>`. In Helm these live under
`global.log_query.providers.<backend>.queries.watcher`; the chart projects that
subtree into the runtime provider. Loki
uses `tenants` for the Basic-auth username and `org_id` for `X-Scope-OrgID`;
both belong in the provider block, not environment overrides or polling policy.
VictoriaLogs accepts a paired unsigned 32-bit `account_id` and `project_id`;
both are mandatory when `require_tenant` is true. Authentication and TLS are
provided through the chart's provider-specific Secret/environment settings,
not through arbitrary YAML headers.

Common polling and field-remapping controls live under
`datasources.log_query_policy`. The old `datasources.loki` key is accepted as
an input compatibility alias for polling policy only. Native query text,
connection identity, and obsolete environment overrides in the old locations
are rejected; move them into the selected provider block.

### Polling and Label Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query_delay` | `int` | `None` | Delay in seconds applied to the query end timestamp |
| `query_limit` | `int` | `None` | Maximum number of log lines to retrieve |
| `query_max_lookback` | `int` | `15` (`900` in Helm) | Maximum query lookback in seconds |
| `start_override` | `string` | `None` | Override start time (format: "YYYY-MM-DD HH:MM:SS") |
| `end_override` | `string` | `None` | Override end time (format: "YYYY-MM-DD HH:MM:SS") |
| `label_remapping_enabled` | `bool` | `false` | Enable label to attribute remapping |
| `label_remapping` | `LabelRemappingConfig` | `None` | Label remapping configuration (see below) |

Both values are integer seconds. A Helm deployment supplies a 60-second query
delay and a 900-second (15-minute) lookback. The delay moves the query end time
into the past, and the lookback is measured back from that delayed end, so the
query window remains exactly 900 seconds wide.

### Label Remapping

The label remapping feature maps provider fields to `Event.Attributes`. This is
useful when stored field names do not match the expected Event schema.

#### Configuration Structure

```yaml
datasources:
  log_query_policy:
    label_remapping_enabled: true
    label_remapping:
      # Simple 1:1 remapping: attribute_name -> provider_field
      simple:
        job_id: slurm_job_id

      # Composite remapping: combine multiple labels into one attribute
      composite:
        - attribute: log.file.path
          template: "/var/log/slurm/{slurm_job_name}_{slurm_job_id}_{slurm_job_attempt}"
          labels: [slurm_job_name, slurm_job_id, slurm_job_attempt]
```

#### Simple Remapping

Simple remapping provides direct 1:1 mapping from provider fields to Event attributes.

| Key | Value | Description |
|-----|-------|-------------|
| `<attribute_name>` | `<provider_field>` | Maps the provider field to the specified attribute name |

**Example:** `job_id: slurm_job_id` maps the provider field `slurm_job_id` to the `Event.Attributes.job_id` field.

#### Composite Remapping

Composite remapping combines multiple provider fields into a single attribute using a template string.

| Field | Type | Description |
|-------|------|-------------|
| `attribute` | `string` | Target attribute name in `Event.Attributes` |
| `template` | `string` | Format string with `{label_name}` placeholders |
| `labels` | `List[string]` | List of provider field names referenced in the template |

**Example:** The composite configuration:
```yaml
- attribute: log.file.path
  template: "/var/log/slurm/{slurm_job_name}_{slurm_job_id}_{slurm_job_attempt}"
  labels: [slurm_job_name, slurm_job_id, slurm_job_attempt]
```

With provider fields `slurm_job_name=myapp`, `slurm_job_id=12345`, `slurm_job_attempt=1`, produces:
```
log.file.path = "/var/log/slurm/myapp_12345_1"
```

### RabbitMQ Configuration

#### Publisher Configuration
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | `string` | Required | RabbitMQ server hostname |
| `username` | `string` | Required | Authentication username |
| `password` | `string` | Required | Authentication password |
| `vhost` | `string` | Required | Virtual host name |
| `exchange` | `string` | Required | Exchange name for publishing |
| `routing_prefix` | `string` | `"anomaly.detected."` | Prefix for routing keys |

## Rules Configuration

Rules define the anomaly detection workflows and their configurations.

### Structure

```yaml
rules:
  - name: "anomaly_detection_rule"
    workflow:
      - name: "straggler_detection"
        type: "log_query"
        bucket_by:
          - "job_id"
          - "log_file_name"
        attributes:
          query:
            regex:
              - 'throughput per GPU'
          # ... other workflow attributes
```

The `straggler_detection` name resolves the query entry in the selected
provider's `queries.workflows` map shown above. Native query text is not kept
in workflow attributes. The optional local `query.regex` list is used for
record processing and RabbitMQ input; it is not sent to the log backend.
An enabled `nooutput` workflow also requires a provider-scoped
`nooutput_validation_query` containing its job-ID filter. Its `query` list
can contain ordered `PRE_TRAINING`/`TRAINING_LOOP` phase maps.

### Rule Configuration Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Unique identifier for the rule |
| `workflow` | `List[RuleWorkflowConfig]` | List of workflow steps for this rule |

### Workflow Step Types

The Watcher supports the following workflow step types:

- `log_query`: Query the selected log-query backend. Legacy `loki_query` is accepted as a compatibility alias.
- `regex_parser`: Regular expression pattern matching
- `moving_avg`: Anomaly detection using moving averages, e.g. straggler detection
- `nooutput`: Detect the absence of expected log output within a configured interval
- `compare_last_datapoint`: Compare the current datapoint against the last recorded value
- `count_over_time`: Count matching events over a time window
- `store_datapoint`: Persist a datapoint for later comparison
- `emit_metric`: Emit a Prometheus metric
- `simple_action`: Dispatch an action, e.g. publishing to the event bus

## Environment Variables

Some configuration values can be overridden using environment variables:

| Environment Variable | Configuration Path | Description |
|---------------------|-------------------|-------------|
| `RABBITMQ_PASSWORD` | `datasources.rabbitmq.publisher.password` | RabbitMQ authentication password |

Credential environment variables include `LOKI_API_TOKEN`, `LOKI_API_KEY_PATH`,
`VICTORIALOGS_API_TOKEN`, `VICTORIALOGS_USERNAME`, `VICTORIALOGS_PASSWORD`, and
the matching certificate paths. Bearer and Basic credentials are mutually
exclusive. URLs, tenant identity, and native queries come from provider YAML;
legacy connection environment overrides are rejected.

Every enabled workflow selected with VictoriaLogs must have a validated entry
in that provider's `queries.workflows` map. No default LogsQL variants are
inferred from the chart's LogQL rules. Multi-backend operation uses independent
provider query maps, including no-output validation templates. See
[`docs/architecture/log-query-backends.md`](../../docs/architecture/log-query-backends.md).
VictoriaLogs 1.39.0 or newer is required for the time-window and tenant-response
contract.

For complete umbrella-chart examples, exact Secret paths, the Helm array
replacement caveat, and the backend-switch checklist, see
[`docs/ops/configure-log-query-backends.md`](../../docs/ops/configure-log-query-backends.md).
