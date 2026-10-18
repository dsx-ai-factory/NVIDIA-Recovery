# Slurm Advisor Controller Grafana Dashboard

This directory contains the health dashboard for the Slurm Advisor Controller
(SAC):

- [`slurm-advisor-controller-health.json`](slurm-advisor-controller-health.json)

The dashboard provides a single view of controller availability,
reconciliation latency, per-stage and source-read latency, job and advice
flow, failures, process health, log-link materialization, and the optional
standalone Slurm watchdog.

## Import The Dashboard

The JSON file is intended to create a new dashboard through the Grafana UI:

1. Open **Dashboards > New > Import** in Grafana.
2. Upload `slurm-advisor-controller-health.json`, or copy the entire file and
   paste it into the import text box.
3. Select the Prometheus-compatible datasource when Grafana prompts for the
   `datasource` variable.
4. Choose the destination folder and import the dashboard.
5. Select one or more values from the **Environment** variable and confirm the
   **SAC scrape health** panel is reporting data.

The dashboard uses the fixed UID `heimdall-sac-health`. Change the UID during
import if another dashboard with that UID already exists or if multiple copies
must coexist.

The checked-in file is an import definition, not a byte-for-byte representation
of the manifest Grafana stores. Grafana may migrate it to a newer dashboard
schema and add or reshape fields when the dashboard is saved. In particular,
copying this raw JSON into the JSON model of an existing, saved dashboard may
fail schema validation or may not update the dashboard as expected.

For an existing dashboard, first export or otherwise preserve its current
definition. Import this file as a temporary new dashboard and verify its
datasource, variables, and panels before replacing the existing dashboard. Do
not assume that a textual comparison between the checked-in import definition
and a Grafana export represents a functional difference; compare panel
queries, variables, and layout semantically.

The default view covers the last three hours and refreshes every 90 seconds.

## Dashboard Variables

- **Prometheus** (`datasource`) selects the Prometheus-compatible datasource.
- **Environment** (`environment_var`) is a multi-select variable populated from
  the `environment` values on
  `slurm_advisor_controller_reconcile_loops_total`. Selecting **All** uses the
  regular expression `.*`.
- **SAC scrape job** (`sac_job_var`) is hidden. It is populated from the `job`
  values on the same controller metric and scopes generic Go and process
  metrics so unrelated workloads are not included.

## Metric And Label Requirements

The selected datasource must contain SAC metrics using their original names.
Most panels query metrics prefixed with `slurm_advisor_controller_`. The
optional watchdog panels query metrics prefixed with `slurm_watchdog_`; those
panels show no data when the watchdog is disabled or not scraped.

The dashboard expects the following labels:

| Label | Requirement |
| --- | --- |
| `environment` | Required on SAC, watchdog, Go runtime, process, and `up` series. It identifies the cluster or customer environment selected by the dashboard. |
| `service` | The controller target's `up` series must have `service="nvidia-recovery-slurm-advisor"`. This is used only by **SAC scrape health**. |
| `job` | Must consistently identify the SAC controller scrape target. A hidden variable derives its values from `slurm_advisor_controller_reconcile_loops_total` and uses them to scope generic `go_*` and `process_*` metrics. |
| `instance` | Recommended on each scrape target. It is displayed in runtime panel legends. |

SAC does not add the deployment `environment` label to its exported metrics.
The scraper, remote-write pipeline, or metrics gateway must add it before the
series reaches the datasource queried by Grafana. Verify that the label is
present on `slurm_advisor_controller_reconcile_loops_total`; the dashboard
uses that metric to populate the **Environment** variable.

The Helm value `controller.environment` controls SAC's workload-routing
behavior; it does not add an `environment` label to exported metrics. Configure
the routing value and the metrics label consistently, but do not treat them as
the same configuration mechanism.

Labels emitted by SAC, including `mode`, `outcome`, `stage`, `source`,
`action`, `skip_reason`, and `decision_reason`, must be preserved. Histogram
series must also retain the `le` label. Removing or rewriting these labels can
collapse series or make percentile and failure panels incorrect.

## Custom Scrapers And Exporters

When Prometheus scrapes the controller directly, attach the dashboard labels
to the scrape target. A static scrape configuration can use target labels:

```yaml
scrape_configs:
  - job_name: nvidia-recovery-slurm-advisor
    static_configs:
      - targets:
          - sac-metrics.example.internal:9090
        labels:
          environment: customer-slurm
          service: nvidia-recovery-slurm-advisor
```

Prometheus supplies the `job` label from `job_name` and normally supplies an
`instance` label from the target address. Choose an `environment` value that is
stable and unique enough to distinguish the cluster in the shared metrics
datasource.

For a Prometheus Operator `ServiceMonitor`, equivalent target relabeling can be
added to the metrics endpoint:

```yaml
spec:
  endpoints:
    - port: ctrl-metrics
      path: /metrics
      relabelings:
        - action: replace
          targetLabel: environment
          replacement: customer-slurm
        - action: replace
          targetLabel: service
          replacement: nvidia-recovery-slurm-advisor
```

The Helm chart can create a `ServiceMonitor` with
`serviceMonitor.enabled=true`, but its current values do not expose endpoint
relabeling. Apply the required target labels in the owning Prometheus setup,
through an environment-specific chart customization, or in the remote-write
pipeline, and verify the stored series before importing the dashboard.

If a custom exporter or gateway proxies SAC metrics:

- preserve the original metric names, histogram buckets, and bounded labels;
- attach `environment` consistently to both application and runtime metrics;
- ensure the controller-specific metrics and its `go_*` and `process_*`
  metrics have the same `job` label; and
- configure `environment` and `service` as scrape target labels so they also
  appear on Prometheus's generated `up` series.

Adding constant labels only inside a custom exporter does not label the `up`
series generated by Prometheus. If **SAC scrape health** is empty while SAC
metrics are present, inspect this query in the selected datasource:

```promql
max(up{environment="customer-slurm", service="nvidia-recovery-slurm-advisor"})
```

Also verify the environment discovery query:

```promql
count by (environment) (slurm_advisor_controller_reconcile_loops_total)
```

## Maintaining The Definition

Keep the checked-in JSON portable: do not commit a concrete datasource UID,
environment selection, or Grafana-generated saved-dashboard manifest. New
queries should continue to use `${datasource}` and filter cluster metrics with
`environment=~"${environment_var:regex}"`.

After editing the dashboard definition, at minimum validate that it is valid
JSON:

```bash
jq empty slurm-advisor-controller-health.json
```

Import the file as a new dashboard in a representative Grafana instance before
using it to replace a deployed dashboard.
