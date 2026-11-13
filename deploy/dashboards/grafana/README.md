# Grafana Dashboards

Grafana dashboard definitions for NVIDIA Recovery services, exported as JSON and managed via the [dashboard update tool](docs/update-dashboard.md).

## Dashboards

| File | Description |
|------|-------------|
| `heimdall-adi-service.json` | ADI (Anomaly Detection & Isolator) |
| `heimdall-job_manager-service.json` | Job Manager |
| `heimdall-services-tracing.json` | NVIDIA Recovery cross-service tracing and recovery outcomes |
| `heimdall-slos.json` | SLO tracking |
| `heimdall-watcher-service.json` | Watcher |
| `system-gpu-information.json` | GPU fleet health, power & utilization (DCGM) |
| `system-gpu-insights.json` | GPU fleet insights, 6-month view (DCGM) |

## Resources

- **Updating dashboards** — [docs/update-dashboard.md](docs/update-dashboard.md)

## Recovery measurement and audit links

Recovery sequences start in Job Manager before attribution and Advice. Normal
SAC reconciliation uses each Advice item's context. Legacy eager requeue and
standalone watchdog release are outside command-attributed tracing coverage.
Their audit events remain available.

Applied Advice, command counts, and later convergence are different observations.
Only the first accepted successful final-release milestone completes restart
latency, using command-return time rather than reporting receipt time. Read the
durable end-to-end histogram for latency; never estimate it from span duration
or application/action counts. DAG release and termination are outside the
failed-job restart SLO. A phase-one sequence marked `active` only means that no
completion report was accepted: it may have terminal Advice, missing generation
linkage, or lost reporting. It does not prove recovery is still executing.

Audit delivery, milestone delivery, and OTLP export have independent bounded
queues and can lose different observations. Use their respective coverage and
error metrics separately; audit persistence does not acknowledge a milestone.
An absent trace can mean sampling, export failure, or expired retention.

The existing audit table and job timeline can link directly to a trace viewer.
Configure the dashboard at runtime with `AUDIT_TRACE_URL_TEMPLATE`, or Helm
`dashboard.app.AUDIT_TRACE_URL_TEMPLATE`, for example:

```yaml
dashboard:
  app:
    AUDIT_TRACE_URL_TEMPLATE: "https://traces.example.test/trace/{trace_id}"
```

The public destination must use HTTP(S) with a fixed host, no embedded
credentials, and a `{trace_id}` placeholder in the path, query, or fragment.
Optional `{span_id}` and `{recovery_sequence_id}` placeholders require the
corresponding explicit event payload fields; when any requested ID is missing
or invalid, the event stays visible without a link. Unknown placeholders are
rejected. Templates are browser-visible configuration: do not include secrets.
Trace detail stays in the trace backend and uses its own access controls.

Links use only immutable event payload IDs, never legacy `correlation_uuid` or
current Advice lookups. Historical events without correlation remain readable.
Events record trace and span IDs only when the span was sampled, so a
sampled-out recovery shows no link. A link can still open a missing trace when
export failed or retention expired; the trace viewer reports missing traces. This does not add a combined audit/trace API or change the
release-wide `global.otelcollector` delivery settings.
