# Grafana Dashboards

Grafana dashboard definitions for NVIDIA Recovery services, exported as JSON and managed via the [dashboard update tool](docs/update-dashboard.md).

## Dashboards

| File | Description |
|------|-------------|
| `heimdall-adi-service.json` | ADI (Anomaly Detection & Isolator) |
| `heimdall-job_manager-service.json` | Job Manager |
| `heimdall-services-tracing.json` | Cross-service tracing |
| `heimdall-slos.json` | SLO tracking |
| `heimdall-watcher-service.json` | Watcher |
| `system-gpu-information.json` | GPU fleet health, power & utilization (DCGM) |
| `system-gpu-insights.json` | GPU fleet insights, 6-month view (DCGM) |

## Resources

- **Updating dashboards** — [docs/update-dashboard.md](docs/update-dashboard.md)
