# Watcher

Watcher detects configured anomaly signals in observability data and publishes
events to the NVIDIA Recovery event bus.

## Configuration

Use `watcher/docs/CONFIG.md` as the public configuration reference. Supply
log-query backend and RabbitMQ credentials through environment variables or
Kubernetes Secret references; do not commit tokens, passwords, tenant
identifiers, or deployment-specific endpoints.

`watcher/tests/fixtures/watcher_config.yaml` is a working local/test example
covering the rules and datasources shapes described in CONFIG.md. Replace its
example endpoints and credentials before using it outside an isolated
development environment.

## Local development

From the repository root:

```bash
uv sync --locked
CONFIG_PATH=watcher/tests/fixtures/watcher_config.yaml uv run --package watcher watcher
```

Run the focused tests with:

```bash
uv run pytest watcher/tests
```

Build the service image from the repository root:

```bash
docker build -f watcher/Dockerfile -t watcher .
```

## Deployment

Use `deploy/helm/watcher/` directly or enable the watcher subchart through
`deploy/helm/nvidia-recovery/`. Configure log collection paths and canonical provider
fields for the operator's own Slurm environment; no cluster-specific path or
endpoint is assumed by this guide.
