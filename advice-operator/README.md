# Advice Operator

The Advice operator polls the NVIDIA Recovery API Gateway over gRPC
and materializes the returned recommendations as namespaced `Advice` custom
resources. Kubernetes integrations, including RunAI, can consume those custom
resources without depending directly on the backend API.

This directory is a manually maintained copy of the controller from the
companion `heimdall-runai-adapter` repository. There is no automated upstream
refresh workflow.

## Configuration

The operator accepts these environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `HEIMDALL_API_GATEWAY_URL` | required | API Gateway gRPC address. URL forms are accepted and normalized to host and port. |
| `HEIMDALL_API_PROTOCOL` | `grpc` | Backend protocol. Only `grpc` is supported. |
| `HEIMDALL_API_INSECURE_SKIP_VERIFY` | `false` | Disable TLS certificate verification. Use only for controlled development environments. |
| `POLL_INTERVAL` | `30s` | Interval between complete Advice synchronization passes. |

The process also exposes Kubebuilder-compatible flags for namespace selection,
leader election, health probes, metrics, and TLS certificates. Run
`go run ./cmd --help` for the complete list.

## Development

This component is vendored for downstream consumers. It is excluded from the
release source archives and supported build/CI matrix; restoring it here does
not qualify it for deployment or release. Dependency and build reconciliation
remain pending.

## Deployment

The standalone Helm chart is in
`deploy/helm/advice-operator`. It installs the Advice CRD, controller,
RBAC, health probes, and optional metrics service. Override the image and API
Gateway values for the target environment.
