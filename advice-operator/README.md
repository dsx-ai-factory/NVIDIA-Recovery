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

Run these commands from the repository root:

```bash
make build-advice-operator
make GO_PACKAGES=advice-operator verify-go
make test-advice-operator-integration
make build-local SERVICES=advice-operator
make test-advice-operator-image
```

The default Go verification set includes this module. Controller tests and the
integration target run local envtest API servers; no Kubernetes cluster or
current kubeconfig context is used. The test tooling downloads pinned envtest
binaries automatically. Set `KUBEBUILDER_ASSETS` to a preinstalled binary
directory for offline runs.

`make GO_PACKAGES=advice-operator generate-go` regenerates the API deep-copy
code, interaction mock, CRD, and RBAC. The generated-artifact check detects
changes to both the canonical CRD and its Helm projection. Tool versions are
pinned in `scripts/ci/go-tool-versions.env`.

Image builds require Docker with Buildx. Advice Operator is explicitly selectable
and is not part of the default Slurm stack image set. The image test checks the
container's runtime identity and starts its public `--help` entry point.

Release source archives include the operator and its standalone chart. Publishing
the Go module still requires replacing the local API Gateway dependency with a
published version and completing the module release workflow described by
[ADR 0057](../docs/decisions/0057-publish-all-go-modules.md).

The inherited `test/e2e` suite is not a supported test target: it still assumes
source-repository deployment targets and an implicit Kubernetes context. The
supported checks above cover compilation, generated assets, reconciliation with
a local API server, and image startup. The [direct Kubernetes development
profile](../docs/development/kubernetes.md) additionally deploys the production
image and standalone chart in Kind. Run `make dev-k8s-ci` followed by
`make dev-k8s-test` to exercise create, update, withdrawal, and owner-validation
behavior through a scoped mock gRPC gateway. Real backend integration and full
workload recovery remain separate qualification work.

## Deployment

The standalone Helm chart is in
`deploy/helm/advice-operator`. It installs the Advice CRD, controller,
RBAC, health probes, and optional metrics service. Override the image and API
Gateway values for the target environment.

The chart includes read-only (`get`) access to JobSets for owner validation,
alongside the existing Run:ai DistributedWorkload permission. For a JobSet
owner, the backend must supply `namespace`, `owner_api_version`
(`jobset.x-k8s.io/v1alpha2`), `owner_kind` (`JobSet`), `owner_name`, and the
actual Kubernetes `owner_uid` in scheduler metadata. The operator rejects
missing, mismatched, or terminating owners. These fields determine
the Advice owner reference and its garbage-collection lifecycle. Incomplete
owner metadata currently produces Advice without an owner reference.

JobSet ownership is supported for now; revisit owner selection and Advice
lifetime when Advice routing is defined. This does not grant the operator
permission to modify JobSets or implement workload recovery.
