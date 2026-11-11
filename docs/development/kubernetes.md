# Direct Kubernetes development

[ADR 0056](../decisions/0056-establish-kubernetes-local-development-foundation.md)
defines this CPU-only environment: Kind, JobSet, the default Kubernetes scheduler,
and isolated staged scenarios. The profile also deploys Advice Operator with a
stateful mock API Gateway to test Advice custom-resource reconciliation. It does
not prove workload recovery correctness. The existing [Slurm/Slinky tutorial](../getting-started.md)
continues to use the root Tiltfile.

Install Make, Docker, Kind (0.31.0 or later), kubectl, Helm, Tilt, and uv. The profile pins
Kubernetes to `kindest/node:v1.35.0` and JobSet to `v0.12.0`; network access is
needed for their public images and release manifests. JobSet is installed with
the upstream [server-side apply procedure](https://jobset.sigs.k8s.io/docs/installation/).

From the repository root:

```bash
# Start Kind and keep Tilt running for interactive development.
make dev-k8s

# In another terminal, run the workload and Advice Operator scenarios.
make dev-k8s-test

# Exercise concurrent ownership/isolation, then repeat against the same cluster.
make dev-k8s-test K8S_TEST_ARGS="-n 2"
make dev-k8s-test K8S_TEST_ARGS="-n 2"

# Stop Tilt with Ctrl-C, then delete this profile's cluster.
make dev-k8s-down
```

For automation, `make dev-k8s-ci`
converges the environment and exits; use `make dev-k8s-test` and
`make dev-k8s-down` afterward.
Override its default 10-minute convergence timeout with `K8S_DEV_CI_TIMEOUT`.
Pass pytest options through `K8S_TEST_ARGS`.
Cluster creation and controller readiness are outside pytest. The test wrapper
waits for JobSet, the mock gateway, and Advice Operator, then opens a temporary
loopback-only port-forward to the mock fixture API. It closes the port-forward
when pytest exits. A failed run
leaves the cluster available for diagnosis until explicit teardown.

The default cluster is `nvr-kubernetes-dev`. Set `KUBERNETES_DEV_CLUSTER` on
every command to use another name. The wrapper stores its kubeconfig and
ownership identity under `deploy/tilt/tmp/<cluster>/`, without touching the
user's kubeconfig. It refuses to adopt an existing unowned cluster or delete a
replacement cluster with a different identity. Repeating `up` reuses the owned
cluster. Teardown deletes the entire owned development cluster, including any
additional workloads deliberately deployed there.

If cluster creation fails after the API becomes reachable, the wrapper records
the cluster identity and returns the creation error. Retry `make dev-k8s` or
remove it with `make dev-k8s-down`. If the new cluster's identity cannot be read,
the wrapper attempts to delete that bootstrap attempt instead. A cleanup failure
is reported and preserves the kubeconfig for manual recovery; existing unowned
clusters are still refused.

Each test invocation writes a separate `run-*` directory beneath that state
directory: JUnit output, trace timeline, and pytest artifacts with failure
diagnostics (workload and Pod state, Events, and logs). Teardown preserves these
reports. For manual inspection, use:

```bash
KUBECONFIG="$PWD/deploy/tilt/tmp/nvr-kubernetes-dev/kubeconfig" kubectl get pods -A
```

To exercise failure reporting, set `KUBERNETES_SMOKE_IMAGE` to a nonexistent
image tag and select one case with
`make dev-k8s-test K8S_TEST_ARGS="-k '[0]'"`. The scenario fails after
its 180-second readiness deadline, records the image-pull Events and Pod state,
and still cleans up its owned workload. Unavailable container logs are recorded
as diagnostic errors, rather than hiding the original scenario failure.

The workload smoke scenario submits a small CPU JobSet, observes its expected
Pods running, then removes only its own resources. Advice scenarios supply
scoped fixture records to the mock gateway and observe the operator's Advice
resources for creation, updates, removal, and owner validation. Owner-guard
scenarios replay Kubernetes watch events from before source changes, detecting
invalid writes even when garbage collection immediately removes them. The operator
consumes the unchanged public gRPC `ListAdvice` API; fixture control belongs to
the mock, not to a production service. Each scenario has its own fixture scope
and workload identity so concurrent runs cannot replace each other's advice.

Tilt builds the production Advice Operator image and renders its standalone
Helm chart. It separately builds the existing mock gateway executable without
Slurm. The standalone chart grants the operator `get` on JobSets so it can validate
their owner UIDs; no additional development-only RBAC is needed. The
operator polls the mock every second across namespaces. Both services live in
`recovery-dev`, and the fixture API is exposed only through a cluster-internal
Service and the test wrapper's temporary loopback port-forward.

These tests qualify the operator's public gRPC-to-Kubernetes boundary against a
controlled backend, not the real API Gateway/backend stack or full recovery.
No Slurm attempts, GPU drivers, or alternative scheduler are required. Further
components can add ordinary Tilt builds and resources as dependencies arrive. Set
`KUBERNETES_DEV_KIND_CONFIG` to a Kind config with additional worker nodes
before creating a new cluster for placement experiments; PyTorchJob and KAI
installation and qualification remain subsequent work.

JobSet ownership is the current integration contract. The backend supplies the
owner API version, kind, name, and UID in Advice scheduler metadata; the
operator validates that identity before writing the owner reference. Kubernetes
garbage collection removes Advice when the JobSet is deleted, while owner
validation skips creation when its lookup finds a missing, mismatched, or
terminating owner. The lookup and Advice write are not atomic, so deletion
starting between them can still race an in-flight write. Advice routing may
require revisiting this ownership and lifecycle relationship; this integration
does not establish a general routing policy.
