# Direct Kubernetes development

[ADR 0056](../decisions/0056-establish-kubernetes-local-development-foundation.md)
defines this CPU-only foundation: Kind, JobSet, the default Kubernetes scheduler,
and isolated staged smoke scenarios. It does not deploy Recovery controllers or
prove recovery correctness. The existing [Slurm/Slinky tutorial](../getting-started.md)
continues to use the root Tiltfile.

Install Make, Docker, Kind (0.31.0 or later), kubectl, Tilt, and uv. The profile pins
Kubernetes to `kindest/node:v1.35.0` and JobSet to `v0.12.0`; network access is
needed for their public images and release manifests. JobSet is installed with
the upstream [server-side apply procedure](https://jobset.sigs.k8s.io/docs/installation/).

From the repository root:

```bash
# Start Kind and keep Tilt running for interactive development.
make dev-k8s

# In another terminal, run the foundation scenarios.
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
Cluster creation and controller readiness are outside pytest. A failed run
leaves the cluster available for diagnosis until explicit teardown.

The default cluster is `nvr-kubernetes-dev`. Set `KUBERNETES_DEV_CLUSTER` on
every command to use another name. The wrapper stores its kubeconfig and
ownership identity under `deploy/tilt/tmp/<cluster>/`, without touching the
user's kubeconfig. It refuses to adopt an existing unowned cluster or delete a
replacement cluster with a different identity. Repeating `up` reuses the owned
cluster. Teardown deletes the entire owned development cluster, including any
additional workloads deliberately deployed there.

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

The initial scenario submits a small CPU JobSet, observes its expected Pods
running, then removes only its own resources. No Slurm attempts, Recovery
services, GPU drivers, or alternative scheduler are required. Component work
can add ordinary Tilt builds and resources as dependencies arrive. Set
`KUBERNETES_DEV_KIND_CONFIG` to a Kind config with additional worker nodes
before creating a new cluster for placement experiments; PyTorchJob and KAI
installation and qualification remain subsequent work.
