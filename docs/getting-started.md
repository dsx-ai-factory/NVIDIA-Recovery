# Try NVIDIA Recovery locally

For infrastructure engineers evaluating the recovery loop. This tutorial creates
a disposable Slurm-on-Kubernetes stack and runs one existing recovery scenario.
You will see a simulated failure produce Advice and a restarted Slurm attempt.

The question to answer is simple: when a job fails, can the system notice,
record a recovery decision, and get another attempt running without a person
requeueing it? You will follow the same job through those steps, with evidence
for each one.

This tutorial explicitly selects degraded Attribution OFF mode; normal builds
and development default to Attribution ON. No physical GPU, training framework,
or private Attribution access is required for this OFF tutorial.
This is a synthetic shell workload exercising the real collection, detection,
Advice, and scheduler interfaces—not a training performance benchmark.

Already have a cluster? Use the [operator installation guide](operators/installation.md).
Already have NVIDIA Recovery installed? Start with [Run jobs](users/running-jobs.md).

## 1. Prepare a dedicated environment

Use a clone of this repository and run commands from its root. You need:

- Docker with a working Docker/Buildx endpoint, Make, Bash, and Git.
- Python 3.12 or newer and `uv` for the scenario runner.
- Network access to fetch public images, charts, tools, and dependencies.
- Enough CPU, memory, and disk for a Kind control plane, **three workers**,
  Slurm/Slinky, the recovery services, MySQL, RabbitMQ, and log collection.
  This builds and runs a full stack; do not crowd another active cluster into
  a resource-constrained Docker VM.
- Free local ports 5001 (demo registry), 3001 (dashboard), and 10350 (Tilt UI).

The startup script uses installed Kind, kubectl, Helm, and Tilt when available;
otherwise it downloads the pinned versions into its tool cache. It does not
replace versions already on your PATH. For component-specific development tools,
see [developer setup](development/getting-started.md).

Check the Docker context and capacity before starting:

```bash
docker context show
docker info
docker buildx version
uv --version
```

Use a local Docker context you own, not an endpoint serving shared workloads.
Check that the demo's cluster and registry names are unused:

```bash
docker ps -a --filter label=io.x-k8s.kind.cluster=nvidia-recovery-demo --format '{{.Names}}'
docker ps -a --filter name='^/nvidia-recovery-demo-registry$' --format '{{.Names}}'
test ! -e /tmp/kind-nvidia-recovery-demo.kubeconfig
test ! -e /tmp/nvidia-recovery-demo-slurm-home
```

Both lists should be empty and both file checks should succeed. Check all three
ports with your operating system's socket tools (for example,
`lsof -nP -iTCP:5001 -sTCP:LISTEN`). If a demo name, path, or registry port is in
use, choose unused names and a registry port, updating every command below.
The dashboard forward is fixed at 3001 in the checked-in Tiltfile; this tutorial
also uses Tilt's default port 10350. Those ports must be free before continuing;
changing a browser URL does not change the listener. Do not delete existing
resources or stop someone else's services to make the example fit.

**Startup recreates an existing Kind cluster with the chosen name.** Cleanup
also removes the named registry. Distinct cluster *and registry* names protect
other local environments.

## 2. Start the stack

In terminal one, from the repository root:

```bash
CLUSTER_NAME=nvidia-recovery-demo \
KIND_REGISTRY_NAME=nvidia-recovery-demo-registry \
KIND_REGISTRY_PORT=5001 \
DEPLOY_KIND_CLUSTER=1 \
DEPLOY_INFRA=1 \
DEPLOY_STACK=1 \
E2E_PROFILE=advisor-act \
E2E_ATTRIBUTION_MODE=unavailable \
make dev
```

Leave this terminal running. Tilt builds the images and deploys the infrastructure
and recovery stack. The first run can take substantially longer than a warm run
because it downloads dependencies and builds images. Wait for Tilt to report
the resources ready before running the scenario.

`advisor-act` explicitly enables scheduler mutations by the controller and
watchdog **inside this disposable cluster**. Observe mode cannot demonstrate
a recovery action. Do not transplant these settings into a shared deployment;
use the [staged operator validation](operators/operations.md) there.

The demo kubeconfig is `/tmp/kind-nvidia-recovery-demo.kubeconfig`.
The startup path creates its own kubeconfig instead of modifying your default
Kubernetes configuration. If startup fails, use the Tilt resource logs first;
the [troubleshooting guide](operators/troubleshooting.md) covers image pulls,
unready services, and missing signals. Do not repeatedly rerun `make dev` to
inspect a failure: that recreates the cluster.

## 3. Run one recovery scenario

In terminal two, from the same repository root:

```bash
DEPLOY_KIND_CLUSTER= \
KUBECONFIG=/tmp/kind-nvidia-recovery-demo.kubeconfig \
RUN_TESTS=1 \
E2E_PROFILE=advisor-act \
E2E_ATTRIBUTION_MODE=unavailable \
E2E_STACK_NAMESPACE=slurm \
E2E_SCENARIOS=10-simple-segfault \
bash -c 'source scripts/ci/e2e/run-tests.sh; run_tests'
```

This invokes the existing scenario runner against the cluster you just created;
it does not create another cluster. The wrapper sets the profile's readiness
checks and runs only [the simple-segfault scenario](../tests/e2e/scenarios/10-simple-segfault/test_simple_segfault_recovery.py).
Keep other jobs out of this dedicated demo queue while it runs.

The [job fixture](../tests/e2e/scenarios/10-simple-segfault/job.sh) prints a recognizable
segmentation-fault marker, waits for collection, and exits with failure. It
does not crash a GPU or inject a real hardware fault.

A passing test establishes this causal chain:

| Question | Evidence the test checks |
| --- | --- |
| Was the failure visible? | The job's marker was collected into Loki. |
| Did the system recognize it? | Watcher persisted a matching detection for the source attempt. |
| What should happen next? | Matching restart Advice was produced. |
| Did the controller act? | A recovery action was recorded. |
| Did another execution start? | Slurm started a subsequent attempt. |

The runner writes its report to
`reports/e2e/advisor-act-loki/job_state/junit.xml` (or the path selected by
`E2E_XUNIT_PATH`). A failed assertion or readiness
check means the demo did not establish this result; inspect its output rather
than treating healthy Pods alone as successful recovery.

## 4. Inspect the result

Open the [local workflow dashboard](http://localhost:3001/dashboard/workflow).
Search for `simple-segfault-requeue`, or use the
[filtered job view](http://localhost:3001/dashboard/workflow?query=simple-segfault-requeue&jobNameMatch=exact).
Inspect attempts zero and one and the job timeline. For the exact restart
Advice record, use the [Gateway inspection examples](users/running-jobs.md#inspect-the-api-when-you-need-the-exact-records);
the dashboard is not an Advice API viewer.

Attempt zero is the execution that failed; attempt one is its replacement.
The restart Advice explains the transition between them. Keeping these as
separate records makes the retry traceable instead of overwriting the failure
with the job's latest state.

The test deliberately cancels its recovered allocation during cleanup.
A final `CANCELLED` state is therefore expected; it does not negate the
verified restart. Cleanup also waits for the dedicated queue to empty.

This result proves scheduler recovery for this scenario. It does **not** prove
checkpoint loading, successful training completion, GPU performance, or
production readiness. Your real workload owns checkpoint creation and restore.

## 5. Remove the demo

Before teardown, retain the JUnit report, exact job/attempt IDs, matching
Advice and audit events, and the relevant application/controller logs. The
test's job cleanup has already cancelled its recovered allocation; do not
interpret that final state as a failed recovery. The report proves the checked
scenario only, not checkpoint restore or successful training.

Loki in this local profile uses filesystem storage on the disposable Kind
node. Deleting the cluster destroys that log storage, regardless of PVC
retention settings. The local profile does not inherit the public umbrella's
31-day log-age retention; a long-running development cluster needs separate
storage monitoring and retention planning. Export needed evidence before
`make dev-down`, `make e2e-down`, or recreating the same cluster.

Stop Tilt with Ctrl-C in terminal one. From the same Docker context and checkout:

```bash
CLUSTER_NAME=nvidia-recovery-demo \
KIND_REGISTRY_NAME=nvidia-recovery-demo-registry \
KIND_REGISTRY_PORT=5001 \
DEPLOY_KIND_CLUSTER=1 \
make dev-down
```

This removes the dedicated cluster, its kubeconfig, and the registry labelled
as owned by that cluster, including the registry's anonymous volumes. A reused
legacy registry without an ownership label is preserved; another cluster's
labelled registry cannot be reused. Inspect any preserved registry's consumers
and mounts before separately deciding its removal. Named Docker volumes are
not removed automatically.
It does not promise a zero-footprint uninstall: downloaded tools, build caches,
Docker images, test reports, and `/tmp/nvidia-recovery-demo-slurm-home` can remain.
Keep the report if you need to investigate a failed run. Review residual files
before manually removing them; do not prune shared Docker data as part of this
tutorial.

## Next steps

- Understand [the complete architecture](../ARCHITECTURE.md), then
  [jobs and attempts](concepts/jobs-and-workflows.md) and [Advice](concepts/advice.md).
- [Install on your infrastructure](operators/installation.md), starting in observe mode.
- [Run a real workload](users/running-jobs.md) on an operator-managed installation.
- [Set up development](development/getting-started.md) or review the [FAQ](faq.md).

## Looking for the previous operator walkthrough?

The installation material formerly on this page now lives in the
[installation](operators/installation.md), [Slurm](operators/slurm.md),
[Slinky](operators/slinky.md), [operations](operators/operations.md), and
[troubleshooting](operators/troubleshooting.md) guides. Existing example values
remain in [docs/examples/getting-started](examples/getting-started).

### Optional external OTLP collector

See [Optional external OTLP collector](operators/operations.md#optional-external-otlp-collector)
for the retained observability configuration.
