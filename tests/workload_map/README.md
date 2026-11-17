# Workload-Map qualification

This is the first implementation of
[ADR 0058](../../docs/decisions/0058-qualify-workload-map-independently.md).
It exercises upstream Workload-Map directly without Advice Operator, a gateway,
or Recovery backend services. The scope is the CRD operations NVIDIA Recovery
needs, not exhaustive SDK or scheduler conformance. PodGroup count and topology are not Recovery requirements.
It is a test tool, not a supported recovery
controller or a complete cluster validator.

The Go probe pins `github.com/dsx-ai-factory/workload-map/karta/sdk v0.3.0`
([upstream SDK](https://github.com/dsx-ai-factory/workload-map/tree/karta/sdk/v0.3.0/karta/sdk)).
It uses the SDK's built-in catalog without definition overrides. Reports include
the SDK version, definition name, and SHA-256 of the JSON-encoded definition.
Go 1.27.3 or newer is required by this SDK. The test module has its own dependency
lock and does not add Workload-Map to a production service.

## Cluster-free checks

From the repository root:

```sh
make verify-workload-map
make build-workload-map-probe
.cache/workload-map-probe --operation inspect < jobset.json
.cache/workload-map-probe --operation instrument --marker example < jobset.json
.cache/workload-map-probe --operation suspend < jobset.json
```

The probe reads one JSON Kubernetes object from stdin. `inspect` returns input
identity metadata, normalized statuses, component replica counts and Pod labels;
it omits container commands, environment and the full manifest. This is an
observation, not a qualification verdict. Unknown definitions and malformed input
fail the command. Extraction errors appear in `capability_errors` alongside usable
results; callers must check that map even when the process exits successfully.
Missing suspend support is reported explicitly. `associate-pods` instead accepts
`{"workload": <object>, "pods": [<objects>]}` and reports SDK classification for
those caller-supplied candidates.

`instrument` adds `qualification.nvidia.com/marker` through the SDK's template
API, refusing a conflicting qualification marker. Focused fixture assertions
check the settings used by Recovery; the probe does not validate arbitrary
template preservation or SDK serialization. `suspend` and `resume` exercise the
SDK root lifecycle API.
Transformations return the full object on stdout but never contact Kubernetes;
treat that output with the same care as the input manifest. Terminal and deleting
workloads are rejected for lifecycle transformations.

An operator can inspect an existing object without modifying it:

```sh
kubectl --context MY_CONTEXT --namespace MY_NAMESPACE get jobset MY_JOB -o json |
  .cache/workload-map-probe --operation inspect
```

The native status may match multiple normalized statuses. `Running` alone does
not establish that every Pod is Ready or that training is progressing. Native
attempt history, restart budgets, and condition transition timestamps are not
normalized by this probe. No support claim is inferred from their absence.

## Live JobSet scenario

Provision the existing disposable CPU-only profile separately:

```sh
KUBERNETES_DEV_CLUSTER=nvr-workload-qualification make dev-k8s-ci
```

Select that cluster explicitly, then run the same entry point locally or in CI:

```sh
export KUBECONFIG="$PWD/deploy/tilt/tmp/nvr-workload-qualification/kubeconfig"
export LAUNCHER_KUBERNETES_JOBSET_CONTEXT=kind-nvr-workload-qualification
export LAUNCHER_KUBERNETES_JOBSET_NAMESPACE=recovery-dev
make test-workload-map-live K8S_TEST_ARGS='-n 2'
```

The scenarios use the existing staged runner and JobSet launcher. Lifecycle cases
create uniquely named JobSets with two or six CPU Pods and compare SDK scale and
classification with independently owned resources. They suspend through the SDK,
wait for complete Pod drain, and observe a suspended interval. After SDK template
instrumentation and resume, they require new Ready Pod UIDs carrying the marker
under the original JobSet name and UID. A successful API write is not a passed
lifecycle check. A separate failing workload exercises the native restart budget.

Writes preserve the original UID and resourceVersion, and cleanup uses the
launcher's exact-object ownership checks. The runner creates no cluster or
namespace and installs no controllers. Use a dedicated test namespace with
permission to create/delete JobSets, list their Jobs and Pods, update owned
JobSets, and read diagnostics. KAI lifecycle runs also require listing namespace
Events to establish scheduler provenance. The environment collector defaults to
`jobset-controller-manager` in `jobset-system`; override
`WORKLOAD_MAP_CONTROLLER_NAMESPACE` and `WORKLOAD_MAP_CONTROLLER_DEPLOYMENT`
for other installations. Restricted read-only inspection uses its separate command.

Each invocation writes a fresh `reports/workload-map/run-*` directory containing
Kubernetes version, installed JobSet controller configuration, JUnit outcomes,
scenario trace, SDK observations, and owned-workload diagnostics. These may
contain cluster identifiers and workload logs; review before sharing. JUnit is
the live pass/fail result; observation snapshots alone are not a verdict.
Reports and build outputs are ignored by version control.

Clean up the disposable environment when finished:

```sh
KUBERNETES_DEV_CLUSTER=nvr-workload-qualification make dev-k8s-down
```

## Read-only cluster inspection

Inspect one selected JobSet or PyTorchJob without creating or modifying cluster
resources. The command requires every cluster-selection argument; it never uses
an ambient context. Controller observation is optional and requires both controller
arguments.

```sh
make inspect-workload-map WORKLOAD_MAP_INSPECT_ARGS="--kubeconfig /path/to/kubeconfig --context MY_CONTEXT --namespace MY_NAMESPACE --resource jobset --name MY_JOB --controller-namespace jobset-system --controller-deployment jobset-controller-manager"
```

The JSON report distinguishes successful reads and SDK extraction from active
qualification. Lifecycle, instrumentation, Pod association and failure recovery
remain `not_tested`. Missing resources are `unsupported`; access or transport
failures are `inconclusive`; observed extraction or deployment failures are `fail`.
Any attempted check that does not pass returns a nonzero exit status. Optional
controller checks require a completed rollout and report image IDs from Pods
linked through ReplicaSet and Deployment owner UIDs.

## Extended controller and scheduler matrix

The optional profile installs Training Operator v1.9.0 and KAI v0.12.0 into an
already-created, ownership-checked disposable Kind cluster. It changes cluster
controllers and admission webhooks, so it is separate from read-only inspection.
Wait for provisioning to finish before submitting any qualification workload.

The pinned KAI chart recreates its Config-owned ServiceAccount on upgrade while
retaining the scheduler Deployment. The profile refreshes that scheduler Pod to
avoid a token bound to the old account UID. It waits on native deployment rollouts
and KAI's `Available` condition; Helm's generic readiness watcher misinterprets
this release's persistent `Reconciling=True` condition.

```sh
KUBERNETES_DEV_CLUSTER=nvr-workload-qualification make dev-k8s-qualification
```

With the explicit kubeconfig, context and namespace selected as above, run all four
combinations:

```sh
make test-workload-map-matrix WORKLOAD_MAP_QUEUE=default-queue K8S_TEST_ARGS='-n 2'
```

Or run each combination using the same entry point:

```sh
# JobSet, default scheduler: two- and six-Pod lifecycle cases, native retry exhaustion.
make test-workload-map-live K8S_TEST_ARGS='-n 2'

# PyTorchJob, default scheduler: role lifecycle and terminal/retry boundaries.
WORKLOAD_MAP_LAUNCHER=kubernetes-pytorch \
  WORKLOAD_MAP_CONTROLLER_NAMESPACE=kubeflow \
  WORKLOAD_MAP_CONTROLLER_DEPLOYMENT=training-operator \
  make test-workload-map-live K8S_TEST_ARGS='-n 2'

# Repeat either command with these additional environment variables for KAI:
export WORKLOAD_MAP_SCHEDULER=kai-scheduler
export WORKLOAD_MAP_QUEUE=default-queue
```

JobSet lifecycle cases use two replicated Jobs with parallelism one or three and
matching completions, producing two or six concurrent Pods. PyTorchJob uses one
Master and two Workers. After complete suspension, the SDK instruments each native
Pod template, then the tests resume the workload and check marker propagation,
selected scheduling, new Pod UIDs, and unchanged parent UID. These are CPU
fixtures; no GPU utilization or training progress is inferred.

Reports retain append-only UTC and elapsed-time observations, all co-matched
statuses, native retry settings and counters, and Pod readiness. Readiness-to-status
lag is bounded by observations; a short
observation window does not prove eventual convergence. Each scenario reports
capabilities as `pass`, `fail`, `unsupported`, or `not_tested`; JUnit remains the
source for overall test and cleanup outcomes.

The SDK's `associate-pods` operation classifies caller-supplied Pods. Its selectors
do not establish workload ownership. Live candidates are independently scoped
through controller owner UIDs, and foreign same-label Pod behavior is covered
offline. Namespace-wide workload discovery is not qualified by this operation.

## Coverage boundary

| Area | Suite coverage |
| --- | --- |
| JobSet and PyTorchJob extraction, marker mutation, native suspend fields, terminal rejection | Cluster-free checks against SDK built-ins and independent fixture expectations. |
| JobSet/default and KAI schedulers | Two- and six-Pod same-object lifecycle, live marker propagation, and native retry exhaustion scenarios. Exact observed results are recorded separately. |
| Pod association | SDK classification checked on independently owned candidates; namespace-wide discovery remains not tested. |
| Correlation labels and placement constraints | Selected settings of representative fixtures checked offline; marker propagation and scheduler/queue preservation checked live. Node exclusion remains not tested. |
| PyTorchJob/default and KAI schedulers | Master/Worker lifecycle, marker propagation, terminal-state and native retry scenarios. Exact observed results are recorded separately. |
| OOM, eviction, gang preemption | Not tested; separate failure and scheduler qualification remains necessary. |
| Checkpoint restore, training progress, recovery latency | Not tested; requires the subsequent full recovery path. |

Support claims must be scoped to the capabilities and exact versions in each
recorded run. CPU lifecycle success does not qualify GPU scheduling, gang behavior,
preemption, recovery services, or arbitrary controller releases. Missing prerequisites
and failed runs are retained as evidence, never converted into passed qualification.

## Initial observation (2026-11-16, before expanded coverage)

Two concurrent lifecycle scenarios passed on a single-node Kind cluster using
Kubernetes v1.35.0, JobSet v0.12.0, the default scheduler, and SDK v0.3.0.
The first run completed in 83.20 seconds; complete Pod drain took 30.7 and
32.9 seconds. All scenario JobSets, Jobs, and Pods were gone after cleanup.
These durations describe this CPU fixture, not a recovery-time benchmark.

The upstream JobSet definition was `jobset-x-k8s-io-jobset-v1alpha2`, with JSON
SHA-256 `bb36cd60991a3e2942a54b72db641f6669d8e66cdf8369e5d586f9e672cee627`.
Both workloads retained their parent UID while replacing both Pod UIDs.
Native `status.restarts` remained zero before and after suspend/resume, so
that counter cannot alone identify these execution boundaries. Three of four final
resumed snapshots across the two initial runs simultaneously matched `Initializing`
and `Running`; consumers must not assume normalized statuses are mutually exclusive. These are observations
of the recorded baseline, not guarantees for other versions.

## Expanded qualification results (2026-11-16)

All 12 live cases passed on the disposable Kind cluster, with Kubernetes v1.35.0,
Workload-Map SDK v0.3.0, JobSet v0.12.0, Training Operator v1.9.0
(`kubeflow/training-operator:v1-5170a36`), and KAI v0.12.0.

| Workload | Scheduler | Result | Retained local report |
| --- | --- | --- | --- |
| JobSet | Default | 3 passed | `run-PWLy5JQt` |
| PyTorchJob | Default | 3 passed | `run-VewU7E3f` |
| JobSet | KAI | 3 passed | `run-kcq6r2Bh` |
| PyTorchJob | KAI | 3 passed | `run-RmCjCjWm` |

Reports are under `reports/workload-map/` and include controller image IDs and
scheduler provenance. Earlier failed runs remain retained; a KAI installation
upgrade left the scheduler using a token bound to a replaced ServiceAccount UID.
The installer now refreshes the scheduler after installation, and both KAI suites
passed after that correction.

The matrix covers the lifecycle and native retry cases described above. PyTorchJob
also co-matched `Failed` and `Initializing` at a native terminal boundary. Status
sets must therefore be interpreted together with native evidence. All test
workloads, Jobs, Pods, PodGroups, and BindRequests were absent after cleanup.

Read-only inspection passed with administrator access and with a restricted
ServiceAccount allowed only to get JobSets. Requesting inaccessible controller
provenance returned `inconclusive` and a nonzero exit status; lifecycle capabilities
remained `not_tested`. Temporary RBAC and workload resources were removed.
These results do not extend the coverage boundary above.

## Local and CI checks

`make verify-workload-map` checks Go formatting, module consistency, lint, vet and
race tests, Python harness/inspection and launcher tests, Ruff, and shell syntax
quality. Install the repository-pinned Go tools with `make install-go-tools` first;
ShellCheck must be available. The cluster-free job in
`.github/workflows/pull-request-and-main.yml` runs the same target. Live runs remain
an explicit local/existing-cluster selection and are not automatically run in CI.

Inspection retains per-capability errors: a failed scale expression does not erase
successful status or template extraction. Transformations return the full workload
object. Live instrumentation requires an owned, suspended and drained workload,
checks the marker on every expected template, and rejects changes outside templates.
There is no recursive preservation validator, serializer exception list, or
normalization report. General serialization regression tests belong upstream.

## Native retry settings

| Workload | Settings under test | Independent terminal evidence |
| --- | --- | --- |
| JobSet | Per-Job `backoffLimit: 0`, Pod `restartPolicy: Never`, `failurePolicy.maxRestarts: 1` | JobSet `status.restarts` reaches one and the controller sets `Failed`. |
| PyTorchJob | Per-role `restartPolicy: Never`, `runPolicy.backoffLimit: 0`, `cleanPodPolicy: None` | A failed container produces the native terminal boundary. |
| PyTorchJob | Per-role `restartPolicy: OnFailure`, `runPolicy.backoffLimit: 1`, `cleanPodPolicy: None` | Container restart counts increase and the controller reports backoff-limit failure. The budget is summed across role Pods, so the test does not demand exactly one restart. |

Each terminal case requires SDK `Failed` and the probe's specific terminal refusal
for both suspend and resume. These are native restart policies, not normalized
Recovery attempt indices. A terminal workload remains outside active-workload
suspend/resume recovery.

## Recovery integration boundaries

The terminal refusal is **probe policy**, not an upstream SDK guarantee. The SDK
can change suspend fields on terminal objects; a Recovery caller must enforce its
own active-workload boundary before invoking it. Native `Failed` also does not
prove that Pods have stopped. Retained PyTorchJob evidence includes running Pods
at failure with `OnFailure`, and deletion/replacement activity around failure with
`Never`, despite `cleanPodPolicy: None`. Do not use that policy or a terminal
condition as a drain guarantee. The active lifecycle tests separately require
complete Pod drain before instrumentation and resume.

During a native JobSet restart, normalized `Initializing` and `Running` can overlap
even with no Ready Pods. Native conditions, Pod observations, and restart counters
remain necessary to interpret execution boundaries.

KAI lifecycle qualification checks `Scheduled` events emitted by `kai-scheduler` for each
exact owned Pod UID before and after resume, alongside Ready Pods and preserved
scheduler/queue configuration. It makes no assertion about PodGroup count,
queue quota enforcement, queue-position preservation, or gang behavior. Events
must be readable; absent evidence cannot produce a KAI pass. The original matrix
predates this event assertion. A fresh focused run on 2026-11-16 passed both
JobSet lifecycle cases (`run-ILUbfNRd`) and the PyTorchJob lifecycle case
(`run-jxjvHwPU`), retaining KAI events for all 22 original/replacement Pod UIDs.
The test namespace had no remaining JobSets, PyTorchJobs, Jobs, or Pods after
cleanup. Native retry/terminal scenarios do not assert scheduler events.

The tested installation uses the JobSet profile defaults, Training Operator's
v1.9.0 standalone overlay (`/manager`, without extra command-line flags), and
KAI's v0.12.0 chart defaults with `default-queue`. No additional controller flag
was needed for the observed native retries or suspend/resume; workload-level
settings are listed above. Training Operator also injects an `init-pytorch`
container using `alpine:3.10`. These are release/tag pins, not an immutable
supply-chain lock; recorded image IDs identify observed controller binaries.

Instrumentation coverage establishes marker propagation and checks selected user
settings on representative Recovery fixtures. Earlier exploration observed that
typed template round trips can omit explicit empty values; that observation is not
an upstream preservation guarantee or a local compatibility policy. Serializer
regressions should be reproduced and tested upstream rather than maintained as a
parallel validator here. Placement-constraint mutation remains unqualified;
preserving fixture constraints does not qualify composing new ones through the SDK.
