# Workload-Map qualification experiment

This is the first implementation of
[ADR 0058](../../docs/decisions/0058-qualify-workload-map-independently.md).
It exercises upstream Workload-Map directly without Advice Operator, a gateway,
or Recovery backend services. It is a test tool, not a supported recovery
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

The probe reads exactly one JSON Kubernetes object from stdin. `inspect` returns
identity, normalized statuses, component replica counts and Pod labels; it omits
container commands, environment and the full manifest. This is an observation,
not a qualification verdict. Unknown definitions, malformed input and extraction
errors fail the command. Missing suspend support is reported explicitly.

`instrument` adds `qualification.nvidia.com/marker` through the SDK's template
API, preserving existing values and refusing a conflicting marker or a lossy
template round trip. `suspend` and `resume` exercise the SDK root lifecycle API.
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

The scenario uses the existing staged runner and JobSet launcher. Each case
creates its own uniquely named JobSet with two CPU Pods, compares SDK identity
and scale with independently observed resources, then suspends through the SDK.
It waits for every original Pod UID to disappear, including terminating Pods
whose intermediate Job owner has disappeared. It observes a suspended interval,
resumes through the SDK, and requires two new Ready Pod UIDs under the original
JobSet name and UID. A successful API write is not a passed lifecycle check.

Writes preserve the original UID and resourceVersion, and cleanup uses the
launcher's exact-object ownership checks. The runner creates no cluster or
namespace and installs no controllers. Use a dedicated test namespace with
permission to create/delete JobSets, list their Jobs and Pods, update owned
JobSets, and read diagnostics. The initial environment-report collector expects
`jobset-controller-manager` in `jobset-system`; supporting arbitrary installations
and restricted inspection roles remains follow-up work.

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

## Coverage boundary

| Area | Initial evidence |
| --- | --- |
| JobSet and PyTorchJob extraction, marker mutation, native suspend fields, terminal rejection | Cluster-free checks against SDK built-ins and independent fixture expectations. |
| JobSet/default scheduler | Live same-object suspend/drain/resume scenario; exact run results are recorded separately. |
| Pod association | Live evidence uses launcher UID ownership, not Workload-Map selectors; selector qualification remains not tested. |
| Correlation labels and placement constraints | Template preservation checked offline; live propagation and node-exclusion behavior remain not tested. |
| PyTorchJob controller and KAI | Not tested by this initial live suite. |
| OOM, eviction, native restart exhaustion, gang preemption | Not tested; required later qualification cases. |
| Checkpoint restore, training progress, recovery latency | Not tested; requires the subsequent full recovery path. |

Passing this experiment does not qualify all four workload/scheduler combinations.
The next increments should add live instrumentation, Workload-Map Pod selectors,
native state/restart cases, PyTorchJob and KAI profiles, and a structured capability
summary distinguishing pass, fail, unsupported and not tested.

## Initial observation (2026-11-16)

Two concurrent lifecycle scenarios passed on a single-node Kind cluster using
Kubernetes v1.35.0, JobSet v0.12.0, the default scheduler, and SDK v0.3.0.
The first run completed in 83.20 seconds; complete Pod drain took 30.7 and
32.9 seconds. All scenario JobSets, Jobs, and Pods were gone after cleanup.
These durations describe this CPU fixture, not a recovery-time benchmark.

The upstream JobSet definition was `jobset-x-k8s-io-jobset-v1alpha2`, with JSON
SHA-256 `bb36cd60991a3e2942a54b72db641f6669d8e66cdf8369e5d586f9e672cee627`.
Both workloads retained their parent UID while replacing both Pod UIDs.
Native `status.restarts` remained zero before and after suspend/resume, so
that counter cannot alone identify these execution boundaries. One resumed
snapshot simultaneously matched `Initializing` and `Running`; consumers must
not assume normalized statuses are mutually exclusive. These are observations
of the recorded baseline, not guarantees for other versions.
