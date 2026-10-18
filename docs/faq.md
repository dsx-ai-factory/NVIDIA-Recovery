# Frequently asked questions

For infrastructure engineers and workload owners evaluating NVIDIA Recovery.
No installation is required to read this page. Start with the
[overview](../README.md) if the project is new to you.

## When would I use NVIDIA Recovery?

Evaluate it when distributed training failures require repeated manual
investigation and intervention: a run crashes overnight, remains allocated
without making progress, or develops a sustained throughput slowdown. Recovery
connects those symptoms to a bounded retry policy and scheduler action, with
a history you can inspect. It is most useful when your workload can resume
from checkpoints and your operators can supply the expected telemetry.

## How is this different from automatically retrying a job?

A scheduler retry can restart an exited job, but an allocated job may be stuck
or slow without exiting. NVIDIA Recovery detects configured training symptoms,
uses job history and policy to decide what should happen, and records that
decision as [Advice](concepts/advice.md). The controller then acts and checks
whether the requested scheduler state was reached. Policy bounds retries;
available attribution evidence can identify nodes to exclude from the retry.
It does not guarantee that every failure is diagnosed or that a retry will succeed.

## Which schedulers can I use?

The implemented integration is Slurm, including Slurm-on-Kubernetes through
Slinky, for NVIDIA GPU clusters from Ampere through Blackwell. Slinky runs
Slurm; it does not make this a Kubernetes-native Job or JobSet recovery service.
The [scheduler-neutral Advice contract](concepts/scheduler-integration.md)
provides an integration boundary, not additional shipped adapters. See the
[installation requirements](operators/installation.md) for version constraints.

## What kinds of failure can it detect?

Watcher recognizes configured crash and hang patterns, missing expected
progress, and sustained drops in reported training throughput. A straggler
signal here means throughput degradation over time, not automatic identification
of the slowest worker. Detection and recovery policy are separate:
straggler-triggered recovery is disabled by default and must be enabled by
the operator. Watcher detectors depend on the messages and measurements the
deployment is configured to collect.

## Does NVIDIA Recovery create or restore checkpoints?

No. The workload writes checkpoints and loads them on a later attempt. NVIDIA
Recovery can observe configured checkpoint signals and use that history in
recovery policy. A restarted Slurm attempt does not prove that the application
loaded a checkpoint or completed training. See [jobs and attempts](concepts/jobs-and-workflows.md).

## What telemetry does it need?

The controller publishes scheduler state. Watcher additionally needs application
logs collected into a supported query backend, with the labels and messages its
configured detectors expect. Job Manager can also detect crashes from failed
scheduler state, including a delayed fallback when no actionable recovery
anomaly was recorded; this does not detect a still-running job's hang or
throughput slowdown.
Node/system logs are a separate evidence source.
An arbitrary log stream is not automatically a useful detector input. Start with
the [installation guide](operators/installation.md) and [log-query configuration](ops/configure-log-query-backends.md).

## Can I use it without private NVIDIA Attribution?

Yes, with explicit degraded OFF configuration. Attribution is enabled by default
and currently needs authorized upstream source access. With OFF selected, the
recovery pipeline, including Anomaly Detection Isolator, works without its
source or credentials. Public detectors recognize configured symptoms in
application logs, including PyTorch, NCCL, CUDA, and NVLink error patterns;
recognizing a symptom is not the same as establishing its root cause.
NVIDIA Attribution adds diagnosis using application and node evidence.
Without it, policy-driven recovery continues, but private root-cause diagnosis
and provider-derived placement exclusions are unavailable. See
[Attribution configuration](public-api/optional-attribution.md).

## Can I observe jobs without allowing scheduler changes?

Yes. The controller supports ignore, observe, and act modes. Acting also requires
operator acknowledgement and passing routing, ownership, and attempt checks;
a job-level opt-in does not bypass those controls. Follow the
[operator validation sequence](operators/operations.md) before enabling actions
on a shared cluster. The [local tutorial](getting-started.md) explicitly enables
actions only in its disposable evaluation cluster.

## Does it drain nodes or repair GPUs?

The documented recovery loop manages workloads. A node exclusion in Advice
constrains placement of that job; it is not a global node drain or hardware
repair. Infrastructure health and repair remain separate operator concerns.

## Are prebuilt images available? Do I need a GPU to try it?

The project distributes source and Helm charts, not prebuilt service images or
executables. Operators build images for their registry. The
[local demo](getting-started.md) uses a synthetic shell workload and needs no
physical GPU, but it still builds and runs a multi-node Kubernetes/Slurm stack.
It is not a lightweight single-process demo. See [release policy](../RELEASE.md).

## Is it production-ready?

No. NVIDIA Recovery is experimental, and interfaces and behavior may change
without compatibility guarantees before a stable release. Passing a local demo
establishes that scenario's behavior, not production scale, availability, or
recovery-time guarantees. See [Support](../SUPPORT.md).

## Where should I go next?

Choose [Try locally](getting-started.md), [Run jobs](users/running-jobs.md),
[Deploy and operate](operators/installation.md), or [Develop](development/getting-started.md).
Use [troubleshooting](operators/troubleshooting.md) for symptoms and
[Support](../SUPPORT.md) for help. Remove secrets and sensitive information before
sharing logs; report vulnerabilities through [Security](../SECURITY.md).
