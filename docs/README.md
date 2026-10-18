# NVIDIA Recovery documentation

When a training run fails, you need to know what happened, whether another
attempt makes sense, and whether it actually started. These guides follow that
path from a local demonstration to running and operating recovery on your
cluster. New to the project? Start with [why NVIDIA Recovery exists](../README.md).

| I want to… | Start here | What I will learn |
| --- | --- | --- |
| Evaluate recovery locally | [Try the local demo](getting-started.md) | Watch a simulated failure produce Advice and a restarted Slurm attempt. |
| Run a job on an installed service | [Run jobs](users/running-jobs.md) | Opt in, follow attempts, and understand recovery decisions. |
| Deploy to my infrastructure | [Install NVIDIA Recovery](operators/installation.md) | Build images, install the backend, and connect Slurm or Slinky. |
| Change or contribute to the project | [Set up development](development/getting-started.md) | Find the owning component and use its edit/test loop. |
| Understand the design | [Architecture](../ARCHITECTURE.md) | Follow a job from submission through observed recovery. |

## Understand the model

The model separates the work you want to finish from the executions and
decisions along the way:

- [Jobs, workflows, and attempts](concepts/jobs-and-workflows.md): follow one
  training run without losing the history of a failed execution.
- [Advice](concepts/advice.md): understand what should happen next and why that
  decision is recorded separately from a scheduler command.
- [Scheduler integration](concepts/scheduler-integration.md): see how a
  controller turns that intent into actions and checks their outcome.

These explain how recovery decisions stay separate from scheduler commands.
The [FAQ](faq.md) covers fit, requirements, and limitations.

## Operate and troubleshoot

After the shared installation, follow the guide for [conventional Slurm](operators/slurm.md)
or [Slinky](operators/slinky.md). Continue with [validation and operations](operators/operations.md)
before enabling actions. Use [troubleshooting](operators/troubleshooting.md)
when jobs, signals, or recovery actions are missing.

Detailed references remain available:

- [Helm charts](../deploy/helm/README.md) and their component configuration.
- [Log-query backends](ops/configure-log-query-backends.md) and
  [Slurm log collection](ops/loki-slurm-runai-logging.md).
- [Slurm stdout path discovery](operators/slurm.md#log-path-discovery), including
  active and finalized accounting fallback, supported patterns, and retry behavior.
- [API Gateway HTTP and gRPC](public-api/api-gateway.md).
- [NVIDIA Attribution](public-api/optional-attribution.md) and its
  [Adapter HTTP contract](public-api/attribution-adapter.md).
- [Slurm Advisor Controller internals](architecture/slurm-advisor-controller.md).
- [Testing](development/testing.md), [engineering quality](quality/go-engineering.md),
  and [architecture decisions](decisions/README.md).

## Participate and get help

Read [Contributing](../CONTRIBUTING.md) for the contribution and CI approval
process, [Support](../SUPPORT.md) for help, and [Security](../SECURITY.md) for
private vulnerability reporting. [Governance](../GOVERNANCE.md),
[Maintainers](../MAINTAINERS.md), the [Code of Conduct](../CODE_OF_CONDUCT.md),
and the [release policy](../RELEASE.md) define the project's policies.

NVIDIA Recovery is experimental and not ready for production use. Attribution
is enabled by default and currently requires authorized upstream source access.
The [local tutorial](getting-started.md) explicitly selects degraded OFF mode
to evaluate recovery without private provider credentials.
