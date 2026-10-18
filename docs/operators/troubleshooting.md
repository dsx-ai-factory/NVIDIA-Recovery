# Troubleshoot an evaluation

For the operator diagnosing an installed NVIDIA Recovery deployment. Keep the
[installation variables](installation.md#set-shared-variables), deployed values,
source revision, exact Slurm allocation ID, and attempt index at hand. These
checks locate the missing stage; they do not authorize scheduler actions.
If an action canary behaves unexpectedly, first use
[rollback to observe](operations.md#roll-back-to-observe-mode) for both processes.

Start with the symptom:

| Symptom | Inspect first |
| --- | --- |
| Job missing from the API/dashboard | [Job discovery](#the-controller-does-not-show-the-job) |
| Job visible, failure signal missing | [Collection and Watcher](#watcher-is-healthy-but-finds-no-logs) |
| Signal exists, advice missing | [Advice pipeline](#a-signal-has-no-advice) |
| Advice exists, scheduler does nothing | [Action gates](#advice-does-not-produce-an-action) |
| Retry or release was unexpected | [Both mutation paths](#an-unexpected-retry-or-release-occurs) |
| Install or readiness failed | [Installation problems](#installation-problems) |

## Watcher is healthy but finds no logs

Recheck the full path:

1. the job writes to the directory Alloy actually tails;
2. Alloy can read newly created files — job output can have
   restrictive permissions; check existing files and the directory's default
   ACLs ([collector requirements](installation.md#collector-requirements));
3. the stream labels match `query_prefix` exactly;
4. `job_id` is queryable and `filename` preserves the collected source path;
5. tenant, authentication, and TLS settings agree on push and query paths; and
6. the marker is returned by a direct Loki query over the correct time window.

For missing scheduler log paths, check [Slurm log-path discovery](slurm.md#log-path-discovery).
Check the log-discovery mode selected at startup. Before Slurm 25.05, SAC parses
SubmitLine during active background enrichment and finalized metadata refresh;
newer or unknown clients use JSON only. Discovered active-step paths are
published on a subsequent poll and become eligible for optional log links.
For legacy discovery, check the recorded output option and supported patterns;
for JSON discovery, check `stdout_expanded`. Inspect SAC's existing
`Slurm log metadata refresh failed` warnings for query or parsing failures.
Valid and cached paths remain available while unresolved sources retry.

## The controller does not show the job

```bash
scontrol show job "$JOB_ID" | grep 'Comment='
squeue --json | grep -E "\"job_id\":$JOB_ID|\"job_id\": $JOB_ID"
```

Check the exact APS environment against the controller's routing configuration.
Confirm the comment is valid JSON containing an object-valued `APS` or
`HEIMDALL` key and the controller can reach the API Gateway.

The controller's `partitionAllowlist` (`SLURM_ADVISOR_PARTITION_ALLOWLIST` for
native Slurm) restricts recovery actions, not job discovery or observed-job
updates. An opted-in job with a matching environment can appear in Heimdall
outside that allowlist. Keep the action allowlist scoped to approved recovery
partitions while investigating missing jobs. See the
[native controller reference](../../deploy/systemd/slurm-advisor-controller/README.md).

For conventional Slurm, set `SLURM_ADVISOR_VERSIONED_SQUEUE_JSON=false` unless
`squeue --json=v0.0.40` works with the host's installed data-parser plugin. If
this flag is set incorrectly the controller will fail to parse `squeue` output
and will not publish any jobs to NVIDIA Recovery.

Read SAC's logs using the command for its deployment.

For Slinky/Kubernetes:

```bash
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  logs deployment/"$SLINKY_CONTROLLER_RELEASE" \
  -c slurm-advisor-controller \
  --since=5m
```

For a colocated native SAC running as a systemd service on the Slurm host:

```bash
sudo journalctl -u slurm-advisor-controller.service --since=-5m --no-pager
```

SAC logs `API Gateway connection established` at INFO with `heimdall_endpoint`
when its gRPC connection reaches `Ready`. This message is emitted once per
process. It confirms the transport is ready but does not confirm authorization
or job publication.

Inspect `controller sync failed` entries and their `error` field for Slurm
polling/parsing or API Gateway RPC failures.

Query the production jobs API for the captured allocation to verify publication:

```bash
API_GATEWAY_PROXY="/api/v1/namespaces/${RELEASE_NAMESPACE}/services/http:${RELEASE_NAME}-api-gateway:8080/proxy"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  get --raw "$API_GATEWAY_PROXY/v1/jobs/by-scheduler-id/$JOB_ID" | jq .
```

For an allocation that has left `squeue`, inspect `sacct --duplicates` and the
controller's accounting logs. See [environment routing and observe checks](operations.md#observe-a-first-job).

## A signal has no advice

Follow the same job and attempt through Watcher, Anomaly Detection Isolator,
and Job Manager. Check RabbitMQ connectivity, event publication/consumption,
backend readiness, and the matching environment. A healthy Pod alone does not
prove the event was processed. Attribution is enabled by default: inspect
provider and adapter health and structured request errors. Absence of diagnosis
is expected only in explicitly degraded Attribution OFF mode.

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" logs deployment/"$RELEASE_NAME-watcher" --since=20m
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  get --raw "$API_GATEWAY_PROXY/v1/advice?limit=250" | jq .
```

Set `API_GATEWAY_PROXY` using the preceding section if this is your entry point.
Follow `nextPageToken` for deployments with more than one page; absence from the
first page does not prove absence of advice. Use the exact attempt index, not
only the workflow name. Inspect the Isolator and Job Manager Pod logs using
names returned by `kubectl get pods`; review event-bus credentials and service
endpoints against the deployed values. The [controller canary](operations.md#run-a-disposable-recovery-canary)
shows the evidence required for a complete failure-to-advice path.

## Advice does not produce an action

Check [the action boundary](operations.md#validate-and-enable-scheduler-recovery):
process mode, scheduler-action acknowledgement, job APS mode, environment,
partition allowlist, scheduler identity, and job requeue eligibility. The
native units pin observe mode in `ExecStart`; editing only their environment
files does not enable actions.

Advice also must match the current attempt and satisfy freshness/eligibility
checks. Inspect controller logs and the exact Slurm accounting records.
An aggregate success metric cannot prove that this particular allocation was
released. Follow the [canary accounting check](operations.md#run-a-disposable-recovery-canary).

## An unexpected retry or release occurs

Return both controller and watchdog to observe mode, then retain the job's APS
comment, accounting records, advice, controller logs, and watchdog logs before
making changes. Determine which process issued the action. Controller recovery
and watchdog fail-open are independent.

An explicit APS `mode: observe` or `ignore` suppresses both controller recovery
and watchdog release for that job once each process observes the policy.
Inspect the watchdog's environment pattern,
partition allowlist, held-state eligibility, and release timeout. The watchdog
can release normal managed jobs and DAG retries; it never releases held DAG
attempt zero. Review the configured requeue limit and every attempt's restart
count before resubmitting anything. See the
[watchdog qualification](operations.md#run-a-watchdog-fail-open-canary).

## A canary remains held

First return both mutation paths to observe mode. Identify the exact captured job
and inspect its advice and controller logs. If the scheduler operator decides
to release it manually:

```bash
scontrol release "$JOB_ID"
```

Never run broad release or cancellation commands against an unfiltered queue.

## Installation problems

### An image cannot be pulled

```bash
export POD_NAME='replace-with-the-failing-pod-name'
export SERVICE_NAME='replace-with-the-NVIDIA Recovery-service-name'
export TARGET_KUBE_CONTEXT="$HEIMDALL_KUBE_CONTEXT"
export TARGET_NAMESPACE="$RELEASE_NAMESPACE"

kubectl --context "$TARGET_KUBE_CONTEXT" \
  -n "$TARGET_NAMESPACE" \
  describe pod "$POD_NAME"
kubectl --context "$TARGET_KUBE_CONTEXT" \
  -n "$TARGET_NAMESPACE" \
  get secret heimdall-registry
docker buildx imagetools inspect "$IMAGE_PREFIX/$SERVICE_NAME:$IMAGE_TAG"
```

For an Advisor Controller Pod, set `TARGET_KUBE_CONTEXT` to
`$SLINKY_KUBE_CONTEXT` and `TARGET_NAMESPACE` to `$SLINKY_NAMESPACE`.

Confirm the Secret is in the release namespace, its server matches the image
hostname, and the service image was published under the expected child
repository.

### Helm reports missing or stale dependencies

```bash
helm dependency update deploy/helm/nvidia-recovery
helm lint deploy/helm/nvidia-recovery \
  --values deploy/helm/nvidia-recovery/values-quickstart.yaml \
  --values nvidia-recovery-values.yaml
```

For the standalone Slinky controller:

```bash
helm lint deploy/helm/slurm-advisor-controller \
  --values slinky-values.yaml
```

### A Pod is not ready

```bash
export POD_NAME='replace-with-the-failing-pod-name'
export TARGET_KUBE_CONTEXT="$HEIMDALL_KUBE_CONTEXT"
export TARGET_NAMESPACE="$RELEASE_NAMESPACE"

kubectl --context "$TARGET_KUBE_CONTEXT" \
  -n "$TARGET_NAMESPACE" \
  get pods
kubectl --context "$TARGET_KUBE_CONTEXT" \
  -n "$TARGET_NAMESPACE" \
  describe pod "$POD_NAME"
kubectl --context "$TARGET_KUBE_CONTEXT" \
  -n "$TARGET_NAMESPACE" \
  logs "$POD_NAME" --all-containers --tail=200
kubectl --context "$TARGET_KUBE_CONTEXT" \
  -n "$TARGET_NAMESPACE" \
  get events \
  --sort-by=.metadata.creationTimestamp
```

Check dependency DNS, network policies, endpoint URLs, TLS trust, Secret names
and keys, database migrations, and resource pressure. A running dependency Pod
does not prove that an application can use it.

### Slinky cannot load Slurm configuration

Inspect the `prepare-slurm-key` init container and `sackd` sidecar:

```bash
export CONTROLLER_POD='replace-with-the-controller-pod-name'

kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  logs "$CONTROLLER_POD" -c prepare-slurm-key
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  logs "$CONTROLLER_POD" -c sackd
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  get secret slurm-auth-key
```

Confirm the Secret contains the `slurm.key` key, the config server is correct,
and all Slinky components use compatible Slurm releases.

## Result and next step

You should be able to name the first missing stage and its evidence. Correct
the relevant configuration, repeat the same scoped check, and return to
[observation](operations.md#observe-a-first-job). Re-enable actions only through
the reviewed canary sequence. When reporting an issue, include the source
revision, scheduler baseline, redacted effective configuration, exact job and
attempt IDs, and relevant logs; omit credentials and private workload data.
