# Run and follow a training job

This guide is for training users on a Slurm cluster where an operator has
already deployed NVIDIA Recovery. It also applies to Slurm running through
Slinky: submit through your site's Slurm login environment as usual. NVIDIA
Recovery is experimental; agree on an evaluation workload with your operator
before relying on automatic recovery.

By the end, your job will have an explicit recovery policy, and you will know
how to find its attempts, recovery Advice, and audit events.

## What changes for your job?

You still submit a Slurm batch script. NVIDIA Recovery watches the resulting
executions and, when authorized, handles the scheduler work needed for a retry.
Your part is to choose an operator-approved recovery policy, make the expected
logs available, and ensure that rerunning your script loads a durable checkpoint.

For example, an overnight training run crashes after saving a checkpoint.
Instead of waiting for you to inspect and requeue it, the service can detect
the failure, decide whether another attempt is allowed, and restart the job.
The new attempt still waits for Slurm resources, then your application loads
the checkpoint. If policy prevents another retry, the history explains where
recovery stopped.

The same model covers configured hang signals and sustained throughput
slowdowns, not just jobs that exit. It requires the progress messages or
throughput measurements expected by the detectors. Ask which detectors and
recovery policies your operator has enabled; straggler-triggered recovery is
disabled by default.

## Before submitting

You need a working Slurm batch script and permission to submit to the selected
partition. Obtain these details from the operator:

- The exact NVIDIA Recovery environment name and the partitions it manages.
- The output directory collected by the site's logging pipeline and any
  required output settings in your batch script.
- The dashboard URL and access method; optionally, the public API Gateway URL
  and credentials for read-only inspection.
- Whether this workload should use `observe` or `act`, and the site's retry
  limits and checkpoint policy.

If those are not available yet, the operator should begin with
[installation](../operators/installation.md). Cluster deployment and log
collection are operator responsibilities.

## Add an explicit job policy

Start from your existing training script. Add these directives before its
first executable command, replacing `customer-slurm` with the exact environment
name and `training-example` with your workflow identifier:

```bash
#SBATCH --requeue
#SBATCH --comment='{"APS":{"workflow_id":"training-example","environment":"customer-slurm","mode":"observe","auto_resume_mode":"requeue","max_requeue_times":1}}'
```

Keep your existing resource requests and training command. Set `#SBATCH --output`
and `#SBATCH --error`, if used, to paths the operator's collector can read.
Adding a comment alone does not make application logs available to NVIDIA
Recovery.

The JSON object is the Advisor Policy Spec (APS). The wrapper is mandatory;
an ordinary comment or an unwrapped policy does not enroll the job. Use the
uppercase `APS` key; `HEIMDALL` is also accepted for compatibility. `workflow_id`
groups the job's workflow history. The environment must match the operator's routing policy;
named environments use exact, case-sensitive values. A malformed comment or
an environment mismatch can keep the job out of the recovery path.

`--requeue` explicitly makes the batch job eligible for Slurm requeue; omitting
it follows the site's `JobRequeue` default. Slurm requeue runs the batch script
from the beginning with the same job ID. See the
[Slurm submission reference](https://slurm.schedmd.com/sbatch.html).

Choose the mode deliberately:

| APS mode | Slurm Advisor Controller behavior |
| --- | --- |
| `ignore` | Skips publication and recovery actions for this job. |
| `observe` | Publishes job state and evaluates recovery without issuing scheduler actions. |
| `act` | Requests recovery actions, subject to the operator's action ceiling and the remaining safety checks. |

An explicit `act` request cannot enable the operator's independent
`SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS` ceiling. When acknowledgment is
false, the controller resolves `act` to `observe`. The process's default mode
alone is not that ceiling. Keep the example in `observe` until your operator
has enabled and validated the action path; then change the job's `mode` to
`act` when submitting a workload intended for automatic recovery.

`auto_resume_mode: requeue` selects the recovery policy; it does not authorize
scheduler actions by itself. `max_requeue_times: 1` requests at most one retry,
and the operator can impose a lower limit. The separately configured watchdog
has its own action controls and may release eligible stale holds; ask the
operator about the policy on your partition.

By default, each attempt must write to a unique log path. This lets NVIDIA
Recovery use the filename to associate checkpoint and anomaly events with the
correct attempt, while protecting against stale paths that Slurm can briefly
return during requeue. If the workload intentionally writes every attempt to
the same file, add `"log_file_paths_shared_across_attempts":true` to the `APS`
object. Job Manager will retain that shared path for every attempt; events
without a trustworthy `slurm_job_attempt` label require a source timestamp
within a recorded attempt's start/end times. A filename shared by retries does
not identify an attempt. Missing or ambiguous time evidence waits for registration
through the existing bounded redelivery path; it never defaults to a newer retry.

Preserve original source timestamps when collecting or replaying shared logs,
and emit `slurm_job_attempt` where possible. If a collector replaces an old
record's timestamp with ingestion time, NVIDIA Recovery cannot infer its original
attempt. Keep clocks synchronized and prefer separate attempt log paths when
source time or attempt labels are unavailable. Restart replay preserves the
identity of the same timestamped source event; this does not promise exactly-once
delivery.

## Supply progress messages for hang detection

The default no-output detector (`noprogress`) starts tracking a job/log file
only after it sees a configured progress marker. Representative markers in
the [Watcher values](../../deploy/helm/watcher/values.yaml) are:

| Phase | Recognized output examples | Default silence threshold |
| --- | --- | --- |
| Pre-training | `before the start of training step`, `using world size` | 960 seconds |
| Training loop | `throughput per GPU`, `approx flops per GPU`, `on_train_start` | 840 seconds |

These are actual query matches, not generic heartbeats: printing only
`training started` will not arm the default detector. The operator can adapt
the provider's `global.log_query.providers.<backend>.queries.watcher.workflows.noprogress`
queries to the application's real messages. A job that never prints a matching
marker is not covered by this no-output mechanism.

When a timer expires, Watcher checks that Job Manager reports the job as
running and that the tracked filename appears in its reported log paths, then
checks for recent job output.
With the default validation query, any recent output suppresses the anomaly;
a stalled training loop that keeps printing unrelated messages may not be
detected. Silence alone does not guarantee a recovery action.

Tune each job explicitly by merging this object into its existing `APS` policy:

```json
{
  "anomaly_detection_config": {
    "log_based_timeout_enabled": true,
    "log_based_timeout_threshold_pretraining": 960,
    "log_based_timeout_threshold_trainingloop": 840
  }
}
```

Thresholds are positive integer seconds. Valid APS policies supply these
defaults even when the fields are omitted, so changing only Watcher's workflow
timeouts may not change an opted-in job's effective thresholds. Setting
`log_based_timeout_enabled` to `false` suppresses this no-output anomaly for
that job; it does not disable other detectors.

Allow time for initialization, checkpoint loading/saving, expected quiet phases,
and log ingestion. Flush progress lines promptly to the collected file;
buffered output can look like a hang. Watcher's default query delay is another
60 seconds. Short thresholds detect silence sooner but increase false alarms;
long thresholds tolerate quiet work but delay detection. Agree on the phase
markers and thresholds with the operator before requesting action mode.

## Submit and confirm registration

Submit your prepared script and retain its Slurm job ID:

```bash
JOB_ID=$(sbatch --parsable train.sbatch)
JOB_ID=${JOB_ID%%;*}
squeue -j "$JOB_ID"
scontrol show job "$JOB_ID"
```

While Slurm retains the job, inspect the stored `Comment` and requeue setting
in `scontrol` output. For completed jobs, consult accounting:

```bash
sacct -j "$JOB_ID" --format=JobID,JobName,State,ExitCode
```

Open the dashboard URL supplied by the operator and go to **Workflows
Overview** (`/dashboard/workflow` relative to the dashboard base). Find your
workflow or job ID and open an attempt. Initial execution is attempt `0`;
Slurm requeue keeps the job ID and increments its attempt index.

Expected result: the job and its initial attempt appear after the controller
publishes them. A successful job or an observe-mode registration check does
not prove the recovery path. A failure-free job also need not produce recovery
Advice.

## Follow attempts, Advice, and audit events

Follow the run by asking three questions:

- **What happened?** Find the failed attempt and its detected issue.
- **What was decided?** Advice records whether that attempt should be restarted
  or terminated, including any placement constraints supported by the evidence.
- **Did it happen?** Find the subsequent attempt, then check the application's
  own checkpoint-load and training-progress messages.

The workflow groups the history; each attempt records one execution. Keeping
the failed execution separate from its replacement lets you understand why
the retry happened without losing the original evidence.

The attempt's **Job Details** page shows its state and recorded checkpoint
information. When the operator enables audit views, **Timeline** shows that
attempt's events and **Audit Events** provides the broader audit view. These
views help distinguish detection, policy decisions, and scheduler activity;
the presence of an event alone is not proof that a retry ran.

In **Audit Events**, enter a job ID to see events across all its attempts, or
`job_id/attempt_index` to inspect one attempt, including attempt `0`. Combine
this with the component, exact event type, and level filters. Applying or
resetting filters starts at the first page; the URL retains the applied filters
for sharing.

### Inspect the API when you need the exact records

For API inspection, use the operator-provided public Gateway base URL,
including any configured URL prefix. Obtain `RECOVERY_ACCESS_TOKEN` through
the site's identity provider. The following examples use the same interface
as the [Gateway reference](../public-api/api-gateway.md):

```bash
export RECOVERY_HTTP_URL=https://recovery.example.com/mission-control/recovery-engine-api
curl --fail-with-body \
  --header "Authorization: Bearer $RECOVERY_ACCESS_TOKEN" \
  "$RECOVERY_HTTP_URL/v1/jobs/by-scheduler-id/$JOB_ID"

curl --fail-with-body --get "$RECOVERY_HTTP_URL/v1/advice" \
  --header "Authorization: Bearer $RECOVERY_ACCESS_TOKEN" \
  --data-urlencode 'limit=100'
```

The jobs response lists matching attempts. Advice records identify `jobId`,
`attemptIndex`, and `heimdallJobId`; match the exact attempt you are inspecting.
Advice expresses desired state and node exclusions. Its reconciliation status
records the controller's observation: `CONVERGED` means the desired scheduler
state was observed, while `ABANDONED` means that generation cannot converge.
See [how Advice works](../concepts/advice.md).

Both endpoints shown here paginate. Pass `nextPageToken` back as the `page`
query parameter for either endpoint. Advice listing has no job-ID query filter;
do not interpret the absence of a match on one page as absence of Advice.
Omitting `onlyActive` retains resolved records in the listing; setting
`onlyActive=true` limits it to Advice still eligible for reconciliation.

## Understand retries and checkpoints

Automatic recovery may requeue a failed job into a held state, apply node
exclusions, and release it when the controller is authorized to act. A retry
still waits for Slurm scheduling and available resources. Recovery does not guarantee
completion or diagnose every failure. Private NVIDIA Attribution is optional
and disabled by default; its absence does not disable the public recovery path.

The effective retry limit is bounded by the site's policy and the job's
requested limit. Site policy can also stop retries after qualifying failures
without a recorded checkpoint. Ask the operator which limit or early-stop
condition applied when a job stops retrying.

Your training application must create checkpoints in durable storage and load
them when its batch script restarts. NVIDIA Recovery can record recognized
checkpoint log events and use that evidence in retry policy; it does not
create checkpoint files, copy model state, or implement your training
framework's resume logic. A checkpoint shown in the dashboard records a
detected event, not a validation that the saved model can be loaded.

## If the job does not appear or recover

Check the stored APS comment, exact environment, selected partition, and
whether output uses the collector's configured directory. For a visible job,
check its mode, retry count, latest attempt state, and available Advice or audit
events. A pending retry can be waiting on normal Slurm scheduling.

Contact the operator with the job ID, workflow ID, attempt index, timestamps,
sanitized batch directives, and the relevant dashboard or API observations.
Ask them to check environment admission, action acknowledgment, log ingestion,
and retry policy using the [troubleshooting guide](../operators/troubleshooting.md).
Keep credentials and private data out of shared logs.

Next, read [jobs, workflows, and attempts](../concepts/jobs-and-workflows.md)
to understand the history across retries, or coordinate an operator-approved
recovery validation through the [operations guide](../operators/operations.md).
