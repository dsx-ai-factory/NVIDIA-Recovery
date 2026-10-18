# Operate and validate recovery

For the Slurm operator verifying a deployed NVIDIA Recovery evaluation. Complete
[installation](installation.md) and either [Slurm](slurm.md) or [Slinky](slinky.md),
and retain the shared shell variables and values files. Run scheduler commands
on a Slurm submission host and kubectl/Helm commands on the operator workstation.
If those are different shells, transfer captured allocation IDs explicitly.

The first result is an opted-in job visible through the API and dashboard while
both mutation paths remain disabled. Later sections require scheduler review:

1. [Observe a first job](#observe-a-first-job).
2. [Stage and prove controller recovery](#validate-and-enable-scheduler-recovery)
   on a dedicated canary partition.
3. [Enable both processes](#promote-both-controller-and-watchdog-to-act-mode)
   and [prove watchdog fail-open](#run-a-watchdog-fail-open-canary).
4. Keep [rollback](#roll-back-to-observe-mode) available throughout evaluation.

For later tasks, [configure external OTLP](#optional-external-otlp-collector)
or [remove the evaluation](#remove-the-evaluation). The scheduler guides retain
the initial [Slurm](slurm.md) and [Slinky](slinky.md) setup procedures.

A successful canary proves that tested path and configuration; it does not
establish production readiness. See [production qualifications](#before-production).

## Slurm job ID reuse

NVIDIA Recovery identifies a job attempt by its numeric Slurm job ID and
attempt number. Reusing a job ID for a different submission while retaining
Recovery database state is unsupported. This includes ID wraparound and
administrative resets that allow IDs from purged jobs to be assigned again.
Purging Slurm records does not clear the corresponding Recovery records or
Advice: a new job can inherit an old job's recovery instruction and be requeued
even when healthy.

Before job IDs wrap or are reset for reuse, stop the Advisor Controller and
watchdog and reinitialize the NVIDIA Recovery database as part of a coordinated
maintenance operation. Preserve any required history in a backup before
reinitialization, and resume recovery only after the fresh database is ready.
Restarting the controller or purging Slurm accounting alone is insufficient.

This limitation concerns different submissions sharing a numeric job ID;
normal requeues of the same submission use separate attempt numbers.

## Observe a first job

Choose an existing, short training script that is safe to run once. Call it
anything you like — `your-training-job.sbatch` is used as a placeholder below.

Add these directives near its other `#SBATCH` lines, replacing the environment
with the exact value selected during installation:

```bash
#SBATCH --requeue
#SBATCH --comment='{"APS":{"workflow_id":"nvidia-recovery-getting-started","environment":"customer-slurm","mode":"observe","auto_resume_mode":"requeue","max_requeue_times":1}}'
```

Keep `#SBATCH --requeue` in this example to make the batch job eligible for
requeue independently of the site's `JobRequeue` default. Omitting it follows
that default; it does not universally disable requeue. See the
[Slurm submission reference](https://slurm.schedmd.com/sbatch.html#OPT_requeue).

The `--comment` value contains the **Advisor Policy Spec (APS)** — a JSON
object that opts this job into NVIDIA Recovery's recovery system. The `APS` key name
is case-sensitive. The fields mean:

- `workflow_id` is the stable name shown in NVIDIA Recovery. Use the same value for
  related attempts of one training workflow.
- `environment` routes the job to a controller whose
  `SLURM_ADVISOR_ENVIRONMENT` has the same exact, case-sensitive value. These
  examples use `customer-slurm`; replace it everywhere with the environment
  selected during installation. Values are not trimmed.
- `mode: observe` forbids controller actions for this job, regardless of the
  controller default. Watchdog fail-open has a separate process-wide gate.
- `auto_resume_mode: requeue` describes the intended recovery policy.
- `max_requeue_times: 1` caps the job-level requeue request. The system-wide
  limit may impose a lower effective value.

The controller value `unspecified` accepts every valid APS environment and is
intended for an unpartitioned single-controller deployment. The value `prod`
also accepts a missing or empty job environment for compatibility with legacy
production jobs. Any other controller value requires an exact job-comment
match. Jobs rejected by environment routing are not published, planned, or
acted on. A valid JSON `APS` or `HEIMDALL` wrapper is mandatory for enrollment;
an ordinary comment or an unwrapped policy does not opt in. Missing, malformed,
or invalid-mode comments never authorize scheduler mutation. Environment,
partition, or username settings do not automatically enroll jobs. There is no
account-based selector.

Submit the job and capture its allocation ID:

```bash
JOB_ID="$(sbatch --parsable ./your-training-job.sbatch)"
JOB_ID="${JOB_ID%%;*}"
printf 'submitted job %s\n' "$JOB_ID"
squeue -j "$JOB_ID" -o '%.18i %.12P %.30j %.8T %.10M %.20R'
```

Confirm Slurm stored the exact comment:

```bash
scontrol show job "$JOB_ID" | grep 'Comment='
```

Then confirm the controller deployment is still using its non-mutating default.
Controller logs are JSON, so every line emitted by this deployment carries the
`default_mode` field.

For Slinky:

```bash
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  logs deployment/"$SLINKY_CONTROLLER_RELEASE" \
  -c slurm-advisor-controller \
  --tail=200 |
  grep '"default_mode":"observe"'
```

For conventional Slurm:

```bash
sudo journalctl \
  -u slurm-advisor-controller.service \
  --since=-10m \
  --no-pager |
  grep '"default_mode":"observe"'
```

An ordinary successful job does not create a failed-job recovery decision, so
it is not expected to produce a job-specific `effective_mode` log line. That
field is logged when the controller plans, skips, or applies a failed-job
requeue. Do not treat an empty `grep "$JOB_ID"` result as an onboarding
failure for this successful job.

Confirm instead that the controller published the exact Slurm job to NVIDIA Recovery
through the API Gateway's production jobs API:

```bash
API_GATEWAY_PROXY="/api/v1/namespaces/${RELEASE_NAMESPACE}/services/http:${RELEASE_NAME}-api-gateway:8080/proxy"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  get --raw "$API_GATEWAY_PROXY/v1/jobs/by-scheduler-id/$JOB_ID" |
  jq .
```

The response must contain the captured Slurm job ID and attempt 0. No
controller log may report that a hold, release, cancellation, requeue, or node
update was applied for this job.

Open the dashboard through a local port-forward:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  port-forward \
  service/"$RELEASE_NAME-dashboard" \
  3001:3001
```

Keep that command running and open the following URL in your browser. If port
3001 is already in use on your workstation, change both the local port in the
`port-forward` command and the port in the URL:

```text
http://localhost:3001/dashboard/workflow
```

The default image is built with an empty base path. A custom image built with
`BASE_PATH` requires that prefix and matching `dashboard.app.base_path` values.
The path above navigates directly to the workflow list. Find
`nvidia-recovery-getting-started`, open it, and
confirm that the displayed Slurm job ID and state agree with `squeue` or
`sacct`:

```bash
sacct -j "$JOB_ID" \
  --duplicates \
  --format=JobID,JobName,State,Start,End,Elapsed,NodeList
```

### Verify completed-job accounting

Let the observe-mode job finish successfully, then check its allocation records:

```bash
sacct -X -j "$JOB_ID" --duplicates --json |
  jq '.jobs[] | {job_id, state, comment: .comment.job}'

kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  get --raw "$API_GATEWAY_PROXY/v1/jobs/by-scheduler-id/$JOB_ID" |
  jq .
```

The completed accounting record must retain the submitted APS JSON, including
its environment and `mode: observe`. After accounting propagation and a
controller poll, the matching attempt must reach `JOB_STATUS_COMPLETED` in
the API and `COMPLETED` in the dashboard. A comment visible only in
`scontrol show job` while the job runs is insufficient.

If the accounting comment is empty and NVIDIA Recovery remains `RUNNING`, complete
the native [comment-retention prerequisite](slurm.md#retain-opt-in-comments-in-accounting)
and repeat with a fresh job. Do not promote the controller to action mode until
this check passes. This completes the non-mutating getting-started milestone.

### Diagnose sacct JSON failures

The Slurm command executor captures stdout and stderr separately. Parsers read
only stdout, so an SSH or Slurm diagnostic on stderr cannot corrupt a valid
response. A nonzero command exit still fails and retains both streams in the
error. Timeouts and cancellation retain their existing behavior.

When a targeted `sacct` command exits successfully but stdout cannot be decoded,
the controller emits an error-level
`Captured sacct output after JSON decode failure` diagnostic. Its JSON `msg`
field includes `job_id`, `source`, `command`, `stdout_bytes`, `stderr_bytes`,
`stdout_quoted`, and `stderr_quoted` so message-only log viewers show both streams.
Syntax
and JSON type errors also include `json_error_offset`, the byte offset reported
by the JSON decoder in stdout (one-based for syntax errors). The identity, byte counts,
and available offset are also retained as structured fields for filtering.

The diagnostic covers these query sources:

| Source | Purpose | Command arguments |
| --- | --- | --- |
| `sacct_status` | Finalized-job status | `-Xj <job-id> --duplicates --json` |
| `sacct_steps` | Active-job log metadata | `-j <job-id> --duplicates --json=v0.0.43` |
| `sacct_finalized` | Finalized-job log metadata | `-j <job-id> --duplicates --json=v0.0.43` |

The command records the controller's arguments before any external SSH wrapper
rewrites. Finalized metadata queries share one response across job attempts;
the controller captures that response once per query.

`stdout_quoted` and `stderr_quoted` each contain their complete, untrimmed
stream as a Go quoted string. After decoding the log JSON, use Go's
`strconv.QuotedPrefix` to extract each quoted value and `strconv.Unquote` to
restore the original bytes, including control characters and invalid UTF-8.
The capture is not truncated by the controller, but
downstream log collectors may impose their own record-size limits.

When stdout decodes successfully but stderr is nonempty, a warning-level
`sacct command wrote to stderr` message captures `stderr_quoted` with the same
command context and byte counts. It does not log stdout or reject the response.
This makes intermittent SSH diagnostics visible even after stream separation
prevents a parse failure. Successful commands with empty stderr and a valid
response emit no raw-output diagnostic. Separating streams cannot repair
non-JSON text emitted on stdout by a remote shell or command.

These diagnostics include unredacted command output, potentially including
job comments, usernames, and paths. Treat these records as sensitive when
sharing them. The existing
reconciliation error or metadata-refresh warning still reports the failure
separately. Metadata discovery failures continue to leave job status available.

## Validate and enable scheduler recovery

> [!CAUTION]
> The following changes authorize processes to hold, cancel, requeue, release,
> or modify a workload. A Slurm operator must review the service identity,
> partitions, failure canary, rollback, and scheduler impact before applying
> them. Test on a dedicated partition without unrelated workloads.

This step moves the deployment from non-mutating observation to a reviewed
recovery configuration. It first validates the controller's complete
detect-advice-requeue-release path on one disposable job. It then enables the
controller and watchdog together as the steady-state customer configuration.

These settings form the scheduler-action boundary:

| Helm value | Native environment variable | Default |
| --- | --- | --- |
| `controller.mode` | `SLURM_ADVISOR_MODE` | `observe` |
| `controller.acknowledgeSchedulerActions` | `SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS` | `false` |
| `controller.environment` | `SLURM_ADVISOR_ENVIRONMENT` | `unspecified` (broad routing) |
| `controller.partitionAllowlist` | `SLURM_ADVISOR_PARTITION_ALLOWLIST` | Empty (no partition restriction) |
| `controller.userAllowlist` | `SLURM_ADVISOR_USER_ALLOWLIST` | Empty (no username mode selection) |
| `watchdog.mode` | `SLURM_WATCHDOG_MODE` | `observe` |
| `watchdog.acknowledgeSchedulerActions` | `SLURM_WATCHDOG_ACKNOWLEDGE_SCHEDULER_ACTIONS` | `false` |
| `watchdog.partitionAllowlist` | `SLURM_WATCHDOG_PARTITION_ALLOWLIST` | Empty (no partition restriction) |

The public Helm defaults are observe with acknowledgement false. Local Tilt's
default `advisor-act` profile explicitly sets both processes to act with
acknowledgement true; select `E2E_PROFILE=advisor-observe` for a non-mutating
local profile. The supplied native systemd units force observe and
acknowledgement false in `ExecStart`, even if environment-file values differ.
Inspect the effective deployment or unit command, not only the defaults.

`ignore` skips publication and recovery for a job; `observe` publishes admitted
jobs without scheduler mutations; `act` permits eligible actions only after all
remaining gates pass.

- `controller.mode` selects the default job policy. A valid explicit APS
  `ignore`, `observe`, or `act` mode takes precedence. Invalid explicit modes
  resolve to `observe`. The watchdog uses the same job policy and requires
  effective job mode `act` before tracking release timers. Its independent
  `watchdog.mode` controls whether those release decisions may mutate Slurm.
- `controller.userAllowlist`, when nonempty, selects `act` for listed users and
  `observe` for other or unknown users only when no explicit job mode is set,
  regardless of the controller default. It does not filter monitoring and is
  not an authorization boundary. Explicit `act` can therefore select an
  unlisted user's job, subject to the other gates. **This is temporary, for
  the initial internal rollout only; it will be deprecated soon and may be
  removed without notice.** Use default `observe` for that rollout; default
  `act` plus a list produces a bootstrap warning. An empty list preserves the
  normal default-mode behavior.
- `acknowledgeSchedulerActions` is a hard deployment permission ceiling. When
  false, the shared job policy downgrades any selected `act` to `observe`. The
  watchdog also has its own acknowledgement and refuses to start its process in
  act mode without it. Setting acknowledgement true does not itself select a
  job or execute an action.
- `controller.environment` and `watchdog.environmentPattern` independently
  scope which APS/HEIMDALL jobs each process may manage. The chart makes an
  unset watchdog pattern inherit the controller environment. A literal pattern
  uses the controller's exact legacy-compatible rules; `*` matches zero or more
  characters, including `/`. Matching is case-sensitive and values are not
  trimmed. Set a standalone systemd watchdog explicitly because it cannot
  inherit another process's environment.
- `partitionAllowlist` limits the partitions where either process may act.
  Its empty default imposes no partition restriction; it is not a deny-all
  setting or a monitoring filter. Use the same reviewed nonempty allowlist
  for both processes in steady state.

Helm supplies the controller default, acknowledgement, and username list to
the watchdog automatically. For standalone services, synchronize
`SLURM_WATCHDOG_JOB_DEFAULT_MODE`, `SLURM_WATCHDOG_JOB_ACTIONS_ACKNOWLEDGED`,
and `SLURM_WATCHDOG_USER_ALLOWLIST` with the corresponding SAC settings and
restart both services after changes. The watchdog job-policy defaults are
`observe`, `false`, and an empty list.

Use these profiles:

| Profile | Controller | Watchdog | Purpose |
| --- | --- | --- | --- |
| Observe | `mode: observe`, acknowledgement false | `mode: observe`, acknowledgement false | Normal non-mutating onboarding and rollback state. |
| Temporary controller canary | Default observe, acknowledgement true, and only the canary APS requests `mode: act` | `mode: observe`, acknowledgement false | Isolates the controller recovery path before watchdog fail-open is authorized. Do not retain this as steady state. |
| Recovery enabled | `mode: act`, acknowledgement true | `mode: act`, acknowledgement true | Reviewed steady state, with the same narrow partition allowlist. |

Controller recovery and watchdog fail-open are independent mutation paths. The
temporary canary profile deliberately tests them separately; this temporary
mismatch is not the recommended customer steady state. The progression is:

1. keep both applications in observe mode while onboarding normal jobs;
2. authorize controller actions only for a dedicated canary;
3. validate the complete detect-advice-requeue path;
4. review and authorize watchdog fail-open behavior separately; and
5. promote both processes to the recovery-enabled profile.

### Stage controller recovery for a Slinky canary

Keep the controller default mode as `observe`, set a narrow partition
allowlist, and open the independent action ceiling:

```yaml
controller:
  mode: observe
  acknowledgeSchedulerActions: true
  partitionAllowlist:
    - nvidia-recovery-canary
```

Replace `nvidia-recovery-canary` with a real Slurm partition that exists on your
cluster and is reserved for testing. Do not use a partition that runs
unrelated production workloads.

Upgrade the standalone controller release:

```bash
helm upgrade "$SLINKY_CONTROLLER_RELEASE" \
  deploy/helm/slurm-advisor-controller \
  --kube-context "$SLINKY_KUBE_CONTEXT" \
  --namespace "$SLINKY_NAMESPACE" \
  --values slinky-values.yaml \
  --wait \
  --timeout 15m
```

A job still needs an explicit `"mode":"act"` comment to request mutation.

Leave the watchdog unchanged until its separate review:

```yaml
watchdog:
  mode: observe
  acknowledgeSchedulerActions: false
```

### Stage controller recovery for a conventional Slurm canary

Keep the environment-file allowlist limited to the canary partition. Create a
named systemd drop-in so the reviewed action override and its rollback target
are explicit:

```bash
sudo install -d -o root -g root -m 0755 \
  /etc/systemd/system/slurm-advisor-controller.service.d
sudo install -o root -g root -m 0644 /dev/stdin \
  /etc/systemd/system/slurm-advisor-controller.service.d/10-action.conf <<'EOF'
[Service]
ExecStart=
ExecStart=/usr/bin/env SLURM_ADVISOR_MODE=observe SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS=true /usr/local/libexec/heimdall-oss/slurm-advisor-controller
EOF
```

Validate, reload, restart, and inspect:

```bash
sudo systemd-analyze verify slurm-advisor-controller.service
sudo systemctl daemon-reload
sudo systemctl restart slurm-advisor-controller.service
sudo systemctl status slurm-advisor-controller.service --no-pager
sudo systemctl cat slurm-advisor-controller.service
```

Do not start the watchdog yet.

### Run a disposable recovery canary

For conventional Slurm, configure the controller and watchdog to send audit
events to the API Gateway using the [service settings](slurm.md#configure-the-native-services).
Apply the controller setting before submitting the canary; keep the watchdog
stopped.

Use a short, checkpoint-safe canary whose job output is in the directory
already proven to reach Loki. Do not use an expensive training job as the first
action-mode test.

Create `nvidia-recovery-canary.sbatch`:

```bash
#!/usr/bin/env bash
#SBATCH --job-name=nvidia-recovery-canary
#SBATCH --partition=nvidia-recovery-canary
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --requeue
#SBATCH --open-mode=append
#SBATCH --output=/shared/slurm/job-logs/slurm-%j.out
#SBATCH --comment='{"APS":{"workflow_id":"nvidia-recovery-canary","environment":"customer-slurm","mode":"act","auto_resume_mode":"requeue","max_requeue_times":1}}'

set -euo pipefail

# This directory must persist across attempts and be writable by the job user.
state_dir=/shared/slurm/nvidia-recovery-canary-state
state_file="$state_dir/$SLURM_JOB_ID.first-attempt-failed"
mkdir -p "$state_dir"

if [[ -f "$state_file" ]]; then
  echo "NVIDIA Recovery recovery canary succeeded after requeue"
  exit 0
fi

touch "$state_file"
echo "Rank0: (1) segmentation fault: NVIDIA Recovery recovery canary"
sync

# Leave time for Alloy to ship the line and for Watcher to query Loki.
sleep 60
exit 1
```

Replace the partition, output directory, state directory, and APS environment
with reviewed site values. The filename matches the logging runbook's
`slurm-<job-id>.out` extraction; a different filename needs a matching collector
configuration. The output directory must be the one already proven to reach
Loki. The state directory must be durable across attempts so the requeued
canary succeeds rather than failing indefinitely.

`#SBATCH --open-mode=append` ensures that the requeued attempt writes to the
same output file rather than truncating it. This is important for Watcher to
see log lines from both attempts in a single stream.

Submit the canary and capture only its allocation ID:

```bash
JOB_ID="$(sbatch --parsable ./nvidia-recovery-canary.sbatch)"
JOB_ID="${JOB_ID%%;*}"
printf 'submitted recovery canary %s\n' "$JOB_ID"
```

Observe the scheduler without issuing manual recovery commands:

```bash
watch -n 5 "squeue -j '$JOB_ID' -o '%.18i %.12P %.30j %.8T %.10M %.20R'"
```

The pipeline is asynchronous. Repeat the following read-only checks for up to
20 minutes after the initial failed attempt, then inspect any missing stage. Do not issue a manual hold, release, or requeue while gathering this
evidence.

First, query the exact Loki selector used by Watcher. The expected response
contains the canary's `job_id`, preserved `filename`, and segmentation fault
line. This query assumes `job_id` is an indexed label; if your collector stores
it as structured metadata, use the same stream selector followed by
`| job_id="<captured-job-id>"`. Do not require or invent an attempt label:

```bash
LOKI_QUERY="{cluster=\"$HEIMDALL_ENVIRONMENT\",appName=\"slurm-app-log\",job_id=\"$JOB_ID\"}"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  run nvidia-recovery-loki-canary-query \
  --rm -i \
  --restart=Never \
  --image=curlimages/curl:8.12.1 \
  --env="LOKI_URL=$LOKI_URL" \
  --env="LOKI_QUERY=$LOKI_QUERY" \
  --command -- \
  sh -ceu 'curl -fsSG "$LOKI_URL/loki/api/v1/query_range" --data-urlencode "query=$LOKI_QUERY" --data-urlencode "limit=20"' |
  grep -F "$JOB_ID" |
  grep -F 'filename' |
  grep -F 'segmentation fault'
```

For an authenticated or TLS-protected Loki deployment, add the same tenant,
credential, CA, and client-certificate options used by the Watcher and log
collector. Do not put credentials directly in this command or in the guide.

Then verify that Watcher published the matched `CRASH` event for the same job:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  logs deployment/"$RELEASE_NAME-watcher" \
  --since=20m |
  grep -F "$JOB_ID" |
  grep -E 'CRASH|Message published'
```

Query the production Advice API and require restart advice for attempt 0:

```bash
API_GATEWAY_PROXY="/api/v1/namespaces/${RELEASE_NAMESPACE}/services/http:${RELEASE_NAME}-api-gateway:8080/proxy"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  get --raw "$API_GATEWAY_PROXY/v1/advice?limit=250" |
  jq -e --arg job_id "$JOB_ID" '
    .advice[] |
    select(
      .jobId == $job_id and
      .attemptIndex == 0 and
      .advice.state == "ADVICE_STATE_RESTARTED"
    )
  '
```

On a fresh getting-started deployment, the `limit=250` response includes the
canary. On a longer-lived deployment, follow `nextPageToken` until the exact
job and attempt are found.

Inspect the controller's actions for this exact job and attempt. The canary's
log marker can produce Advice before the script exits: a still-running job
follows the active-job restart path, while an already-failed job follows the
failed-job path. Requiring the exact `applied failed-job requeuehold` log line
would reject a valid recovery through the first path. Retain the job-specific
action logs and audit events, then use the higher Slurm attempt below as the
restart proof. For Slinky:

```bash
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  logs deployment/"$SLINKY_CONTROLLER_RELEASE" \
  -c slurm-advisor-controller \
  --since=20m |
  jq --arg job_id "$JOB_ID" 'select(.job_id == $job_id)'
```

For conventional Slurm:

```bash
sudo journalctl \
  -u slurm-advisor-controller.service \
  --since=-20m \
  --no-pager --output=cat |
  jq --arg job_id "$JOB_ID" 'select(.job_id == $job_id)'
```

The controller logs `applied advice job release` with job and attempt fields
when it applies that action. Advice release is also exported as a bounded
Prometheus metric. For Slinky, verify that at least one successful
act-mode advice release has been recorded:

```bash
CONTROLLER_METRICS_PROXY="/api/v1/namespaces/${SLINKY_NAMESPACE}/services/http:${SLINKY_CONTROLLER_RELEASE}:9090/proxy"
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  get --raw "$CONTROLLER_METRICS_PROXY/metrics" |
  grep '^slurm_advisor_controller_advice_actions_total{' |
  grep 'action="advice_release"' |
  grep 'mode="act"' |
  grep 'outcome="success"'
```

For conventional Slurm:

```bash
curl -fsS http://127.0.0.1:19091/metrics |
  grep '^slurm_advisor_controller_advice_actions_total{' |
  grep 'action="advice_release"' |
  grep 'mode="act"' |
  grep 'outcome="success"'
```

This metric is aggregate and therefore supports, but does not by itself prove,
the canary's release. Use Slurm accounting as the job-specific authority:

```bash
SACCT_ROWS="$(
  sacct -X \
    --duplicates \
    --noheader \
    --parsable2 \
    -j "$JOB_ID" \
    --format=JobIDRaw,State,ExitCode,Restarts,Submit,Start,End,NodeList
)"
printf '%s\n' "$SACCT_ROWS"

printf '%s\n' "$SACCT_ROWS" |
  awk -F '|' -v job_id="$JOB_ID" '
    $1 == job_id && $2 == "COMPLETED" && ($4 + 0) >= 1 { recovered = 1 }
    END { exit recovered ? 0 : 1 }
  '
```

The assertion succeeds only when the exact allocation has a later completed
record with `Restarts` at least one. This proves that the held requeued attempt
was released and that attempt 1 completed successfully.

Before opening the Timeline, verify persisted controller audit events through
the Notifications query API. On the operator workstation, use the captured
fresh canary ID and query attempts 0 and 1:

```bash
NOTIFICATIONS_PROXY="/api/v1/namespaces/${RELEASE_NAMESPACE}/services/http:${RELEASE_NAME}-notifications:8080/proxy"
for attempt in 0 1; do
  kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
    get --raw "$NOTIFICATIONS_PROXY/v1/audit/events?job_id=$JOB_ID&attempt_index=$attempt&event_producer=slurm-advisor-controller&limit=100" |
    jq '.items[] | {event_type, event_produced_at, payload}'
done
```

Allow the audit flush interval (30 seconds by default) and backend processing
time. Across the two responses, require fresh successful `slurm.requeue_hold`
and `slurm.release` actions (`payload.success: true`). An empty result means
controller audit delivery has not been verified, even if Slurm recovered the
job. Confirm that the controller's audit destination is `gateway`
(`SLURM_ADVISOR_AUDIT_TRAIL_DESTINATION` for systemd or
`controller.auditTrail.destination` for Helm), then check gateway reachability
and Notifications delivery errors. Changing the audit destination does not
replay old journal events.

Finally, open the dashboard using the port-forward from
[the first-job check](#observe-a-first-job). The workflow must show attempts
0 and 1 and the same controller recovery audit events for the captured job ID.
This proves controller delivery; watchdog delivery is checked separately when
running the [watchdog fail-open canary](#run-a-watchdog-fail-open-canary).

If any stage fails, return the controller to observe mode before diagnosing.
Cancel only the captured canary allocation during cleanup:

```bash
scancel "$JOB_ID"
rm -f "/shared/slurm/nvidia-recovery-canary-state/$JOB_ID.first-attempt-failed"
```

### Promote both controller and watchdog to act mode

The watchdog can release eligible held workloads when NVIDIA Recovery does not decide
before `releaseTimeout`. Its service identity and partition allowlist deserve
the same scheduler-action review as the controller. The controller-only canary
configuration was temporary; the customer steady state keeps both processes in
the same mode with the same allowlist.

For Slinky, set both processes to the recovery-enabled profile after review:

```yaml
controller:
  mode: act
  acknowledgeSchedulerActions: true
  partitionAllowlist:
    - nvidia-recovery-canary
watchdog:
  mode: act
  acknowledgeSchedulerActions: true
  environmentPattern: null   # Continue to inherit controller.environment
  partitionAllowlist:
    - nvidia-recovery-canary
```

Apply the values and wait for both Deployments:

```bash
helm upgrade "$SLINKY_CONTROLLER_RELEASE" \
  deploy/helm/slurm-advisor-controller \
  --kube-context "$SLINKY_KUBE_CONTEXT" \
  --namespace "$SLINKY_NAMESPACE" \
  --values slinky-values.yaml \
  --wait \
  --timeout 15m

kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  rollout status deployment/"$SLINKY_CONTROLLER_RELEASE"
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  rollout status deployment/"$SLINKY_CONTROLLER_RELEASE-watchdog"
```

For conventional Slurm, replace the temporary controller override with act
mode and create the watchdog override:

```bash
sudo install -o root -g root -m 0644 /dev/stdin \
  /etc/systemd/system/slurm-advisor-controller.service.d/10-action.conf <<'EOF'
[Service]
ExecStart=
ExecStart=/usr/bin/env SLURM_ADVISOR_MODE=act SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS=true /usr/local/libexec/heimdall-oss/slurm-advisor-controller
EOF

sudo install -d -o root -g root -m 0755 \
  /etc/systemd/system/slurm-watchdog.service.d
sudo install -o root -g root -m 0644 /dev/stdin \
  /etc/systemd/system/slurm-watchdog.service.d/10-action.conf <<'EOF'
[Service]
ExecStart=
ExecStart=/usr/bin/env SLURM_WATCHDOG_MODE=act SLURM_WATCHDOG_ACKNOWLEDGE_SCHEDULER_ACTIONS=true SLURM_WATCHDOG_JOB_DEFAULT_MODE=act SLURM_WATCHDOG_JOB_ACTIONS_ACKNOWLEDGED=true /usr/local/libexec/heimdall-oss/slurm-watchdog
EOF
```

Validate and start both services:

```bash
sudo systemd-analyze verify \
  slurm-advisor-controller.service \
  slurm-watchdog.service
sudo systemctl daemon-reload
sudo systemctl restart slurm-advisor-controller.service
sudo systemctl enable --now slurm-watchdog.service
sudo systemctl status \
  slurm-advisor-controller.service \
  slurm-watchdog.service \
  --no-pager
```

An explicit APS `mode: observe` keeps an individual job non-mutating for both
controller recovery and watchdog release, including for users on the temporary
list. An explicit `ignore` also prevents both action paths. To keep an operator
hold on a matching managed job or DAG retry, set its mode to `observe` and allow
both processes to observe that change before applying the hold.

### Run a watchdog fail-open canary

The watchdog releases normal managed jobs and DAG retries that remain held past
the configured timeout. It never releases a held DAG attempt 0. Test retry
release with a separate disposable non-DAG canary. Its explicit `act` mode is
required by the shared policy. To prove the watchdog releases it independently,
arrange a reviewed controller pause for this test; other jobs in that controller's
scope will temporarily lack SAC reconciliation. Leave the shared policy settings
unchanged so the watchdog remains authorized.

For Slinky, record the current controller replica count and scale only its
Deployment down:

```bash
WATCHDOG_CANARY_CONTROLLER_REPLICAS="$(
  kubectl --context "$SLINKY_KUBE_CONTEXT" \
    -n "$SLINKY_NAMESPACE" \
    get deployment "$SLINKY_CONTROLLER_RELEASE" -o jsonpath='{.spec.replicas}'
)"
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  scale deployment/"$SLINKY_CONTROLLER_RELEASE" --replicas=0
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  rollout status deployment/"$SLINKY_CONTROLLER_RELEASE"
```

For conventional Slurm:

```bash
sudo systemctl stop slurm-advisor-controller.service
```

Create `nvidia-recovery-watchdog-canary.sbatch`:

```bash
#!/usr/bin/env bash
#SBATCH --job-name=nvidia-recovery-watchdog-canary
#SBATCH --partition=nvidia-recovery-canary
#SBATCH --time=00:20:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --requeue
#SBATCH --output=/shared/slurm/job-logs/slurm-%j.out
#SBATCH --comment='{"APS":{"workflow_id":"nvidia-recovery-watchdog-canary","environment":"customer-slurm","mode":"act","auto_resume_mode":"requeue","max_requeue_times":1}}'

set -euo pipefail

if (( ${SLURM_RESTART_COUNT:-0} >= 1 )); then
  echo "NVIDIA Recovery watchdog canary succeeded after release"
  exit 0
fi

echo "NVIDIA Recovery watchdog canary initial attempt is running"
sleep 1200
```

Replace the partition, output directory, and APS environment with reviewed
site values.
Submit it and wait until the initial attempt is running:

```bash
WATCHDOG_JOB_ID="$(sbatch --parsable ./nvidia-recovery-watchdog-canary.sbatch)"
WATCHDOG_JOB_ID="${WATCHDOG_JOB_ID%%;*}"

watch -n 5 "squeue -j '$WATCHDOG_JOB_ID' -o '%.18i %.12P %.30j %.8T %.10M %.20R'"
```

Stop the watch after the job reaches RUNNING. If it finishes or fails before
then, inspect its accounting record and stop this canary. With the Slurm
operator's approval, create the held higher attempt that
exercises fail-open:

```bash
scontrol requeuehold "$WATCHDOG_JOB_ID"

watch -n 5 "squeue -j '$WATCHDOG_JOB_ID' -o '%.18i %.12P %.30j %.8T %.10M %.20R'"
```

The job remains held until the configured `releaseTimeout` elapses. Confirm the
watchdog applied the release. For Slinky:

```bash
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  logs deployment/"$SLINKY_CONTROLLER_RELEASE-watchdog" \
  -c slurm-watchdog \
  --since=20m |
  grep -F "$WATCHDOG_JOB_ID" |
  grep -F 'watchdog release candidate' |
  grep -F '"outcome":"applied"'
```

For conventional Slurm:

```bash
sudo journalctl \
  -u slurm-watchdog.service \
  --since=-20m \
  --no-pager |
  grep -F "$WATCHDOG_JOB_ID" |
  grep -F 'watchdog release candidate' |
  grep -F '"outcome":"applied"'
```

Finally, require a completed higher attempt in Slurm accounting:

```bash
WATCHDOG_SACCT_ROWS="$(
  sacct -X \
    --duplicates \
    --noheader \
    --parsable2 \
    -j "$WATCHDOG_JOB_ID" \
    --format=JobIDRaw,State,ExitCode,Restarts,Submit,Start,End,NodeList
)"
printf '%s\n' "$WATCHDOG_SACCT_ROWS"

printf '%s\n' "$WATCHDOG_SACCT_ROWS" |
  awk -F '|' -v job_id="$WATCHDOG_JOB_ID" '
    $1 == job_id && $2 == "COMPLETED" && ($4 + 0) >= 1 { recovered = 1 }
    END { exit recovered ? 0 : 1 }
  '
```

After the audit flush interval, verify that Notifications persisted the
watchdog's event for the released higher attempt (attempt 1 in this canary):

```bash
NOTIFICATIONS_PROXY="/api/v1/namespaces/${RELEASE_NAMESPACE}/services/http:${RELEASE_NAME}-notifications:8080/proxy"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  get --raw "$NOTIFICATIONS_PROXY/v1/audit/events?job_id=$WATCHDOG_JOB_ID&attempt_index=1&event_producer=slurm-watchdog&limit=100" |
  jq -e '.items[] | select(.event_type == "watchdog_release_completed" and .payload.success == true)'
```

Require a fresh successful event for the captured job. If absent, check the
watchdog's gateway endpoint and audit destination settings.

Cancel only the captured canary allocation if it is still active:

```bash
scancel "$WATCHDOG_JOB_ID"
```

Restore the controller after the test, including if the canary fails. For Slinky:

```bash
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  scale deployment/"$SLINKY_CONTROLLER_RELEASE" \
  --replicas="$WATCHDOG_CANARY_CONTROLLER_REPLICAS"
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  rollout status deployment/"$SLINKY_CONTROLLER_RELEASE"
```

For conventional Slurm:

```bash
sudo systemctl start slurm-advisor-controller.service
```

### Roll back to observe mode

Rollback stops future automatic scheduler actions. **It intentionally leaves
already-held jobs held.** Record the affected allocations and investigate their
ownership before deciding their disposition; neither changing modes nor
uninstalling the backend reverses a hold.

For Slinky, restore:

```yaml
controller:
  mode: observe
  acknowledgeSchedulerActions: false
watchdog:
  mode: observe
  acknowledgeSchedulerActions: false
```

Apply the same `helm upgrade` command used for promotion and confirm both
Deployments roll out successfully. Verify both acknowledgement values are false
and the controller logs show `default_mode=observe`.

For conventional Slurm, remove only the two named action drop-ins, stop the
watchdog, and restart the controller:

```bash
sudo systemctl disable --now slurm-watchdog.service
sudo rm -f /etc/systemd/system/slurm-advisor-controller.service.d/10-action.conf
sudo rm -f /etc/systemd/system/slurm-watchdog.service.d/10-action.conf
sudo systemctl daemon-reload
sudo systemctl restart slurm-advisor-controller.service
sudo systemctl cat slurm-advisor-controller.service
```

Confirm the effective controller command contains `SLURM_ADVISOR_MODE=observe`
and `SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS=false`.

After both mutation paths are disabled, inventory pending jobs with read-only
commands. For each candidate, set `JOB_ID` to one exact allocation ID:

```bash
squeue --states=PENDING -o '%.18i %.16u %.16P %.30j %.24R'
scontrol show job "$JOB_ID"
sacct -X --duplicates -j "$JOB_ID" --parsable2 \
  --format=JobIDRaw,User,Partition,State,Restarts,Comment%2048
```

Retain the current reason, priority, APS/HEIMDALL policy, restart count, matching
Advice, and controller/watchdog action logs. Record which exact holds were
applied by NVIDIA Recovery and which were imposed by an operator. A managed
job's comment and held state alone do not establish ownership: the controller
does not add a separate ownership marker when issuing a Slurm hold.

For each reviewed hold, the operator must choose one disposition:

- **Leave held** while investigating, preserving checkpoints and placement
  constraints. This is the default when ownership or dependencies are unclear.
- **Release** that specific allocation only after reviewing its retry policy,
  exclusions, and DAG prerequisites: `scontrol release "$JOB_ID"`.
- **Cancel** that specific allocation when recovery is no longer wanted:
  `scancel "$JOB_ID"`.

The last two commands mutate the selected workload and require the operator's
explicit decision. Do not bulk-release pending jobs or remove another
operator's hold. Held DAG attempt zero may be an intentional dependency gate.

## Optional external OTLP collector

The optional dependency checker and bundled OpenTelemetry collector are disabled
by default. Enabling either requires an explicitly selected compatible image;
no replacement collector is qualified by this walkthrough.

Skip this section when the evaluation environment does not provide an OTLP
collector. The `opentelemetry-collector.enabled: false` setting in the example
values disables the chart's bundled collector dependency; it does not prevent
NVIDIA Recovery services from exporting to an existing collector through
`global.otelcollector`.

First confirm that the collector's OTLP endpoint is reachable from the
NVIDIA Recovery release namespace. Use the collector's cluster-internal Service when
it runs in the same Kubernetes cluster, for example:

```bash
export OTEL_COLLECTOR_ENDPOINT=http://otel-collector.telemetry.svc.cluster.local:4317
```

If the collector requires bearer authentication, place only the bearer token
in a protected file and create a namespace-local Secret. Do not put the token
or a complete `Authorization` header in `nvidia-recovery-values.yaml`.

```bash
export OTEL_COLLECTOR_SECRET_NAME=nvidia-recovery-otel-collector
export OTEL_COLLECTOR_TOKEN_FILE=/secure/path/otel-collector-token

test -r "$OTEL_COLLECTOR_TOKEN_FILE"
chmod go-rwx "$OTEL_COLLECTOR_TOKEN_FILE"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  --namespace "$RELEASE_NAMESPACE" \
  create secret generic "$OTEL_COLLECTOR_SECRET_NAME" \
  --from-file=token="$OTEL_COLLECTOR_TOKEN_FILE" \
  --dry-run=client \
  -o yaml |
  kubectl --context "$HEIMDALL_KUBE_CONTEXT" apply -f -
```

Add the following fields to the existing `global` section in the generated
`nvidia-recovery-values.yaml`. Replace the endpoint and Secret name with the values
selected above:

```yaml
global:
  otelcollector:
    enabled: true
    endpoint: http://otel-collector.telemetry.svc.cluster.local:4317
    protocol: grpc
    auth:
      enabled: true
      secretName: nvidia-recovery-otel-collector
      secretKey: token
```

For a collector that accepts unauthenticated OTLP, skip Secret creation and
set `auth.enabled: false`; `secretName` and `secretKey` can remain empty. The
global setting injects `OTEL_EXPORTER_OTLP_*` variables into every NVIDIA Recovery
application container, but does not activate a service's telemetry SDK. To
enable the API Gateway's existing SDK during this walkthrough, also add:

```yaml
api-gateway:
  otel:
    enabled: true
    # Keep false with bearer auth; set true only for an unauthenticated collector.
    insecure: false
```

For API Gateway, `otel.insecure` authorizes export without a token; the
endpoint's `http://` or `https://` scheme controls transport security. Leave
it `false` when `global.otelcollector.auth.enabled` is `true`.

Apply the updated backend values with the `helm upgrade --install` command in
[installation](installation.md#render-and-install), after rendering and reviewing
endpoint/protocol injection and Secret references. Verify the enabled service's
SDK exports to your selected collector; injected environment variables alone
do not prove telemetry delivery.

## Remove the evaluation

Preserve evidence and decide data retention **before** uninstalling a release,
deleting a namespace, scaling Loki down, or deleting a Kind cluster. Save the
source revision, sanitized values, canary job/attempt IDs, accounting records,
matching Advice/audit events, and relevant application/controller logs outside
the cluster. Record each resource's owner and intended disposition:

| Resource | Retention/removal decision |
| --- | --- |
| MySQL, RabbitMQ, and Loki PVCs/PVs | Record claim, backing volume, storage class, reclaim policy, and backup location. Each service may retain state independently. |
| Backend and scheduler image-pull Secrets | `heimdall-registry` exists in both installation namespaces; retain either Secret if other workloads use it. |
| Slurm authentication Secret | Retain `slurm-auth-key` while any scheduler workload still needs it. |
| Alloy or another collector | The backend installation does not own an externally installed collector. Identify its Helm release, DaemonSet, or native service and stop/remove only evaluation-owned collection after exporting logs. |
| Native units, binaries, and configuration | Retain or remove explicitly after both scheduler mutation paths are disabled. |
| Local registry, host job directory, and reports | Follow the [local teardown boundary](../getting-started.md#5-remove-the-demo); shared registries and named volumes require separate review. |

Inventory the release and storage with read-only commands:

```bash
helm list --kube-context "$HEIMDALL_KUBE_CONTEXT" --namespace "$RELEASE_NAMESPACE"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" -n "$RELEASE_NAMESPACE" \
  get statefulset,pvc
kubectl --context "$HEIMDALL_KUBE_CONTEXT" get pv \
  -o custom-columns=NAME:.metadata.name,CLAIM_NAMESPACE:.spec.claimRef.namespace,CLAIM:.spec.claimRef.name,RECLAIM:.spec.persistentVolumeReclaimPolicy,STORAGECLASS:.spec.storageClassName
```

For bundled filesystem Loki, logs reside on its PVC. The pinned Loki chart
17.2.0 defaults to automatic PVC deletion; this repository overrides
`loki.singleBinary.persistence.whenDeleted` and `whenScaled` to `Retain`.
An existing deployment may still have the older `Delete` policy. Check the
live StatefulSet **before uninstall**, rather than assuming the current values
file changed it:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" -n "$RELEASE_NAMESPACE" \
  get statefulset loki -o json |
  jq -e '(.spec.persistentVolumeClaimRetentionPolicy.whenDeleted // "Retain") == "Retain"
    and (.spec.persistentVolumeClaimRetentionPolicy.whenScaled // "Retain") == "Retain"'
kubectl --context "$HEIMDALL_KUBE_CONTEXT" -n "$RELEASE_NAMESPACE" \
  get pvc -o json |
  jq '[.items[] | {name: .metadata.name, volume: .spec.volumeName,
    owners: .metadata.ownerReferences}]'
```

If the retention check fails, stop: export/back up the required data and apply
the intended policy through the operator-owned Helm values before continuing.
Wait for reconciliation and verify the actual Loki claims have no stale
StatefulSet/Pod owner references that would delete them. A `Retain` policy
protects against StatefulSet deletion/scale-down, not explicit PVC or namespace
deletion, loss of the storage backend, or removal of a Kind node. It is not a
backup. See [Kubernetes PVC retention](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#persistentvolumeclaim-retention).
Loki's existing 31-day log-age retention is a separate compactor policy; it
does not preserve a deleted volume.

After preserving evidence and verifying retention, [roll back](#roll-back-to-observe-mode)
and stop scheduler-side processes.
Cancel only test allocations whose IDs were captured by this guide. For
conventional Slurm, stop both services before removing the backend:

```bash
sudo systemctl disable --now \
  slurm-advisor-controller.service \
  slurm-watchdog.service
```

For Slinky, remove the standalone Advisor Controller release:

```bash
helm uninstall "$SLINKY_CONTROLLER_RELEASE" \
  --kube-context "$SLINKY_KUBE_CONTEXT" \
  --namespace "$SLINKY_NAMESPACE"
```

Before removing the backend, decide whether to retain or delete its data. Do
not postpone this decision until after `helm uninstall`: the bundled Loki
StatefulSet can own its log-storage claim with a `whenDeleted: Delete` policy,
so uninstalling the release can delete that PVC automatically. Whether deleting
a PVC also permanently deletes its storage depends on the bound persistent
volume's reclaim policy.

First inspect all claims in the release namespace:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  get pvc
```

If bundled Loki is enabled, inspect its live retention policy, the ownership of
its claim, and the persistent volume's reclaim policy:

```bash
export LOKI_STATEFULSET=loki
export LOKI_PVC=storage-loki-0

kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  get statefulset "$LOKI_STATEFULSET" \
  -o jsonpath='{.spec.persistentVolumeClaimRetentionPolicy}{"\n"}'

kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  get pvc "$LOKI_PVC" \
  -o custom-columns='NAME:.metadata.name,OWNERS:.metadata.ownerReferences[*].name,VOLUME:.spec.volumeName'

LOKI_PV="$(kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  get pvc "$LOKI_PVC" \
  -o jsonpath='{.spec.volumeName}')"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  get persistentvolume "$LOKI_PV" \
  -o custom-columns='NAME:.metadata.name,RECLAIM-POLICY:.spec.persistentVolumeReclaimPolicy'
```

If Loki data must survive the uninstall, first take a storage-provider snapshot
or another verified backup when available. Then change both StatefulSet
retention actions to `Retain`:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  patch statefulset "$LOKI_STATEFULSET" \
  --type=merge \
  -p '{"spec":{"persistentVolumeClaimRetentionPolicy":{"whenDeleted":"Retain","whenScaled":"Retain"}}}'
```

Do not continue until the live policy reports `Retain` for deletion and scaling
and the Loki StatefulSet is no longer listed as an owner of the PVC:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  get statefulset "$LOKI_STATEFULSET" \
  -o jsonpath='{.spec.persistentVolumeClaimRetentionPolicy}{"\n"}'

kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  get pvc "$LOKI_PVC" \
  -o jsonpath='{.metadata.ownerReferences}{"\n"}'
```

An empty owner-reference result is expected for the retained Loki claim. The
StatefulSet patch protects the PVC from this uninstall; it is not a backup and
does not protect against later manual PVC or namespace deletion. Keep the
namespace and claim if a same-name Loki reinstall must reuse the data.

After making and verifying the retention decision for Loki, MySQL, and any
other claims, remove the NVIDIA Recovery backend release:

```bash
helm uninstall "$RELEASE_NAME" \
  --kube-context "$HEIMDALL_KUBE_CONTEXT" \
  --namespace "$RELEASE_NAMESPACE"
```

After uninstall, compare the remaining MySQL, RabbitMQ, and Loki claims with
the inventory and retention decisions recorded above. Claims explicitly
retained before uninstall, including the bundled MySQL claim, may remain:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  get pvc
```

Do not delete a PVC or namespace that contains customer data, retained
evaluation data, or shared resources.

For the Slinky path, also remove the Slurm authentication Secret if it is no
longer needed:

```bash
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  delete secret slurm-auth-key
```

Do not delete this Secret if other Slinky workloads in the same namespace
depend on it.

This guide does not issue an explicit PVC or namespace deletion command.
However, uninstalling a chart can still remove a PVC through a StatefulSet
retention policy, as described for bundled Loki above. Removal of conventional
Slurm binaries, unit files, and configuration is also left to the operator.
Include Secrets and any evaluation-owned collector in that final review;
uninstalling the backend alone is not a complete cleanup inventory.

## Before production

Before promoting the evaluation:

- replace bundled MySQL and RabbitMQ with operated, backed-up services; follow
  [external MySQL with TLS](../ops/external-mysql.md) to configure and verify
  both Job Manager and Notifications;
- operate Loki with appropriate authentication, retention, replication,
  storage, and backups;
- configure API and dashboard authentication;
- add TLS or mTLS for controller-to-gateway transport;
- use scoped, rotated registry and service credentials;
- define resource requests, limits, disruption policy, and monitoring;
- restrict network paths and scheduler service identities;
- review notification delivery and data exposure;
- validate rollback and fail-open behavior under component outages; and
- repeat action-mode validation with human review for each production
  partition.

For current behavior and safety constraints, see the
[Slurm Advisor Controller architecture](../architecture/slurm-advisor-controller.md),
[security policy](../../SECURITY.md), and accepted
[architecture decisions](../decisions/README.md).

## Result and next step

Retain the source revision, values, exact job/attempt evidence, and approved
partition scope. Use [troubleshooting](troubleshooting.md) for a missing stage;
expand the recovery scope only after repeating the reviewed checks for that
partition and completing the production qualifications above.
