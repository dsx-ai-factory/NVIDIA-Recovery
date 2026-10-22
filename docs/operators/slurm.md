# Connect conventional Slurm

For the Slurm administrator installing a native Advisor Controller on a Linux
control-plane host. Complete [backend installation](installation.md) first;
you need its private gateway address, the same source revision, Go 1.27.3,
and the site's Slurm configuration and service identity. This guide starts the
controller in observe mode and leaves the watchdog stopped.

Use exactly one controller per Slurm cluster. Run the build and systemd commands
on the Linux host (or copy binaries from a compatible Linux build host).
The backend's gRPC endpoint must be reachable on port 9090 over a restricted
private network. The evaluation connection is plaintext.

Before onboarding jobs, verify the shared
[Slurm accounting prerequisites](installation.md#slurm-accounting-prerequisites):
the site-managed `slurm.conf` must include `AccountingStoreFlags=job_comment`
while preserving any existing flags. Verify the effective configuration and a
newly submitted job's completed accounting comment using the read-only checks
there; existing accounting rows are not repaired retroactively.

## Record site settings

| Setting | Source |
| --- | --- |
| Gateway | Private address recorded at the end of backend installation |
| Environment | Exact `HEIMDALL_ENVIRONMENT` selected during installation |
| Partitions | `sinfo -s` or `scontrol show partitions`; choose a narrow allowlist |
| Slurm configuration | Site-managed `SLURM_CONF`, commonly `/etc/slurm/slurm.conf` |
| Completed-job comments | Effective `AccountingStoreFlags` must include `job_comment` |
| Output directory | Actual `#SBATCH --output` path and the directory your collector reads |

### Retain opt-in comments in accounting

The Slurm job comment (`--comment`) contains the **Advisor Policy Spec (APS)**,
the job's JSON recovery policy. See the
[first-job example](operations.md#observe-a-first-job). The controller reads
this comment from `squeue` while the job runs and from `sacct` after it finishes.
Slurm must retain the comment in accounting; otherwise the completed record is
treated as `not_onboarded` and the dashboard can remain at its earlier `RUNNING`
state.

On the Slurm control-plane host, inspect the effective configuration:

```bash
scontrol show config | grep -E 'AccountingStorageType|AccountingStoreFlags'
```

Require working accounting storage and add `job_comment` to the site's existing
comma-separated `AccountingStoreFlags` in `slurm.conf`. Preserve every other
configured flag. For example, an existing `job_script` becomes:

```ini
AccountingStoreFlags=job_script,job_comment
```

If no flags were configured, use `AccountingStoreFlags=job_comment`. Have the
Slurm administrator distribute the change through the site's configuration
management and apply it during an approved configuration change:

```bash
sudo scontrol reconfigure
scontrol show config | grep AccountingStoreFlags
```

The effective value must include `job_comment`. See Slurm's
[AccountingStoreFlags reference](https://slurm.schedmd.com/archive/slurm-25.05-latest/slurm.conf.html#OPT_AccountingStoreFlags).
Verify a **new** completed observe-mode job using the
[first-job accounting check](operations.md#verify-completed-job-accounting).
This setting does not reconstruct comments missing from earlier accounting
records or repair their stale NVIDIA Recovery state.

## Build and install

Build the Advisor Controller and watchdog binaries on the Linux host, or on a
compatible Linux build host, from the controller module:

```bash
cd slurm-advisor-controller
mkdir -p dist
go build -trimpath -o dist/slurm-advisor-controller ./cmd/slurm-advisor-controller
go build -trimpath -o dist/slurm-watchdog ./cmd/slurm-watchdog
cd ..
```

The binaries execute the host's `squeue`, `sacct`, `scontrol`, and `scancel`
commands. Before installing them, run these checks as the service account that
will operate the controller:

```bash
sudo -u slurm env SLURM_CONF=/etc/slurm/slurm.conf squeue --json >/dev/null
sudo -u slurm env SLURM_CONF=/etc/slurm/slurm.conf sacct --json -X -S now-1hour >/dev/null
sudo -u slurm env SLURM_CONF=/etc/slurm/slurm.conf scontrol ping
```

Use a dedicated non-root observer account if it can read every required job.
The example uses the system `slurm` account for compatibility with clusters
that enable Slurm `PrivateData`. That account may be scheduler-privileged, so
observe-only safety also depends on the command-pinned systemd mode below. The
authoritative systemd installation reference is
[`deploy/systemd/slurm-advisor-controller/README.md`](../../deploy/systemd/slurm-advisor-controller/README.md).

Install the binaries and the checked-in observe-only configuration:

```bash
sudo install -d -o root -g root -m 0755 /usr/local/libexec/heimdall-oss
sudo install -o root -g root -m 0755 \
  slurm-advisor-controller/dist/slurm-advisor-controller \
  /usr/local/libexec/heimdall-oss/slurm-advisor-controller
sudo install -o root -g root -m 0755 \
  slurm-advisor-controller/dist/slurm-watchdog \
  /usr/local/libexec/heimdall-oss/slurm-watchdog
sudo install -d -o root -g root -m 0755 /etc/heimdall-oss
sudo install -o root -g root -m 0640 \
  deploy/systemd/slurm-advisor-controller/slurm-advisor-controller.env.example \
  /etc/heimdall-oss/slurm-advisor-controller.env
sudo install -o root -g root -m 0640 \
  deploy/systemd/slurm-advisor-controller/slurm-watchdog.env.example \
  /etc/heimdall-oss/slurm-watchdog.env
sudo install -o root -g root -m 0644 \
  deploy/systemd/slurm-advisor-controller/slurm-advisor-controller.service \
  /etc/systemd/system/slurm-advisor-controller.service
sudo install -o root -g root -m 0644 \
  deploy/systemd/slurm-advisor-controller/slurm-watchdog.service \
  /etc/systemd/system/slurm-watchdog.service
```

## Configure the native services

Edit `/etc/heimdall-oss/slurm-advisor-controller.env`. The completed file
should contain the following site-specific settings:

```ini
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
SLURM_CONF=/etc/slurm/slurm.conf
HEIMDALL_ENDPOINT=nvidia-recovery.example.com:9090
SLURM_ADVISOR_AUDIT_TRAIL_DESTINATION=gateway
SLURM_ADVISOR_LOG_LEVEL=info
SLURM_ADVISOR_POLL_INTERVAL=15s
SLURM_EXEC_TIMEOUT=5s
SLURM_ADVISOR_LOG_METADATA_ENRICHMENT_TIMEOUT=2s
SLURM_ADVISOR_JOB_UPSERT_WORKER_POOL_SIZE=8
SLURM_ADVISOR_ACCOUNTING_WORKER_POOL_SIZE=8
SLURM_ADVISOR_ACCOUNTING_TIMEOUT=5m
SLURM_ADVISOR_ADVICE_LOOP_TIMEOUT=15s
SLURM_ADVISOR_ADVICE_ITEM_TIMEOUT=30s
SLURM_ADVISOR_ADVICE_MAX_AGE=5m
SLURM_ADVISOR_REQUEUE_HOLD_DELAY=1s
SLURM_ADVISOR_ENABLE_EAGER_REQUEUE=false
SLURM_ADVISOR_ENVIRONMENT=customer-slurm
SLURM_ADVISOR_PARTITION_ALLOWLIST=gpu,debug
# Temporary initial internal rollout only; deprecated soon, removable without notice.
# No explicit job mode: listed users act, others observe; explicit mode wins.
# Empty uses the controller default. Monitoring and safety gates still apply.
SLURM_ADVISOR_USER_ALLOWLIST=
SLURM_ADVISOR_VERSIONED_SQUEUE_JSON=false
SLURM_ADVISOR_UPDATE_ELIGIBLE_TIME_AFTER_REQUEUE=false
SLURM_ADVISOR_METRICS_ENABLED=true
SLURM_ADVISOR_METRICS_ADDR=127.0.0.1:19091
```

Leave `SLURM_ADVISOR_ENABLE_EAGER_REQUEUE=false` to wait for restart Advice
before requeueing failed jobs. Setting it to `true` restores eager recovery for
rollout or rollback, including the risk that it creates a successor before Job
Manager issues a retry-limit or checkpoint-progress stop. Existing scheduler
action gates still apply. The standalone process and the Helm chart's
`controller.enableEagerRequeue` both default to `false` for Advice-only recovery.

Replace `HEIMDALL_ENDPOINT` (recorded during backend installation),
`SLURM_ADVISOR_ENVIRONMENT`, and the partition allowlist with your site values.
Also confirm `SLURM_ADVISOR_VERSIONED_SQUEUE_JSON`: set it to `true` only when
`squeue --json=v0.0.40` succeeds on the host. If set incorrectly (for example,
`true` when the host Slurm data-parser plugin does not support that schema
version), the controller will fail to parse job state and will not publish jobs
to NVIDIA Recovery.

Edit `/etc/heimdall-oss/slurm-watchdog.env` but do not start the watchdog. The
completed file should contain:

```ini
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
SLURM_CONF=/etc/slurm/slurm.conf
SLURM_WATCHDOG_POLL_INTERVAL=15s
SLURM_WATCHDOG_ENVIRONMENT_PATTERN=customer-slurm
SLURM_WATCHDOG_RELEASE_TIMEOUT=10m
SLURM_WATCHDOG_CLEANUP_GRACE_PERIOD=10m
SLURM_WATCHDOG_SLURM_EXEC_TIMEOUT=5s
SLURM_WATCHDOG_PARTITION_ALLOWLIST=gpu,debug
# Match SAC mode and acknowledgement; separate watchdog action gates still apply.
SLURM_WATCHDOG_JOB_DEFAULT_MODE=observe
SLURM_WATCHDOG_JOB_ACTIONS_ACKNOWLEDGED=false
# Same list as SLURM_ADVISOR_USER_ALLOWLIST; restart both services after changes.
# Temporary initial internal rollout only; deprecated soon, removable without notice.
SLURM_WATCHDOG_USER_ALLOWLIST=
SLURM_WATCHDOG_VERSIONED_SQUEUE_JSON=false
SLURM_WATCHDOG_LOG_LEVEL=info
SLURM_WATCHDOG_AUDIT_TRAIL_DESTINATION=gateway
SLURM_WATCHDOG_AUDIT_GATEWAY_ENDPOINT=heimdall.example.com:9090
SLURM_WATCHDOG_METRICS_ENABLED=true
SLURM_WATCHDOG_METRICS_ADDR=127.0.0.1:19092
```

Replace both copied `*_AUDIT_TRAIL_DESTINATION=logging` settings with `gateway`
for dashboard audit history. `logging` writes events only to the local journal;
a local `sent audit event` message does not prove delivery to Notifications.
The controller reuses `HEIMDALL_ENDPOINT`. The watchdog needs its own
`SLURM_WATCHDOG_AUDIT_GATEWAY_ENDPOINT`, set to the same private gateway gRPC
address on port 9090, without an HTTP scheme or path. Its connection is for
audit delivery only and does not make its release decisions depend on the
gateway. Keep the watchdog stopped until its later action review.

For an authenticated gateway, configure the corresponding controller and
watchdog service-account token paths as described in the
[native audit reference](../../deploy/systemd/slurm-advisor-controller/README.md#audit-buffering).
The transport remains plaintext and requires the restricted private network
described above. On an existing installation, restart the controller after
editing its environment file; restart the watchdog only if it is already
approved and running. Previously logged events are not replayed. Before
checking the dashboard Timeline, verify fresh persisted controller events in
the [recovery canary](operations.md#run-a-disposable-recovery-canary).

Install both environment files as root-owned files:

```bash
sudo chown root:root \
  /etc/heimdall-oss/slurm-advisor-controller.env \
  /etc/heimdall-oss/slurm-watchdog.env
sudo chmod 0640 \
  /etc/heimdall-oss/slurm-advisor-controller.env \
  /etc/heimdall-oss/slurm-watchdog.env
```

The checked-in systemd units pin `observe` mode and scheduler-action
acknowledgement to `false` in `ExecStart`. Do not replace those command-line
safety settings during initial deployment. `munge.service` is the Slurm
authentication daemon; both units are ordered after it so the native Slurm
commands are available before either process starts.

## Validate and start the observer

On a new installation, validate the units and confirm neither service is active:

```bash
sudo chown root:root \
  /etc/systemd/system/slurm-advisor-controller.service \
  /etc/systemd/system/slurm-watchdog.service
sudo chmod 0644 \
  /etc/systemd/system/slurm-advisor-controller.service \
  /etc/systemd/system/slurm-watchdog.service
sudo systemd-analyze verify \
  /etc/systemd/system/slurm-advisor-controller.service \
  /etc/systemd/system/slurm-watchdog.service
sudo systemctl daemon-reload
sudo systemctl is-enabled slurm-advisor-controller.service
sudo systemctl is-active slurm-advisor-controller.service
sudo systemctl is-enabled slurm-watchdog.service
sudo systemctl is-active slurm-watchdog.service
```

The four state checks must show that neither service is enabled or active.


A nonzero status from `is-enabled` or `is-active` is expected for disabled or
inactive services. Investigate an already-running installation before replacing
its effective configuration.

Wait until the backend workloads are ready and the private API Gateway is
reachable from the Slurm host. Confirm `HEIMDALL_ENDPOINT` in the controller's
environment file matches the recorded address, then start only the observer:

```bash
sudo systemctl enable --now slurm-advisor-controller.service
sudo systemctl status slurm-advisor-controller.service --no-pager
sudo journalctl -u slurm-advisor-controller.service --since=-5m --no-pager
sudo systemctl is-enabled slurm-watchdog.service
sudo systemctl is-active slurm-watchdog.service
```

Confirm that the controller is active. Review the controller journal for Slurm
polling, parsing, and API Gateway errors.

SAC logs `API Gateway connection established` at INFO with `heimdall_endpoint`
when its gRPC connection reaches `Ready`. This message is emitted once per
process. It confirms the transport is ready but does not confirm authorization
or job publication.

To verify job publication through the API Gateway, start and
[observe a first job](operations.md#observe-a-first-job): submit the opted-in
observe-mode job, capture its Slurm allocation ID, and allow at least one
configured polling interval for publication.

Confirm the job appears in the dashboard. This establishes successful job
publication through the gateway.

## Log-path discovery

SAC discovers the parent allocation's stdout and numeric custom-step stdout
paths for each exact job attempt. It excludes synthetic `.batch` and `.extern`
steps, deduplicates paths, and publishes them as
`scheduler_metadata.slurm_log_file_paths` for the backend to associate logs
with the attempt. Discovery reads scheduler metadata; it does not read, copy,
or collect the log files.

### Active and finalized attempts

Before reconciliation starts, SAC runs `sacct --version` once to select its
process-lifetime log-discovery mode. This prints the local client version
without querying accounting. Releases before Slurm 25.05 use a direct text
query; 25.05 and later use expanded JSON paths. A failed or unrecognized
version probe uses JSON discovery and logs a warning. The probe uses the
log-enrichment deadline and is not repeated.

Active attempts use expanded parent paths from `squeue`, with an
attempt-checked `scontrol` fallback. Background step discovery uses the
selected mode. Legacy mode runs one text query per job per refresh, requesting
`JobIDRaw,DBIndex,Submit,Start,WorkDir,JobID,JobName,SubmitLine`. `--array`
prints concrete tasks; `JobID` supplies array identity and `JobName` supplies
`%x` expansion without a supplemental JSON query. Unix timestamps are requested
through `SLURM_TIME_FORMAT=%s`. Embedded newlines and pipes in `SubmitLine`
are retained within the same accounting record. Allocation rows retain
accounting response order and use the same timestamp grouping as JSON to assign attempt indices;
`DBIndex` joins each numeric step to its allocation, so reused IDs such as `.0`
after a requeue do not mix paths across attempts. Slurm's `None` and `Unknown`
timestamp values are treated as missing; a valid submission time still permits
discovery when the allocation's start time is missing. Allocations missing both
identity timestamps remain invalid. Only the active attempt's
step options are expanded; historical output parsing failures cannot suppress
it, and the worker does not reparse the parent output option.

For newer or unknown clients, a refresh uses one metadata query with
`sacct --duplicates --json=v0.0.43` and publishes the expanded paths it returns.
Missing `stdout_expanded` fields leave that source incomplete and retry with
backoff; they do not trigger a text query. The JSON parser version is unchanged.

Finalized legacy refreshes use one text metadata query for all due attempts of
each job, in addition to the ordinary JSON status query. Any valid expanded
parent paths already returned by that status query are retained, including
when text discovery fails. Complete native parent paths avoid reparsing the
parent command. Expanded JSON parent paths take precedence over parsed paths.

Text discovery covers accounting output such as the checked-in Slurm 23.02.7
fixture, which omits expanded step stdout. Background completion updates the
exact-attempt cache; a subsequent active-job poll publishes the recovered
step paths. Those paths become eligible for the optional log-link materializer
while the job is running. Active polling never waits for the text query.

### Failures and retry

Parent and step completeness are tracked separately. An unsupported parent
output option does not discard valid step paths, and a step parsing
failure does not discard a valid parent path. Independently recovered paths
are published while only the unresolved source retries. Malformed rows or
ambiguous allocation identities prevent using that text response's parsed
paths. If the text query fails or exceeds its command budget or deadline,
valid JSON paths are still published. An incomplete source retains its cached
paths and adds valid newly discovered paths, then retries with backoff;
a complete source succeeds independently. A later complete refresh
replaces the incomplete step result. Missing log metadata never changes the
observed job state or prevents its publication.

Active discovery shares the background work item's deadline, worker limits,
and bounded command retries. Finalized discovery shares the existing enrichment
deadline and command budget.
`SLURM_ADVISOR_LOG_METADATA_ENRICHMENT_TIMEOUT` defaults to `2s`, and
`SLURM_ADVISOR_LOG_METADATA_REFRESH_INTERVAL` defaults to `5m`; the Helm values
are `controller.logMetadataEnrichmentTimeout` and
`controller.logMetadataRefreshInterval`. These settings apply to both active
background discovery and finalized metadata refresh.

## Result and next step

The controller is active in observe mode and the watchdog remains disabled.
Completing the [first-job sanity check](operations.md#observe-a-first-job)
confirms publication to the backend.
Use [troubleshooting](troubleshooting.md) for missing jobs or connection errors.
