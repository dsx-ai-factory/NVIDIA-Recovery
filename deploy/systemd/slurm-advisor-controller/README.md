# Native Slurm systemd units

These units run the Slurm advisor controller and watchdog directly on a Slurm
control-plane host. They are an alternative to the Kubernetes chart when the
processes must use the host's native Slurm client.

Both checked-in units are deliberately observe-only. Their `ExecStart` commands
pin the mode to `observe` and scheduler-action acknowledgement to `false`, so an
environment-file edit cannot enable Slurm mutations. The advisor does create or
update observed job records in NVIDIA Recovery even though it cannot mutate Slurm. Its
partition allowlist does not limit job discovery or those upserts. The watchdog
only logs release candidates and keeps its tracking state in memory.

The units use the native `slurm` service account as a compatibility default so
they can retain a complete scheduler view on installations that enable Slurm
`PrivateData`. That account is commonly scheduler-privileged: Linux capability
restrictions do not remove its Slurm RPC authority, so observe-only safety still
depends on the command-pinned application mode. Prefer a dedicated unprivileged
observer identity when it can read every job required from `squeue` and `sacct`,
and override `User=` with a local systemd drop-in. Do not run either daemon as
root, and review the service identity again before any action-mode deployment.

## Filesystem layout

| Path | Purpose |
| --- | --- |
| `/usr/local/libexec/heimdall-oss/slurm-advisor-controller` | Advisor binary |
| `/usr/local/libexec/heimdall-oss/slurm-watchdog` | Watchdog binary |
| `/etc/heimdall-oss/slurm-advisor-controller.env` | Site-specific advisor configuration |
| `/etc/heimdall-oss/slurm-watchdog.env` | Site-specific watchdog configuration |
| `/etc/systemd/system/slurm-advisor-controller.service` | Advisor unit |
| `/etc/systemd/system/slurm-advisor-controller.service.d/log-links.conf` | Optional log-link destination write grant |
| `/etc/systemd/system/slurm-watchdog.service` | Watchdog unit |

Use binaries built for the host architecture and compatible with the installed
Slurm client. The Go daemons are static, but they execute `squeue`, `sacct`, and
potentially `scontrol` from `PATH`.

## Install

From the repository root, set the first two paths to extracted or locally built
binaries:

```bash
ADVISOR_BINARY=/path/to/slurm-advisor-controller
WATCHDOG_BINARY=/path/to/slurm-watchdog

sudo install -d -o root -g root -m 0755 /usr/local/libexec/heimdall-oss
sudo install -o root -g root -m 0755 "${ADVISOR_BINARY}" /usr/local/libexec/heimdall-oss/slurm-advisor-controller
sudo install -o root -g root -m 0755 "${WATCHDOG_BINARY}" /usr/local/libexec/heimdall-oss/slurm-watchdog

sudo install -d -o root -g root -m 0755 /etc/heimdall-oss
sudo install -o root -g root -m 0640 deploy/systemd/slurm-advisor-controller/slurm-advisor-controller.env.example /etc/heimdall-oss/slurm-advisor-controller.env
sudo install -o root -g root -m 0640 deploy/systemd/slurm-advisor-controller/slurm-watchdog.env.example /etc/heimdall-oss/slurm-watchdog.env

sudo install -o root -g root -m 0644 deploy/systemd/slurm-advisor-controller/slurm-advisor-controller.service /etc/systemd/system/slurm-advisor-controller.service
sudo install -o root -g root -m 0644 deploy/systemd/slurm-advisor-controller/slurm-watchdog.service /etc/systemd/system/slurm-watchdog.service
```

Edit both environment files for the site's Slurm paths, partitions, environment
routing, and metrics ports. Set the advisor's `HEIMDALL_ENDPOINT` to the API
gateway's gRPC endpoint.
The empty checked-in endpoint intentionally prevents advisor startup until it is
configured. Replace the example environment name and partition allowlists too.
An empty partition allowlist means all partitions; the nonempty `replace-me`
value is a fail-closed placeholder for any later action-mode review. Set
`SLURM_WATCHDOG_ENVIRONMENT_PATTERN` explicitly: a literal uses the advisor's
exact legacy-compatible environment semantics, while `*` matches zero or more
characters (including `/`) and therefore admits every valid APS environment.
The watchdog is a separate process and cannot inherit
`SLURM_ADVISOR_ENVIRONMENT` from the advisor service.
The advisor environment also sets independent bounded concurrency for NVIDIA Recovery
upserts and targeted Slurm accounting. Keep
`SLURM_ADVISOR_JOB_UPSERT_WORKER_POOL_SIZE` and
`SLURM_ADVISOR_ACCOUNTING_WORKER_POOL_SIZE` conservative until dependency latency
and saturation justify tuning them; the accounting phase timeout remains a hard
safety ceiling.
When gateway authentication is disabled, omit
`SLURM_ADVISOR_SERVICE_ACCOUNT_TOKEN_PATH`; an explicitly empty value is
invalid. gRPC remains plaintext whether or not bearer-token authentication is
enabled; authentication does not add TLS, and a token must never traverse an
untrusted network. Keep this connection on a restricted private network and
keep environment files root-owned because they may later contain a token path
or other deployment-specific data.

## Audit buffering

The advisor and watchdog always emit recovery lifecycle events. Each process uses a bounded, best-effort in-memory buffer; reaching
capacity drops new events instead of waiting for Notifications delivery.

Set `*_AUDIT_TRAIL_DESTINATION` to `logging` (the default, also used for empty
values) or `gateway`. The controller reuses its existing `HEIMDALL_ENDPOINT`
connection and credentials. The watchdog opens an audit-only connection to
`SLURM_WATCHDOG_AUDIT_GATEWAY_ENDPOINT` and optionally reads the projected token
at `SLURM_WATCHDOG_SERVICE_ACCOUNT_TOKEN_PATH` before each RPC. Tokens rotate
without reconnecting. Both use the existing plaintext gateway transport; token
authentication does not add TLS. HTTP audit destination URLs are not supported.

The max batch size, buffer size, and flush interval must
all be positive. Normal service shutdown closes and drains the
process-local buffer before exit.

## Optional log-link materialization

The stock advisor unit remains write-denying: `ProtectSystem=strict` and
`ProtectHome=true` are unchanged, and it has no `ReadWritePaths` grant. The
environment example sets `SLURM_ADVISOR_LOG_LINKS_ENABLED=false`. This is
separate from scheduler-action safety: changing it to `true` authorizes the
advisor to create and retire links even though the unit still pins scheduler
mode to `observe`. Jobs whose effective mode is `ignore` remain excluded.

To opt in, choose one dedicated destination and at least one normalized absolute
source root. The destination must not equal, contain, be contained by, overlap,
or alias any source root through a bind mount or another backing-store mapping;
verify the effective mounts rather than relying on path spelling alone. Prepare
the destination as the only directory writable by the advisor identity:

```bash
sudo install -d -o slurm -g slurm -m 0750 /var/lib/heimdall-oss/log-links
sudo install -d -o root -g root -m 0755 \
  /etc/systemd/system/slurm-advisor-controller.service.d
sudo install -o root -g root -m 0644 \
  deploy/systemd/slurm-advisor-controller/slurm-advisor-controller-log-links.conf.example \
  /etc/systemd/system/slurm-advisor-controller.service.d/log-links.conf
```

The example drop-in grants exactly:

```systemd
[Service]
ReadWritePaths=/var/lib/heimdall-oss/log-links
```

If the site chooses a different destination, replace that exact path in both
the drop-in and `SLURM_ADVISOR_LOG_LINKS_DESTINATION`. `ReadWritePaths` neither
creates the directory nor changes Unix ownership and mode, so both must be
correct before restart. Run only one advisor materializer against a destination.
If a collector uses another service identity, grant it read and directory
traversal without granting write access; a dedicated shared group may be more
appropriate than the example `slurm:slurm` ownership.

Uncomment the complete log-link settings in
`/etc/heimdall-oss/slurm-advisor-controller.env`, change
`SLURM_ADVISOR_LOG_LINKS_ENABLED` to `true`, and replace the JSON source-root
and optional pattern arrays. The single quotes in the example are systemd
EnvironmentFile quoting: they preserve the JSON double quotes and are not sent
to the process. For array tasks, a pattern containing `{job_id}` is evaluated
with both the concrete task ID and its array-parent job ID.

Filesystem synchronization is asynchronous inside the advisor process. One
worker serializes link and manifest operations, with one latest-wins pending
snapshot while the worker is busy. A controller poll does not wait for those
operations, and a newer snapshot may replace an obsolete pending snapshot.
Stopping the service signals cancellation to in-flight work and drops pending
work without a shutdown drain; the next process recovers the manifest and
submits current active-job state. No additional unit, queue storage, environment
variable, or write grant is required. The single advisor service remains the
only writer.

Treat every configured source root as trusted. The advisor intentionally creates
a link before a missing Slurm target appears. An identity that can later replace
that target or an ancestor below the source root with another symbolic link can
influence what a following collector reads. The advisor's read-only view of a
source does not constrain other writers, and periodic validation cannot close
this time-of-check/time-of-use race. Do not enable the feature for a source tree
writable by untrusted workloads unless the collector enforces beneath-root path
resolution when it opens each file.

A collector following the absolute links must see the destination and every
source root at the same absolute paths as the advisor. Native services already
share the host namespace unless separately hardened. A containerized collector
must bind-mount each path at that identical path and should mount the
destination and sources read-only. The collector is a reader; it must not
create, replace, or clean up links.

Run the collector as a dedicated unprivileged identity in a hardened mount
namespace that exposes only the required destination and source roots. It must
not expose service credentials, secret files, home directories, the host root,
or unrelated mounts that a substituted source link could reach. Avoid passing
collector credentials through its filesystem or environment. `ProtectSystem`,
`ProtectHome`, read-only bind mounts, and an explicit filesystem allowlist are
appropriate starting points for a separate native collector unit, but they must
be validated against that collector's actual runtime requirements. Read-only
mounts prevent collector writes; they do not constrain a source writer.

Review the hardening namespace before selecting paths:

- `ProtectHome=true` hides `/home`, `/root`, and `/run/user`; a source or
  destination there will remain inaccessible.
- `PrivateTmp=true` gives the advisor private `/tmp` and `/var/tmp` mounts, so
  host or collector paths there do not name the same files.
- `ProtectSystem=strict` makes the filesystem hierarchy read-only except for
  the exact `ReadWritePaths` destination. Adding a broad parent path defeats
  the intended least-privilege boundary.
- These controls do not grant read or traversal permission. Every source
  directory and target still must be readable by the configured `User=`.

Validate the repository contract and installed effective unit before enabling:

```bash
make test-slurm-advisor-controller-systemd
sudo systemd-analyze verify /etc/systemd/system/slurm-advisor-controller.service
sudo systemctl daemon-reload
sudo systemctl cat slurm-advisor-controller.service
sudo systemd-analyze security slurm-advisor-controller.service
```

Treat link changes as eventually consistent when validating a restart. Confirm
log-link metrics or a known destination entry rather than assuming a successful
controller poll has already materialized its snapshot. The stock
`TimeoutStopSec=45s` remains a process-lifecycle bound, not a promise to drain
pending filesystem work.

After restart, use `systemctl status` and the journal to confirm configuration
without printing path-derived customer data. Inspect a known destination entry
with `readlink` or `lstat`; do not use a command that follows and rewrites the
link.

To roll back, restore `SLURM_ADVISOR_LOG_LINKS_ENABLED=false`, remove only the
optional `log-links.conf` drop-in, run `systemctl daemon-reload`, and restart
the advisor. Disabling the feature intentionally leaves its links and ownership
manifest in place. Review the manifest and collector configuration before any
deliberate cleanup; never recursively delete the shared directory as part of a
unit rollback.

Validate and load the units:

```bash
sudo systemd-analyze verify /etc/systemd/system/slurm-advisor-controller.service /etc/systemd/system/slurm-watchdog.service
sudo systemctl daemon-reload
```

Start only the advisor for an observe-only deployment:

```bash
sudo systemctl enable --now slurm-advisor-controller.service
sudo systemctl status slurm-advisor-controller.service --no-pager
sudo journalctl -u slurm-advisor-controller.service --since=-5m --no-pager
```

Install the watchdog unit and configuration but do not enable or start it until
its scheduler-action behavior has been separately reviewed. On a fresh install,
confirm its state without changing an existing deployment:

```bash
sudo systemctl is-enabled slurm-watchdog.service
sudo systemctl is-active slurm-watchdog.service
```

Moving either daemon into action mode requires a deliberate unit-file change,
an explicit acknowledgement, and a new safety review. An environment-file-only
change is intentionally insufficient.
