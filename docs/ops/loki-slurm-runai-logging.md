# Centralized Logging for Slurm and Kubernetes with Grafana Alloy → Loki

**Goal:** Ship AI/GPU job logs and node system logs from both bare-metal/VM
Slurm compute nodes and Run:ai (Kubernetes) GPU nodes into a single Grafana Loki
backend, queryable in Grafana, with GPU/job metadata for correlation.

**Author's note on sourcing:** Version numbers and component syntax below were
verified against official Grafana/NVIDIA docs in July 2026. Pin these exact
versions in your own repo; do not track `latest` in production.

---

## 1. Verified component versions (July 2026)

| Component | Version | Role |
|-----------|---------|------|
| Grafana Alloy (agent) | `v1.17.1` | Log collection agent (systemd + DaemonSet) |
| `grafana/alloy` Helm chart | `1.10.1` (app `v1.17.1`) | K8s deployment of Alloy |
| Grafana Loki | `v3.7.2` (shipped by chart `17.2.0`; `3.7.3` is the latest standalone) | Log storage/query backend |
| `grafana-community/loki` Helm chart | `17.2.0` (appVersion `3.7.2`) | K8s deployment of Loki (chart moved to the community repo — see §4) |
| NVIDIA dcgm-exporter | `4.8.2` (DCGM `4.5.3`) | GPU metrics + pod/job metadata |
| NVIDIA GPU Operator | `v26.3.3` | Manages drivers + dcgm-exporter on k8s |
| Grafana | `>= 11.x` | Query UI / dashboards |

> Promtail is **end-of-life (2026-03-02)**. Do not deploy it. Alloy is the
> official successor and is Grafana's OpenTelemetry Collector distribution.

---

## 2. Architecture

```
   SLURM COMPUTE NODES                    RUN:AI / KUBERNETES GPU NODES
   (bare-metal / VM, systemd)             (Kubernetes)
   ┌───────────────────────────┐          ┌───────────────────────────┐
   │ alloy.service (systemd)   │          │ Alloy DaemonSet (Helm)    │
   │  loki.source.journal ─────┐          │  discovery.kubernetes     │
   │   (syslog, slurmd,        │          │  discovery.relabel        │
   │    slurmctld, kernel)     │          │  loki.source.kubernetes   │
   │  loki.source.file ────────┤          │  loki.source.kubernetes_events
   │   (job stdout: slurm-%j)  │          │  loki.process             │
   │  loki.process ────────────┤          │  loki.write ──────────────┐
   │  loki.write ──────────────┼──┐       └───────────────────────────┼──┐
   └───────────────────────────┘  │                                   │  │
                                   ▼                                   ▼  │
                              ┌─────────────────────────────────────────┐
                              │            GRAFANA LOKI (v3.7.3)         │
                              └──────────────────┬──────────────────────┘
                                                 ▼
                                         ┌────────────────┐
                                         │    GRAFANA     │
                                         └────────────────┘

   GPU METADATA
   • Run:ai/k8s:  dcgm-exporter DaemonSet, -k flag → pod/namespace/container labels
   • Slurm:       dcgmi stats in prolog/epilog, keyed on $SLURM_JOBID
   Correlate logs↔GPU metrics in Grafana on shared labels (job_id, pod, gpu_uuid).
```

The same Alloy binary and configuration language serve these deployment shapes.
Slinky uses a node-system DaemonSet plus one shared application reader (§6.4).

### 2.1 NVIDIA Recovery Slurm log contract

All NVIDIA Recovery Slurm profiles—conventional Slurm, Slinky/Kind, and BCM—use
the same source-domain contract even when their collector lifecycle or node
identity mapping differs:

| Domain | Required queryable fields | Ownership |
| --- | --- | --- |
| Application output | `cluster`, `appName="slurm-app-log"`, `job_id`; preserve `filename` | One scheduler job; exact attempts are resolved from scheduler state, source path, and time. |
| Node system logs | `cluster`, `component="journal"`, validated Slurm `node_name` | One node and time window; workload, workflow, and attempt fields are forbidden. |

`slurm_job_attempt` is optional supporting evidence only when a trusted
producer knows the restart count directly. Alloy must not infer it. Extra
bounded labels such as `unit`, `syslog_identifier`, `facility`, and `level` are
allowed, but consumers must not depend on provider-specific aliases.

System-log qualification writes through the node's normal syslog ingress (for
example, `logger`) or, on trusted disposable Kubernetes nodes only, through an
exact-file `/var/log/syslog` hostPath. It must then rely on the independently
deployed node collector. Posting a fixture directly to Loki does not exercise
syslog ingestion and does not qualify the deployment.

---

## 3. Prerequisites

- A reachable Loki push endpoint (e.g. `http://loki.example.com:3100/loki/api/v1/push`).
  If Loki is multi-tenant, every agent must send an `X-Scope-OrgID` header.
- Network path from every Slurm node and every k8s node to Loki (TCP 3100 by default).
- On Slurm nodes: `systemd` (for journald) and permission to read the Slurm job
  output directory.
- On k8s: Helm 3, cluster-admin for the logging namespace, and the NVIDIA GPU
  Operator already installed (or plan to install it, §8).
- Decide a **label taxonomy up front** (§9) — retrofitting labels in Loki is painful.

---

## 4. Deploy Loki (the backend)

If you already run Loki, skip to §5.

> **NVIDIA Recovery bundled option.** The NVIDIA Recovery umbrella chart
> (`deploy/helm/nvidia-recovery`) ships this same single-binary Loki as an optional
> sub-chart: set `global.loki.bundled.enabled=true`. Query consumers (Watcher,
> Attribution, and the dependency checker) must explicitly name its gateway under
> `global.log_query.providers.loki.application.url` and `.system.url`.
> See `deploy/helm/README.md`. The rest
> of this section is the standalone install of the identical configuration.

> **Query selection is separate.** `global.log_query.backend` selects the
> in-process log-query implementation and defaults to `loki`.
> `global.loki.bundled`, this write path, and Alloy destinations remain
> independent. Selecting `victorialogs` neither deploys VictoriaLogs nor
> redirects ingestion, and it requires validated native LogsQL variants plus
> VictoriaLogs 1.39.0 or newer, separate message/timestamp/field qualification,
> and causal E2E qualification. See
> [Log-Query Backends](../architecture/log-query-backends.md).

Minimal single-binary install for a small/
mid cluster (scale to microservices mode later):

> **Chart repo moved.** As of ~March 2026 the OSS Loki Helm chart is maintained
> in the **`grafana-community`** repo, not `grafana`. The Alloy chart (§6.2) is
> still `grafana/alloy`; only Loki moved.

```bash
helm repo add grafana-community https://grafana-community.github.io/helm-charts
helm repo update

# values-loki.yaml — single-binary (SSD) mode, filesystem or object store
cat > values-loki.yaml <<'EOF'
loki:
  auth_enabled: false        # set true + X-Scope-OrgID for multi-tenant
  commonConfig:
    replication_factor: 1
  schemaConfig:
    configs:
      - from: "2024-04-01"
        store: tsdb
        object_store: filesystem   # use s3/gcs in production
        schema: v13
        index:
          prefix: loki_index_
          period: 24h
  limits_config:
    retention_period: 744h         # 31 days; tune to your needs
  # retention_period alone does NOT delete data — the compactor must run with
  # retention enabled, or storage grows unbounded until the PVC fills.
  compactor:
    retention_enabled: true
    delete_request_store: filesystem   # match object_store above (s3/gcs in prod)
    compaction_interval: 10m
deploymentMode: SingleBinary
singleBinary:
  replicas: 1
  persistence:
    enabled: true
    whenDeleted: Retain
    whenScaled: Retain
    size: 200Gi
# disable microservice components in single-binary mode
backend: { replicas: 0 }
read:    { replicas: 0 }
write:   { replicas: 0 }
EOF

# Confirm the pinned chart's Loki appVersion before installing:
helm show chart grafana-community/loki --version 17.2.0 | grep appVersion   # → 3.7.2

helm upgrade --install loki grafana-community/loki \
  --namespace logging --create-namespace \
  --version 17.2.0 \
  -f values-loki.yaml
```

> Production: use object storage (S3/GCS/MinIO), `replication_factor: 3`, and
> the `distributed`/SSD scalable mode. The above is a functional starting point.

Point Grafana at Loki as a data source (URL `http://loki.<ns>.svc:3100`, or the
external LB address for Slurm-side visibility).

> **⚠️ Bare-metal Slurm nodes need an externally-reachable Loki.** The chart above
> exposes Loki as a Kubernetes **`ClusterIP`**, reachable only from inside the
> cluster. The Slurm-side Alloy (§5) runs *outside* Kubernetes, so its
> `loki.write` URL **cannot** be `loki.<ns>.svc` — a `ClusterIP` is not enough.
> Expose Loki via a `NodePort` (push to `<any-k8s-node-ip>:<nodePort>`), a
> `LoadBalancer`, or an ingress, and point the Slurm `loki.write` URL at that
> address. In-cluster collectors (§6) can keep using the `ClusterIP`.

---

## 5. Slurm compute nodes — Alloy as a systemd service

### 5.1 Install Alloy

> **⚠️ Durability on image-based / stateless compute nodes.** If compute nodes
> boot from a shared image or are reprovisioned on reboot (BCM `/cm/images/…`,
> Warewulf, xCAT, cloud golden images), a **live** `apt/dnf install` of Alloy plus
> `/etc/alloy/config.alloy` will **not** survive the next reimage — logging
> silently stops. Install Alloy and its config **into the node image** (and enable
> the service there), or manage both through your provisioning/config layer (BCM
> overlay, Ansible, image build). Two portable gotchas when baking into an image
> chroot: (a) `systemctl enable` no-ops in a chroot ("Running in chroot; ignoring")
> — create the `multi-user.target.wants/alloy.service` symlink by hand instead; and
> (b) add a temporary `/etc/resolv.conf` so the in-chroot package manager can
> resolve the Grafana repo. See **§5.5** for a full tested BCM walkthrough. Validate
> by reprovisioning one node and re-checking `systemctl is-active alloy` and that
> its lines still reach Loki.

The exact package version string may carry a distro revision (e.g. `1.17.1-1`) —
check what your repo offers (`apt-cache madison alloy` / `dnf --showduplicates
list alloy`) and pin to it.

Debian/Ubuntu:
```bash
sudo mkdir -p /etc/apt/keyrings
wget -q -O - https://apt.grafana.com/gpg.key | \
  sudo gpg --dearmor -o /etc/apt/keyrings/grafana.gpg
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | \
  sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install -y alloy=1.17.1-1        # pin the version (field-tested)
```

RHEL/Rocky/SLES:
```bash
sudo tee /etc/yum.repos.d/grafana.repo <<'EOF'
[grafana]
name=grafana
baseurl=https://rpm.grafana.com
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://rpm.grafana.com/gpg.key
sslverify=1
EOF
sudo dnf install -y alloy-1.17.1
```

### 5.2 Validate node identity and grant read access

Alloy runs as user `alloy`. Let it read the journal and Slurm job output:
```bash
sudo usermod -a -G systemd-journal alloy
# If Slurm writes job output to a shared dir, ensure 'alloy' can read it.
# Set BOTH a recursive ACL (existing files) AND a default ACL (files created
# LATER by jobs) — the default ACL is the part that matters, since jobs create
# new output files after this runs, often mode 0600 under a restrictive umask:
#   sudo setfacl -R  -m u:alloy:rX /var/spool/slurm/job-logs   # existing
#   sudo setfacl -d -m u:alloy:rX /var/spool/slurm/job-logs    # future files
```

The reference journal pipeline maps journald's host field to the canonical
Slurm `node_name`. Before rollout, prove that the node reports the same name to
both systems:

```bash
NODE_NAME="$(hostname -s)"
scontrol show node "$NODE_NAME" >/dev/null
JOURNAL_NODE_NAME="$(journalctl -n 1 -o json | jq -r '._HOSTNAME')"
test "$JOURNAL_NODE_NAME" = "$NODE_NAME"
```

If this check fails, add a deployment-specific relabel or explicit per-node
value that maps to the scheduler-visible Slurm `NodeName`. Do not silently use
a pod, VM, or collector hostname as `node_name`.

Confirm where Slurm writes stdout/stderr. Common cases:
- Default: files land in the **job's submission directory** (`--output=` path) —
  often on shared storage. If so, tail that shared path from one collector, or
  set a cluster-wide `--output` convention.
- Many sites set a fixed pattern, e.g. `/var/spool/slurm/job-logs/slurm-%j.out`
  via job submit plugin or user convention. Adjust the glob in §5.3 to match.

> **⚠️ Job-log capture needs a path the collector can *read*.** `loki.source.file`
> ships job stdout only if the file lands where its glob points **and** the `alloy`
> user can read it. Slurm's default `--output` is the submission directory (often
> shared `/home/<user>/`), created with the submitting user's ownership/umask. If
> home dirs are `0700`, `alloy` cannot even traverse in — **no job logs ship, and
> there is no error**. Fix by adopting a cluster-wide convention that writes to a
> collector-readable location — e.g. a `job_submit` Lua plugin (or user convention)
> using `--output=/var/spool/slurm/job-logs/slurm-%j.out`. Making the dir
> world-traversable (sticky `1777`) is **not enough on its own**: slurmd creates
> each output file as the job user, and under a restrictive umask (077) that file
> is `0600` — `alloy` can traverse in but still cannot *read* it. Use a **default
> ACL** so every future job file is readable (`setfacl -d -m u:alloy:rX` above), or
> relax the umask for that path. Host syslog via journald (§5.3) has no such
> dependency — only the file source does.

### 5.3 Alloy config

Write `/etc/alloy/config.alloy`:

```alloy
// ---- Loki endpoint (shared by all sources) --------------------------------
loki.write "default" {
  endpoint {
    url = "http://loki.example.com:3100/loki/api/v1/push"
    // For multi-tenant Loki, uncomment:
    // headers = { "X-Scope-OrgID" = "hpc" }
  }
  external_labels = {
    // Keep this identical to NVIDIA Recovery's environment and Watcher query_prefix.
    cluster = "customer-slurm",
  }
}

// ---- Host system logs via journald ----------------------------------------
// Captures syslog-class messages, kernel, slurmd, slurmctld, munge, etc.
loki.source.journal "host" {
  forward_to    = [loki.process.host.receiver]
  labels        = { component = "journal" }
  max_age       = "12h"
  relabel_rules = loki.relabel.journal.rules
}

// Promote useful journal fields to labels (keep cardinality sane).
loki.relabel "journal" {
  forward_to = []   // rules-only; consumed by loki.source.journal above
  rule {
    source_labels = ["__journal__systemd_unit"]
    target_label  = "unit"
  }
  rule {
    source_labels = ["__journal__hostname"]
    target_label  = "node_name"
  }
  rule {
    source_labels = ["__journal_syslog_identifier"]
    target_label  = "syslog_identifier"
  }
}

loki.process "host" {
  forward_to = [loki.write.default.receiver]
}

// ---- Slurm job stdout/stderr ----------------------------------------------
// Adjust path_targets to match your site's --output convention.
local.file_match "slurm_jobs" {
  path_targets = [
    { __path__ = "/var/spool/slurm/job-logs/slurm-*.out" },
    { __path__ = "/var/spool/slurm/job-logs/slurm-*.err" },
    { __path__ = "/var/spool/slurm/job-logs/dcgm-*.txt" },   // §7.2 GPU reports
  ]
}

loki.source.file "slurm_jobs" {
  targets    = local.file_match.slurm_jobs.targets
  forward_to = [loki.process.slurm_jobs.receiver]
}

loki.process "slurm_jobs" {
  // Extract the numeric job or array-task id from the filename into job_id.
  // Matches slurm-<id>.out/.err, slurm-<array>_<task>.out/.err, and the
  // dcgm-<id>.txt reports from §7.2.
  stage.regex {
    source     = "filename"
    expression = "(?:slurm|dcgm)-(?P<job_id>\\d+(?:_\\d+)?)\\.(?:out|err|txt)$"
  }
  stage.labels {
    values = { job_id = "" }
  }
  stage.static_labels {
    values = { appName = "slurm-app-log" }
  }
  forward_to = [loki.write.default.receiver]
}
```

Alloy converts each target's internal `__path__` value into the `filename`
label automatically. Preserve it: NVIDIA Recovery uses the scheduler-discovered path,
time window, and unique evidence marker to resolve exact attempts without
requiring the collector to synthesize a restart count.

> **Cardinality warning:** `job_id` as a Loki label is high-cardinality. For very
> large clusters (>~10k jobs/day) prefer keeping `job_id` in the log *line* and
> use `stage.structured_metadata` (Loki 3.x) instead of a stream label, then
> filter with `| job_id="123"` at query time. The example above uses a label for
> simplicity; switch to structured metadata if stream counts explode.

### 5.4 Enable and verify

```bash
sudo systemctl enable --now alloy
sudo systemctl status alloy
# Validate config without starting:
alloy fmt /etc/alloy/config.alloy
# Live component/debug UI (bind carefully; default 12345):
#   alloy run /etc/alloy/config.alloy --server.http.listen-addr=127.0.0.1:12345
sudo journalctl -u alloy -f     # watch for push errors
```

Confirm in Grafana Explore:
```logql
{cluster="customer-slurm", component="journal", node_name="gpu-001"} | line_format "{{.unit}} {{ __line__ }}"
{cluster="customer-slurm", appName="slurm-app-log", job_id="123456"}
```

### 5.5 Baking Alloy into an image-based node (BCM example)

For the durability caveat in §5.1: this is the concrete, **field-tested** procedure
to bake Alloy into an image-based node so logging survives a reboot/reprovision.
Validated on **BCM (Base Command Manager)**; the same shape applies to
Warewulf/xCAT chroots and cloud golden images (chroot in, install, symlink the
service, rebuild/redeploy the image).

On BCM, compute nodes boot from a software image at `/cm/images/<image>`. A live
`apt install` on a running node is wiped on the next reprovision — install into the
**image** instead:

```bash
# Run on the HEAD node. <image> = the node's software image (e.g. slurm-image).
IMG=/cm/images/slurm-image

# 1) The image ships without resolv.conf; give the chroot DNS *temporarily* so
#    apt can resolve apt.grafana.com. Note the pre-existing state so we can
#    restore it — see step 3 (do NOT leave the head node's resolv.conf baked in).
HAD_RESOLV=0; [ -e "$IMG/etc/resolv.conf" ] && HAD_RESOLV=1
cp -f /etc/resolv.conf "$IMG/etc/resolv.conf"

# 2) Install + configure Alloy INSIDE the image chroot.
cm-chroot-sw-img "$IMG" <<'CHROOT'
mkdir -p /etc/apt/keyrings
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor -o /etc/apt/keyrings/grafana.gpg
echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" \
  > /etc/apt/sources.list.d/grafana.list
apt-get update && apt-get install -y alloy=1.17.1-1
usermod -aG systemd-journal alloy

# Drop the §5.3 config into the image. IMPORTANT: paste your REAL §5.3 config
# between the CFG markers below — the placeholder line is only a reminder. A
# comment-only config leaves Alloy *running but shipping nothing*, and every
# verify step below still shows green. Do not skip this.
cat > /etc/alloy/config.alloy <<'CFG'
# >>> REPLACE THIS LINE with the full §5.3 /etc/alloy/config.alloy
# >>> (loki.write + loki.source.journal + loki.relabel + slurm_jobs) <<<
CFG
# Fail loudly if the placeholder was left in place:
grep -q 'loki.write' /etc/alloy/config.alloy \
  || { echo 'ERROR: config.alloy has no real config — paste your §5.3 config'; exit 1; }

mkdir -p /var/spool/slurm/job-logs && chmod 1777 /var/spool/slurm/job-logs
# Default ACL so future job output files (created 0600 by jobs) stay readable:
setfacl -d -m u:alloy:rX /var/spool/slurm/job-logs 2>/dev/null || true

# NOTE: `systemctl enable` fails in a chroot ("Running in chroot; ignoring").
# Create the wants symlink by hand — that's exactly what enable does. Resolve the
# unit's real path (/usr/lib on merged-/usr, /lib otherwise) and ensure the target
# .wants dir exists first (a minimal image may not have created it yet):
UNIT="$(ls /usr/lib/systemd/system/alloy.service /lib/systemd/system/alloy.service 2>/dev/null | head -1)"
mkdir -p /etc/systemd/system/multi-user.target.wants
ln -sf "$UNIT" /etc/systemd/system/multi-user.target.wants/alloy.service
CHROOT

# 3) Restore the image's resolv.conf — do NOT bake the head node's (often
#    internal-only) nameservers into every compute node: it can break DNS on
#    booted nodes and leaks internal DNS IPs (a private surface) into the image.
if [ "$HAD_RESOLV" -eq 0 ]; then rm -f "$IMG/etc/resolv.conf"; fi
#    (If HAD_RESOLV=1, restore your original from backup/version control instead.)

# 4) Apply to already-running nodes (dry-run is the default; -w actually writes).
#    Newly-provisioned nodes pick it up automatically on next boot.
cmsh -c "device use node003; imageupdate -w"      # or simply reboot the node
```

Verify the bake landed in the image (from the head node, no chroot needed):
```bash
ls -l    /cm/images/slurm-image/usr/bin/alloy
readlink /cm/images/slurm-image/etc/systemd/system/multi-user.target.wants/alloy.service
grep '^alloy:' /cm/images/slurm-image/etc/passwd
grep -q loki.write /cm/images/slurm-image/etc/alloy/config.alloy && echo "config: real" || echo "config: PLACEHOLDER — fix it"
```

Then, after the node syncs/reboots, verify on the node itself — and confirm lines
actually reach Loki (a running Alloy with a stub config passes the first two
checks but ships nothing), e.g. with the §11 walkthrough:
```bash
systemctl is-active alloy             # -> active
journalctl -u alloy | grep -i error   # -> none (aside from a benign remotecfg noop)
# then from a host that can reach Loki, per §11.1:
#   loki_q '{cluster="customer-slurm", component="journal", node_name="node003"}'
```

---

## 6. Run:ai / Kubernetes GPU nodes — Alloy as a DaemonSet

Run:ai's built-in workload log viewer is **ephemeral** — logs disappear once a
workload reaches a terminal state, and Run:ai ships no durable forwarder. A
node-level Alloy DaemonSet tailing `/var/log/pods` is the durable path.

### 6.1 Alloy config (ConfigMap contents)

Save as `alloy-k8s-config.alloy` (referenced by the Helm values below):

```alloy
// ---- Loki endpoint --------------------------------------------------------
loki.write "default" {
  endpoint {
    url = "http://loki.logging.svc.cluster.local:3100/loki/api/v1/push"
    // headers = { "X-Scope-OrgID" = "runai" }   // if multi-tenant
  }
  external_labels = {
    cluster  = "gpu-prod",
    nodetype = "runai-gpu",
  }
}

// ---- Discover pods scheduled on THIS node ---------------------------------
// NODE_NAME must be injected via the downward API (spec.nodeName) in the
// DaemonSet — see the Helm values in §6.2. Do NOT use HOSTNAME/constants.hostname
// here: in a DaemonSet pod those resolve to the POD name, not the node name, so
// the field selector would match zero pods and no logs would ship.
discovery.kubernetes "pods" {
  role = "pod"
  selectors {
    role  = "pod"
    field = "spec.nodeName=" + sys.env("NODE_NAME")
  }
}

// ---- Relabel k8s metadata → Loki labels + resolve on-disk log path --------
discovery.relabel "pod_logs" {
  targets = discovery.kubernetes.pods.targets

  rule { source_labels = ["__meta_kubernetes_namespace"]
         target_label  = "namespace" }
  rule { source_labels = ["__meta_kubernetes_pod_name"]
         target_label  = "pod" }
  rule { source_labels = ["__meta_kubernetes_pod_container_name"]
         target_label  = "container" }
  rule { source_labels = ["__meta_kubernetes_pod_label_app_kubernetes_io_name"]
         target_label  = "app" }

  // Run:ai stamps workload identity as pod labels/annotations. Promote them so
  // job logs are queryable by Run:ai job. Verify exact key on your version:
  //   kubectl get pod <p> -o jsonpath='{.metadata.labels}'
  rule { source_labels = ["__meta_kubernetes_pod_label_project"]
         target_label  = "runai_project" }
  rule { source_labels = ["__meta_kubernetes_pod_label_release"]
         target_label  = "runai_job" }

  // NOTE: loki.source.kubernetes tails via the Kubernetes API and does NOT read
  // __path__. If you switch to the loki.source.file alternative (see the note
  // below §6.1), add __path__ and container_runtime rules here, e.g.:
  //   rule {
  //     source_labels = ["__meta_kubernetes_pod_uid", "__meta_kubernetes_pod_container_name"]
  //     separator = "/"  action = "replace"
  //     replacement = "/var/log/pods/*$1/*.log"  target_label = "__path__"
  //   }
}

// ---- Tail pod container logs via the Kubernetes API ------------------------
loki.source.kubernetes "pod_logs" {
  targets    = discovery.relabel.pod_logs.output
  forward_to = [loki.process.pod_logs.receiver]
}

loki.process "pod_logs" {
  stage.static_labels { values = { job = "kubernetes-pods" } }
  forward_to = [loki.write.default.receiver]
}

// ---- Kubernetes cluster events as logs ------------------------------------
loki.source.kubernetes_events "events" {
  job_name   = "integrations/kubernetes/eventhandler"
  log_format = "logfmt"
  forward_to = [loki.process.events.receiver]
}

loki.process "events" {
  stage.static_labels { values = { job = "kubernetes-events" } }
  forward_to = [loki.write.default.receiver]
}
```

> `loki.source.kubernetes` tails via the Kubernetes API (works without a
> hostPath mount). If you prefer reading files directly from `/var/log/pods`,
> use `loki.source.file` with a hostPath mount instead — either is valid; the
> API approach survives log-rotation edge cases better and is the documented
> default.

### 6.2 Deploy with Helm

```bash
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

cat > values-alloy.yaml <<'EOF'
controller:
  type: daemonset
alloy:
  configMap:
    create: true
    # paste the config from §6.1 here, or use --set-file (below)
  # Inject the node name so the §6.1 field selector can filter pods to this node.
  extraEnv:
    - name: NODE_NAME
      valueFrom:
        fieldRef:
          fieldPath: spec.nodeName
  # Mount node log dirs (needed only if you switch to loki.source.file):
  mounts:
    varlog: true
  clustering:
    enabled: false
  # Run on GPU nodes too (tolerate the Run:ai/GPU taints your cluster uses):
  # tolerations:
  #   - operator: Exists
EOF

# Inject the Alloy config file directly:
helm upgrade --install alloy grafana/alloy \
  --namespace logging --create-namespace \
  --version 1.10.1 \
  -f values-alloy.yaml \
  --set-file alloy.configMap.content=alloy-k8s-config.alloy
```

Give the Alloy ServiceAccount RBAC to list/watch pods, nodes, and events (the
chart creates a suitable ClusterRole by default; verify it includes `events`,
`pods`, `pods/log`, `namespaces`, `nodes`).

### 6.3 Verify

```bash
kubectl -n logging rollout status ds/alloy
# Check EVERY Alloy pod (one per node), not just one, for push failures.
# Portable across kubectl versions (the `logs ds/... --all-pods` flag needs
# kubectl >= 1.32); iterate the DaemonSet's pods instead:
for p in $(kubectl -n logging get pod -l app.kubernetes.io/name=alloy -o name); do
  kubectl -n logging logs "$p" | grep -i "level=error" && echo "  ^ in $p"
done
```

In Grafana Explore:
```logql
{nodetype="runai-gpu", namespace="<runai-project-ns>"}
{job="kubernetes-pods", runai_job="<workload>"}
{job="kubernetes-events"} |= "OOMKilled"
```

### 6.4 Slinky node-system and application profile

Use two Alloy releases in the Slurm namespace:

| Release | Sources | Placement | Positions |
| --- | --- | --- | --- |
| `alloy` | The worker's exact syslog file, read-only | One DaemonSet pod per Slurm worker | A persistent directory on that worker |
| `alloy-application` | Shared job-log links and their source files, read-only | One Deployment replica, `Recreate` | Dedicated `alloy-application-positions` PVC |

Do not tail the shared application tree from the DaemonSet: every worker would
read the same files. The examples below replace the Run:ai configuration in
§6.1–6.2 for a Slinky deployment; do not combine those pipelines.

The namespace is `slurm`, the stream cluster is `customer-slurm`, and the
worker label is `heimdall.nvidia.com/slurm-worker=true`. This retained label key
matches the [Kind worker labels](../../scripts/ci/e2e/values/kind-node-separated.yaml),
[Alloy selector](../../scripts/ci/e2e/services/values/alloy.yaml), and
[positions-directory provisioning](../../scripts/ci/e2e/cluster.sh).
Adjust the namespace,
cluster, endpoint, and mapping together for your deployment. Slinky compute
identities run in pods, but a system record must carry the validated
scheduler-visible Slurm `NodeName`. The mapping below assumes one stable Slurm
compute identity per Kubernetes worker. Validate that placement and add a rule
for every worker; unmapped workers are deliberately dropped. A movable or
many-to-one mapping needs an independently validated identity scheme before
this profile can qualify it.

Both complete pipelines below use Loki. For VictoriaLogs, change **both**
`loki.write` URLs to the deployed ingestion endpoint, for example
`http://victorialogs.monitoring.svc:9428/insert/loki/api/v1/push?disable_message_parsing=1`.
Keep message parsing disabled to preserve opaque application bodies. Query
consumers still need `global.log_query.backend=victorialogs` and native LogsQL
configuration under `global.log_query.providers.victorialogs`; changing the
write URL does not translate queries. See [Log-Query Backends](../architecture/log-query-backends.md).

#### 6.4.1 Node-system collector

Save as `alloy-slinky-system.alloy`:

```alloy
local.file_match "system_logs" {
  path_targets = [{
    __path__  = "/var/log/syslog",
    cluster   = "customer-slurm",
    component = "journal",
    host_node = sys.env("K8S_NODE_NAME"),
  }]
}

discovery.relabel "system_logs" {
  targets = local.file_match.system_logs.targets

  rule {
    source_labels = ["host_node"]
    regex         = "k8s-worker-01"
    target_label  = "node_name"
    replacement   = "slurm-node-01"
  }
  rule {
    source_labels = ["host_node"]
    regex         = "k8s-worker-02"
    target_label  = "node_name"
    replacement   = "slurm-node-02"
  }
  rule {
    source_labels = ["node_name"]
    regex         = ".+"
    action        = "keep"
  }
  rule {
    regex  = "host_node"
    action = "labeldrop"
  }
}

loki.source.file "system_logs" {
  targets    = discovery.relabel.system_logs.output
  forward_to = [loki.write.default.receiver]
}

loki.write "default" {
  endpoint {
    url = "http://loki.logging.svc.cluster.local:3100/loki/api/v1/push"
    // headers = { "X-Scope-OrgID" = "hpc" } // if multi-tenant
  }
}
```

Before installation, provision the positions directory on **every** selected
worker and grant UID `401` read access to its syslog file. Preserve those
permissions through log rotation. For example, on each worker:

```bash
sudo install -d -o 401 -g 401 -m 0750 /var/lib/alloy-system-positions
sudo setfacl -m u:401:r /var/log/syslog
```

Save as `values-alloy-slinky-system.yaml`. This mounts only the exact host
syslog file; use `/var/log/messages` in both files on RHEL-based workers.

```yaml
controller:
  type: daemonset
  nodeSelector:
    heimdall.nvidia.com/slurm-worker: "true"
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 0
      maxUnavailable: 1
  volumes:
    extra:
      - name: node-syslog
        hostPath:
          path: /var/log/syslog
          type: File
      - name: positions
        hostPath:
          path: /var/lib/alloy-system-positions
          type: Directory
alloy:
  storagePath: /var/lib/alloy-positions
  enableReporting: false
  mounts:
    extra:
      - name: node-syslog
        mountPath: /var/log/syslog
        readOnly: true
      - name: positions
        mountPath: /var/lib/alloy-positions
  securityContext:
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    capabilities:
      drop: [ALL]
global:
  podSecurityContext:
    runAsNonRoot: true
    runAsUser: 401
    runAsGroup: 401
    seccompProfile:
      type: RuntimeDefault
serviceAccount:
  create: true
  automountServiceAccountToken: false
rbac:
  create: false
configReloader:
  enabled: false
```

The pinned chart supplies `K8S_NODE_NAME` from `spec.nodeName`.
Add `controller.tolerations` if the Slurm workers are tainted. Host positions
survive pod replacement on that worker; replacing or reimaging the worker also
requires preserving its positions directory. Test the site's rotation policy:
an exact-file hostPath binds the mounted file, so replacement by rename may
require replacing the collector pod to follow the new file.

#### 6.4.2 Shared application reader

This example uses the Advisor Controller's link destination at `/home/log-links`
and source tree at `/home/logs`, backed by existing `slurm-log-links` and
`slurm-job-logs` PVCs in `slurm`. Mount the destination **and every link source
at the same absolute paths as the controller**. The backing storage must support
access from both the controller and collector; configure scheduling to match
any volume topology constraints. Follow the trusted-source and filesystem
isolation requirements in the [Advisor Controller log-link guide](../../deploy/helm/slurm-advisor-controller/README.md#optional-log-link-materialization).

Save as `alloy-slinky-application.alloy`:

```alloy
local.file_match "slurm_logs" {
  path_targets = [
    {
      __path__ = "/home/log-links/**/*.out",
      cluster  = "customer-slurm",
      appName  = "slurm-app-log",
    },
    {
      __path__ = "/home/log-links/**/*.log",
      cluster  = "customer-slurm",
      appName  = "slurm-app-log",
    },
  ]
}

loki.source.file "slurm_logs" {
  targets    = local.file_match.slurm_logs.targets
  forward_to = [loki.process.slurm_logs.receiver]
}

loki.process "slurm_logs" {
  stage.regex {
    source     = "filename"
    expression = "(?:.*)[_-](?P<job_id>\\d+)(?:_date_.*|\\.(?:out|log))$"
  }
  stage.labels {
    values = { job_id = "" }
  }
  forward_to = [loki.write.default.receiver]
}

loki.write "default" {
  endpoint {
    url = "http://loki.logging.svc.cluster.local:3100/loki/api/v1/push"
    // headers = { "X-Scope-OrgID" = "hpc" } // if multi-tenant
  }
}
```

The filename expression matches the controller's job-ID link convention.
Preserve the `filename` supplied by `loki.source.file`; it supports exact-attempt
resolution without synthesizing an attempt number. For direct collection of
shared output without links, mount that output tree read-only and use its
validated filename convention from §5.3 in this singleton reader.

Create a separate positions claim. This example uses the default StorageClass;
set `storageClassName` for the site's durable storage. It must support the
collector's `fsGroup`, or be provisioned writable by UID/GID `401:401`.

Save as `alloy-application-positions.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: alloy-application-positions
  namespace: slurm
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
```

Save as `values-alloy-slinky-application.yaml`:

```yaml
controller:
  type: deployment
  replicas: 1
  updateStrategy:
    type: Recreate
  volumes:
    extra:
      - name: log-links
        persistentVolumeClaim:
          claimName: slurm-log-links
          readOnly: true
      - name: log-sources
        persistentVolumeClaim:
          claimName: slurm-job-logs
          readOnly: true
      - name: positions
        persistentVolumeClaim:
          claimName: alloy-application-positions
alloy:
  storagePath: /var/lib/alloy-positions
  enableReporting: false
  mounts:
    extra:
      - name: log-links
        mountPath: /home/log-links
        readOnly: true
      - name: log-sources
        mountPath: /home/logs
        readOnly: true
      - name: positions
        mountPath: /var/lib/alloy-positions
  securityContext:
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    capabilities:
      drop: [ALL]
global:
  podSecurityContext:
    runAsNonRoot: true
    runAsUser: 401
    runAsGroup: 401
    fsGroup: 401
    fsGroupChangePolicy: OnRootMismatch
    seccompProfile:
      type: RuntimeDefault
serviceAccount:
  create: false
  name: alloy
  automountServiceAccountToken: false
rbac:
  create: false
configReloader:
  enabled: false
```

Keep the replica count at one and disable autoscaling. `Recreate` stops the old
reader before starting its replacement during Deployment upgrades. For manual
replacement, scale to zero and wait for the old pod to finish before scaling
back to one; direct pod deletion and force deletion do not provide the same
non-overlap guarantee. `ReadWriteOnce` alone does not enforce one reader.

Alloy writes live offsets under `alloy.storagePath`, in a directory keyed by
its component name. Keep `loki.source.file "slurm_logs"`, its targets, and its
positions PVC stable across upgrades. `legacy_positions_file` is only an
import from Grafana Agent's old format, not a persistent-state destination.
A first deployment or loss of positions can replay existing files; removed
and rediscovered targets can also replay. This topology prevents deliberate
parallel readers of the shared tree and retains offsets across ordinary pod
replacement. It does **not** promise global exactly-once delivery.

#### 6.4.3 Install and verify both releases

Label the validated workers and adapt both node mapping rules before running
these commands. The PVCs for application sources must already exist. When
migrating a combined DaemonSet, fully stop its old shared-tree readers before
starting the singleton. Old `emptyDir` offsets cannot be recovered after their
pods are removed; account for initial replay during migration.

```bash
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update grafana
kubectl label node k8s-worker-01 k8s-worker-02 \
  heimdall.nvidia.com/slurm-worker=true --overwrite

# Finish replacing any old combined collector before starting the shared reader.
helm upgrade --install alloy grafana/alloy \
  --namespace slurm --create-namespace --version 1.10.1 \
  -f values-alloy-slinky-system.yaml \
  --set-file alloy.configMap.content=alloy-slinky-system.alloy \
  --wait --timeout 5m
kubectl -n slurm rollout status daemonset/alloy --timeout=5m

kubectl apply -f alloy-application-positions.yaml
helm upgrade --install alloy-application grafana/alloy \
  --namespace slurm --version 1.10.1 \
  -f values-alloy-slinky-application.yaml \
  --set-file alloy.configMap.content=alloy-slinky-application.alloy \
  --wait --timeout 5m
kubectl -n slurm rollout status deployment/alloy-application --timeout=5m
kubectl -n slurm get pods -l app.kubernetes.io/name=alloy -o wide
```

Confirm one node collector per selected worker and one shared application
reader. Generate syslog through the worker's normal ingress (§11.1) and output
through a real Slurm job (§11.2), then query the selected provider for the
required fields in §2.1. Check every worker's mapped `node_name`, preserve
application `filename`, and ensure system records have no workload or attempt
identity. Restart the collectors through their workload rollout and repeat
with a fresh marker; inspect saved offsets and ensure old markers were not
replayed in that test window. These checks qualify the tested lifecycle only.

Production Slurm compute pods must not receive a writable syslog hostPath. The
disposable E2E profile may grant an exact-file writable mount to its trusted
fault fixture, but the independently deployed Alloy mount stays read-only and
the fixture never receives backend credentials.

---

## 7. GPU / job metadata enrichment (DCGM)

Logs alone don't tell you GPU health. Add DCGM metrics and correlate at query
time on shared labels.

### 7.1 Kubernetes / Run:ai — dcgm-exporter DaemonSet

If you run the **NVIDIA GPU Operator (§8)**, dcgm-exporter is deployed for you.
To enable Kubernetes pod mapping, ensure the exporter runs with the `-k` flag
(`DCGM_EXPORTER_KUBERNETES=true`) — it reads the kubelet PodResources API
(`/var/lib/kubelet/pod-resources`) and appends `namespace`/`pod`/`container`
labels to each GPU metric. GPU Operator sets this up by default.

Standalone (without GPU Operator):
```bash
helm repo add gpu-helm-charts https://nvidia.github.io/dcgm-exporter/helm-charts
helm repo update
helm upgrade --install dcgm-exporter gpu-helm-charts/dcgm-exporter \
  --namespace gpu-monitoring --create-namespace \
  --set 'extraEnv[0].name=DCGM_EXPORTER_KUBERNETES' \
  --set 'extraEnv[0].value=true'
```

Scrape `dcgm-exporter:9400/metrics` from Prometheus. Metrics arrive labeled with
`namespace`, `pod`, `container`, `gpu`, `UUID`, `modelName`, `Hostname`.

### 7.2 Slurm — per-job GPU stats via prolog/epilog

Add DCGM job statistics keyed on `$SLURM_JOB_ID`. Note the prolog/epilog run in
`slurmd`'s environment: the job id is exposed as `SLURM_JOB_ID` (not the legacy
`SLURM_JOBID` alias, which is unset here), the allocated GPU ordinals as
`SLURM_JOB_GPUS`, and `CUDA_VISIBLE_DEVICES` is **not** exported. In your Slurm
**prolog**:
```bash
#!/bin/bash
# /etc/slurm/prolog.d/50-dcgm-stats.sh
# SLURM_JOB_GPUS holds the ordinals allocated to this job (e.g. "0,1").
GROUP=$(dcgmi group -c "job_${SLURM_JOB_ID}" -a "${SLURM_JOB_GPUS}" | \
        grep -oP 'group ID of \K\d+')
dcgmi stats -g "${GROUP}" -e            # enable stats recording for the group
dcgmi stats -g "${GROUP}" -s "${SLURM_JOB_ID}"
```
In your **epilog**:
```bash
#!/bin/bash
# /etc/slurm/epilog.d/50-dcgm-stats.sh
dcgmi stats -x "${SLURM_JOB_ID}"                       # stop recording
dcgmi stats -v -j "${SLURM_JOB_ID}" \
  > "/var/spool/slurm/job-logs/dcgm-${SLURM_JOB_ID}.txt"   # per-GPU report
```
Because the report file lands in the same job-log dir and is named
`dcgm-<id>.txt`, the §5.3 `loki.source.file` config (whose regex already matches
`slurm|dcgm-<id>.{out,err,txt}` — extend the glob to include `dcgm-*.txt`) ships
it to Loki keyed on the same `job_id`. Now
`{cluster="customer-slurm", appName="slurm-app-log", job_id="123456"}` returns both
the job's stdout **and** its GPU utilization/health report.

For richer time-series GPU telemetry on Slurm, also run **dcgm-exporter** on each
compute node and scrape it with Prometheus. dcgm-exporter does **not** ship a
systemd unit with the DCGM package — run the container, or install the binary and
create a unit yourself:
```bash
# Option A: container
sudo docker run -d --restart=always --gpus all --cap-add SYS_ADMIN \
  -p 9400:9400 nvcr.io/nvidia/k8s/dcgm-exporter:4.8.2-4.5.3-ubuntu22.04
# Option B: create /etc/systemd/system/dcgm-exporter.service wrapping the
# dcgm-exporter binary, then: sudo systemctl enable --now dcgm-exporter
```

> **⚠️ Same image-durability caveat as §5.1.** On image-based / stateless nodes,
> bake this container/service and its unit into the node image or provisioning
> layer (§5.5) — a live install is wiped on the next reprovision.

---

## 8. NVIDIA GPU Operator (if not already installed)

On the Kubernetes/Run:ai side, the GPU Operator (`v26.3.3`) manages drivers,
the container toolkit, device plugin, and dcgm-exporter:

```bash
helm repo add nvidia https://helm.ngc.nvidia.com/nvidia
helm repo update
helm upgrade --install gpu-operator nvidia/gpu-operator \
  --namespace gpu-operator --create-namespace \
  --version v26.3.3 \
  --set dcgmExporter.enabled=true
```
> Run:ai typically expects the GPU Operator (or its components) already present.
> Do not double-install drivers. If Run:ai's installer manages the operator,
> configure dcgm-exporter through Run:ai's supported path instead.

---

## 9. Label taxonomy (decide before rollout)

Use a **small, shared** set of low-cardinality stream labels. The Slurm labels
below are NVIDIA Recovery's portable contract; Run:ai workload identity remains a
separate scheduler-specific mapping:

| Label | Slurm value | Run:ai value | Cardinality |
|-------|-------------|--------------|-------------|
| `cluster` | `customer-slurm` | `gpu-prod` | low ✅ |
| `appName` | `slurm-app-log` for application output | optional provider mapping | low ✅ |
| `component` | `journal` for node system logs | optional provider mapping | low ✅ |
| `node_name` | validated Slurm `NodeName` on system logs | validated Kubernetes node when used | medium |
| `job` | optional compatibility label | `kubernetes-pods` / `kubernetes-events` | low ✅ |
| `namespace` | — | Run:ai project ns | medium |
| `unit` | slurmd/slurmctld/… | — | medium |
| `job_id` / `runai_job` | `$SLURM_JOB_ID` | workload name | **high — see note** |

**High-cardinality identifiers** (`job_id`, `pod`, `gpu_uuid`) should live in
**structured metadata** (Loki 3.x) or in the log line, *not* as stream labels,
on large clusters. Correlate GPU metrics ↔ logs at query time in Grafana by
matching on these shared values.

---

## 10. Validation checklist

- [ ] Loki reachable from a Slurm node: `curl -sS --max-time 30 -o /dev/null -w '%{http_code}\n' http://loki.example.com:3100/loki/api/v1/status/buildinfo` (→ `200`)
- [ ] Loki itself is ready from a pod: `kubectl -n logging run loki-check --rm -i --restart=Never --image=curlimages/curl -- -sS --max-time 30 -o /dev/null -w '%{http_code}\n' http://loki.logging.svc:3100/ready` (→ `200`)
- [ ] Check pod removed: `kubectl -n logging delete pod loki-check --ignore-not-found`
- [ ] `alloy fmt /etc/alloy/config.alloy` passes on Slurm nodes
- [ ] `systemctl status alloy` active; `journalctl -u alloy` shows no push errors
- [ ] `kubectl -n logging rollout status ds/alloy` complete; no `level=error` push failures
- [ ] Grafana Explore returns Slurm rows for `{cluster="customer-slurm", appName="slurm-app-log"}` and Run:ai rows for `{nodetype="runai-gpu"}`
- [ ] Host syslog present with the validated node: `{cluster="customer-slurm", component="journal", node_name="gpu-001"} |= "kernel"`
- [ ] A completed Run:ai workload's logs still queryable *after* it terminated
- [ ] A finished Slurm job: `{cluster="customer-slurm", appName="slurm-app-log", job_id="<id>"}` returns stdout **and** `dcgm-<id>.txt`
- [ ] GPU metrics carry pod labels: `DCGM_FI_DEV_GPU_UTIL{pod!=""}` in Prometheus
- [ ] Retention/limits set on Loki; stream cardinality sane (`loki_ingester_streams`)

---

## 11. End-to-end verification walkthrough

These procedures prove the pipeline works by **generating a known log line, then
querying Loki for it**. Run them after the checklist in §10 passes. Each test
writes a unique marker so you can find exactly your line and not a coincidence.

### 11.0 A command-line Loki query helper

You do not need Grafana to verify — query Loki's HTTP API directly. Run this on
any host that can reach Loki (a Slurm node, or `kubectl exec` into a pod):

```bash
export LOKI_ADDR="http://loki.example.com:3100"
# export LOKI_TENANT="hpc"     # only if Loki auth_enabled/multi-tenant

# loki_q '<LogQL selector>'  → prints matching log lines (newest first), or a
# clear error if the query itself failed (endpoint/auth/tenant), so a real
# "no lines delivered" is never confused with an unreachable/misconfigured Loki.
# Portable: no curl >= 7.76 features, safe under `set -u`, and any bash >= 4.
# Widen the time window with LOKI_LOOKBACK (seconds) — e.g. the §11.4 durability
# check uses a larger value so older-but-retained lines still match.
loki_q() {
  local start; start="$(( $(date +%s) - ${LOKI_LOOKBACK:-900} ))000000000"  # ns
  local hdr=()                                              # build header safely
  [[ -n "${LOKI_TENANT:-}" ]] && hdr=(-H "X-Scope-OrgID: ${LOKI_TENANT}")
  local resp
  # ${hdr[@]+"${hdr[@]}"} — expands to nothing when empty, even under `set -u`
  resp="$(curl -sS -G "${LOKI_ADDR}/loki/api/v1/query_range" \
    ${hdr[@]+"${hdr[@]}"} \
    --data-urlencode "query=$1" \
    --data-urlencode "start=${start}" \
    --data-urlencode 'limit=50' \
    --data-urlencode 'direction=backward')" \
    || { echo "QUERY FAILED (curl/network could not reach ${LOKI_ADDR})" >&2; return 1; }
  # Loki returns {"status":"success",...} on success; anything else is an error
  # (auth/tenant/proxy), NOT a delivery problem — surface it instead of hiding it.
  if ! printf '%s' "$resp" | jq -e '.status == "success"' >/dev/null 2>&1; then
    printf 'QUERY FAILED (endpoint/auth/tenant — NOT a delivery problem):\n%s\n' \
      "$resp" >&2
    return 1
  fi
  printf '%s' "$resp" | jq -r 'if (.data.result | length) == 0
                        then "NO MATCHING LINES (recheck the steps above)"
                        else .data.result[].values[][1] end'
}
```

> Alternative: Grafana's `logcli` — `export LOKI_ADDR=...; logcli query '<selector>'
> --since=15m --limit=50` (raise `--since` for older lines; add `--org-id=hpc` if
> multi-tenant).

> **Structured-metadata note.** The queries below select `job_id`/`pod` inside the
> `{}` stream matcher because the example configs (§5.3, §6.1) emit them as stream
> labels. If you followed the at-scale guidance in §9 / Pitfall 4 and moved them to
> `stage.structured_metadata`, they are **not** selectable in `{}` — use a line/label
> filter instead, e.g.
> `{cluster="customer-slurm", appName="slurm-app-log"} | job_id="$JOBID"`.

Confirm Loki is healthy first. The gateway proxies only `/loki/api/v1/`, so it
returns `404` for `/ready`. Check the API through `LOKI_ADDR`, then check
readiness against the Loki service directly. The Alloy image ships neither
`curl` nor `wget`, so run the in-cluster check from a short-lived curl pod:

```bash
curl -sS --max-time 30 -o /dev/null -w '%{http_code}\n' "${LOKI_ADDR}/loki/api/v1/status/buildinfo"   # → 200
kubectl -n logging run loki-check --rm -i --restart=Never --image=curlimages/curl -- \
  -sS --max-time 30 -o /dev/null -w '%{http_code}\n' http://loki.logging.svc:3100/ready   # → 200
kubectl -n logging delete pod loki-check --ignore-not-found                           # cleanup
```

### 11.1 Verify host syslog / console messages → Loki (Slurm node)

On a Slurm compute node running Alloy as a systemd service (§5):

```bash
MARKER="nvidia-recovery-verify-syslog-$(date +%s)"

# (a) user-space message through the node's normal syslog ingress
logger -t nvidia-recovery-verify -p user.info "$MARKER user-space message"

# (b) kernel/console message — appears on the console and in the kernel log,
#     which journald (and therefore Alloy) also captures. Needs root.
echo "$MARKER kmsg console message" | sudo tee /dev/kmsg

echo "wrote marker: $MARKER"
sleep 10          # allow scrape + push
```

Now confirm both arrived in Loki:

```bash
NODE_NAME="$(hostname -s)"
loki_q "{cluster=\"customer-slurm\", component=\"journal\", node_name=\"$NODE_NAME\"} |= \"$MARKER\""
```

**Expected:** two lines containing `$MARKER` — the `logger` line (syslog
identifier `nvidia-recovery-verify`) and the `/dev/kmsg` line (from the kernel). If you
see nothing: check `journalctl -u alloy -f` for push errors, that `alloy` is in
the `systemd-journal` group (§5.2), that the journal hostname was validated as
the Slurm `NodeName`, and that `LOKI_ADDR`/tenant are correct. Do not work around
the failure by posting the marker directly to Loki.

### 11.2 Simulate an ML job log → Loki (Slurm)

Submit a short job that emits training-style stdout into the tailed job-log path
(§5.3). No GPU is required to exercise the log path. **The `--output` directory
must be writable by the submitting user and must be the one Alloy tails in §5.3**
— `/var/spool/slurm` is the SlurmdSpoolDir and is usually *not* user-writable, so
replace the path below with your site's job-log dir if it differs:

```bash
JOBLOG_DIR=/var/spool/slurm/job-logs        # must be writable + tailed (§5.3)
JOBID=$(sbatch --parsable \
  --job-name=ml-log-verify \
  --output="${JOBLOG_DIR}/slurm-%j.out" \
  --wrap='for e in $(seq 1 5); do
            echo "epoch=$e loss=0.0$e acc=0.9$e";
            sleep 2;
          done;
          echo "TRAINING COMPLETE"')
JOBID="${JOBID%%;*}"          # federated Slurm returns "jobid;cluster"; keep id
echo "submitted Slurm job $JOBID"

# Do NOT blind-sleep: a busy cluster may leave the job PENDING/CONFIGURING for
# minutes. Wait until its output file appears (job is running and writing) or the
# job has left the queue (finished), whichever comes first.
echo "waiting for job $JOBID to run and emit output…"
for _ in $(seq 1 120); do
  state="$(squeue -h -j "$JOBID" -o '%T' 2>/dev/null)"
  [[ -z "$state" || -s "${JOBLOG_DIR}/slurm-${JOBID}.out" ]] && break
  sleep 5
done
sleep 10          # let final lines flush and Alloy tail + push
```

Confirm the job's log lines are in Loki, keyed by `job_id`:

```bash
loki_q "{cluster=\"customer-slurm\", appName=\"slurm-app-log\", job_id=\"$JOBID\"} |= \"epoch\""
loki_q "{cluster=\"customer-slurm\", appName=\"slurm-app-log\", job_id=\"$JOBID\"} |= \"TRAINING COMPLETE\""
```

**Expected:** five `epoch=…` lines plus `TRAINING COMPLETE`. If empty: check the
job actually ran and wrote output (`sacct -j "$JOBID" -o State,ExitCode`; confirm
`${JOBLOG_DIR}/slurm-${JOBID}.out` exists and is non-empty), that the dir is the
one your `--output` convention uses (§5.3, Pitfall 2), and that the §5.3
`local.file_match` glob covers it.

### 11.3 Simulate an ML job log → Loki (Run:ai / Kubernetes)

Pick the interface you use. Both are captured by the same node-level Alloy
DaemonSet (§6). Note the two forms take **different identifiers**: `runai submit`
takes a Run:ai **project name** (`--project`), while `kubectl` takes a Kubernetes
**namespace** (`-n`) — these are related but not always the same string.

**Option A — Run:ai CLI (recommended on a Run:ai cluster).** Lands on a GPU node,
which is where the DaemonSet runs:

```bash
runai submit ml-log-verify \
  --project <runai-project-name> \
  --image busybox \
  -- sh -c 'for s in $(seq 1 10); do echo "step=$s loss=0.$((s%9)) acc=0.9"; sleep 2; done; echo TRAINING COMPLETE'
```

Wait until the workload is actually **Running** (a busy GPU cluster may leave it
Pending for minutes — do not blind-sleep), then confirm in Loki. Run:ai names the
pod with a generated suffix, so the robust query is by **namespace** (replace
`<ns>` with the project's namespace, from `runai describe job ml-log-verify`):

```bash
# Wait (bounded — never hang) for the workload to run before querying. Breaks on
# Running or Succeeded; gives up after ~5 min so a Pending/Failed job can't stall
# the shell. The alternation is anchored so a stray "Succeeded: 0" counter line
# in `runai describe` output does not match early.
for _ in $(seq 1 60); do
  runai describe job ml-log-verify --project <runai-project-name> 2>/dev/null \
    | grep -qiE 'STATUS:[[:space:]]*(Running|Succeeded)' && break
  sleep 5
done
sleep 15                          # let it emit + Alloy push

loki_q '{job="kubernetes-pods", namespace="<ns>"} |= "step="'
# Nicer, if your Run:ai version surfaces the workload name as the promoted
# runai_job label (§6.1 — verify the key):
#   loki_q '{job="kubernetes-pods", runai_job="ml-log-verify"} |= "step="'
```

**Option B — plain kubectl (works in any Run:ai project namespace).** Here the
pod name is exactly `ml-log-verify`, so query by `pod`:

```bash
NS=<runai-project-ns>
kubectl -n "$NS" run ml-log-verify \
  --image=busybox --restart=Never \
  --overrides='{"spec":{"nodeSelector":{"nvidia.com/gpu.present":"true"}}}' \
  -- sh -c 'for s in $(seq 1 10); do echo "step=$s loss=0.$((s%9)) acc=0.9"; sleep 2; done; echo TRAINING COMPLETE'

# Wait (bounded — never hang) for the pod to actually start before querying — a
# Pending pod produces no logs yet. Breaks on Running or (for this short job)
# Succeeded; a single poll loop caps the wait at ~3 min rather than chaining two
# 180s `kubectl wait` calls that could stall ~6 min on an unschedulable pod.
for _ in $(seq 1 36); do
  phase="$(kubectl -n "$NS" get pod ml-log-verify -o jsonpath='{.status.phase}' 2>/dev/null)"
  [[ "$phase" == "Running" || "$phase" == "Succeeded" ]] && break
  sleep 5
done
sleep 10
loki_q '{job="kubernetes-pods", pod="ml-log-verify"} |= "step="'
```

**Expected:** ten `step=…` lines. If empty: check the pod is not still `Pending`
(`kubectl -n "$NS" get pod ml-log-verify`); ensure it landed on a node where the
DaemonSet is scheduled (the `nodeSelector` above pins it to a GPU node; adjust for
your cluster and any Run:ai/GPU taints — Pitfall 7); that the DaemonSet pod there
is `Running` (`kubectl -n logging get pod -o wide`); and that `NODE_NAME` is
injected (§6.2) so the field selector matches.

### 11.4 Prove durability — the Run:ai ephemeral-log gap

This is the whole reason for the pipeline: Run:ai's built-in log view disappears
when a workload terminates, but Loki must retain it. **Delete the workload, then
re-query Loki:**

```bash
# Delete the workload you created:
#   Option A:  runai delete job ml-log-verify --project <runai-project-name>
#   Option B:  kubectl -n <runai-project-ns> delete pod ml-log-verify

# Run:ai's own log view is now gone; Loki should still have the lines.
# Widen the query window so retained-but-older lines still match (default is
# only 15 min — see §11.0), and reuse the SAME selector that worked in §11.3:
#   Option A:  LOKI_LOOKBACK=7200 loki_q '{job="kubernetes-pods", namespace="<ns>"} |= "TRAINING COMPLETE"'
#   Option B:  LOKI_LOOKBACK=7200 loki_q '{job="kubernetes-pods", pod="ml-log-verify"} |= "TRAINING COMPLETE"'
```

**Expected:** `TRAINING COMPLETE` still returns from Loki after the workload is
deleted. That confirms durable retention independent of Run:ai. If it returns
nothing, first re-run with a larger `LOKI_LOOKBACK` — the lines may simply be
older than the query window rather than missing.

### 11.5 Clean up the test artifacts

```bash
# Slurm: remove the verification job log once confirmed (JOBLOG_DIR/JOBID from §11.2)
rm -f "${JOBLOG_DIR}/slurm-${JOBID}.out"     # use sudo only if the dir is root-owned
# Kubernetes: pod already deleted in 11.4; nothing persists in-cluster.
# Loki entries age out per your retention_period (§4); no manual cleanup needed.
```

---

## 12. Common pitfalls

1. **Deploying Promtail** — EOL since 2026-03-02. Use Alloy.
2. **Slurm job output path assumptions** — the `slurm-%j.out` glob is a
   *convention*, not a default. Confirm your site's `--output` path; if jobs
   write to per-user home dirs on shared storage, tail that path from a single
   collector rather than every node.
3. **`alloy` user can't read the journal** — must be in `systemd-journal` group.
4. **Unvalidated node identity** — `node_name` must match the scheduler-visible
   Slurm `NodeName`; do not assume a pod, VM, or collector hostname is equal.
5. **Label cardinality explosion** — never put `pod`, `job_id`, or `gpu_uuid`
   as stream labels at scale; use structured metadata.
6. **Run:ai ephemeral logs** — do not rely on Run:ai's UI for retention; the
   Alloy DaemonSet is the durable source of truth.
7. **Multi-tenant Loki without `X-Scope-OrgID`** — pushes silently 4xx. Set the
   header in every `loki.write`.
8. **DaemonSet not landing on GPU nodes** — add tolerations matching your
   Run:ai/GPU node taints, or Alloy won't schedule there.
9. **dcgm-exporter without `-k`** — GPU metrics won't carry pod labels, breaking
   log↔metric correlation.

---

## 13. Primary references

- Grafana Alloy — https://grafana.com/docs/alloy/latest/
- Alloy Helm chart values — https://github.com/grafana/alloy/blob/main/operations/helm/charts/alloy/values.yaml
- Kubernetes Deployment replacement strategies — https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#strategy
- Alloy: collect logs in Kubernetes — https://grafana.com/docs/alloy/latest/collect/logs-in-kubernetes/
- Alloy `loki.source.journal` — https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.journal/
- Alloy `loki.source.file` — https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.file/
- Alloy `loki.source.kubernetes` — https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.kubernetes/
- Alloy `loki.source.kubernetes_events` — https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.kubernetes_events/
- Promtail → Alloy migration (EOL notice) — https://grafana.com/docs/loki/latest/setup/migrate/migrate-to-alloy/
- Loki install (Helm) — https://grafana.com/docs/loki/latest/setup/install/helm/
- Loki HTTP query API (`query_range`) — https://grafana.com/docs/loki/latest/reference/loki-http-api/
- LogCLI — https://grafana.com/docs/loki/latest/query/logcli/
- NVIDIA dcgm-exporter — https://docs.nvidia.com/datacenter/dcgm/latest/gpu-telemetry/dcgm-exporter.html
- DCGM job stats on Slurm — https://developer.nvidia.com/blog/job-statistics-nvidia-data-center-gpu-manager-slurm/
- NVIDIA GPU Operator — https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/index.html
- Run:ai logs collection — https://run-ai-docs.nvidia.com/saas/infrastructure-setup/procedures/logs-collection
