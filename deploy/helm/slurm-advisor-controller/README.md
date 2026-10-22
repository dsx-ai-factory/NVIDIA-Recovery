# Slurm Advisor Controller Helm Chart

This standalone chart runs the NVIDIA Recovery Slurm advisor controller and the
independent fail-open watchdog as separate Kubernetes Deployments and Pods. Each
application Pod runs its own Slinky `sackd` sidecar and owns its Slurm
authentication and runtime volumes. The chart supports Slinky Slurm only. The
repository distributes source and build tooling; it does not publish controller
or watchdog images.

## Prerequisites

- A Slinky Slurm cluster with configless support enabled.
- The chart installed in the Slinky cluster namespace; it adds the configured
  controller Service to Pod DNS searches so configless StatefulSet hostnames
  such as `slurm-controller-0` resolve.
- Network reachability to the Slurm controller and NVIDIA Recovery Advisor API.
- An existing Kubernetes Secret containing `slurm.key`.
- Locally built controller and watchdog images whose Slurm release matches the
  chart's `sackd.image.tag`.

Build the application images from the repository root:

```bash
make build-local \
  SERVICES="slurm-advisor-controller slurm-watchdog" \
  IMAGE_TAG=dev
```

Both builds default to
`ghcr.io/slinkyproject/sackd:25.05-ubuntu24.04`. Override the shared
`SLINKY_SLURM_IMAGE` only when intentionally validating a different, matching
Slinky release.

The chart enables the adapter's versioned `squeue` JSON mode by default. The
image packages the matching `v0.0.40` Slurm data-parser plugin used by the Go
adapter, avoiding drift in the unversioned schema across Slurm releases.

Create the authentication Secret from an operator-managed file; do not put the
key bytes in Helm values:

```bash
kubectl -n slurm create secret generic slurm-auth-key \
  --from-file=slurm.key=/secure/operator/path/slurm.key
```

Each application Pod independently projects that key read-only, then its
non-root init container copies only the key into that Pod's memory-backed
`/etc/slurm` volume with mode `0600`. All runtime containers mount their local
`/etc/slurm` read-only, use Slinky's numeric Slurm identity `401:401`, and share
only their Pod-local writable memory-backed `/run/slurm` directory containing
the local `sackd` configuration cache and authentication socket.

Failed-job recovery shares a `30s` budget per controller tick, configured by
`controller.failedJobRecoveryTimeout` (environment variable
`SLURM_ADVISOR_FAILED_JOB_RECOVERY_TIMEOUT`). The value must be a positive Go
duration. It covers all failed jobs and their local requeue retries, allowing
the controller to continue to Advice reconciliation when the budget expires.
Unfinished recovery resumes on later ticks within the existing three-command
attempt limit; budget expiry alone does not abandon a job. This budget is
separate from `controller.slurmExecTimeout`, which limits an individual Slurm
command and defaults to `5s`. The watchdog command timeout
(`watchdog.slurmExecTimeout`) also defaults to `5s`.

## Install

For a local Kind/Slinky deployment with images tagged `dev`:

```bash
helm upgrade --install slurm-advisor-controller \
  deploy/helm/slurm-advisor-controller \
  --namespace slurm \
  --set image.tag=dev \
  --set watchdog.image.tag=dev \
  --set controller.heimdallEndpoint=mock-heimdall:9090
```

The controller and watchdog both default to `observe`, so neither process
mutates scheduler state. The controller also accepts `ignore`; that default
neither upserts nor plans work for jobs unless their valid APS comment selects
another mode or the temporary username selector applies. A controller default
of `act` requires two values and fails during Helm rendering if acknowledgement
is missing:

```yaml
controller:
  mode: act
  acknowledgeSchedulerActions: true
```

Controller mode is resolved per job from the default and an optional `mode`
inside an object-valued `APS` or `HEIMDALL` Slurm comment. Supported values are
`ignore`, `observe`, and `act`. Values use the existing APS convention: value
casing is ignored, surrounding whitespace is not trimmed, and field names are
case-sensitive. Missing, malformed, unrelated, or invalid comments never
authorize scheduler mutation. `controller.acknowledgeSchedulerActions` is an
independent hard ceiling: without it, a job-level `act` request becomes
non-mutating `observe`.

Before mode resolution, `controller.environment` routes jobs by the optional
APS/HEIMDALL `environment` string. Any non-empty controller value is accepted;
job values are compared exactly and case-sensitively without trimming. The
default `unspecified` accepts every valid APS environment. The value `prod`
also accepts an absent or empty job environment for compatibility with legacy
production jobs. Every other controller value requires an exact, non-empty
job-comment match. A rejected job is not published to NVIDIA Recovery, considered for
log links or recovery, or acted on in Slurm.

For the initial internal cluster rollout, set `controller.userAllowlist` to a
list such as `[alice, bob]`, with `controller.mode: observe` and
`controller.acknowledgeSchedulerActions: true`. For jobs without an explicit
mode, listed users select `act` and other or unknown users select `observe`.
Explicit valid APS/HEIMDALL modes always win, including `act` for an unlisted
user and `observe` or `ignore` for a listed user. Invalid modes resolve to
`observe`. Empty (the default) restores ordinary default-mode resolution.
Names match submitting Slurm usernames exactly and case-sensitively.

The list does not filter monitoring, bootstrap, accounting, enrichment, or
Advice ownership. Otherwise admitted jobs remain visible unless their effective
mode is `ignore`. The action acknowledgement, environment, partition, and
recovery safeguards still apply. Default `act` plus a nonempty list is accepted
but produces a bootstrap warning because the intended rollout default is
`observe`. Restart/redeploy to change the list.

**This is a temporary initial internal rollout control. It will be deprecated
soon and may be removed without notice; it is not a long-term supported
interface.**

Helm renders `controller.userAllowlist` as `SLURM_ADVISOR_USER_ALLOWLIST` and
`SLURM_WATCHDOG_USER_ALLOWLIST`, and passes `controller.mode` and
`controller.acknowledgeSchedulerActions` to the watchdog through
`SLURM_WATCHDOG_JOB_DEFAULT_MODE` and
`SLURM_WATCHDOG_JOB_ACTIONS_ACKNOWLEDGED`. There are no separate watchdog
job-policy overrides. The watchdog shares SAC's job selection and clears held
timers when a fresh observation no longer resolves to `act`; renewed eligibility
starts a fresh timeout. Its own process mode, acknowledgement, timeout, and DAG
safeguards remain additional constraints. See the
[job action policy](../../../docs/architecture/slurm-advisor-controller.md).

Use explicit, distinct environment values when multiple controllers share one
Slurm job set. Multiple controllers configured as `unspecified` can observe the
same valid jobs and therefore do not provide ownership isolation.

The watchdog has its own independent `watchdog.mode`,
`watchdog.acknowledgeSchedulerActions`, and `watchdog.environmentPattern`
gates. An unset (`null`) watchdog pattern inherits `controller.environment`.
A literal pattern has the same `unspecified`, `prod`, absent, empty, exact,
case-sensitive, and no-trimming behavior as controller routing. `*` matches
zero or more characters, including `/`; all other characters are literal.
Set the pattern to `"*"` only when one watchdog is intentionally responsible
for every valid APS/HEIMDALL environment. Invalid or non-string environment
fields never match, including under `*`. Enabling either action path changes
workloads and requires explicit human scheduler-action review.

The values `"*"` and `unspecified` are cluster-global compatibility scopes and
do not provide environment ownership isolation. Overlapping wildcard patterns
also admit the same jobs in more than one watchdog. Deploy multiple
action-capable watchdogs only with reviewed, non-overlapping patterns when
exclusive ownership is required.

The controller Deployment and metrics Service use the Helm release's base
fullname. The watchdog Deployment and metrics Service append `-watchdog`. Set
`watchdog.enabled=false` to omit the watchdog Deployment, Pod, Service, and
ServiceMonitor while retaining the controller stack.

## Configuration Contract

The chart maps typed values to the complete existing controller and watchdog
environment contracts. It intentionally has no `extraEnv`, arbitrary volume,
autoscaling, or PodDisruptionBudget escape hatch. `controller.replicas` and
`watchdog.replicas` default to `1` and accept only `0` (shutdown) or `1` in every
mode. Helm schema validation rejects larger counts until replica coordination
exists. Each Deployment uses the `Recreate` strategy to avoid overlap during
normal upgrades; separate releases or manual scaling still require operator
coordination for exclusive ownership.

`controller.enableEagerRequeue` maps to
`SLURM_ADVISOR_ENABLE_EAGER_REQUEUE` and defaults to `false`, matching the
controller's environment default and the systemd example. Failed-job
observations are published, and requeue waits for applicable restart Advice.
Set it to `true` only to restore compatibility eager recovery.

Eager recovery can create a successor before Job Manager issues a retry-limit
or checkpoint-progress stop. The flag does not bypass action mode,
acknowledgement, or other scheduler safety checks, and backend delays never
change its value automatically. See [ADR 0045](../../../docs/decisions/0045-require-advice-before-slurm-requeue.md).

`controller.adviceMaxAge` limits scheduler actions for non-durable Advice to
generations whose desired state changed within the configured duration. SAC
records an older generation as `ABANDONED: ADVICE_EXPIRED`, after which Job
Manager excludes it from later `only_active` queries. `RELEASED` and
`TERMINATED` are durable desired states and remain actionable for their exact
attempt until fresh Slurm state satisfies them. The limit defaults to `5m`,
approximating Slurm's usual `MinJobAge` retention window after a job leaves
`squeue`; tune it to the cluster's Slurm configuration.

`controller.logMetadataRefreshInterval` controls how long the controller
caches each job attempt's Slurm-reported stdout paths before refreshing
supplemental accounting metadata. It defaults to `5m`; failed refreshes retain
the last known good paths and back off up to this interval.

`controller.logMetadataEnrichmentTimeout` bounds each active-job or
finalized-job enrichment pass over supplemental `sacct` and `scontrol`
log-metadata queries. A controller sync may run both passes, each with its own
timeout. The setting defaults to `2s`; increase it when accounting queries
approach that duration or when a busy cluster needs several jobs enriched per
pass.

`controller.jobUpsertWorkerPoolSize` bounds simultaneous job publications to
NVIDIA Recovery. `controller.accountingWorkerPoolSize` independently bounds targeted
single-job `sacct` requests, while `controller.accountingTimeout` bounds that
complete phase. Defaults are `8`, `8`, and `5m`. Keep the two worker-pool sizes
separate so a slow dependency cannot consume the other's capacity.

The controller endpoint is configured by `controller.heimdallEndpoint`. The
existing Secret is selected by `slurm.authSecret.name` and
`slurm.authSecret.key`. `sackd.runtimeDirectory` is fixed to `/run/slurm`, the
location shared only within each application Pod. Controller, watchdog, and
`sackd` image repository/tag values are independently configurable, but
operators must keep all Slurm components on the same release.

`podLabels` cannot replace the reserved
`app.kubernetes.io/name`, `app.kubernetes.io/instance`, or
`app.kubernetes.io/component` selector labels. The component label keeps the
controller and watchdog rollout, restart, and traffic boundaries independent.

Controller and watchdog Prometheus listeners are enabled separately under
`metrics.controller` and `metrics.watchdog`. Each enabled listener has its own
component-selected ClusterIP Service. Set `serviceMonitor.enabled=true` when the
Prometheus Operator CRD is installed; the chart then creates one ServiceMonitor
per enabled component endpoint.

Controller tick latency is exposed through
`slurm_advisor_controller_reconcile_duration_seconds`. Per-stage latency is
exposed through `slurm_advisor_controller_reconcile_stage_duration_seconds`
with bounded `stage` and `outcome` labels. Both histograms use buckets
from one second through ten minutes, plus Prometheus's implicit `+Inf` bucket.

`networkPolicy.enabled` defaults to `false`. Enabling it requires explicit,
non-empty ingress and egress rule lists because the chart cannot infer the
operator's DNS, NVIDIA Recovery, Slurm, or Prometheus topology.

The component selector is immutable. Releases installed from an earlier Draft
MR revision that used one shared Pod must be uninstalled and reinstalled before
testing this two-Deployment topology. This migration note applies only to the
unreleased Draft chart; there is no supported released upgrade path yet.

## Command timeout monitoring

With controller metrics enabled, SAC exposes
`slurm_advisor_controller_slurm_command_timeouts_total{command}`. Command names
are bounded to `squeue`, `sacct`, `scontrol`, `scancel`, and `other`; no arguments,
job IDs, or stderr become labels. Series start at zero so the first timeout can
be included in a rate or increase after scraping begins.

The counter increments when an issued command fails after termination by its
execution deadline or an earlier enclosing phase deadline. Normal shutdown
cancellation, errors returned promptly by Slurm, dry runs, and commands that
never started do not count. It covers controller command execution, including
accounting and background metadata discovery.

For example, this expression detects at least three timeouts per command in
five minutes:

```promql
sum by (command) (
  increase(slurm_advisor_controller_slurm_command_timeouts_total[5m])
) >= 3
```

Use deployment/cluster selectors or grouping when monitoring multiple SAC
installations. Choose the threshold and notification route for the deployment;
this chart does not install an alert rule. Repeated timeouts merit operator
investigation of Slurm responsiveness, scheduler connectivity, command logs,
and phase durations. A nearly exhausted phase or an aggressively short metadata
deadline can also interrupt a command, so the count alone is not proof of an outage.

## Controller log export

The controller always writes structured logs to its local output. When deployed
through the `nvidia-recovery` umbrella chart, configure the same release-wide collector
settings used by the other backend services to also export controller logs:

```yaml
global:
  otelcollector:
    enabled: true
    endpoint: http://otel-collector:4317
    protocol: grpc
    auth:
      enabled: true
      secretName: otel-collector-auth
      secretKey: token
```

The referenced Secret must exist in the release namespace and its key must
contain only the bearer token. The existing `heimdall.otelEnv` helper supplies
`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL`, and
`OTEL_EXPORTER_OTLP_TOKEN`, then constructs `OTEL_EXPORTER_OTLP_HEADERS` using
Kubernetes environment expansion. Do not put the token itself in Helm values.

Controller log export requires both the endpoint and token. `grpc` is the
default protocol; `http/protobuf` (also accepted as `http`) sends logs to the
endpoint's `/v1/logs` path. Use an `https://` endpoint for TLS.
`controller.logLevel` applies to both local and exported logs.

For OTLP events with a top-level `error`, the controller also appends the
concise `err.Error()` value to the log message. The wrapper still forwards the
original error field, stack trace, and all other structured fields to the OTLP
bridge for its normal semantic mapping. Local stderr output is not transformed.
Alerts against the OTLP stream should not require an exact message match for
these events because the message includes the event-specific error text.

As specified in [ADR 0012](../../../docs/decisions/0012-standardize-global-otlp-collector-configuration.md),
the shared exporter environment integration requires the umbrella chart;
standalone chart installations retain local logging. The watchdog receives
the same environment settings in an umbrella release, but does not consume
them for log export.

## Audit buffering

The retired `controller.auditTrail.enabled` and `watchdog.auditTrail.enabled`
values are no longer accepted. Remove them from values files and retained release
values before upgrading.

The controller and watchdog always start their in-memory audit-event buffers.
The buffers are bounded and best effort: producers drop new events
when their channel is full. Configure the channel capacity, flush batch size,
and periodic flush interval together:

```yaml
controller:
  auditTrail:
    destination: gateway
    maxBatchSize: 100
    bufferSize: 1000
    flushInterval: 30s
watchdog:
  auditTrail:
    destination: gateway
    maxBatchSize: 100
    bufferSize: 1000
    flushInterval: 30s
```

The chart requires all three tuning values to be positive. `controller.auditTrail.destination` and `watchdog.auditTrail.destination` select
`logging` or `gateway` (the corresponding `*_AUDIT_TRAIL_DESTINATION` variables).
The Helm default is `gateway`. `logging` writes event metadata without payloads
to the application log, and an empty value retains that binary-level fallback.
HTTP URLs are not accepted as destinations.

With `gateway`, events use `AuditService.CreateAuditEvents` on the controller's
existing `HEIMDALL_ENDPOINT` connection, with the same projected ServiceAccount
credentials as Job Manager. There is no separate audit destination URL. API
Gateway forwards ingestion to the audit service over HTTP. The Notifications
service always accepts audit ingestion; configure the gateway's
`audit.endpoint` for the deployment's service name.

The watchdog opens an audit-only gateway connection when its audit destination
is `gateway`. `watchdog.auditGatewayEndpoint` defaults to the controller endpoint.
It reuses `controller.kubernetesAuth` settings and the chart's ServiceAccount for
its projected token. It does not call Job Manager or fetch Advice; scheduler
recovery remains driven by local Slurm state.

Delivery retries gRPC `Unavailable`, `DeadlineExceeded`, and `ResourceExhausted`
up to five total attempts, each with a ten-second deadline, using exponential
backoff with jitter. Partial acceptance is terminal: accepted events are counted
and rejected events are dropped without replaying the batch. Pod shutdown drains
queued events before closing the shared gateway connection.


## Recovery milestone reporting

After each Advice-driven scheduler command, the controller reports the
command's result to Job Manager as a recovery milestone. Job Manager uses it to
mark the recovery complete and to measure restart latency. Reporting runs on one
background worker with a bounded in-memory queue, so it never delays scheduler
commands:

```yaml
controller:
  milestoneReporting:
    queueCapacity: 256   # reports waiting for delivery; a full queue drops new ones
    rpcTimeout: 2s       # bound on each delivery attempt
    maxAttempts: 3       # includes the first attempt; temporary failures only
    retryDelay: 100ms    # pause between attempts
    shutdownTimeout: 2s  # time to drain queued reports when the pod stops
```

All five values must be positive. Only temporary failures are retried:
unavailable or overloaded backends, timeouts, and unmapped backend errors.
Rejections such as an unknown recovery sequence, invalid input, or refused
credentials are not retried, because the same report would fail again and
would delay the reports queued behind it. Retries are safe because Job Manager
deduplicates reports by command occurrence.

When the backend is unavailable, each report can occupy the worker for up to
`maxAttempts × rpcTimeout + (maxAttempts − 1) × retryDelay`. Size
`queueCapacity` for the commands expected during that outage. Reports are kept
only in memory, so a controller restart loses any still queued. Watch
`nvidia_recovery_milestone_reports_total` by `result`: `success`, `retry`,
`error` (temporary failures exhausted), `rejected` (not retried), `queue_full`,
`stopped`, `shutdown_drop`, and `unavailable`.
## Optional log-link materialization

`controller.logLinks.enabled` defaults to `false`. In that state the chart
renders only `SLURM_ADVISOR_LOG_LINKS_ENABLED=false`; it adds no workload
filesystem volume, mount, or privilege. Enabling the option authorizes
filesystem writes independently from scheduler-action mode. An observe-mode
controller may therefore create and retire links, while jobs whose effective
mode is `ignore` remain excluded.

The storage contract accepts only an existing PVC (`claimName`) or an existing
host directory (`hostPath`) for each mount. Set exactly one backing field on
each object. A `hostPath` is rendered with type `Directory`, never
`DirectoryOrCreate`, so the kubelet cannot create an operator-owned path.

```yaml
controller:
  logLinks:
    enabled: true
    destination:
      mountPath: /var/lib/heimdall-oss/log-links
      claimName: heimdall-log-links
    sources:
      - mountPath: /mnt/slurm/jobs
        claimName: slurm-job-logs
      - mountPath: /mnt/slurm/app-logs
        hostPath: /srv/slurm/app-logs
    applicationPatterns:
      - '^app-{job_id}[.]log$'
      - '^worker-{job_id}[.]log$'
```

The chart renders source-root and application-pattern values as JSON arrays so
commas in paths or regular expressions are preserved. The controller container
mounts sources read-only and the destination writable. Its `sackd` sidecar,
Slurm-key init container, watchdog Pod, and watchdog `sackd` sidecar receive
none of these mounts or settings. Existing one-replica, `Recreate`, non-root,
read-only-root-filesystem, and dropped-capability controls remain in force.
For array tasks, a pattern containing `{job_id}` is evaluated with both the
concrete task ID and its array-parent job ID.

The destination must use backing storage distinct from every source. The chart
rejects a shared PVC name and equal or ancestor/descendant host paths because a
writable destination alias would otherwise bypass the source's read-only
mount. Source PVCs are requested read-only at both the volume and
container-mount layers. Kubernetes values cannot detect two differently named
storage objects or host paths that alias the same backing tree, so operators
must also ensure the configured backing stores are genuinely distinct.

Filesystem synchronization is asynchronous inside the controller process. One
worker serializes link and manifest operations, with one latest-wins pending
snapshot while the worker is busy. A controller poll does not wait for those
operations to finish, and an obsolete pending snapshot may be coalesced into a
newer one. Pod termination signals cancellation to in-flight work and drops
pending work without draining it; the replacement Pod recovers the manifest and
submits a fresh snapshot. This requires no additional Deployment, sidecar, queue
storage, value, or environment variable. The single replica limit and `Recreate`
strategy keep one active writer across normal upgrades.

Treat every configured source root as a trusted filesystem boundary. The
controller intentionally creates a link before a missing Slurm target appears.
After validation, any identity that can replace that target or an ancestor below
the source root with another symbolic link can influence what a following
collector reads. Mounting the source read-only in the controller does not
constrain writers of the underlying PVC or host directory, and periodic
validation cannot close this time-of-check/time-of-use race. Do not enable this
feature for source trees writable by untrusted workloads unless the collector
enforces beneath-root path resolution when it opens each file.

Prepare every host directory and PVC before installation. The destination must
be writable by the controller's `401:401` identity; source directories and log
files need only be readable. A hostPath is node-local unless it fronts shared
storage, so place the controller and collector accordingly or use PVCs backed by
shared storage.

A collector following the absolute symbolic links must mount the destination
and every source at the same absolute `mountPath` values seen by the controller.
Mount the destination read-only in the collector and run exactly one
materializer writer for it. This chart deliberately does not install or
configure Alloy, OpenTelemetry Collector, or Loki.

Sandbox that collector independently. Its filesystem namespace should expose
only the required destination and source mounts, all read-only, and must not
include service-account tokens, credentials, Secret volumes, unrelated PVCs or
host paths, or the host root filesystem that a substituted source link could
reach. Disable automatic service-account-token mounting, avoid filesystem or
environment-based secrets, and run the collector as a dedicated unprivileged
identity. Read-only mounts protect the volumes from the collector; they do not
prevent a source writer from redirecting an ordinary symbolic link.

Scan, cleanup, retention, and scan-timeout defaults are `1m`, `1h`, `24h`, and
`5s`. The default numeric bounds are a path-length limit of 4096, 4096 paths
per sync, 32 patterns, 64 scan directories, 4096 entries per directory, 512
matches per scan, and 16384 owned links. All are typed `controller.logLinks`
values. Helm rejects enabled configurations with missing storage, non-positive
values, non-normalized or duplicate mount paths, a source equal to or nested
under the destination, or a storage object that does not select exactly one
backing type.

Validate an enabled values file before rollout:

```bash
helm lint deploy/helm/slurm-advisor-controller --values log-links-values.yaml
helm template slurm-advisor-controller \
  deploy/helm/slurm-advisor-controller \
  --namespace slurm \
  --values log-links-values.yaml >/dev/null
```

Treat link changes as eventually consistent when validating a rollout. Confirm
log-link metrics or a known destination entry rather than assuming a successful
controller poll has already materialized its snapshot.

To roll back, set `controller.logLinks.enabled=false` and perform a normal Helm
upgrade. The replacement Pod has no external log-link mounts. Disabling does
not remove existing links or the ownership manifest; inspect those records and
the collector configuration before any deliberate filesystem cleanup.

## Cross-cluster authentication with public JWT

When SAC runs in a Slinky cluster and API Gateway runs in a separate Kubernetes
cluster, use the gateway's public gRPC listener behind TLS ingress.

The gateway must trust the compute cluster's token issuer and signing keys.
Matching ServiceAccount names and audiences in two independent clusters does
not establish that trust. The internal TokenReview mode asks the gateway's own
Kubernetes API to validate the token, so an independently issued compute-cluster
token is rejected with HTTP 401 or gRPC `Unauthenticated`.

### Configure the two charts

1. Obtain the compute cluster's exact issuer and JWKS location from its
   discovery document:

   ```bash
   kubectl --context <compute-context> get --raw /.well-known/openid-configuration
   ```

   Use `issuer` unchanged. Set `jwksUrl` to an HTTPS endpoint serving that
   issuer's public signing keys. It must be reachable from the gateway Pod
   without bearer credentials, and its certificate must be trusted by the
   gateway. A cluster-local discovery URL may need a reachable endpoint; do not
   substitute the gateway cluster's keys. For a private JWKS CA, use the gateway
   chart's `volumes` and `volumeMounts` to mount its PEM CA certificate from a
   ConfigMap at `/etc/ssl/certs/compute-issuer.pem` in the gateway container.
   Publish only public keys, never the ServiceAccount signing private key.

2. Apply these API Gateway chart overrides in the backend cluster, replacing
   the example issuer, JWKS URL and ingress hostname. The TLS Secret must exist
   in the gateway namespace and cover the ingress hostname; install a
   gRPC-capable ingress controller such as ingress-nginx separately.

   ```yaml
   externalService:
     enabled: true
   auth:
     insecure: false
     oidc:
       enabled: false
     kubernetes:
       enabled: false
     jwt:
       enabled: true
       issuer: https://issuer.compute.example.com
       jwksUrl: https://issuer.compute.example.com/openid/v1/jwks
       audiences: heimdall-api-gateway
       subjectClaim: sub
       allowedSubjects:
         - system:serviceaccount:slurm:sac
   ingress:
     enabled: true
     ingressClassName: nginx
     fqdn: recovery.example.com
     grpc:
       enabled: true
     tls:
       secretName: recovery-gateway-tls
   ```

   Under the umbrella chart, set `api-gateway.enabled: true` and nest these
   gateway overrides under `api-gateway`.
   The subject restriction applies to this JWT provider; other enabled
   authentication providers are independent. An empty `allowedSubjects` list
   permits any otherwise-valid subject, so retain the exact SAC identity.

3. Apply these SAC chart overrides in the compute cluster's `slurm`
   namespace. The chart creates the named ServiceAccount. To reuse an existing
   account in that namespace, set `serviceAccount.create: false` and keep its
   name explicit. Change the gateway's allowed subject if either name or
   namespace changes.

   ```yaml
   serviceAccount:
     create: true
     name: sac
   controller:
     heimdallEndpoint: recovery.example.com:443
     kubernetesAuth:
       enabled: true
       audience: heimdall-api-gateway
       expirationSeconds: 3600
     gatewayTLS:
       enabled: true
       serverName: recovery.example.com
       caConfigMap:
         name: ""
         key: ca.crt
   ```

   Use `host:port`, without an HTTP scheme or path. Empty CA ConfigMap name uses
   system trust. For a private ingress CA, create a ConfigMap containing its
   PEM CA bundle in the SAC namespace and set `caConfigMap.name` to that name;
   the bundle replaces system roots. `serverName` must match the certificate
   and ingress hostname, including when the endpoint uses an IP address.
   Ingress terminates TLS; keep its plaintext hop to the gateway Pod private.

`controller.kubernetesAuth.enabled` projects the compute ServiceAccount token;
it does **not** require gateway TokenReview or TokenReview RBAC. Both producers
use the chart's ServiceAccount. Watchdog gateway audit delivery inherits these
TLS and token settings and, by default, the controller's endpoint.

### Verify and rotate

Confirm SAC is calling the public TLS endpoint and receives successful
responses; a TLS handshake alone does not prove JWT authentication. For 401 or
`Unauthenticated`, check the issuing cluster's JWKS, exact issuer, audience,
namespace and ServiceAccount subject. For certificate errors, check the CA
bundle and server name. Do not print or copy projected bearer tokens into logs,
Helm values or Secrets.

Kubelet rotates the projected token, and SAC/watchdog reread it on each RPC
without restarting. JWT validation checks signature, issuer, audience, expiry
(with a one-minute clock-skew allowance) and the configured subject. Offline
JWKS validation does not observe Pod or ServiceAccount deletion immediately;
an issued token can remain valid until expiry plus that allowance. JWKS keys
are loaded at gateway startup, so restart the gateway after signing-key changes
and retain old public keys until outstanding tokens expire.

## Kubernetes service authentication

The controller can authenticate its calls to the API gateway's internal gRPC
endpoint with a Kubernetes ServiceAccount token. This option is for
Kubernetes/Slinky deployments and is disabled by default. It must be enabled on
both this chart and the API gateway chart as one coordinated configuration.

Prerequisite: the gateway's Kubernetes API must accept the controller's token.
Same-cluster placement satisfies the issuing-authority requirement. Independent
clusters do not gain trust from matching identity and audience strings; use
[public JWT authentication](#cross-cluster-authentication-with-public-jwt) for
that layout.

### How it works

1. A ServiceAccount gives the controller Pod a Kubernetes identity. Its
   username is always `system:serviceaccount:<namespace>:<name>`.
2. When `controller.kubernetesAuth.enabled=true`, the chart adds an explicit
   `serviceAccountToken` projected volume to the controller Pod. This is not a
   Secret stored in Helm values. Kubernetes writes a short-lived JWT into
   `/var/run/secrets/heimdall/service-account-token` for the controller's
   ServiceAccount and the configured audience.
3. The controller reads that file immediately before each gRPC request and
   attaches it as `authorization: Bearer <token>`. Reading on every request
   lets it use Kubernetes' replacement file after token rotation without a
   restart.
4. The gateway sends the received token to Kubernetes TokenReview with the
   same audience. Kubernetes verifies the credential and audience; the gateway
   then compares the returned username to its configured exact expected
   ServiceAccount username.

Kubernetes manages projected-token lifetime and refresh. The chart's default
`expirationSeconds` is 3600, and it requires at least 600 seconds. Do not copy
the token into a Secret or application configuration. A token tied to a deleted
Pod or ServiceAccount is no longer a usable credential when Kubernetes checks
it through TokenReview. Expiry is enforced on a subsequent authenticated
request; deleting a Pod is the Kubernetes revocation action for its bound
projected token. Kubernetes may allow a token for an object that is pending
deletion for up to 60 seconds, so it is not an instantaneous emergency
revocation mechanism.

### Enable it

Use a named controller ServiceAccount so its identity is predictable. Choose a
nonempty audience and configure that exact same string in both charts. The
gateway expected identity must include the namespace where the controller is
installed, not necessarily the gateway's namespace.

For a controller release in namespace `slurm`:

```yaml
# controller-values.yaml
serviceAccount:
  create: true
  name: slurm-advisor-controller

controller:
  heimdallEndpoint: heimdall-api-gateway-internal:9090
  kubernetesAuth:
    enabled: true
    audience: heimdall-api-gateway
    expirationSeconds: 3600
```

Configure the gateway release with matching values:

```yaml
# gateway-values.yaml
serviceAccount:
  create: true
  name: heimdall-api-gateway

auth:
  kubernetes:
    enabled: true
    audience: heimdall-api-gateway
    expectedServiceAccount: system:serviceaccount:slurm:slurm-advisor-controller
```

The gateway chart creates the TokenReview `ClusterRole` and
`ClusterRoleBinding` when its option is enabled. The controller does not need
TokenReview RBAC: kubelet obtains the projected token for its Pod. The gateway
needs that narrow RBAC permission because it is the component that submits
TokenReview requests.

### Diagnose safely

Never print, copy, decode, or put the bearer token in logs, tickets, shell
history, or a Secret. These checks verify configuration without exposing it:

```bash
# Confirm the rendered controller identity and the projected-token mount.
kubectl -n slurm get pod -l app.kubernetes.io/component=controller
kubectl -n slurm describe pod <controller-pod>
kubectl -n slurm exec <controller-pod> -c slurm-advisor-controller -- \
  sh -c 'test -s /var/run/secrets/heimdall/service-account-token'

# Confirm that the gateway identity can perform only the TokenReview operation.
kubectl auth can-i --as=system:serviceaccount:slurm:heimdall-api-gateway \
  create tokenreviews.authentication.k8s.io
kubectl get clusterrole,clusterrolebinding -l app.kubernetes.io/instance=heimdall-api-gateway
```

If requests are rejected, first compare the three values: the controller
ServiceAccount name and namespace, the shared audience, and the gateway's
`expectedServiceAccount`. Then inspect gateway and controller error messages;
they must identify only the authentication failure, never the token itself.
Also ensure the controller's `heimdallEndpoint` still names the gateway's
internal gRPC Service and that the gateway ServiceAccount is non-default. Helm
fails rendering when enabled values omit the audience or the gateway expected
identity; the controller chart also rejects projected-token lifetimes below 600
seconds.

### Roll back

To return to the default unauthenticated internal-endpoint behavior, first
disable the controller projection so it stops sending a bearer token, then
disable gateway validation:

```bash
helm upgrade slurm-advisor-controller deploy/helm/slurm-advisor-controller \
  --namespace slurm --reuse-values \
  --set controller.kubernetesAuth.enabled=false
helm upgrade heimdall-api-gateway deploy/helm/api-gateway \
  --namespace slurm --reuse-values \
  --set auth.kubernetes.enabled=false
```

Do not leave only one side enabled as a steady configuration: a controller that
sends a token to a gateway that does not validate it gains no authentication,
and a validating gateway with no projected controller token rejects controller
calls.

### Security boundary

This internal TokenReview configuration authenticates the caller only. Its
default controller-to-internal-gateway gRPC connection is plaintext and does not
use mTLS. Therefore a bearer token is not confidential against a network
observer. Keep the endpoint within the intended cluster network and plan
TLS/mTLS separately when transport confidentiality or peer authentication is
required. This internal authentication mode relies on Kubernetes TokenReview;
the public JWT mode described above uses issuer JWKS validation instead.

## Validation

From the repository root:

```bash
deploy/helm/scripts/check-charts.sh \
  deploy/helm/slurm-advisor-controller/values.yaml
```
