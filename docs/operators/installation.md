# Install the backend

For the Kubernetes operator deploying NVIDIA Recovery for evaluation. You need
an existing Slurm or Slinky cluster and permission to install in the backend
cluster. The result is a ready backend and its private API Gateway address;
then continue with exactly one scheduler integration.

> [!WARNING]
> NVIDIA Recovery is experimental. This profile uses single-instance MySQL,
> RabbitMQ, and Loki, unauthenticated application endpoints, and local dashboard
> access. Public distribution covers source and Helm charts; no prebuilt public
> NVIDIA Recovery application images are provided. Attribution is enabled and
> currently requires authorized upstream source access.

Run workstation commands in Bash. Retain the selected source revision, exported
variables, and operator-owned values files across the [Slurm](slurm.md),
[Slinky](slinky.md), and [operations](operations.md) guides. Exports do not edit
YAML or Slurm job comments; use the same selected values in each file.

Follow the shared installation in this order:

1. [Check prerequisites](#prerequisites-and-supported-baseline) and [set variables](#set-shared-variables).
2. [Build and publish images](#select-source-and-publish-images).
3. [Prepare values and Secrets](#configure-the-namespace-and-values).
4. [Arrange private gateway access](#arrange-private-gateway-access) and [log collection](#configure-log-storage-and-ingestion).
5. [Render and install](#render-and-install), then choose Slurm or Slinky.

Paths and configuration keys containing `heimdall` remain compatibility-sensitive
names.

## Prerequisites and supported baseline

### Build workstation

The build workstation requires:

- the NVIDIA Recovery source checkout;
- Make;
- Docker with Buildx and a running Docker daemon;
- registry write access;
- Helm 3;
- `curl` and `jq`;
- `kubectl` with access to the NVIDIA Recovery backend cluster and, for Slinky, the
  scheduler cluster; and
- network access to the base-image registries and Helm repositories.

The default installation includes NVIDIA Attribution and requires its pinned
upstream source. Anonymous source access was unavailable on 2026-10-06; see
[NVIDIA Attribution](../public-api/optional-attribution.md) for authorized builds,
publication prerequisites, and explicit degraded OFF operation. The provider
currently requires an amd64 node; native amd64 qualification remains required
and arm64 qualification is pending.

The conventional-Slurm path also requires Go at the version declared in
`slurm-advisor-controller/go.mod` (currently 1.27.1) on a compatible Linux
build host.

### Target environment

For SAC in a separate Slinky cluster, use the
[cross-cluster JWT and TLS configuration](../../deploy/helm/slurm-advisor-controller/README.md#cross-cluster-authentication-with-public-jwt).
The gateway must trust the compute cluster's issuer and signing keys; matching
ServiceAccount names and audiences alone is insufficient.

This guide assumes the NVIDIA Recovery backend (API Gateway, Job Manager, Dashboard,
and supporting services) is deployed in its own Kubernetes cluster. The Slurm
cluster — whether a conventional Slurm head node or a Slinky Kubernetes cluster
— can be in the **same** cluster as the NVIDIA Recovery backend or in a **separate**
cluster, as long as the Advisor Controller has private network reachability to
the configured API Gateway endpoint. For TLS ingress, this is the ingress
hostname and port (typically 443); direct Service access uses gRPC port 9090.

The target environment requires:

- an existing Slurm or Slinky cluster (separate from or co-located with the
  NVIDIA Recovery backend cluster);
- a supported log-query backend reachable by NVIDIA Recovery's Watcher and a
  qualified ingestion path for the same storage — the components do not need
  to be in the same cluster (this guide selects the bundled single-node Loki profile);
- Grafana Alloy, or an equivalent collector, able to read the training-job
  output and push to Loki;
- a default Kubernetes `StorageClass` in the NVIDIA Recovery backend cluster for the
  bundled MySQL volume;
- enough capacity for the enabled NVIDIA Recovery services; and
- private network reachability from the Advisor Controller (on the Slurm side)
  to the NVIDIA Recovery API Gateway.

The bundled RabbitMQ installation profile requires a **minimum allocation of
4 CPUs and 4 GiB memory** per broker. The umbrella chart sets both requests and
limits to CPU `"4"` and memory `4Gi`; quickstart and getting-started installations
inherit that allocation. Include it in cluster capacity planning and preserve
it in site overlays. This minimum is not a production sizing or throughput claim.

### Supported Slurm versions

The evaluation baseline for this walkthrough is:

- conventional Slurm 25.05; or
- Slinky `slurm` and `slurm-operator` Helm charts 0.3.1, with the Slinky Slurm
  images, Advisor Controller image, watchdog image, and `sackd` image all built
  from the `25.05-ubuntu24.04` image family.

Other Slurm or Slinky versions require separate compatibility validation before
deployment. Do not mix Slurm release families across the Slinky cluster,
Advisor Controller, watchdog, and `sackd`.

### Slurm accounting prerequisites

Plan for [Slurm job ID reuse](operations.md#slurm-job-id-reuse): the Recovery
database must be reinitialized before numeric job IDs wrap or are reset for
reuse. Retaining Recovery state across reuse by different submissions is
unsupported.

Both native Slurm and Slinky must retain the job's APS/HEIMDALL comment in
accounting so the controller can identify opted-in jobs after they leave the
live queue. In the site-managed Slurm configuration, include
`job_comment` in `AccountingStoreFlags`:

```ini
AccountingStoreFlags=job_comment
```

If other flags are already set, append `job_comment` to that comma-separated
list; do not replace it. Apply the configuration through the site's normal
Slurm change process. For Slinky, change the Slurm cluster's managed
configuration, not the standalone Advisor Controller chart. See the
[Slurm configuration reference](https://slurm.schedmd.com/slurm.conf.html#OPT_AccountingStoreFlags).

Run these read-only checks from the same Slurm environment used by the
controller. For `JOB_ID`, use a newly submitted opted-in job after the setting
is active, and check accounting again after completion:

```bash
scontrol show config | grep -E 'AccountingStorageType|AccountingStoreFlags'
scontrol show job "$JOB_ID"
sacct -X --duplicates -j "$JOB_ID" --parsable2 \
  --format=JobIDRaw,State,Restarts,Comment%2048
```

Require `job_comment` in the effective configuration and the intact policy in
the completed job's `Comment`. Old accounting rows are not backfilled by this
change; use newly submitted jobs for validation.

## Set shared variables

Export the following on the workstation with the source checkout, Docker,
Helm, and kubectl. Replace example values for your environment. The Slinky
variables are required only for that scheduler integration.

```bash
export IMAGE_PREFIX=registry.example.com/acme/nvidia-recovery
export IMAGE_TAG=v1
export RELEASE_NAME=heimdall
export RELEASE_NAMESPACE=nvidia-recovery-system
export HEIMDALL_ENVIRONMENT=customer-slurm
export HEIMDALL_KUBE_CONTEXT=nvidia-recovery-cluster
export SLINKY_KUBE_CONTEXT=slinky-cluster
export SLINKY_NAMESPACE=slurm
export SLINKY_CONTROLLER_RELEASE=slurm-advisor-controller
```

**`IMAGE_PREFIX`** — the registry repository that will contain one child
repository per service. Do not add a service name or a trailing slash. For
example, the Job Manager image will be stored at
`$IMAGE_PREFIX/job-manager:$IMAGE_TAG`.

**`RELEASE_NAME`** — the Helm release name for the NVIDIA Recovery backend. The
walkthrough assumes `heimdall`; internal Service and Deployment names are
derived from it and used in later commands and example values. A different
release name requires updating every derived resource name.

**`HEIMDALL_ENVIRONMENT`** — a short, lowercase label that identifies this
Slurm cluster across NVIDIA Recovery's components. It appears as the `environment`
field in Watcher and controller configuration, and as the `cluster` label in
Loki log queries (for example, `cluster="customer-slurm"`). Use the same value
consistently whenever you reference this cluster in NVIDIA Recovery.

**`SLINKY_CONTROLLER_RELEASE`** — the Helm release name for the **standalone
Advisor Controller chart** (`deploy/helm/slurm-advisor-controller`) installed
in the Slinky cluster. This is the logical name of the Helm installation, not
a pod or Deployment name. Kubernetes resource names (Deployments, Services,
ConfigMaps) in `$SLINKY_NAMESPACE` are derived from this value. If you manage
multiple Slinky clusters, use a distinct release name for each. This journey uses the standalone controller even when the clusters are co-located.

Check the tools and configured context:

```bash
docker version
docker buildx version
helm version
curl --version
jq --version
kubectl config get-contexts
kubectl --context "$HEIMDALL_KUBE_CONTEXT" get storageclass
```

## Select source and publish images

Publishing images writes to the selected registry and can replace an existing
tag. Select the source revision and destination under your site's change
process before running the publication command.

Use the repository URL and an explicitly selected immutable release tag
or full commit SHA. This guide does not designate a qualified release:

```bash
export HEIMDALL_REPOSITORY_URL=https://github.com/dsx-ai-factory/NVIDIA-Recovery.git
export HEIMDALL_REF='<selected release tag or full commit SHA>'

git clone "$HEIMDALL_REPOSITORY_URL" nvidia-recovery
cd nvidia-recovery
git fetch origin "$HEIMDALL_REF"
git checkout --detach FETCH_HEAD
git status --short --branch
git rev-parse HEAD
```

If the repository is already cloned, start with `cd` and run the remaining
commands. Authenticate with the SSH key or HTTPS credential associated with
your approved repository account; do not place a personal access token in the
clone URL, shell history, or this repository.

Do not build a customer deployment from a working tree with unexplained local
changes.

Authenticate Docker using the registry provider's normal login process:

```bash
docker login "$(printf '%s' "$IMAGE_PREFIX" | cut -d/ -f1)"
```

Build and publish the ten services required by this walkthrough, including
Attribution Adapter and Attribution Service. The default selection includes all
ten; Advice Operator remains excluded:

```bash
make build-publish \
  IMAGE_PREFIX="$IMAGE_PREFIX" \
  IMAGE_TAG="$IMAGE_TAG"
```

The default build publishes `linux/amd64` images. Attribution arm64 qualification
is pending. Services that do not include Attribution can still be selected for
other supported architectures explicitly. An explicit amd64 build is:

```bash
make build-publish \
  IMAGE_PREFIX="$IMAGE_PREFIX" \
  IMAGE_TAG="$IMAGE_TAG" \
  PLATFORMS=linux/amd64
```

The build is not transactional. If a later service fails, earlier images may
already exist under `IMAGE_TAG`. Fix the failure and rerun the same command
before installing.

Confirm that every required image is published. This check prints `found` only
after a successful inspection and exits nonzero if any image cannot be verified.
Resolve every failure before installing; Docker's error output remains visible.
For explicit degraded OFF operation, run `export ATTRIBUTION_ENABLED=false`
before building and running this check; it skips the two Attribution images.

```bash
(
  failed=0
  for service in \
    anomaly-detection-isolator \
    api-gateway \
    dashboard \
    job-manager \
    notifications \
    watcher \
    slurm-advisor-controller \
    slurm-watchdog \
    attribution-adapter \
    attribution-service
  do
    if [[ "${ATTRIBUTION_ENABLED:-true}" == false && "$service" == attribution-* ]]; then
      continue
    fi
    image="$IMAGE_PREFIX/$service:$IMAGE_TAG"
    if docker buildx imagetools inspect "$image" >/dev/null; then
      echo "found $image"
    else
      echo "could not verify $image" >&2
      failed=1
    fi
  done
  exit "$failed"
)
```

## Configure the namespace and values

Create the release namespace:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  create namespace "$RELEASE_NAMESPACE" \
  --dry-run=client \
  -o yaml |
  kubectl --context "$HEIMDALL_KUBE_CONTEXT" apply -f -
```

Create an image-pull Secret using a short-lived registry token. This keeps it
out of saved values and shell history and unsets it after use. The generated
YAML contains the base64-encoded credential and is piped directly to Kubernetes;
the plaintext token is briefly present in local process arguments. Use the
registry provider's credential integration when it offers a safer approach.

```bash
read -r -p "Registry username: " REGISTRY_USERNAME
read -r -s -p "Registry token: " REGISTRY_TOKEN
printf '\n'

kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  create secret docker-registry heimdall-registry \
  --docker-server="$(printf '%s' "$IMAGE_PREFIX" | cut -d/ -f1)" \
  --docker-username="$REGISTRY_USERNAME" \
  --docker-password="$REGISTRY_TOKEN" \
  --dry-run=client \
  -o yaml |
  kubectl --context "$HEIMDALL_KUBE_CONTEXT" apply -f -

unset REGISTRY_TOKEN
```

The Secret must be named `heimdall-registry` because that name is referenced
by `global.imagePullSecrets` in the values file below.

Copy the checked-in evaluation overlay to an operator-owned file:

```bash
cp docs/examples/getting-started/nvidia-recovery-values.yaml nvidia-recovery-values.yaml
```

Use the [checked-in values](../examples/getting-started/nvidia-recovery-values.yaml)
as the authoritative configuration. Replace its example registry, image tags,
and environment. Attribution and its workloads are enabled and owned by Helm.
Each service has its own image repository and tag fields.

Replace the registry prefix, all image tags, and the environment with your
selected values. Preserve the repository shape in the template: individual
charts append their service names. The overlay is applied after
[values-quickstart.yaml](../../deploy/helm/nvidia-recovery/values-quickstart.yaml).
It keeps KPI services, Grafana, OpenTelemetry, and the bundled
Advisor Controller disabled. The scheduler controller is installed separately.
Optional telemetry is described in [operations](operations.md#optional-external-otlp-collector).

Job Manager, Watcher, and ADI discover the default Notifications Service in the
same Helm release. Job Manager uses that URL for recovery notifications and,
unless overridden, `/v1/audit/events` for audit delivery. A custom Notifications
name or port requires explicit URLs: `job-manager.service_config.notification_service_url`,
`watcher.config.service.audit_trail.destination`, and
`anomaly-detection-isolator.audit_trail.destination`. Job Manager's separate
`service_config.audit_trail.destination` may override only its audit receiver;
an explicit empty destination (`""`) selects logging-only audit output.

The MySQL PVC contains persistent state and may survive uninstall. Review its
StorageClass, sizing, retention, and backup policy before retaining useful data.
The bundled infrastructure is intended only for evaluation.

## Arrange private gateway access

Conventional Slurm hosts and Slinky in another cluster need a routable private
API Gateway address. The default ClusterIP cannot be reached outside its
cluster. For cross-cluster SAC with public JWT authentication, follow the
[TLS ingress setup](../../deploy/helm/slurm-advisor-controller/README.md#cross-cluster-authentication-with-public-jwt)
and configure private reachability to the ingress hostname and port. Set
`controller.heimdallEndpoint` to that address, for example
`recovery.example.com:443`; retain ClusterIP for the gateway Services behind
this ingress.

For direct Service access, if your platform supports an internal load balancer, set
`api-gateway.service_type: LoadBalancer` and configure its documented
private-load-balancer annotations in both `externalService.annotations` and
`internalService.annotations`. Annotation values must be strings.

The following shows where to put the annotation maps in
`nvidia-recovery-values.yaml`. Replace the example keys and values in both maps
with the ones supplied by your platform operator:

```yaml
api-gateway:
  externalService:
    enabled: true
    annotations:
      example.com/internal-load-balancer: "true"
  internalService:
    annotations:
      example.com/internal-load-balancer: "true"
  service_type: LoadBalancer
```

The platform operator is responsible for private routing and network
restrictions. `service_type` applies to both API Gateway Services. The evaluation
gRPC transport is plaintext; restrict both Services to the intended private networks.

During [render and install](#render-and-install), confirm both rendered Services
carry the selected annotations. After installation, inspect both Services:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  --namespace "$RELEASE_NAMESPACE" \
  get service "$RELEASE_NAME-api-gateway" "$RELEASE_NAME-api-gateway-internal" \
  -o json |
  jq '.items[] | {name: .metadata.name, type: .spec.type,
    annotations: .metadata.annotations, addresses: .status.loadBalancer.ingress}'
```

Each Service must show `type: LoadBalancer`, the configured annotation map, and
an assigned address. Verify with the platform operator that the addresses (or
the addresses resolved from load-balancer hostnames) and access rules belong to
the intended private networks. An empty `addresses` field means provisioning
is incomplete; inspect Service events and the load-balancer controller.
For this direct-Service setup, record the internal Service's private address
on port 9090 as the controller's `HEIMDALL_ENDPOINT`.

For direct Service access from Slinky in the backend cluster, retain ClusterIP and use
`heimdall-api-gateway-internal.<release-namespace>.svc.cluster.local:9090`.

## Configure log storage and ingestion

This profile does not install a log collector. The umbrella chart can optionally
deploy a bundled single-node Loki backend for evaluation purposes, but it is
not an HA production configuration and does not replace a managed Loki
deployment.

This NVIDIA Recovery getting-started path deliberately uses Loki for both
ingestion and queries. Production deployments may instead select VictoriaLogs
for queries, but that does not redirect the collector or provision
VictoriaLogs. Follow
[Configure Log-Query Backends](../ops/configure-log-query-backends.md), supply all
required native LogsQL variants, and qualify the VictoriaLogs ingestion path
before changing `global.log_query.backend`.

### Bundled evaluation Loki

The checked-in `nvidia-recovery-values.yaml` example enables the bundled
backend and configures the application query connection. The following shows
both provider domains; include the system connection when enabling a consumer
that needs system logs:

```yaml
global:
  loki:
    bundled:
      enabled: true
  log_query:
    backend: loki
    providers:
      loki:
        application:
          url: http://loki-gateway
          tenants: ""
          org_id: ""
        system:
          url: http://loki-gateway
          tenants: ""
          org_id: ""
```

This deploys Loki in single-binary mode with one replica and filesystem
storage. It is reachable inside the NVIDIA Recovery backend cluster at
`http://loki-gateway.<RELEASE_NAMESPACE>.svc.cluster.local`. The explicit
provider URLs above use its same-namespace service name. Do not use this
configuration for production — review authentication, retention, storage
sizing, object storage, replication, and backups before promoting.

A collector outside the NVIDIA Recovery backend cluster — including Alloy on a
conventional Slurm node or in a separate Slinky cluster — cannot reach a
Kubernetes `ClusterIP`. Expose the bundled Loki gateway through a private
ingress, internal load balancer, or private NodePort and use that routable
address as the collector's Loki push endpoint. Keep Watcher on the
cluster-internal gateway URL; it runs inside the backend cluster and benefits
from avoiding the external route.

### Existing Loki

Set both provider-domain addresses and disable the bundled backend:

```yaml
global:
  loki:
    bundled:
      enabled: false
  log_query:
    backend: loki
    providers:
      loki:
        application:
          url: https://loki.example.com
          tenants: ""
          org_id: ""
        system:
          url: https://loki.example.com
          tenants: ""
          org_id: ""
```

Set the shell variable `LOKI_URL` to the same address for manual verification. It does not configure or
override the deployed query clients; those use the provider blocks.

For an authenticated endpoint, prepare one protected credential file. For Basic
authentication it contains the password; for a direct token it contains the full
Authorization value, such as `Bearer <token>`. Create a namespace-local Secret:

```bash
export LOKI_SECRET_NAME=nvidia-recovery-loki-query
export LOKI_CREDENTIAL_FILE=/secure/path/loki-credential
test -r "$LOKI_CREDENTIAL_FILE"
chmod go-rwx "$LOKI_CREDENTIAL_FILE"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  create secret generic "$LOKI_SECRET_NAME" \
  --from-file=token="$LOKI_CREDENTIAL_FILE" \
  --dry-run=client -o yaml |
  kubectl --context "$HEIMDALL_KUBE_CONTEXT" apply -f -
```

The provider's `tenants` value selects Basic authentication; without it the
credential is sent verbatim as Authorization. The separate `org_id` value sets
`X-Scope-OrgID`, not the authentication method. Do not put credentials in values
files, exported shell tokens, or command arguments.

If the endpoint uses Basic authentication, add the following settings and
replace the example username. Watcher mounts the password file; Attribution
uses the explicit application- and system-domain identity with its credential
Secrets for the default Attribution-enabled installation. Omit the
`attribution-adapter` blocks only for explicitly degraded OFF operation.

```yaml
global:
  log_query:
    providers:
      loki:
        application:
          tenants: <basic-auth username>
        system:
          tenants: <basic-auth username>

watcher:
  credentials:
    loki:
      app_secret_name: nvidia-recovery-loki-query
      secret_key_name: token

attribution-adapter:
  loki:
    loki_cluster: customer-slurm
  credentials:
    loki:
      app_api_token_secret_name: nvidia-recovery-loki-query
      sys_api_token_secret_name: nvidia-recovery-loki-query
```

If the endpoint uses a direct API token, use the Secret-backed environment
mode instead. The Attribution Adapter intentionally shares this one token
between its application and system Loki clients.

```yaml
watcher:
  credentials:
    loki:
      api_token_secret_name: nvidia-recovery-loki-query
      api_token_secret_key_name: token

attribution-adapter:
  credentials:
    loki:
      api_token_secret_name: nvidia-recovery-loki-query
      api_token_secret_key_name: token
```

Choose one credential source per component. Helm rejects configuring both the
file-backed and direct-token settings. Shell exports do not configure the
deployed Pods; the values above connect the Kubernetes Secret to the Watcher
and Attribution Adapter containers.

---

Regardless of which option you choose, do not continue to action-mode testing
until a representative training-job log is queryable through the exact selector
configured for Watcher.

### Collector requirements

For detailed Alloy installation and Slurm/Kubernetes collector configurations,
follow [Centralized logging for Slurm and Kubernetes](../ops/loki-slurm-runai-logging.md).
At minimum, complete its prerequisites, collector setup, validation checklist,
and Slurm end-to-end verification.

NVIDIA Recovery keeps application and system logs in separate Loki domains. Use the
same minimum contract for conventional Slurm, Slinky, and development
environments:

| Log domain | Required queryable fields | Optional fields |
| --- | --- | --- |
| Slurm application output | `cluster`, `appName="slurm-app-log"`, `job_id`; preserve `filename` | `job_name`, validated `node_name`, producer-supplied `slurm_job_attempt` |
| Node system logs | `cluster`, `component="journal"`, validated Slurm `node_name` | bounded fields such as `unit`, `syslog_identifier`, `facility`, and `level` |

Watcher selects application streams only. Its `query_prefix` is
`{cluster="customer-slurm", appName="slurm-app-log"}` with the cluster value
replaced by `$HEIMDALL_ENVIRONMENT`; the collector must also make `job_id`
queryable. A file collector cannot reliably know a Slurm restart count, so it
must not invent `slurm_job_attempt`. NVIDIA Recovery resolves exact attempts from
scheduler state, the preserved source path, and time bounds.

System logs belong to a node and time, not to a workload. Never attach
`job_id`, `slurm_job_id`, a workflow ID, or an attempt ID to a system stream.
Map the collector's node identity to the scheduler-visible Slurm `NodeName` and
validate that mapping before rollout.

For the default provider, the Attribution Adapter's `queries.attribution.system` template selects only `cluster` and
`component="journal"`; the workload allocation and time bounds are separate
inputs. Every returned system stream must still carry `node_name` so the
adapter can correlate it with that allocation.

The collector lifecycle differs by environment, but the labels and ownership
boundary do not:

| Profile | Collector placement | Required identity/storage check |
| --- | --- | --- |
| Conventional Slurm | Alloy systemd service on every compute node, or one trusted collector for a shared application-log tree | Validate the journal or host name against `scontrol show node`; make every job-output file readable to the application collector. |
| Slinky | Alloy DaemonSet on every Kubernetes node that hosts a Slurm compute pod | Explicitly map Kubernetes node to Slurm `NodeName`; mount only the exact host syslog source read-only and mount the shared job-output or Advisor link storage read-only. |

The linked runbook gives conventional Slurm, Slinky, and image-based examples.
A writable syslog hostPath is permitted only for trusted fault injection on a
disposable Slinky/Kind node; production workloads must not receive it.

Job output files may be created with restrictive permissions. The Alloy process
must be able to read files created after it starts. On shared Slurm log
directories, set a default ACL so new files are readable by the Alloy user:

```bash
sudo setfacl -m u:alloy:rX /shared/slurm/job-logs
sudo setfacl -d -m u:alloy:rX /shared/slurm/job-logs
```

Replace `/shared/slurm/job-logs` with the actual job output directory and
`alloy` with the OS user that runs the Alloy process.

Complete both Slurm checks in the logging runbook: submit its job-output marker
and emit its system marker through the node's normal syslog ingress with
`logger`. The checks pass only when Loki returns the application marker with
`job_id` and `filename`, and the system marker with the allocated
`node_name` and no workload labels. Posting a fixture directly to Loki does not
qualify syslog collection. The runbook also covers existing-file permissions. Complete marker checks after
the backend is running and before action-mode testing.

## Render and install

The commands use the source chart. If your release provides an OCI chart,
substitute its exact reference and `--version`; this guide does not designate
a qualified release. Run from the source root.

The chart dependencies include both local NVIDIA Recovery subcharts and public
third-party charts. Refresh them from the repository root:

```bash
helm repo add grafana https://grafana.github.io/helm-charts --force-update
helm repo add grafana-community \
  https://grafana-community.github.io/helm-charts \
  --force-update
helm repo add open-telemetry \
  https://open-telemetry.github.io/opentelemetry-helm-charts \
  --force-update
helm repo update grafana grafana-community open-telemetry
helm dependency update deploy/helm/nvidia-recovery
```

Use both values files below: `values-quickstart.yaml` configures the bundled
MySQL database; `nvidia-recovery-values.yaml` supplies the service images and
log-provider configuration required by default-on Attribution.

For [explicit degraded OFF operation](../public-api/optional-attribution.md#explicit-degraded-off-mode),
build images with `ATTRIBUTION_ENABLED=false` and append
`--values docs/examples/getting-started/attribution-disabled-values.yaml` after
`--values nvidia-recovery-values.yaml` in both commands below. In that mode,
confirm the Attribution Adapter and Service are absent and ADI has
`attribution_enabled=false` instead of checking for the two provider workloads.

Render the NVIDIA Recovery backend before installing its workloads:

```bash
helm template "$RELEASE_NAME" deploy/helm/nvidia-recovery \
  --namespace "$RELEASE_NAMESPACE" \
  --values deploy/helm/nvidia-recovery/values-quickstart.yaml \
  --values nvidia-recovery-values.yaml \
  >/tmp/nvidia-recovery-backend-rendered.yaml
```

Review `/tmp/nvidia-recovery-backend-rendered.yaml` and confirm:

- Attribution Adapter and Attribution Service are present; Advice Operator is absent;
- every NVIDIA Recovery service image repository matches `$IMAGE_PREFIX/<service>`;
- bundled MySQL, RabbitMQ, and Loki images come from the expected public
  registries or your approved mirrors;
- the target namespace is `$RELEASE_NAMESPACE`;
- `imagePullSecrets` references `heimdall-registry`;
- when external OTLP export is configured, every NVIDIA Recovery application
  container has the selected endpoint and protocol, and authenticated
  containers reference the selected Secret rather than containing the token
  value;
- all Service types are `ClusterIP` (or `LoadBalancer` only where explicitly
  intended); and
- no unexpected hostname, registry, or namespace appears.

Install or update the NVIDIA Recovery backend:

```bash
helm upgrade --install "$RELEASE_NAME" deploy/helm/nvidia-recovery \
  --kube-context "$HEIMDALL_KUBE_CONTEXT" \
  --namespace "$RELEASE_NAMESPACE" \
  --create-namespace \
  --values deploy/helm/nvidia-recovery/values-quickstart.yaml \
  --values nvidia-recovery-values.yaml \
  --wait \
  --timeout 15m
```

Check the backend release, Pods, and recent events:

```bash
helm status "$RELEASE_NAME" \
  --kube-context "$HEIMDALL_KUBE_CONTEXT" \
  --namespace "$RELEASE_NAMESPACE"
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  get pods --namespace "$RELEASE_NAMESPACE" -o wide
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  get events --namespace "$RELEASE_NAMESPACE" \
  --sort-by=.metadata.creationTimestamp
```

All enabled Deployments and the MySQL, RabbitMQ, and bundled Loki Pods must
become ready, including Attribution Adapter and Attribution Service. Missing
provider source, image, configuration, or connectivity is a failed enabled
installation. Check provider `/v2/healthz`, adapter `/healthz`, and a representative
attribution request as described in [NVIDIA Attribution](../public-api/optional-attribution.md).

For bundled Loki, set the manual-check URL. For external Loki, use your selected
endpoint instead. This variable does not configure deployed consumers.

```bash
export LOKI_URL="http://loki-gateway.${RELEASE_NAMESPACE}.svc.cluster.local"
```

Confirm that the Loki query API is reachable from inside the NVIDIA Recovery backend
cluster. The command below works for an unauthenticated in-cluster Loki; for
an external or multi-tenant Loki add the site's TLS, authentication, and
`X-Scope-OrgID` headers:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" \
  -n "$RELEASE_NAMESPACE" \
  run nvidia-recovery-loki-check \
  --rm -i \
  --restart=Never \
  --image=curlimages/curl:8.12.1 \
  --command -- \
  curl -fsS "$LOKI_URL/loki/api/v1/status/buildinfo"
```

Verify private reachability from the scheduler controller to its configured
API Gateway endpoint. For TLS ingress, use the ingress hostname and port
(for example, `recovery.example.com:443`). For direct Service access through a
private load balancer, complete the
[Service annotation and address verification](#arrange-private-gateway-access)
and use the internal Service's private address on port 9090.

## Result and next step

You have a ready backend, a private API address, and an ingestion plan. Install
exactly one scheduler integration: [conventional Slurm](slurm.md) or
[Slinky](slinky.md). Use [troubleshooting](troubleshooting.md#installation-problems)
if installation or readiness fails.
