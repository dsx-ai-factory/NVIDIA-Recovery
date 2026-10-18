# Connect Slinky

For the Kubernetes and Slurm operator adding NVIDIA Recovery to an existing
Slinky cluster. Complete [backend installation](installation.md) first and keep
its source checkout, shared shell variables, image tag, and gateway address.
You also need Slinky kubectl access and its administrator-managed Slurm key.
The result is an observing controller and watchdog in the Slinky namespace.

Use exactly one controller per Slurm cluster. This guide installs the standalone
controller chart alongside Slinky, whether the backend shares that Kubernetes
cluster or runs in a separate reachable cluster. Keep Slurm, the controller,
watchdog, and `sackd` on the same release family: the evaluation baseline uses
Slinky charts 0.3.1 and `25.05-ubuntu24.04` images.

## Verify scheduler access

```bash
kubectl --context "$SLINKY_KUBE_CONTEXT" get namespace "$SLINKY_NAMESPACE"
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" get service slurm-controller
```

Record a different configless Service name if your installation uses one.
On a Slurm submission host, use `sinfo -s` to choose the same narrow partition
allowlist for controller and watchdog. Confirm that the collector reads your
jobs' actual output path and has the node mapping required by the
[Slinky logging profile](../ops/loki-slurm-runai-logging.md#64-slinky-node-system-and-application-profile).

Verify the shared [Slurm accounting prerequisites](installation.md#slurm-accounting-prerequisites)
from that submission environment. Include `job_comment` in the Slinky Slurm
cluster's managed `AccountingStoreFlags`, preserving existing flags; this is a
scheduler setting, not a value in `slinky-values.yaml` for the standalone
controller. Check the effective setting and a newly submitted job's completed
accounting comment with the read-only commands in that section.

## Create namespace-local credentials

The Slinky cluster needs its own image-pull Secret, even if the backend cluster
already has a Secret with the same name. Use a short-lived token. As in backend
installation, the token is briefly present in process arguments; prefer your
provider's Kubernetes credential integration when available:

```bash
read -r -p "Registry username: " REGISTRY_USERNAME
read -r -s -p "Registry token: " REGISTRY_TOKEN
printf '\n'

kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  create secret docker-registry heimdall-registry \
  --docker-server="$(printf '%s' "$IMAGE_PREFIX" | cut -d/ -f1)" \
  --docker-username="$REGISTRY_USERNAME" \
  --docker-password="$REGISTRY_TOKEN" \
  --dry-run=client \
  -o yaml |
  kubectl --context "$SLINKY_KUBE_CONTEXT" apply -f -

unset REGISTRY_TOKEN
```

Obtain the existing `auth/slurm` shared key from the Slurm administrator or
approved secrets store. Use its actual location; do not substitute the separate
`auth/jwt` key (`jwt_hs256.key`). The chart projects the Secret into each
controller/watchdog Pod and prepares `/etc/slurm/slurm.key` for the `sackd`
sidecar. See Slurm's [native authentication reference](https://slurm.schedmd.com/authentication.html)
for this key's role.

```bash
export SLURM_KEY_PATH=/secure/operator/path/slurm.key

kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  create secret generic slurm-auth-key \
  --from-file=slurm.key="$SLURM_KEY_PATH" \
  --dry-run=client \
  -o yaml |
  kubectl --context "$SLINKY_KUBE_CONTEXT" apply -f -
```

Do not print the key bytes or place them in Helm values or shell history.

## Configure the integration

Copy the Slinky values template to an operator-owned file:

```bash
cp docs/examples/getting-started/slinky-values.yaml slinky-values.yaml
```

Use the [maintained Slinky values template](../examples/getting-started/slinky-values.yaml)
as the configuration reference. Update every `# REPLACE` field:

- every `registry.example.com/acme/nvidia-recovery/<service>` → `$IMAGE_PREFIX/<service>`
- every `v1` tag → `$IMAGE_TAG`
- `customer-slurm` → `$HEIMDALL_ENVIRONMENT`
- `nvidia-recovery-api-gateway.example.internal:9090` → the private API Gateway
  address recorded during backend installation
- `slurm-controller:6817` if the configless Slurm Service has a different name
  or port
- `partitionAllowlist: []` → the Slurm partitions selected for this cluster, for
  example `["gpu", "debug"]`

Leave both applications in observe mode:

```yaml
controller:
  mode: observe
  acknowledgeSchedulerActions: false
watchdog:
  mode: observe
  acknowledgeSchedulerActions: false
```

Review the edited values and resolve every example endpoint and registry,
including `nvidia-recovery-api-gateway.example.internal:9090`. Rendered output must
contain the reachable private gateway address selected during installation.

For a shared backend cluster, the gateway is
`heimdall-api-gateway-internal.<release-namespace>.svc.cluster.local:9090`.
For separate clusters, use the routable private address from installation.
Shell exports do not replace placeholders inside YAML. The standard configless
Slurm endpoint is `slurm-controller:6817`; use your actual Service name.
Leave `watchdog.environmentPattern: null` to inherit `controller.environment`.

## Render and install

```bash
helm lint deploy/helm/slurm-advisor-controller --values slinky-values.yaml
helm template "$SLINKY_CONTROLLER_RELEASE" \
  deploy/helm/slurm-advisor-controller \
  --namespace "$SLINKY_NAMESPACE" --values slinky-values.yaml \
  >/tmp/nvidia-recovery-slinky-controller-rendered.yaml
```

Review the image repositories, matching `sackd` release, namespace, private
API/configless endpoints, and pull/key Secrets. Both applications must render
`observe` mode with scheduler-action acknowledgement `false`. Resolve every
placeholder before applying.

Then install the standalone controller in the Slinky cluster:

```bash
helm upgrade --install "$SLINKY_CONTROLLER_RELEASE" \
  deploy/helm/slurm-advisor-controller \
  --kube-context "$SLINKY_KUBE_CONTEXT" \
  --namespace "$SLINKY_NAMESPACE" \
  --create-namespace \
  --values slinky-values.yaml \
  --wait \
  --timeout 15m
```

Verify the two controller workloads and both containers in each Pod:

```bash
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  get pods \
  -l app.kubernetes.io/name=slurm-advisor-controller
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  logs deployment/"$SLINKY_CONTROLLER_RELEASE" \
  -c slurm-advisor-controller \
  --tail=100
kubectl --context "$SLINKY_KUBE_CONTEXT" \
  -n "$SLINKY_NAMESPACE" \
  logs deployment/"$SLINKY_CONTROLLER_RELEASE-watchdog" \
  -c slurm-watchdog \
  --tail=100
```

The logs must show observe mode. Review Slurm authentication, configless
connection, and API Gateway errors before continuing.
SAC logs `API Gateway connection established` at INFO with `heimdall_endpoint`
when its gRPC connection reaches `Ready`. This message is emitted once per
process. It confirms the transport is ready but does not confirm authorization
or job publication. Use the first-job check below to confirm job publication.

## Result and next step

The controller and watchdog are ready in observe mode. Continue with the
[first-job sanity check](operations.md#observe-a-first-job) to confirm publication
to the backend.
For auth or configless errors, use [troubleshooting](troubleshooting.md#slinky-cannot-load-slurm-configuration).
