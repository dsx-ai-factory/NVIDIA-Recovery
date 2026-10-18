# API Gateway Helm Chart

## Service annotations

Set `externalService.annotations` for the Service named
`<gateway-fullname>` and `internalService.annotations` for
`<gateway-fullname>-internal`. Both accept maps of annotation keys to string
values; quote values such as `"true"`. Empty maps omit the annotations field.
`externalService.enabled` controls only the public-listener Service; the
internal-listener Service is always rendered.

`service_type` applies to both Services and defaults to `ClusterIP`. When using
`LoadBalancer`, configure your platform's private-load-balancer annotations on
both Services and restrict access to the intended private networks. See
[Arrange private gateway access](../../../docs/operators/installation.md#arrange-private-gateway-access)
for a values-file example and verification commands. With the umbrella chart,
place these settings under `api-gateway`.

## Audit Trail API and event ingestion

The public gRPC Service exposes `heimdall.v1.AuditService/CreateAuditEvents`,
and the public HTTP Service exposes its generated `POST /v1/audit/events`
binding under `urlPrefix`, following the Job Manager API pattern. API Gateway
authenticates the caller and translates the request to the downstream Audit
Trail HTTP API. The HTTP gateway uses the same JSON and status conventions as
Job Manager, including HTTP 200 for a successful event submission.

Configure the downstream base URL with `audit.endpoint`, which sets
`HEIMDALL_AUDIT_ENDPOINT`:

```yaml
audit:
  endpoint: http://heimdall-notifications:8080
```

The Notifications service is being renamed Audit Trail. Its existing
Kubernetes Service name remains in the default and example URLs until that
rename is deployed; update `audit.endpoint` when the Service name changes.
Audit Trail does not need its own ingress. The gateway exposes audit ingestion
through its generated HTTP and gRPC handlers. Caller credentials are not
forwarded downstream.

To expose gRPC through the same public ingress used by Job Manager, enable:

```yaml
ingress:
  enabled: true
  fqdn: advisor-api.prod.example.com
  grpc:
    enabled: true
    base_path: /
```

SAC uses a single API Gateway endpoint and gRPC connection for both
job/advice requests and audit events. This ingress routes those calls to the
public API Gateway Service's `grpc` port. Configure `controller.gatewayTLS`
in the SAC chart to connect through TLS ingress with projected ServiceAccount
tokens. Set `ingress.grpc.enabled` to `true` for gRPC or leave it `false` to
expose HTTP instead.

For SAC in a separate Slinky cluster, follow the
[cross-cluster public JWT setup](../slurm-advisor-controller/README.md#cross-cluster-authentication-with-public-jwt).
It configures compute-cluster issuer/JWKS trust, the exact SAC subject and
verified TLS; it does not enable internal TokenReview.

## Kubernetes service authentication for the internal endpoint

This chart can validate calls to the API gateway's internal gRPC endpoint from
the Slurm advisor controller using a Kubernetes projected ServiceAccount token.
It is **off by default**. Enable it only together with
`controller.kubernetesAuth.enabled` in the `slurm-advisor-controller` chart.
The gateway's own Kubernetes API must accept that token: same-cluster placement
satisfies the issuer prerequisite, while independent clusters with matching
ServiceAccount names and audiences still return HTTP 401 / gRPC
`Unauthenticated`. Use the public JWT setup above for separate clusters.

### What this authenticates

A Kubernetes ServiceAccount is an identity assigned to a Pod. In this setup,
the controller Pod runs as a named ServiceAccount. Kubernetes projects a
short-lived JWT for that identity into the controller Pod, with an audience that
identifies this gateway as the intended recipient. The controller reads that
file for each gRPC call and sends it in the standard `authorization: Bearer ...`
metadata.

The gateway does not trust the JWT merely because it looks like a JWT. It sends
the presented token and the configured audience to the Kubernetes
`authentication.k8s.io` TokenReview API. The API server reports whether the
token is authentic for that audience and returns the caller username. The
gateway then requires that username to exactly equal the configured controller
identity:

```text
system:serviceaccount:<namespace>:<service-account-name>
```

This two-part check matters: a valid token with the wrong audience is rejected,
and a token for a different ServiceAccount is rejected even if it has the same
audience.

### Enable the gateway side

Choose a nonempty audience string shared only by these two components, and use
the exact namespace and ServiceAccount name that the controller chart renders.
For example, when both charts are installed in `slurm`, with the controller
release and ServiceAccount named `slurm-advisor-controller`:

```yaml
# gateway-values.yaml
auth:
  kubernetes:
    enabled: true
    audience: heimdall-api-gateway
    expectedServiceAccount: system:serviceaccount:slurm:slurm-advisor-controller

# TokenReview needs a non-default identity for the gateway Pod.
serviceAccount:
  create: true
  name: heimdall-api-gateway
```

Install or upgrade with those values:

```bash
helm upgrade --install heimdall-api-gateway deploy/helm/api-gateway \
  --namespace slurm \
  --values gateway-values.yaml
```

When enabled, the chart renders a cluster-scoped `ClusterRole` and
`ClusterRoleBinding` granting the gateway ServiceAccount only `create` on
`tokenreviews.authentication.k8s.io`. TokenReview is cluster-scoped, so a
namespaced Role cannot grant this permission. This permission lets the gateway
ask Kubernetes to validate a supplied credential; the chart-created role does
not grant access to Pods, Secrets, or ServiceAccounts.

The gateway uses its normal in-cluster ServiceAccount credentials to call
TokenReview. The chart enables its ServiceAccount token mount in this mode for
that purpose. The gateway ServiceAccount is distinct from the controller
ServiceAccount it expects as a caller.

### Matching controller values

The controller must use the same audience:

```yaml
# controller-values.yaml
serviceAccount:
  create: true
  name: slurm-advisor-controller

controller:
  kubernetesAuth:
    enabled: true
    audience: heimdall-api-gateway
    expirationSeconds: 3600
```

See the [Slurm advisor controller chart README](../slurm-advisor-controller/README.md#kubernetes-service-authentication)
for the controller-side configuration, safe diagnostics, rotation behavior, and
rollback procedure.

## External Ingress TLS

The chart can expose the public API Gateway Service through its existing
Ingress and terminate HTTPS at that Ingress. This is independent of Kubernetes
ServiceAccount authentication for the internal controller endpoint.

Ingress and DNS are cluster-operated prerequisites. Set `ingress.fqdn` to a
hostname that resolves to the cluster Ingress controller. The chart does not
install an Ingress controller, cert-manager, a certificate issuer, or a
certificate authority.

### Use an operator-managed TLS Secret

Create a Kubernetes TLS Secret using the certificate lifecycle approved by the
cluster operator, then reference its name:

```yaml
ingress:
  enabled: true
  fqdn: advisor-api.prod.example.com
  tls:
    secretName: heimdall-api-gateway-tls
```

The Secret must be in the API Gateway release namespace and contain a
certificate valid for `advisor-api.prod.example.com`. The chart does not modify a
manually managed Secret.

### Use an existing cert-manager installation

If cert-manager and the selected issuer are already installed and operated by
the cluster, enable Certificate rendering:

```yaml
ingress:
  enabled: true
  fqdn: advisor-api.prod.example.com
  tls:
    secretName: heimdall-api-gateway-tls
    certManager:
      enabled: true
      issuerRef:
        name: platform-ca
        kind: ClusterIssuer
        group: cert-manager.io
```

The chart renders a `cert-manager.io/v1 Certificate` that asks the configured
issuer for `advisor-api.prod.example.com` and stores the resulting keypair in
`heimdall-api-gateway-tls`. cert-manager renews the certificate according to
its issuer policy. Enabling this option on a cluster without the cert-manager
Certificate API causes Kubernetes installation to fail; leave it disabled and
use an operator-managed Secret instead.

TLS encrypts traffic from an external client to the Ingress. Standard Ingress
TLS terminates there; this setting does not add TLS or mTLS to the
Ingress-to-API-Gateway-Pod hop or to the controller-to-internal-gateway gRPC
connection.

### Security boundary

This option authenticates the controller to the gateway; it does **not** enable
TLS or mTLS. The default controller-to-internal-gateway gRPC connection is
plaintext. A bearer token sent on that connection is not confidential against a
network observer. Keep the endpoint on the intended cluster network and treat
mTLS/TLS as a separate required hardening step where confidentiality or
transport peer authentication is needed. This internal authentication mode does
not perform local JWT issuer or JWKS validation; Kubernetes TokenReview is the
validating authority.
