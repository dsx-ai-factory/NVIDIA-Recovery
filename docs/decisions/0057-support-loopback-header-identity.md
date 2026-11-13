# ADR 0057: Support header identity on loopback API listeners

Status: accepted

Date: 2026-11-10

> **Implementation note (2026-11-10):** The default-off gateway mode is implemented;
> security review and deployment integration remain required before enabling it.

## Context

Some deployments authenticate specialized credentials in a colocated proxy while
retaining standard OIDC and session authentication in the gateway. Revalidating
those credentials as human OIDC tokens rejects callers already authenticated by
the proxy. The gateway needs a narrowly scoped identity handoff without embedding
provider-specific authentication logic.

## Decision

Introduce the internal support setting AuthConfig.TrustHeaderBasedIdentity,
disabled by default. On public listeners when enabled, accept exactly X-NVR-Auth-Mechanism
and X-NVR-Client-Id as identity evidence. Malformed assertions reject; absent
assertions continue through normal authentication. No user or scope assertion is
part of this contract.

Require literal loopback bindings for the public HTTP and gRPC listeners and
disallow global insecure mode. Internal listeners ignore identity assertions and
retain their existing bindings and optional Kubernetes authentication. The
internal Service remains directly reachable inside the cluster; it is outside
the proxy authentication boundary. Public traffic must reach only the proxy's
ports, and deployment must prohibit host networking. All processes in the Pod
share the public listeners' trust boundary.

The proxy must strip caller-supplied assertions and inject values only after
successful authentication. The gateway strips metadata aliases and unsupported
reserved fields, maps validated HTTP assertions into gRPC metadata, and strips
assertions entirely on OIDC flow handlers. Ordinary mode does not trust headers.

## Consequences

Specialized authentication can remain outside the gateway while existing OIDC
providers remain the fallback. No identity signing secret is required inside
the Pod boundary. Compromise of another process in the same Pod can forge an
assertion; this feature does not provide container-level isolation.

Application validation cannot establish whether host networking or unintended
proxies are configured. Deployment validation and human security review remain
required. This support mode is not added to standard public deployment examples.
