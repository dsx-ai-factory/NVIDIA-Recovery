# NVIDIA Attribution

NVIDIA Attribution is enabled by default in the Helm release, image build,
Tilt development stack, and local E2E harness. ADI calls the Attribution Adapter,
which queries workload and node evidence and calls the NVIDIA Attribution provider.
The provider Deployment and Service are owned by Helm alongside the adapter.
Follow the [operator installation guide](../operators/installation.md) for the
complete stack.

## Source and release prerequisites

The build source is [dsx-ai-factory/attribution](https://github.com/dsx-ai-factory/attribution).
Authorized source access remains necessary; public CI receives no private
provider credentials. Its explicitly degraded E2E lane does not qualify the
default Attribution-enabled release.

All default build paths use commit
`7d6dff168bc282f471828fc8468e02de0df17480`, matching the protected qualification
workflow. The generated SDK is now under
`service/service-app/nvidia_attribution_client`, with the Python namespace
`nvidia_attribution_client`. Source preparation also includes the root `LICENSE`
required by the upstream service Dockerfile, including for older sparse caches.

The adapter creates sessions and reads results through V2, and ingests existing
log formats through the provider's V1 compatibility endpoint, which shares those
sessions. Prometheus vector and matrix results are flattened into the supported
`prometheus_metrics` format; samples must retain `__name__` and the configured
hostname label. This compatibility update does not enable the currently disabled
metric collection queries.
Provider-boundary tests and local image checks do not constitute deployed GPU
qualification.

The adapter build temporarily installs the client from this pinned checkout as
a named BuildKit context. Public source/client publication and conversion to a
verified pinned dependency in `pyproject.toml` and `uv.lock` remain release
prerequisites. No speculative package coordinate is used.

Default image publication targets `linux/amd64`. Both Attribution builds select
amd64, including local loads; both Deployments select amd64 nodes.
Native amd64 end-to-end qualification remains required, and arm64 Attribution
qualification is pending. Emulated builds do not satisfy the native gate.

## Build and configure

With authorized upstream access, the ordinary build includes both provider images:

```bash
make build-publish IMAGE_PREFIX="$IMAGE_PREFIX" IMAGE_TAG="$IMAGE_TAG"
```

For a pre-existing checkout, set `ATTRIBUTION_SOURCE_DIR` to its absolute path
and `ATTRIBUTION_SOURCE_URL` to its exact origin. The resolver verifies the
immutable commit, origin, clean state, and required files before building any
images. Optional SSH deploy keys require an explicit GitHub SSH source URL;
never embed credentials in a URL. A failed source fetch is an enabled-provider
failure; it never automatically switches to OFF.

[The installation values](../examples/getting-started/nvidia-recovery-values.yaml)
configure image registry/tag, workload and node log selectors, and disable slow
analysis until epilog ingestion is qualified. The chart derives same-release
provider and adapter URLs. For an external provider, set
`attribution-service.enabled=false` and `attribution-adapter.fact.url` to its
HTTP(S) endpoint. For an external adapter, set `attribution-adapter.enabled=false`
and `anomaly-detection-isolator.attribution_service_url` explicitly. An empty
explicit endpoint is invalid when its consumer is enabled.

For standalone ADI, configure `attribution_service_url` before startup; enabled
ADI rejects a missing or invalid HTTP(S) endpoint. The adapter similarly requires
`fact_url` and its generated client. Network errors during a request remain
structured Attribution errors so recovery can continue with visible degradation.

Check provider `/v2/healthz` and adapter `/healthz` separately, then send a
representative request through the [Adapter API](attribution-adapter.md).
Healthy Pods alone are not attribution evidence.

## Explicit degraded OFF mode

To build without provider source, select OFF explicitly:

```bash
ATTRIBUTION_ENABLED=false make build-publish \
  IMAGE_PREFIX="$IMAGE_PREFIX" IMAGE_TAG="$IMAGE_TAG"
```

Apply [attribution-disabled-values.yaml](../examples/getting-started/attribution-disabled-values.yaml)
after the installation values for the corresponding degraded release:

```yaml
anomaly-detection-isolator:
  attribution_enabled: false
attribution-adapter:
  enabled: false
attribution-service:
  enabled: false
```

For local development or E2E, use `E2E_ATTRIBUTION_MODE=unavailable`; the default
is `available`. OFF omits provider images and workloads and returns no
attributions. It exercises recovery without provider diagnosis and does not
qualify Attribution. Upgrading the Helm release with this override removes its
provider and adapter workloads; no separately applied provider manifest remains.
