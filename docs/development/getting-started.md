# Start developing

This guide is for contributors changing NVIDIA Recovery. It takes you from a
source checkout to a focused build and a check appropriate to your change.
To run a local Slurm/Slinky deployment first, follow the
[local tutorial](../getting-started.md).

NVIDIA Recovery is experimental. The shipped scheduler integration is Slurm,
including Slurm on Kubernetes through Slinky. Start with the
[architecture and job lifecycle](../../ARCHITECTURE.md): the backend produces
scheduler-neutral Advice, and the Slurm Advisor Controller owns scheduler
actions. NVIDIA Attribution is enabled by default; see its
[source and qualification prerequisites](../public-api/optional-attribution.md).

## Choose your component and tools

You need a source checkout, Git, Make, and network access to the public
dependency sources. Install the tools for the area you plan to change:

| Area | Source | Tools and version authority |
| --- | --- | --- |
| Python services and shared libraries | `watcher/`, `job_manager/`, `anomaly_detection_isolator/`, `notifications/`, `recovery_libs/` | Python 3.12 or later and uv; see the root [Python workspace](../../pyproject.toml) and [CI uv setup](../../.github/actions/setup-uv-cache/action.yml). |
| API Gateway and Slurm integration | `api-gateway/`, `slurm-advisor-controller/` | Go at each module's `go.mod` version; [pinned Go tools](../../scripts/ci/go-tool-versions.env) are installed by `make install-go-tools`. |
| Dashboard | `dashboard/` | Node.js and pnpm; see [CI versions](../../.github/workflows/pull-request-and-main.yml) and [package scripts](../../dashboard/package.json). |
| Images, Helm, and local stack | `deploy/`, `scripts/build-images.sh` | Docker with Buildx; Helm for charts; Kind, kubectl, and Tilt for the local stack. See the [local tutorial](../getting-started.md). |
| Documentation | `docs/`, `scripts/docs/` | Node.js, npm, Python, and uv for the published-site checks; see [documentation tooling](../../scripts/docs/README.md). |

The checked-in CI configuration is the version reference for a reproducible
development environment. Read [AGENTS.md](../../AGENTS.md) and any component's
nested instructions before editing. Go changes also follow the
[Go engineering guide](../quality/go-engineering.md).

## Set up dependencies

Run commands from the repository root unless stated otherwise. For a Python
package, sync its locked dependencies; for example:

```bash
uv sync --locked --package watcher
```

Use package names from [the package mapping](../../scripts/ci/lib.sh), such as
`job-manager`, `anomaly-detection-isolator`, `notifications`, or `recovery-libs`.
The [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/)
shares one lockfile; `--package` selects the member to install, and development
dependencies are included by default.

For a Go component, download its dependencies and install the verification
tools:

```bash
(cd api-gateway && go mod download)
make install-go-tools
```

Replace `api-gateway` with `slurm-advisor-controller` when working on the
controller or watchdog. Each is a separate Go module.

For the dashboard:

```bash
pnpm --dir dashboard install --frozen-lockfile
```

Install the repository's commit hook once per clone:

```bash
uv run --locked pre-commit install --install-hooks
```

The hook adds or repairs NVIDIA SPDX source headers; CI runs the non-mutating
License-Eye check. Review hook changes along with your own edits.

Expected result: dependencies for your chosen component are available and its
focused checks in the [testing guide](testing.md) can run. You do not need a
Slurm or Kubernetes cluster for ordinary component unit tests.

## Build images

The distribution model is source plus Helm charts. Build the images you need
locally; no project-published application images are required:

```bash
make build-local SERVICES="job-manager watcher" IMAGE_TAG=dev
```

Omit `SERVICES` to build all eight public services. This loads native-architecture
images into the local Docker image store. The defaults exclude Attribution and
Advice Operator. List accepted service names with:

```bash
scripts/build-images.sh --list
```

`IMAGE_TAG` defaults to `latest` and takes precedence over `BUILD_IMAGE_TAG`.
Use a tag tied to the source revision for deployment and set matching Helm
image values. Building images does not start a cluster or Tilt.

### Publish your builds to a registry

Publishing requires write access to your registry, an existing Docker login,
and a Buildx builder that supports the selected platforms. This command
pushes images and inspects the published references:

```bash
make build-publish \
  IMAGE_PREFIX=registry.example.com/acme/nvidia-recovery \
  IMAGE_TAG=dev \
  SERVICES="job-manager watcher"
```

The default `PLATFORMS=linux/amd64,linux/arm64` publishes a shared image index
for both architectures. For example, the Job Manager reference is
`registry.example.com/acme/nvidia-recovery/job-manager:dev`. Override
`PLATFORMS=linux/arm64` when only that platform is needed.

Docker Desktop's containerd image store can support multi-platform builds.
If the selected builder cannot, create a container-backed builder:

```bash
docker buildx create --name nvidia-recovery --driver docker-container --use
docker buildx inspect --bootstrap
```

Rancher Desktop needs a Docker/Moby-compatible socket and Buildx for these
targets; a containerd/nerdctl-only setup does not provide this Docker CLI path.
For installation using the resulting images, continue with the
[operator installation guide](../operators/installation.md).

## Make and verify a focused change

Choose the smallest check that proves the affected behavior. For example:

```bash
scripts/ci/test-python.sh --package watcher
make GO_PACKAGES=api-gateway verify-go
make lint-typescript test-typescript
```

These are alternatives for different components, not a checklist for every
change. The [testing guide](testing.md) explains when to use component tests,
the Slurm controller harness, or the complete disposable stack. For an
interactive environment, use the [local tutorial](../getting-started.md);
`make dev` uses an action-capable profile by default and recreates its selected
Kind cluster.

Provider-free component tests require no private Attribution credentials.
The full development and image-build defaults include Attribution and currently
require authorized upstream access. Set `E2E_ATTRIBUTION_MODE=unavailable` for
explicit degraded development or `ATTRIBUTION_ENABLED=false` for degraded image
builds; see [NVIDIA Attribution](../public-api/optional-attribution.md).

Portable agent capability changes belong in `agent-capabilities/`; generate their
projections with `python3 scripts/sync-agent-capabilities.py --write`.

Next, review your scoped diff and follow [CONTRIBUTING.md](../../CONTRIBUTING.md)
for DCO, review expectations, and exact-head approval of external PR CI.
