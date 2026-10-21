#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# env.sh — default configuration for the Heimdall e2e harness.
# Override any variable by exporting it before calling e2e.sh.
#
# Safe to source multiple times. All tuneable values live here — no hardcoded
# names or versions in the other scripts.
#
# Boolean toggles use the empty/non-empty convention: an empty value is OFF,
# any non-empty value is ON. Use the enabled() helper in lib.sh to test them.
# This avoids case/spelling pitfalls (true/True/TRUE/1/yes all mean ON).

# Run once per shell — re-sourcing (every script sources env.sh) is a no-op.
[[ -n "${_HEIMDALL_ENV_LOADED:-}" ]] && return 0
_HEIMDALL_ENV_LOADED=1

_e2e_env_error() {
  echo "[ERROR] $*" >&2
  exit 1
}

# Absolute path to this directory, used to resolve other paths regardless of
# the caller's working directory.
_ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Repo root via git, falling back to the path relative to this script when not
# in a git checkout (e.g. an exported tarball).
if [[ -z "${REPO_ROOT:-}" ]]; then
  REPO_ROOT="$(git -C "$_ENV_DIR" rev-parse --show-toplevel 2>/dev/null)" \
    || REPO_ROOT="$(cd "$_ENV_DIR/../../.." && pwd)"
fi
export REPO_ROOT

# Log storage is independent of the scheduler profile and attribution lane.
E2E_LOG_BACKEND="${E2E_LOG_BACKEND-loki}"
case "$E2E_LOG_BACKEND" in
  loki | victorialogs) ;;
  *) _e2e_env_error "invalid E2E_LOG_BACKEND=$E2E_LOG_BACKEND; expected loki or victorialogs" ;;
esac
export E2E_LOG_BACKEND

# --- Cluster ----------------------------------------------------------------

# Non-empty (default) creates a Kind cluster. Empty uses an existing cluster
# via the caller-supplied KUBECONFIG.
# Note: '-' not ':-' so an explicit empty value (DEPLOY_KIND_CLUSTER=) stays
# empty (off); only an unset variable gets the default.
DEPLOY_KIND_CLUSTER="${DEPLOY_KIND_CLUSTER-true}"

# Kind cluster name. Set to a unique value when running multiple clusters in
# parallel locally (e.g. CLUSTER_NAME=heimdall-dev-2).
CLUSTER_NAME="${CLUSTER_NAME:-heimdall-e2e}"

# Optional pull-through mirror/proxy for docker.io image pulls. Docker Hub's
# anonymous rate limit can return 429 Too Many Requests for pods that pull
# public docker.io images (e.g. bitnamilegacy/os-shell, bitnamilegacy/rabbitmq
# via the NVIDIA Recovery chart's rabbitmq/mysql subcharts). When set, cluster.sh
# configures a containerd registry mirror for docker.io on every Kind node at
# cluster-creation time, falling back to registry-1.docker.io if the mirror
# does not have the image. Empty (default) leaves Docker Hub unchanged.
DOCKER_HUB_REGISTRY_MIRROR="${DOCKER_HUB_REGISTRY_MIRROR:-}"
export DOCKER_HUB_REGISTRY_MIRROR

# Local image registry used to push built images into the Kind cluster without
# kind load docker-image. The registry container is started by cluster.sh and
# connected to the Kind bridge network so every node can reach it by name.
KIND_REGISTRY_NAME="${KIND_REGISTRY_NAME:-kind-registry}"
KIND_REGISTRY_PORT="${KIND_REGISTRY_PORT:-5000}"
export KIND_REGISTRY_NAME KIND_REGISTRY_PORT

# KUBECONFIG handling depends on the mode:
if [[ -n "${DEPLOY_KIND_CLUSTER}" ]]; then
  # Creating a cluster: we OWN the kubeconfig as a standalone build artifact.
  # Force a deterministic path and deliberately ignore any inherited
  # KUBECONFIG — inheriting the caller's (typically ~/.kube/config) is exactly
  # what we must avoid reading, writing, or deleting.
  export KUBECONFIG="/tmp/kind-${CLUSTER_NAME}.kubeconfig"
else
  # Using an existing cluster: the caller must supply KUBECONFIG.
  if [[ -z "${KUBECONFIG:-}" ]]; then
    echo "[ERROR] KUBECONFIG is not set and DEPLOY_KIND_CLUSTER is empty." \
         "Set DEPLOY_KIND_CLUSTER to any non-empty value to create a Kind cluster, or" \
         "export KUBECONFIG=/path/to/kubeconfig to use an existing cluster." >&2
    exit 1
  fi
fi

# Path to a Kind config template. The default uses two labeled workers so each
# Slinky compute identity has its own host syslog and node-local Alloy target.
# A custom template may use __SLURM_SHARED_HOME__ to request the same narrowly
# scoped shared-home bind mount as the reference topology.
KIND_CONFIG="${KIND_CONFIG:-$_ENV_DIR/values/kind-node-separated.yaml}"

# Non-empty leaves the cluster running after the pipeline finishes (or fails)
# so you can inspect it: KUBECONFIG=<path> kubectl get pods -A. Empty (default)
# tears the cluster down on exit. Only applies to clusters this harness creates.
KEEP_CLUSTER="${KEEP_CLUSTER:-}"

# Non-empty (default) runs the test phase. Empty skips it — useful for standing
# up a persistent local dev cluster (with KEEP_CLUSTER) without running scenarios.
# '-' not ':-' so an explicit empty value (RUN_TESTS=) stays empty (off).
RUN_TESTS="${RUN_TESTS-true}"

# Advice-only recovery is the default for the whole test deployment. Non-empty
# enables eager recovery for its dedicated opt-in scenario; do not toggle mid-suite.
E2E_ENABLE_EAGER_REQUEUE="${E2E_ENABLE_EAGER_REQUEUE:-}"
export E2E_ENABLE_EAGER_REQUEUE

# --- Architecture -----------------------------------------------------------

# Target architecture for tool downloads. Detected from uname -m unless the
# caller overrides ARCH explicitly.
if [[ -z "${ARCH:-}" ]]; then
  case "$(uname -m)" in
    x86_64 | amd64) ARCH="amd64" ;;
    aarch64 | arm64) ARCH="arm64" ;;
    *) ARCH="amd64" ;;
  esac
fi

# --- Profile resolution ------------------------------------------------------

# Profile selector for the Slurm Advisor Controller safety mode. Action mode is
# the current cutover default; observe mode remains available for non-mutating
# validation.
E2E_PROFILE="${E2E_PROFILE:-advisor-act}"

# Attribution-backed assertions are the default. Explicit unavailable mode is
# degraded operation and does not fetch or build the provider workloads.
E2E_ATTRIBUTION_MODE="${E2E_ATTRIBUTION_MODE:-available}"
case "$E2E_ATTRIBUTION_MODE" in
  unavailable | available) ;;
  *)
    _e2e_env_error \
      "invalid E2E_ATTRIBUTION_MODE=$E2E_ATTRIBUTION_MODE; expected unavailable or available"
    ;;
esac

ATTRIBUTION_SOURCE_URL="${ATTRIBUTION_SOURCE_URL:-https://github.com/dsx-ai-factory/attribution.git}"
ATTRIBUTION_SOURCE_REF="${ATTRIBUTION_SOURCE_REF:-7d6dff168bc282f471828fc8468e02de0df17480}"
if [[ "$E2E_ATTRIBUTION_MODE" == "available" ]]; then
  if [[ -z "$ATTRIBUTION_SOURCE_URL" || "$ATTRIBUTION_SOURCE_URL" == -* ]]; then
    _e2e_env_error "ATTRIBUTION_SOURCE_URL must be non-empty and must not begin with '-'"
  fi
  if [[ ! "$ATTRIBUTION_SOURCE_REF" =~ ^[0-9a-f]{40}$ ]]; then
    _e2e_env_error "ATTRIBUTION_SOURCE_REF must be a full lowercase Git commit SHA"
  fi
fi

# Optional space-separated exact scenario-name allowlist. Empty runs the full
# suite.
E2E_SCENARIOS="${E2E_SCENARIOS:-}"

E2E_PYTEST_WORKERS="${E2E_PYTEST_WORKERS:-4}"

# Opt-in authentication smoke. It is intentionally separate from the normal
# advisor profiles so existing E2E jobs keep their current, feature-off values.
E2E_KUBERNETES_SERVICE_AUTH_SMOKE="${E2E_KUBERNETES_SERVICE_AUTH_SMOKE:-}"

# Local image tag used for e2e builds, Kind image imports, and Helm image tags.
BUILD_IMAGE_TAG="${BUILD_IMAGE_TAG:-latest}"

HELM_RELEASE="${HELM_RELEASE:-nvidia-recovery}"
export HELM_RELEASE
HELM_NAMESPACE="${HELM_NAMESPACE:-heimdall}"
SLURM_ADVISOR_RELEASE="${SLURM_ADVISOR_RELEASE:-slurm-advisor-controller}"

# Match the charts' default fullname, including the provider's reserved suffix.
_e2e_workload_name() {
  local component="$1" name="$HELM_RELEASE"
  if [[ "$component" == "attribution-service" ]]; then
    name="${name:0:43}"
    printf '%s-attribution-service' "${name%-}"
    return
  fi
  [[ "$name" == *"$component"* ]] || name+="-$component"
  name="${name:0:63}"
  printf '%s' "${name%-}"
}

_E2E_COMMON_STACK_DEPLOYMENTS="$(_e2e_workload_name job-manager) $(_e2e_workload_name anomaly-detection-isolator) $(_e2e_workload_name watcher)"
if [[ "$E2E_ATTRIBUTION_MODE" == "available" ]]; then
  _E2E_COMMON_STACK_DEPLOYMENTS+=" $(_e2e_workload_name attribution-adapter) $(_e2e_workload_name attribution-service)"
fi
case "$E2E_PROFILE" in
  advisor-observe)
    _E2E_PROFILE_HELM_VALUES="scripts/ci/e2e/values/stack-local.yaml"
    _E2E_PROFILE_DEPLOY_ADVISOR_STACK="1"
    _E2E_PROFILE_SLURM_ADVISOR_VALUES="scripts/ci/e2e/values/slurm-advisor-controller-local.yaml"
    SLURM_ADVISOR_CONTROLLER_MODE="observe"
    SLURM_ADVISOR_CONTROLLER_ACKNOWLEDGE_SCHEDULER_ACTIONS="false"
    _E2E_PROFILE_STACK_DEPLOYMENTS="$_E2E_COMMON_STACK_DEPLOYMENTS ${SLURM_ADVISOR_RELEASE} ${SLURM_ADVISOR_RELEASE}-watchdog"
    ;;
  advisor-act)
    _E2E_PROFILE_HELM_VALUES="scripts/ci/e2e/values/stack-local.yaml"
    _E2E_PROFILE_DEPLOY_ADVISOR_STACK="1"
    _E2E_PROFILE_SLURM_ADVISOR_VALUES="scripts/ci/e2e/values/slurm-advisor-controller-local.yaml"
    SLURM_ADVISOR_CONTROLLER_MODE="act"
    SLURM_ADVISOR_CONTROLLER_ACKNOWLEDGE_SCHEDULER_ACTIONS="true"
    _E2E_PROFILE_STACK_DEPLOYMENTS="$_E2E_COMMON_STACK_DEPLOYMENTS ${SLURM_ADVISOR_RELEASE} ${SLURM_ADVISOR_RELEASE}-watchdog"
    ;;
  *) _e2e_env_error "invalid E2E_PROFILE=$E2E_PROFILE; expected one of: advisor-observe, advisor-act" ;;
esac

if [[ -n "$E2E_KUBERNETES_SERVICE_AUTH_SMOKE" && "$E2E_PROFILE" != "advisor-observe" ]]; then
  _e2e_env_error "E2E_KUBERNETES_SERVICE_AUTH_SMOKE requires E2E_PROFILE=advisor-observe"
fi

# --- Images -----------------------------------------------------------------

# Matching Slinky image settings used to package the Advisor controller and
# watchdog with the required Slurm client binaries and libraries.
E2E_SLINKY_SLURM_TAG="${E2E_SLINKY_SLURM_TAG:-25.05-ubuntu24.04}"
[[ "$E2E_SLINKY_SLURM_TAG" != *@* ]] || \
  _e2e_env_error "E2E_SLINKY_SLURM_TAG must be a tag; use E2E_SLINKY_SACKD_IMAGE for a digest-pinned donor"
if [[ -z "${E2E_SLINKY_SACKD_IMAGE:-}" ]]; then
  if [[ "$E2E_SLINKY_SLURM_TAG" == "25.05-ubuntu24.04" ]]; then
    E2E_SLINKY_SACKD_IMAGE="ghcr.io/slinkyproject/sackd:25.05-ubuntu24.04@sha256:dd1f0b6030229b3d602e3d80e6d50cc4f9fd17b325312521dfa07376fffc7569"
  else
    E2E_SLINKY_SACKD_IMAGE="ghcr.io/slinkyproject/sackd:${E2E_SLINKY_SLURM_TAG}"
  fi
fi

# Non-empty (default) loads locally-built Docker images into the cluster.
# Empty skips the load (useful against a cluster that pulls from a registry).
# '-' not ':-' so an explicit empty value (LOAD_IMAGES=) stays empty (off).
LOAD_IMAGES="${LOAD_IMAGES-true}"

# Space-separated set of images load-images.sh loads into the cluster (when
# LOAD_IMAGES is on). Entries may be a full "repo:tag" or a bare name (then
# :BUILD_IMAGE_TAG is appended). Empty = nothing to load.
#
# The caller may set this directly for advanced partial-load workflows.
E2E_IMAGES_TO_LOAD="${E2E_IMAGES_TO_LOAD:-}"

# --- Helm OCI dependency mirror ----------------------------------------------

# Optional mirror/proxy for the NVIDIA Recovery chart's Bitnami OCI dependencies
# (rabbitmq, mysql). Docker Hub's anonymous pull rate limit can throttle or
# fail `helm dependency update` with "429 Too Many Requests" when resolving
# oci://registry-1.docker.io/bitnamicharts. When set,
# deploy/tilt/refresh-nvidia-recovery-deps.sh resolves dependencies from this value
# in a temporary copy of the charts, leaving the tracked Chart.yaml and
# Chart.lock untouched. Empty (default) leaves Docker Hub unchanged.
BITNAMI_OCI_REGISTRY_MIRROR="${BITNAMI_OCI_REGISTRY_MIRROR:-}"
export BITNAMI_OCI_REGISTRY_MIRROR

# --- Stack deployment -------------------------------------------------------

# Empty (default) skips the Helm install so the harness can be used for cluster
# setup and test scaffolding without built images. Set to any non-empty value
# to Helm-install the NVIDIA Recovery umbrella chart after cluster setup.
DEPLOY_STACK="${DEPLOY_STACK:-}"

# Helm values file(s), relative to the repo root (or absolute paths).
HELM_VALUES="${HELM_VALUES:-$_E2E_PROFILE_HELM_VALUES}"

# How long install.sh waits for the umbrella release to become Ready (`helm
# --wait` and the follow-up `kubectl wait`). This is a whole-stack cold start
# (rabbitmq + mysql + the heimdall services), so it needs a far larger budget
# than POD_READY_TIMEOUT, which sizes single-pod waits. Matches the per-chart
# SERVICE_WAIT_TIMEOUT used for the infra installs. The heimdall services
# fail-fast and crash-restart until rabbitmq/mysql accept connections, so the
# release legitimately takes minutes to converge on a cold runner.
DEPLOY_STACK_TIMEOUT="${DEPLOY_STACK_TIMEOUT:-600}"

# Standalone Slurm Advisor Controller release. Advisor profiles enable this
# release and WFC profiles reject advisor-only knobs so both recovery executors
# cannot be deployed accidentally in the same e2e stack.
DEPLOY_SLURM_ADVISOR_STACK="${DEPLOY_SLURM_ADVISOR_STACK:-$_E2E_PROFILE_DEPLOY_ADVISOR_STACK}"
SLURM_ADVISOR_VALUES="${SLURM_ADVISOR_VALUES:-$_E2E_PROFILE_SLURM_ADVISOR_VALUES}"
SLURM_ADVISOR_TIMEOUT="${SLURM_ADVISOR_TIMEOUT:-300}"

# --- Pre-suite stack readiness gate -----------------------------------------
# Before scenarios run, run-tests.sh runs the e2e-stack-gate command
# (recovery_libs.testing.stack_gate) to wait for the recovery-stack components
# to be Ready, so the suite fails fast and clearly
# instead of every scenario timing out against a half-deployed or disrupted
# stack. Opt-in: an empty namespace (default) disables the gate, so smoke-only
# runs against a cluster without the stack are unaffected. Set
# E2E_STACK_NAMESPACE to the namespace holding the umbrella release (e.g. the
# Slinky Slurm namespace) to enable it; the workload-list defaults below match
# the umbrella release ($HELM_RELEASE-*).
E2E_STACK_NAMESPACE="${E2E_STACK_NAMESPACE:-}"
E2E_STACK_DEPLOYMENTS="${E2E_STACK_DEPLOYMENTS:-$_E2E_PROFILE_STACK_DEPLOYMENTS}"
# The mysql sub-chart pins fullnameOverride to nvidia-recovery-mysql
# (deploy/helm/nvidia-recovery/values.yaml), not a release-name-derived name.
E2E_STACK_STATEFULSETS="${E2E_STACK_STATEFULSETS:-$(_e2e_workload_name rabbitmq) nvidia-recovery-mysql}"
E2E_STACK_TIMEOUT="${E2E_STACK_TIMEOUT:-180}"

# --- Infrastructure provisioning --------------------------------------------
# Shared services installed into the cluster by provision.sh (services/*.sh).

# Empty (default) skips the provision phase. Set to any non-empty value to
# install the shared services (prometheus, loki, alloy, slurm) after the
# cluster is up. '-' not ':-' so an explicit empty value stays empty.
DEPLOY_INFRA="${DEPLOY_INFRA-}"

# Compatibility URL for callers inspecting an existing Loki deployment. Test
# evidence selects its service proxy from E2E_LOG_BACKEND; fixtures enter storage
# through Slurm output files and node syslog collected by Alloy.
E2E_LOKI_URL="${E2E_LOKI_URL-http://loki-gateway.monitoring.svc.cluster.local}"
if [[ "$E2E_LOG_BACKEND" == "loki" && -z "${LOKI_URL+x}" && -n "${DEPLOY_INFRA:-}" ]]; then
  LOKI_URL="$E2E_LOKI_URL"
fi
if [[ -n "${LOKI_URL+x}" ]]; then
  export LOKI_URL
fi

E2E_VICTORIALOGS_URL="${E2E_VICTORIALOGS_URL-http://victorialogs.monitoring.svc.cluster.local:9428}"
if [[ "$E2E_LOG_BACKEND" == "victorialogs" && -z "${VICTORIALOGS_URL+x}" && -n "${DEPLOY_INFRA:-}" ]]; then
  VICTORIALOGS_URL="$E2E_VICTORIALOGS_URL"
fi
if [[ -n "${VICTORIALOGS_URL+x}" ]]; then
  export VICTORIALOGS_URL
fi
# Optional production-interface trace evidence. Name an operator-managed Tempo
# query Service as namespace/service:port (for example,
# monitoring/tempo-query-frontend:3200), or give E2E_TEMPO_URL to skip the port
# forward. Empty keeps recovery scenarios backend-independent.
E2E_TEMPO_SERVICE="${E2E_TEMPO_SERVICE:-}"
E2E_TEMPO_URL="${E2E_TEMPO_URL:-}"
export E2E_TEMPO_SERVICE E2E_TEMPO_URL

# How long each service install waits for its pods (`helm --wait`). Generous
# because first-pull of charts like kube-prometheus-stack/slurm is slow.
SERVICE_WAIT_TIMEOUT="${SERVICE_WAIT_TIMEOUT:-600}"

# Chart versions, pinned for reproducibility.
# cert-manager is installed first (00-) — the slurm-operator webhook needs it to
# mint its self-signed certs (the chart ships its own selfSigned issuer + CA).
CERT_MANAGER_CHART_VERSION="${CERT_MANAGER_CHART_VERSION:-v1.16.2}"
PROMETHEUS_CHART_VERSION="${PROMETHEUS_CHART_VERSION:-77.13.0}"
LOKI_CHART_VERSION="${LOKI_CHART_VERSION:-6.34.0}"
ALLOY_CHART_VERSION="${ALLOY_CHART_VERSION:-1.10.1}"
SLURM_CHART_VERSION="${SLURM_CHART_VERSION:-0.3.1}"
SLURM_OPERATOR_CHART_VERSION="${SLURM_OPERATOR_CHART_VERSION:-0.3.1}"

# Label applied to scraped metrics so they can be attributed to this cluster.
CLUSTER_LABEL="${CLUSTER_LABEL:-e2e}"

# Kind-only backing for the PersistentVolumes that implement the E2E shared
# filesystem contract. Workload charts consume named claims. A multi-node Kind
# profile must expose this same backing on every node or provide an RWX class.
SLURM_SHARED_HOME="${SLURM_SHARED_HOME:-/tmp/${CLUSTER_NAME}-slurm-home}"

# Local E2E pod paths are a fixed cross-component contract: jobs write targets
# under /home/logs and the Advisor owns flat links under /home/log-links. Only
# their disposable-cluster backing root, SLURM_SHARED_HOME, is configurable.

# Empty (default) installs Prometheus without the GPU (DCGM) scrape config.
# Set to any non-empty value to add the gpu-operator scrape — only useful once
# a (fake or real) GPU operator exposes DCGM metrics.
DEPLOY_GPU_SCRAPE="${DEPLOY_GPU_SCRAPE-}"

# --- Tool versions ----------------------------------------------------------
# Pinned for reproducible, verifiable downloads. prepare.sh verifies the
# SHA256 of each downloaded artifact against the upstream checksum.

KUBECTL_VERSION="${KUBECTL_VERSION:-v1.33.3}"
KIND_VERSION="${KIND_VERSION:-v0.27.0}"
HELM_VERSION="${HELM_VERSION:-v3.17.3}"
TILT_VERSION="${TILT_VERSION:-v0.37.5}"

# --- Tool cache -------------------------------------------------------------

# Persistent cache directory, outside the repo so downloaded binaries survive
# across runs/cluster recreations and can never be committed. Honors
# XDG_CACHE_HOME; override CACHE_DIR (e.g. CACHE_DIR=/cache in CI) to point at
# a shared runner cache.
CACHE_DIR="${CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/heimdall-e2e}"

# Available attribution tests use a commit-addressed checkout outside the
# repository. Unavailable mode leaves this path untouched.
ATTRIBUTION_SOURCE_DIR="${ATTRIBUTION_SOURCE_DIR:-$CACHE_DIR/attribution-source/$ATTRIBUTION_SOURCE_REF}"

# Directory where prepare.sh caches downloaded binaries. Prepended to PATH
# below so every phase resolves the cached tools.
TOOLS_DIR="${TOOLS_DIR:-$CACHE_DIR/tools}"

# Ensure the tool cache is on PATH for every script that sources env.sh — not
# just prepare.sh — so auto-downloaded binaries are found by later phases.
# Guarded so repeated sourcing does not stack duplicate entries.
case ":$PATH:" in
  *":$TOOLS_DIR:"*) ;;
  *) export PATH="$TOOLS_DIR:$PATH" ;;
esac

# --- Timeouts (seconds) -----------------------------------------------------

DEV_CI_TIMEOUT="${DEV_CI_TIMEOUT:-90m}"
NODE_READY_TIMEOUT="${NODE_READY_TIMEOUT:-120}"
DNS_READY_TIMEOUT="${DNS_READY_TIMEOUT:-120}"
POD_READY_TIMEOUT="${POD_READY_TIMEOUT:-180}"
