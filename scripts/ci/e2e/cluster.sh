#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# cluster.sh — Kind cluster lifecycle for the Heimdall e2e harness.
#
# Provides two functions consumed by e2e.sh:
#
#   cluster_up       — create the Kind cluster (or validate an existing
#                      KUBECONFIG when DEPLOY_KIND_CLUSTER is empty), then wait
#                      for nodes and core system pods to be Ready.
#
#   cluster_teardown — delete the Kind cluster and remove the kubeconfig
#                      artifact we created. Called by the EXIT trap in e2e.sh.
#
# When creating a cluster, kind writes the kubeconfig to $KUBECONFIG (set by
# env.sh) via --kubeconfig; ~/.kube/config is never read or modified.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
source "$SCRIPT_DIR/lib.sh"

# ---------------------------------------------------------------------------
# cluster_up
# ---------------------------------------------------------------------------
cluster_up() {
  banner "Phase: Cluster"

  if ! enabled "$DEPLOY_KIND_CLUSTER"; then
    _use_existing_kubeconfig
    return
  fi

  _start_registry
  _create_kind_cluster
  _connect_registry
  _configure_registry_hosts
  if [[ -n "${DOCKER_HUB_REGISTRY_MIRROR:-}" ]]; then
    _configure_docker_hub_mirror
  fi
  _wait_for_cluster
  _prepare_kind_alloy_positions
  # After _wait_for_cluster so the API server is known-ready: this is the only
  # registry step that talks to Kubernetes rather than to the nodes directly.
  _publish_registry_configmap
}

# ---------------------------------------------------------------------------
# _use_existing_kubeconfig
# Validates the caller-supplied KUBECONFIG and uses it as-is.
# ---------------------------------------------------------------------------
_use_existing_kubeconfig() {
  log "DEPLOY_KIND_CLUSTER is empty — using existing KUBECONFIG"

  # env.sh already guarantees KUBECONFIG is non-empty in this mode; verify the
  # file exists and the cluster is reachable before proceeding.
  if [[ ! -f "$KUBECONFIG" ]]; then
    error "KUBECONFIG file not found: $KUBECONFIG"
  fi

  log "Using kubeconfig: $KUBECONFIG"
  kubectl cluster-info --request-timeout=10s &>/dev/null \
    || error "Cannot reach the cluster at KUBECONFIG=$KUBECONFIG"
  success "Cluster reachable via provided KUBECONFIG"
  if enabled "${DEPLOY_INFRA:-}"; then
    local context
    context="$(kubectl config current-context)"
    if [[ "$context" == "kind-$CLUSTER_NAME" ]]; then
      _prepare_kind_alloy_positions
    else
      warn "Before installing Alloy on existing workers, provision /var/lib/alloy-system-positions as UID:GID 401:401 mode 0750."
      warn "Run on every Slurm worker: sudo install -d -o 401 -g 401 -m 0750 /var/lib/alloy-system-positions"
    fi
  fi
}

# Bootstrap only verified Kind containers, including an existing owned Kind
# cluster. External clusters keep operator ownership of their node filesystem.
_prepare_kind_alloy_positions() {
  local nodes node owner
  nodes="$(kubectl get nodes -l heimdall.nvidia.com/slurm-worker=true -o jsonpath='{.items[*].metadata.name}')" \
    || error "Cannot discover Slurm workers for Alloy positions bootstrap"
  [[ "$nodes" =~ [^[:space:]] ]] \
    || error "No Slurm workers found for Alloy positions bootstrap; check the heimdall.nvidia.com/slurm-worker=true label"
  # Verify every target before mutating any filesystem.
  for node in $nodes; do
    owner="$(docker inspect --format '{{ index .Config.Labels "io.x-k8s.kind.cluster" }}' "$node")" \
      || error "Cannot verify Kind ownership of worker $node; provision Alloy positions manually before retrying"
    [[ "$owner" == "$CLUSTER_NAME" ]] \
      || error "Refusing Alloy positions bootstrap on worker $node: expected Kind owner $CLUSTER_NAME, got $owner"
  done
  for node in $nodes; do
    docker exec "$node" install -d -o 401 -g 401 -m 0750 /var/lib/alloy-system-positions
  done
}

# ---------------------------------------------------------------------------
# _start_registry
# Starts a local Docker registry reachable by Kind nodes. Idempotent: if a
# container is unlabelled or belongs to this cluster it is reused. An unlabelled
# registry is external/shared and will not be removed by teardown.
# ---------------------------------------------------------------------------
_start_registry() {
  local name="${KIND_REGISTRY_NAME:-kind-registry}"
  local port="${KIND_REGISTRY_PORT:-5000}"

  if docker inspect "$name" &>/dev/null; then
    local owner
    owner="$(docker inspect --format '{{with .Config.Labels}}{{with index . "nvidia-recovery.cluster"}}{{.}}{{end}}{{end}}' "$name")" \
      || error "Could not inspect registry ownership: $name"
    if [[ -n "$owner" && "$owner" != "$CLUSTER_NAME" ]]; then
      error "Registry '$name' belongs to cluster '$owner'; select a different KIND_REGISTRY_NAME and KIND_REGISTRY_PORT"
    fi
    log "Local registry '$name' already running — reusing"
    return
  fi

  log "Starting local registry '$name' on port $port ..."
  docker run -d \
    --restart=always \
    --name "$name" \
    --label "nvidia-recovery.cluster=$CLUSTER_NAME" \
    -p "127.0.0.1:${port}:5000" \
    registry:2
  success "Local registry started"
}

# ---------------------------------------------------------------------------
# _connect_registry
# Connects the registry container to the Kind bridge network so every Kind
# node can reach it by name.
# ---------------------------------------------------------------------------
_connect_registry() {
  local name="${KIND_REGISTRY_NAME:-kind-registry}"

  if docker network inspect kind 2>/dev/null \
      | grep -q "\"Name\": \"${name}\""; then
    log "Registry '$name' already connected to Kind network"
    return
  fi

  log "Connecting registry '$name' to Kind network ..."
  docker network connect kind "$name"
  success "Registry connected"
}

# ---------------------------------------------------------------------------
# _configure_registry_hosts
# Installs a containerd certs.d drop-in on every node so images tagged
# <registry>:<port>/... resolve over plain HTTP to the local registry. Runs
# after cluster creation: containerd re-reads hosts.toml per pull, so no
# restart is required.
#
# The Kind config sets registry.config_path, which makes containerd ignore
# [registry.mirrors] outright, so this drop-in is the only thing that makes
# the local registry reachable.
# ---------------------------------------------------------------------------
_configure_registry_hosts() {
  local name="${KIND_REGISTRY_NAME:-kind-registry}"
  local port="${KIND_REGISTRY_PORT:-5000}"
  local registry_dir="/etc/containerd/certs.d/${name}:${port}"
  local node

  for node in $(kind get nodes --name "$CLUSTER_NAME"); do
    docker exec "$node" mkdir -p "$registry_dir" \
      || error "Could not create $registry_dir on node $node"

    # cp /dev/stdin avoids depending on a shell/heredoc inside the node image.
    if ! cat <<EOF | docker exec -i "$node" cp /dev/stdin "$registry_dir/hosts.toml"
[host."http://${name}:${port}"]
  capabilities = ["pull", "resolve"]
EOF
    then
      error "Could not write $registry_dir/hosts.toml on node $node"
    fi
  done

  success "Local registry endpoint configured on all nodes: ${name}:${port}"
}

# ---------------------------------------------------------------------------
# _publish_registry_configmap
# Advertises the local registry per KEP-1755. Without it Tilt reports no local
# registry and skips the direct-push path.
# ---------------------------------------------------------------------------
_publish_registry_configmap() {
  local name="${KIND_REGISTRY_NAME:-kind-registry}"
  local port="${KIND_REGISTRY_PORT:-5000}"

  kubectl apply -f - <<EOF >/dev/null \
    || error "Could not publish the local-registry-hosting ConfigMap"
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${port}"
    hostFromContainerRuntime: "${name}:${port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF

  success "Published local-registry-hosting ConfigMap (localhost:${port})"
}

# ---------------------------------------------------------------------------
# _configure_docker_hub_mirror
# Installs a containerd certs.d drop-in on every node so docker.io pulls go
# through DOCKER_HUB_REGISTRY_MIRROR, falling back to Docker Hub when the
# mirror does not serve the image. Runs after cluster creation: containerd
# re-reads hosts.toml per pull, so no restart is required.
#
# This is the upstream kind pattern, and the only one that works here — the
# Kind config sets registry.config_path, which makes containerd ignore
# [registry.mirrors] outright.
# ---------------------------------------------------------------------------
_configure_docker_hub_mirror() {
  local registry_dir="/etc/containerd/certs.d/docker.io"
  local node

  for node in $(kind get nodes --name "$CLUSTER_NAME"); do
    docker exec "$node" mkdir -p "$registry_dir" \
      || error "Could not create $registry_dir on node $node"

    # cp /dev/stdin avoids depending on a shell/heredoc inside the node image.
    if ! cat <<EOF | docker exec -i "$node" cp /dev/stdin "$registry_dir/hosts.toml"
server = "https://registry-1.docker.io"

[host."${DOCKER_HUB_REGISTRY_MIRROR}"]
  capabilities = ["pull", "resolve"]
EOF
    then
      error "Could not write $registry_dir/hosts.toml on node $node"
    fi
  done

  success "docker.io mirror configured on all nodes: $DOCKER_HUB_REGISTRY_MIRROR"
}

# ---------------------------------------------------------------------------
# _create_kind_cluster
# ---------------------------------------------------------------------------
_create_kind_cluster() {
  log "Creating Kind cluster '$CLUSTER_NAME' (arch: $ARCH)"

  case "$SLURM_SHARED_HOME" in
    "" | / | /tmp | /var | /var/lib)
      error "SLURM_SHARED_HOME must be an explicit child directory: $SLURM_SHARED_HOME"
      ;;
  esac
  mkdir -p -- "$SLURM_SHARED_HOME"

  local -a create_args=(create cluster --name "$CLUSTER_NAME" --kubeconfig "$KUBECONFIG")
  local rendered_kind_config=""

  # Preserve partially started containers when the caller explicitly asks to
  # keep the cluster. Kind can then export useful systemd and container logs
  # instead of deleting the only evidence of an early boot failure.
  if enabled "${KEEP_CLUSTER:-}"; then
    create_args+=(--retain)
  fi

  if [[ -n "$KIND_CONFIG" ]]; then
    [[ -f "$KIND_CONFIG" ]] || error "KIND_CONFIG file not found: $KIND_CONFIG"
    rendered_kind_config="$(mktemp)"
    sed "s|__SLURM_SHARED_HOME__|${SLURM_SHARED_HOME}|g" \
      "$KIND_CONFIG" > "$rendered_kind_config"
    create_args+=(--config "$rendered_kind_config")
    log "Using kind config template: $KIND_CONFIG"

    # certs.d drop-ins are only read when the config enables config_path, so a
    # mirror against a template without it would be silently ignored.
    if [[ -n "${DOCKER_HUB_REGISTRY_MIRROR:-}" ]] \
      && ! grep -q 'config_path' "$rendered_kind_config"; then
      error "DOCKER_HUB_REGISTRY_MIRROR is set but KIND_CONFIG ($KIND_CONFIG)" \
            "does not set registry config_path; the mirror would be ignored"
    fi
  elif [[ -n "${DOCKER_HUB_REGISTRY_MIRROR:-}" ]]; then
    error "DOCKER_HUB_REGISTRY_MIRROR requires a KIND_CONFIG that sets registry" \
          "config_path; Kind defaults do not enable certs.d"
  else
    log "No KIND_CONFIG set — using Kind defaults (single control-plane node)"
  fi

  # Reject invalid configuration before touching an existing same-name cluster.
  # Idempotent pre-clean removes a stale cluster only after those checks pass.
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    warn "Cluster '$CLUSTER_NAME' already exists — deleting before recreating"
    kind delete cluster --name "$CLUSTER_NAME" \
      || error "Could not remove the existing Kind cluster; preserving its access artifacts"
  fi

  # The EXIT trap may clean up only once creation of the replacement starts.
  # shellcheck disable=SC2034 # Consumed by e2e.sh after this sourced function.
  E2E_CLUSTER_SETUP_STARTED=true
  if ! kind "${create_args[@]}"; then
    [[ -z "$rendered_kind_config" ]] || rm -f -- "$rendered_kind_config"
    error "Kind cluster creation failed"
  fi
  [[ -z "$rendered_kind_config" ]] || rm -f -- "$rendered_kind_config"
  success "Kind cluster '$CLUSTER_NAME' created"
}

# ---------------------------------------------------------------------------
# _wait_for_cluster
# Blocks until nodes and core system pods are Ready.
# ---------------------------------------------------------------------------
_wait_for_cluster() {
  log "Waiting for nodes (timeout: ${NODE_READY_TIMEOUT}s) ..."
  kubectl wait node --all \
    --for=condition=Ready \
    --timeout="${NODE_READY_TIMEOUT}s" \
    || { dump_node_and_system_diagnostics; error "Nodes did not become Ready within ${NODE_READY_TIMEOUT}s"; }
  success "All nodes Ready"

  log "Waiting for kube-dns (timeout: ${DNS_READY_TIMEOUT}s) ..."
  kubectl wait pod \
    -n kube-system \
    -l k8s-app=kube-dns \
    --for=condition=Ready \
    --timeout="${DNS_READY_TIMEOUT}s" \
    || { dump_node_and_system_diagnostics; error "kube-dns did not become Ready within ${DNS_READY_TIMEOUT}s"; }
  success "kube-dns Ready"

  log "Waiting for remaining kube-system pods (timeout: ${POD_READY_TIMEOUT}s) ..."
  kubectl wait pod \
    -n kube-system \
    --all \
    --for=condition=Ready \
    --timeout="${POD_READY_TIMEOUT}s" \
    || { dump_node_and_system_diagnostics; warn "Some kube-system pods are not Ready (continuing)"; }
}

# ---------------------------------------------------------------------------
# cluster_teardown — called by the EXIT trap in e2e.sh
# ---------------------------------------------------------------------------
cluster_teardown() {
  # Only tear down what we created. When using an existing cluster, never
  # delete the cluster or the caller-supplied kubeconfig.
  enabled "$DEPLOY_KIND_CLUSTER" || return 0

  # KEEP_CLUSTER leaves the cluster up for inspection.
  if enabled "${KEEP_CLUSTER:-}"; then
    log "KEEP_CLUSTER set — leaving cluster '$CLUSTER_NAME' running for inspection"
    log "  Inspect:  KUBECONFIG=$KUBECONFIG kubectl get pods -A"
    log "  Teardown: kind delete cluster --name $CLUSTER_NAME"
    return 0
  fi

  log "Deleting Kind cluster '$CLUSTER_NAME' ..."
  if ! kind delete cluster --name "$CLUSTER_NAME"; then
    warn "Cluster delete failed — preserving kubeconfig and registry for diagnosis"
    return 1
  fi

  if [[ -n "${KUBECONFIG:-}" && -f "$KUBECONFIG" ]]; then
    rm -f "$KUBECONFIG"
    log "Removed kubeconfig artifact: $KUBECONFIG"
  fi

  local registry_name="${KIND_REGISTRY_NAME:-kind-registry}"
  if docker inspect "$registry_name" &>/dev/null; then
    local owner
    owner="$(docker inspect --format '{{with .Config.Labels}}{{with index . "nvidia-recovery.cluster"}}{{.}}{{end}}{{end}}' "$registry_name")"
    if [[ "$owner" == "$CLUSTER_NAME" ]]; then
      log "Removing owned local registry '$registry_name' and its anonymous volumes ..."
      docker rm -fv "$registry_name" || warn "Registry removal failed"
    else
      log "Preserving registry '$registry_name': not owned by cluster '$CLUSTER_NAME'"
    fi
  fi
}
