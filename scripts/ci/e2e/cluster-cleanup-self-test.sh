#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Exercise registry ownership and teardown without a Docker daemon or cluster.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
export CLUSTER_NAME=cleanup-test KIND_REGISTRY_NAME=cleanup-test-registry
export DEPLOY_KIND_CLUSTER=true KEEP_CLUSTER=
export KUBECONFIG="$test_root/kubeconfig"
# shellcheck source=cluster.sh
source "$SCRIPT_DIR/cluster.sh"
KUBECONFIG="$test_root/kubeconfig"

commands="$test_root/commands"
registry_exists=true
registry_owner="$CLUSTER_NAME"
delete_status=0

# Reject every external operation except the inspected lifecycle interface.
docker() {
  case "$1" in
    inspect)
      [[ "$registry_exists" == true ]] || return 1
      if [[ "${2:-}" == --format ]]; then printf '%s' "$registry_owner"; fi
      ;;
    run|rm) printf 'docker %s\n' "$*" >>"$commands" ;;
    *) echo "unexpected Docker operation: $*" >&2; return 99 ;;
  esac
}
kind() {
  if [[ "$*" == 'get clusters' ]]; then
    printf '%s\n' "$CLUSTER_NAME"
    return 0
  fi
  [[ "$*" == "delete cluster --name $CLUSTER_NAME" ]] || return 99
  printf 'kind %s\n' "$*" >>"$commands"
  return "$delete_status"
}

# Invalid configuration must preserve an existing same-name cluster. These
# cases previously deleted it before reporting the configuration error.
touch "$KUBECONFIG"
printf 'kind: Cluster\n' >"$test_root/kind.yaml"
for invalid in shared-home missing-config mirror-config; do
  : >"$commands"
  if (
    E2E_CLUSTER_SETUP_STARTED=false
    trap '[[ "$E2E_CLUSTER_SETUP_STARTED" == false ]] || cluster_teardown' EXIT
    SLURM_SHARED_HOME="$test_root/slurm-home"
    KIND_CONFIG="$test_root/kind.yaml"
    DOCKER_HUB_REGISTRY_MIRROR=''
    case "$invalid" in
      shared-home) SLURM_SHARED_HOME=/ ;;
      missing-config) KIND_CONFIG="$test_root/missing.yaml" ;;
      mirror-config) DOCKER_HUB_REGISTRY_MIRROR=mirror.example.com ;;
    esac
    _create_kind_cluster
  ); then
    echo "accepted invalid configuration: $invalid" >&2
    exit 1
  fi
  [[ -f "$KUBECONFIG" && ! -s "$commands" ]]
done

# Only registries created here receive the ownership label.
: >"$commands"
registry_exists=false
_start_registry
grep -F -- "--label nvidia-recovery.cluster=$CLUSTER_NAME" "$commands" >/dev/null
registry_exists=true

# Reusing another cluster's owned registry must fail without mutation.
: >"$commands"
registry_owner=other-cluster
if (_start_registry); then echo 'accepted another cluster registry' >&2; exit 1; fi
[[ ! -s "$commands" ]]

# Legacy/shared registries remain usable and survive selected-cluster teardown.
for registry_owner in '' other-cluster; do
  : >"$commands"
  touch "$KUBECONFIG"
  if [[ -z "$registry_owner" ]]; then _start_registry; fi
  cluster_teardown
  [[ ! -e "$KUBECONFIG" ]]
  [[ "$(cat "$commands")" == "kind delete cluster --name $CLUSTER_NAME" ]]
done

# Owned registry cleanup includes its anonymous volumes.
: >"$commands"
registry_owner="$CLUSTER_NAME"
touch "$KUBECONFIG"
cluster_teardown
[[ ! -e "$KUBECONFIG" ]]
grep -Fx "docker rm -fv $KIND_REGISTRY_NAME" "$commands" >/dev/null

# A failed delete retains both access and registry data for diagnosis.
: >"$commands"
touch "$KUBECONFIG"
delete_status=1
if cluster_teardown; then echo 'ignored failed cluster deletion' >&2; exit 1; fi
[[ -f "$KUBECONFIG" ]]
[[ "$(cat "$commands")" == "kind delete cluster --name $CLUSTER_NAME" ]]
delete_status=0

# Retained and externally supplied clusters trigger no destructive commands.
for mode in retained external; do
  : >"$commands"
  if [[ "$mode" == retained ]]; then
    KEEP_CLUSTER=1 cluster_teardown
  else
    DEPLOY_KIND_CLUSTER='' cluster_teardown
  fi
  [[ -f "$KUBECONFIG" && ! -s "$commands" ]]
done

printf 'ok: teardown preserves unowned resources and removes owned registry volumes\n'
