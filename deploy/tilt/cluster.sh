#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

export CLUSTER_NAME="${CLUSTER_NAME:-heimdall-e2e-dev}"

source "$repo_root/scripts/ci/e2e/prepare.sh"
source "$repo_root/scripts/ci/e2e/cluster.sh"

# env.sh resolves this default before Kind is created. Tilt must inherit the
# exact same path so its hostPath PersistentVolumes refer to the directory that
# cluster.sh mounted into every node.
export SLURM_SHARED_HOME
export E2E_LOG_BACKEND

case "${1:-}" in
  up)
    shift
    (( $# > 0 )) || { echo "usage: $0 up COMMAND [ARG ...]" >&2; exit 2; }
    prepare
    cluster_up
    exec "$@"
    ;;
  down)
    tilt down || true
    KEEP_CLUSTER='' cluster_teardown
    ;;
  *)
    echo "usage: $0 {up COMMAND [ARG ...]|down}" >&2
    exit 2
    ;;
esac
