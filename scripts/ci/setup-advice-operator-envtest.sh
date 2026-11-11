#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Print the envtest binary directory; never select a user's Kubernetes cluster.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
ci_require_command go
ci_load_go_tool_versions

if [[ -n "${KUBEBUILDER_ASSETS:-}" ]]; then
    for binary in etcd kube-apiserver kubectl; do
        if [[ ! -x "$KUBEBUILDER_ASSETS/$binary" ]]; then
            echo "missing envtest executable: $KUBEBUILDER_ASSETS/$binary" >&2
            exit 1
        fi
    done
    printf '%s\n' "$KUBEBUILDER_ASSETS"
else
    go run "sigs.k8s.io/controller-runtime/tools/setup-envtest@$SETUP_ENVTEST_VERSION" \
        use --bin-dir "$PWD/.cache/envtest" -p path "$ENVTEST_K8S_VERSION"
fi
