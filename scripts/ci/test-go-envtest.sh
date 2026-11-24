#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Runs the Kubernetes job controller's manager test against a real API server
# and etcd. setup-envtest downloads the pinned binaries into .cache/envtest on
# first use and verifies them against the published release checksums.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/lib.sh"
# shellcheck source=go-tool-versions.env
source "$SCRIPT_DIR/go-tool-versions.env"
: "${SETUP_ENVTEST_VERSION:?SETUP_ENVTEST_VERSION is required}"
: "${ENVTEST_K8S_VERSION:?ENVTEST_K8S_VERSION is required}"

ci_require_command go
tools_bin="${GO_TOOLS_BIN:-$REPO_ROOT/.cache/go-tools/bin}"
mkdir -p "$tools_bin"
ci_log "installing setup-envtest $SETUP_ENVTEST_VERSION"
GOBIN="$tools_bin" go install -p=1 \
    "sigs.k8s.io/controller-runtime/tools/setup-envtest@$SETUP_ENVTEST_VERSION"

# The test skips when KUBEBUILDER_ASSETS is empty, so a failed or empty
# resolution must stop here rather than let the run pass without an API server.
if ! assets="$("$tools_bin/setup-envtest" use "$ENVTEST_K8S_VERSION" --bin-dir "$REPO_ROOT/.cache/envtest" -p path)"; then
    echo "setup-envtest could not provide Kubernetes $ENVTEST_K8S_VERSION binaries" >&2
    exit 1
fi
if [[ -z "$assets" || ! -x "$assets/kube-apiserver" || ! -x "$assets/etcd" ]]; then
    echo "envtest binaries for Kubernetes $ENVTEST_K8S_VERSION are unavailable: '$assets'" >&2
    exit 1
fi
apiserver_version="$("$assets/kube-apiserver" --version)"
if [[ "$apiserver_version" != "Kubernetes v$ENVTEST_K8S_VERSION" ]]; then
    echo "expected Kubernetes v$ENVTEST_K8S_VERSION in $assets; found: $apiserver_version" >&2
    exit 1
fi

ci_log "testing kubernetes-job-controller against $apiserver_version"
cd "$REPO_ROOT/kubernetes-job-controller"
KUBEBUILDER_ASSETS="$assets" go test -count=1 -race -run '^TestManagerAgainstAPIServer$' .
