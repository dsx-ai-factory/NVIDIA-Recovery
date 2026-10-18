#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# 01-install-prometheus.sh — install kube-prometheus-stack into the cluster.
#
# Run by provision.sh. Idempotent via `helm upgrade --install`. Values are
# rendered from values/prometheus.yaml (placeholders substituted from env.sh).
# The GPU (DCGM) scrape overlay is applied only when DEPLOY_GPU_SCRAPE is set.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../env.sh"
source "$SCRIPT_DIR/../lib.sh"

banner "Service: kube-prometheus-stack ${PROMETHEUS_CHART_VERSION}"

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update >/dev/null
helm repo update prometheus-community >/dev/null

# Render placeholders into temp values files (cleaned up on exit).
rendered="$(mktemp)"
gpu_rendered="$(mktemp)"
trap 'rm -f "$rendered" "$gpu_rendered"' EXIT
sed "s|__CLUSTER_LABEL__|${CLUSTER_LABEL}|g" "$SCRIPT_DIR/values/prometheus.yaml" > "$rendered"

helm_args=(
  upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack
  --namespace monitoring --create-namespace
  --version "$PROMETHEUS_CHART_VERSION"
  --values "$rendered"
  --cleanup-on-fail
  --wait --timeout "${SERVICE_WAIT_TIMEOUT}s"
)

if enabled "$DEPLOY_GPU_SCRAPE"; then
  log "DEPLOY_GPU_SCRAPE set — applying the GPU (DCGM) scrape overlay"
  sed "s|__CLUSTER_LABEL__|${CLUSTER_LABEL}|g" "$SCRIPT_DIR/values/prometheus-gpu.yaml" > "$gpu_rendered"
  helm_args+=(--values "$gpu_rendered")
fi

helm "${helm_args[@]}"
success "kube-prometheus-stack installed (namespace: monitoring)"
