#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# 02-install-loki.sh — install the selected local log store into the cluster.
#
# Run by provision.sh. Idempotent via `helm upgrade --install`. No placeholders
# in the values, so they are passed through directly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../env.sh"
source "$SCRIPT_DIR/../lib.sh"

if [[ "$E2E_LOG_BACKEND" == "victorialogs" ]]; then
  kubectl apply -f "$REPO_ROOT/deploy/tilt/services/victorialogs.yaml"
  kubectl rollout status deployment/victorialogs --namespace monitoring --timeout="${SERVICE_WAIT_TIMEOUT}s"
  success "victorialogs installed (namespace: monitoring)"
  exit 0
fi

banner "Service: grafana/loki ${LOKI_CHART_VERSION}"

helm repo add grafana https://grafana.github.io/helm-charts --force-update >/dev/null
helm repo update grafana >/dev/null

helm upgrade --install loki grafana/loki \
  --namespace monitoring --create-namespace \
  --version "$LOKI_CHART_VERSION" \
  --values "$SCRIPT_DIR/values/loki.yaml" \
  --cleanup-on-fail \
  --wait --timeout "${SERVICE_WAIT_TIMEOUT}s"

success "loki installed (namespace: monitoring)"
