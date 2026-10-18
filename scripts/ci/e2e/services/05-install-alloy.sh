#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# 05-install-alloy.sh — install Grafana Alloy to ship Slurm logs to the selected store.
#
# Run by provision.sh. Loads the fixed local-E2E pipeline config
# (values/alloy-config.river) into a ConfigMap and installs the chart
# (values/alloy.yaml). The `/home/log-links` path and `slurm` cluster label
# match the Advisor, mounts, local Watcher, and attribution selectors. No
# service-account identity is fixed. Idempotent (ConfigMap via apply, chart via
# `helm upgrade --install`).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../env.sh"
source "$SCRIPT_DIR/../lib.sh"

banner "Service: grafana/alloy ${ALLOY_CHART_VERSION}"

helm repo add grafana https://grafana.github.io/helm-charts --force-update >/dev/null
helm repo update grafana >/dev/null

# Render the same destination and collection pipeline used by Tilt.
alloy_config="$(mktemp)"
trap 'rm -f "$alloy_config"' EXIT
"$REPO_ROOT/deploy/tilt/apply-alloy-config.sh" \
  "$SCRIPT_DIR/values/alloy-config.river" "$alloy_config" "$E2E_LOG_BACKEND"

helm upgrade --install alloy grafana/alloy \
  --namespace slurm \
  --version "$ALLOY_CHART_VERSION" \
  --values "$SCRIPT_DIR/values/alloy.yaml" \
  --cleanup-on-fail \
  --wait --timeout "${SERVICE_WAIT_TIMEOUT}s"

# The completed node-only Helm rollout drains the old mixed readers before
# the singleton starts. Shared application offsets from ephemeral old pods
# are not automatically migrated; a first upgrade may replay retained files.

"$REPO_ROOT/deploy/tilt/apply-alloy-config.sh" \
  "$SCRIPT_DIR/values/alloy-application-config.river" "$alloy_config" "$E2E_LOG_BACKEND" alloy-application-config
helm upgrade --install alloy-application grafana/alloy \
  --namespace slurm \
  --version "$ALLOY_CHART_VERSION" \
  --values "$SCRIPT_DIR/values/alloy.yaml" \
  --values "$SCRIPT_DIR/values/alloy-application.yaml" \
  --cleanup-on-fail \
  --wait --timeout "${SERVICE_WAIT_TIMEOUT}s"

success "node syslog and singleton application collectors installed (namespace: slurm)"
