#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

helm repo list | grep -q '^grafana[[:space:]]' \
  || helm repo add grafana https://grafana.github.io/helm-charts >/dev/null
helm repo list | grep -q '^open-telemetry[[:space:]]' \
  || helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null
# Bundled Loki sub-chart (grafana-community, not grafana — the OSS Loki chart
# moved repos ~March 2026). dependency update resolves every dependency
# regardless of its condition, so the repo is needed even when the bundled
# Loki is disabled.
helm repo list | grep -q '^grafana-community[[:space:]]' \
  || helm repo add grafana-community https://grafana-community.github.io/helm-charts >/dev/null

# Optional mirror/proxy for the NVIDIA Recovery chart's Bitnami OCI dependencies
# (rabbitmq, mysql). Docker Hub's anonymous pull rate limit can throttle or
# fail dependency update with "429 Too Many Requests" when resolving
# oci://registry-1.docker.io/bitnamicharts. When set, resolve dependencies in
# a temporary copy of the charts with that repository rewritten, and copy back
# only the fetched charts/ directory. The tracked Chart.yaml and Chart.lock
# never change, so Tilt's watch on Chart.yaml does not fire, an interrupted
# run leaves the tree clean, and the mirror's host never lands in the lock.
chart_dir="deploy/helm/nvidia-recovery"

# Replace the chart's charts/ with the contents of $1, keeping the current
# charts/ if the copy fails.
replace_charts() {
  local staged
  staged="$(mktemp -d "${chart_dir}.charts.XXXXXX")"
  if ! cp -R "$1/." "${staged}/"; then
    rm -rf "${staged}"
    echo "error: could not copy mirrored dependencies; kept the existing ${chart_dir}/charts" >&2
    return 1
  fi
  rm -rf "${chart_dir}/charts"
  mv "${staged}" "${chart_dir}/charts"
}

update_from_mirror() {
  local mirror
  work="$(mktemp -d)"
  trap 'rm -rf "${work}"' EXIT
  cp -R deploy/helm "${work}/helm"
  mirror="$(printf '%s' "$1" | sed 's/[&#\\]/\\&/g')"
  sed -i.bak "s#oci://registry-1\.docker\.io/bitnamicharts#${mirror}#g" "${work}/helm/nvidia-recovery/Chart.yaml"
  helm dependency update --skip-refresh "${work}/helm/nvidia-recovery"
  replace_charts "${work}/helm/nvidia-recovery/charts"
}

if [[ -n "${BITNAMI_OCI_REGISTRY_MIRROR:-}" ]]; then
  echo "Resolving ${chart_dir} Bitnami dependencies through BITNAMI_OCI_REGISTRY_MIRROR"
  update_from_mirror "${BITNAMI_OCI_REGISTRY_MIRROR}"
else
  echo "Resolving ${chart_dir} Bitnami dependencies from Docker Hub (BITNAMI_OCI_REGISTRY_MIRROR is unset)"
  helm dependency update --skip-refresh "${chart_dir}"
fi
