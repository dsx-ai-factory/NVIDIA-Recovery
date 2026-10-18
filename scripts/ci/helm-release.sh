#!/bin/sh

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

chart_dir="${HELM_CHART_DIR:-deploy/helm/nvidia-recovery}"
output_dir="${HELM_OUTPUT_DIR:-dist/helm}"
chart_name="nvidia-recovery"

fail() {
  echo "error: $*" >&2
  exit 1
}

usage() {
  echo "usage: $0 package <version>" >&2
  echo "       $0 publish [--skip-existing] <registry> <oci-repository> <username>" >&2
  exit 2
}

package_chart() {
  [ "$#" -eq 1 ] || usage

  version="${1#v}"
  app_version="v${version}"

  mkdir -p "$output_dir"
  [ -f "$chart_dir/Chart.lock" ] || fail "Chart.lock is required; update dependencies in a reviewed change"
  # dependency build requires the HTTP repositories even with an existing lock.
  # Register them for clean release and nightly runners.
  helm repo add grafana https://grafana.github.io/helm-charts
  helm repo add grafana-community https://grafana-community.github.io/helm-charts
  helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
  helm dependency build "$chart_dir"
  helm package "$chart_dir" \
    --version "$version" \
    --app-version "$app_version" \
    --destination "$output_dir"

  archive="$output_dir/${chart_name}-${version}.tgz"
  [ -f "$archive" ] || fail "packaged chart is missing: $archive"
  helm lint "$archive" --values deploy/helm/scripts/fixtures/umbrella-provider-values.yaml
  helm template release-check "$archive" --values deploy/helm/scripts/fixtures/umbrella-provider-values.yaml >/dev/null
  helm show chart "$archive" >"$output_dir/Chart.yaml"
  printf '%s\n' "$app_version" >"$output_dir/app-version"
  printf '%s\n' "$version" >"$output_dir/version"
}

publish_chart() {
  skip_existing=false
  if [ "${1:-}" = "--skip-existing" ]; then
    skip_existing=true
    shift
  fi

  [ "$#" -eq 3 ] || usage

  registry="$1"
  repository="$2"
  username="$3"
  password="${HELM_REGISTRY_PASSWORD:-}"

  [ -n "$password" ] || fail "HELM_REGISTRY_PASSWORD is required"
  [ -f "$output_dir/version" ] || fail "package version is missing"

  version="$(cat "$output_dir/version")"
  archive="$output_dir/${chart_name}-${version}.tgz"
  reference="$repository/$chart_name"
  [ -f "$archive" ] || fail "package archive is missing: $archive"

  printf '%s' "$password" | helm registry login "$registry" \
    --username "$username" \
    --password-stdin

  if helm show chart "$reference" --version "$version" >/dev/null 2>&1; then
    if [ "$skip_existing" = true ]; then
      echo "$reference:$version already exists; skipping"
      return 0
    fi
    fail "$reference:$version already exists"
  fi

  helm push "$archive" "$repository"
  helm registry logout "$registry" >/dev/null 2>&1 || true
}

command="${1:-}"
[ "$#" -gt 0 ] && shift

case "$command" in
  package) package_chart "$@" ;;
  publish) publish_chart "$@" ;;
  *) usage ;;
esac
