#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: deploy/helm/scripts/check-charts.sh [--self-test] [FILES...]

Runs helm lint and helm template for Helm charts affected by FILES.
Log-query consumer charts use explicit provider values from the checked-in
lint fixture so required connection configuration is validated.

FILES should be repository-relative paths, as passed by pre-commit. Paths under
deploy/helm/<chart>/ are mapped to deploy/helm/<chart> when that directory has
a Chart.yaml file.

--self-test  run built-in regression fixtures
USAGE
}

error() {
  printf 'error: %s\n' "$*" >&2
}

charts_for_files() {
  local path
  local chart

  for path in "$@"; do
    case "$path" in
      deploy/helm/scripts/fixtures/umbrella-provider-values.yaml)
        printf '%s\n' deploy/helm/nvidia-recovery
        ;;
      deploy/helm/scripts/fixtures/attribution-provider-values.yaml)
        printf '%s\n' deploy/helm/attribution-adapter
        ;;
      deploy/helm/scripts/fixtures/log-query-provider-values.yaml)
        printf '%s\n' deploy/helm/watcher deploy/helm/nvidia-recovery
        ;;
      deploy/helm/*/*)
        chart="${path#deploy/helm/}"
        chart="${chart%%/*}"
        if [[ -f "deploy/helm/$chart/Chart.yaml" ]]; then
          printf 'deploy/helm/%s\n' "$chart"
        fi
        ;;
    esac
  done | sort -u
}

values_for_chart() {
  case "$1" in
    deploy/helm/attribution-adapter)
      printf '%s\n' deploy/helm/scripts/fixtures/attribution-provider-values.yaml
      ;;
    deploy/helm/nvidia-recovery)
      printf '%s\n' deploy/helm/scripts/fixtures/umbrella-provider-values.yaml
      ;;
    deploy/helm/watcher)
      printf '%s\n' deploy/helm/scripts/fixtures/log-query-provider-values.yaml
      ;;
  esac
}

check_charts() {
  local chart
  local charts_file

  charts_file="$(mktemp)"
  trap 'rm -f "$charts_file"' RETURN

  charts_for_files "$@" > "$charts_file"

  if [[ ! -s "$charts_file" ]]; then
    echo "ok: no Helm charts to check"
    return 0
  fi

  if ! command -v helm >/dev/null 2>&1; then
    error "helm is required when Helm chart files are checked"
    echo "changed charts:" >&2
    sed 's/^/- /' "$charts_file" >&2
    return 1
  fi

  while IFS= read -r chart; do
    [[ -z "$chart" ]] && continue
    local release_name
    local values_file
    release_name="heimdall-preflight-$(basename "$chart" .tgz)"
    values_file="$(values_for_chart "$chart")"
    echo "Checking Helm chart: $chart"
    if [[ -n "$values_file" ]]; then
      helm lint "$chart" --values "$values_file"
      helm template "$release_name" "$chart" --values "$values_file" >/dev/null
    else
      helm lint "$chart"
      helm template "$release_name" "$chart" >/dev/null
    fi
    echo "ok: $chart"
  done < "$charts_file"
}

run_case() {
  local name="$1"
  local expected="$2"
  shift 2

  local actual
  if "$@" >/dev/null 2>&1; then
    actual="pass"
  else
    actual="fail"
  fi

  if [[ "$actual" != "$expected" ]]; then
    error "self-test '$name' expected $expected but got $actual"
    return 1
  fi

  printf 'ok: %s (%s)\n' "$name" "$expected"
}

write_fake_helm() {
  local bin_dir="$1"

  mkdir -p "$bin_dir"
  cat > "$bin_dir/helm" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$HELM_CHART_CHECK_LOG"
case "${1:-}" in
  lint|template)
    exit 0
    ;;
  *)
    exit 2
    ;;
esac
EOF
  chmod +x "$bin_dir/helm"
}

write_fixture_chart() {
  local root="$1"
  local chart_name="${2:-api-gateway}"
  local chart="$root/deploy/helm/$chart_name"

  mkdir -p "$chart/templates"
  cat > "$chart/Chart.yaml" <<EOF
apiVersion: v2
name: $chart_name
version: 0.1.0
EOF
  cat > "$chart/values.yaml" <<'EOF'
replicaCount: 1
EOF
  cat > "$chart/templates/configmap.yaml" <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-config
EOF
}

self_test() {
  local script_path
  script_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/check-charts.sh"

  local tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN

  local repo="$tmp/repo"
  local fake_bin="$tmp/fake-bin"
  local log="$tmp/helm.log"

  mkdir -p "$repo"
  write_fixture_chart "$repo"
  write_fake_helm "$fake_bin"

  : > "$log"
  run_case "chart file runs helm lint and template" "pass" \
    bash -c 'cd "$1" && HELM_CHART_CHECK_LOG="$2" PATH="$3:$PATH" "$4" deploy/helm/api-gateway/values.yaml' \
    _ "$repo" "$log" "$fake_bin" "$script_path"
  local expected_log
  expected_log=$'lint deploy/helm/api-gateway\ntemplate heimdall-preflight-api-gateway deploy/helm/api-gateway'
  local actual_log
  actual_log="$(cat "$log")"
  if [[ "$actual_log" != "$expected_log" ]]; then
    error "chart file did not invoke expected Helm commands"
    printf 'expected:\n%s\nactual:\n%s\n' "$expected_log" "$actual_log" >&2
    return 1
  fi
  echo "ok: chart file invoked expected Helm commands"

  : > "$log"
  run_case "duplicate chart files run once" "pass" \
    bash -c 'cd "$1" && HELM_CHART_CHECK_LOG="$2" PATH="$3:$PATH" "$4" deploy/helm/api-gateway/values.yaml deploy/helm/api-gateway/templates/configmap.yaml' \
    _ "$repo" "$log" "$fake_bin" "$script_path"
  actual_log="$(cat "$log")"
  if [[ "$actual_log" != "$expected_log" ]]; then
    error "duplicate chart files should run Helm once per chart"
    printf 'expected:\n%s\nactual:\n%s\n' "$expected_log" "$actual_log" >&2
    return 1
  fi
  echo "ok: duplicate chart files run once"

  local provider_values="deploy/helm/scripts/fixtures/log-query-provider-values.yaml"
  local consumer
  for consumer in attribution-adapter watcher nvidia-recovery; do
    write_fixture_chart "$repo" "$consumer"
    : > "$log"
    run_case "$consumer uses explicit provider values" "pass" \
      bash -c 'cd "$1" && HELM_CHART_CHECK_LOG="$2" PATH="$3:$PATH" "$4" "$5"' \
      _ "$repo" "$log" "$fake_bin" "$script_path" "deploy/helm/$consumer/values.yaml"
    provider_values="$(values_for_chart "deploy/helm/$consumer")"
    expected_log="$(printf 'lint deploy/helm/%s --values %s\ntemplate heimdall-preflight-%s deploy/helm/%s --values %s' \
      "$consumer" "$provider_values" "$consumer" "$consumer" "$provider_values")"
    actual_log="$(cat "$log")"
    if [[ "$actual_log" != "$expected_log" ]]; then
      error "$consumer did not use explicit provider values for both Helm commands"
      printf 'expected:\n%s\nactual:\n%s\n' "$expected_log" "$actual_log" >&2
      return 1
    fi
  done

  : > "$log"
  run_case "provider fixtures check their consumers" "pass" \
    bash -c 'cd "$1" && HELM_CHART_CHECK_LOG="$2" PATH="$3:$PATH" "$4" "$5" "$6"' \
    _ "$repo" "$log" "$fake_bin" "$script_path" \
    deploy/helm/scripts/fixtures/log-query-provider-values.yaml \
    deploy/helm/scripts/fixtures/attribution-provider-values.yaml
  for consumer in attribution-adapter watcher nvidia-recovery; do
    provider_values="$(values_for_chart "deploy/helm/$consumer")"
    if ! grep -Fx "lint deploy/helm/$consumer --values $provider_values" "$log" >/dev/null; then
      error "provider fixture did not check $consumer"
      return 1
    fi
  done

  : > "$log"
  run_case "non-chart helm path skips helm" "pass" \
    bash -c 'cd "$1" && HELM_CHART_CHECK_LOG="$2" PATH="$3:$PATH" "$4" deploy/helm/README.md' \
    _ "$repo" "$log" "$fake_bin" "$script_path"
  if [[ -s "$log" ]]; then
    error "non-chart Helm path should not invoke Helm"
    cat "$log" >&2
    return 1
  fi
  echo "ok: non-chart Helm path skipped Helm"

  local no_helm_bin="$tmp/no-helm-bin"
  mkdir -p "$no_helm_bin"
  for tool in bash basename mktemp rm sed sort; do
    ln -s "$(command -v "$tool")" "$no_helm_bin/$tool"
  done
  run_case "chart file without helm fails" "fail" \
    bash -c 'cd "$1" && PATH="$2" bash "$3" deploy/helm/api-gateway/values.yaml' \
    _ "$repo" "$no_helm_bin" "$script_path"
}

if [[ "${1:-}" == "--self-test" ]]; then
  shift
  if [[ "$#" -ne 0 ]]; then
    error "--self-test does not accept file arguments"
    usage >&2
    exit 2
  fi
  self_test
  exit 0
fi

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

check_charts "$@"
