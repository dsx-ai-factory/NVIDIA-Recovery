#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Fail when the Slurm Advisor Controller chart's OTLP environment helper
# renders differently from the umbrella chart's shared heimdall.otelEnv.
#
# ADR 0012 makes the umbrella helper the one OTLP contract. The controller
# chart keeps a copy because it is also installed standalone (for example in a
# separate Slinky namespace), where the umbrella helper does not exist. Both
# helpers are rendered from the same global.otelcollector values in a scratch
# chart, so the check compares the resulting environment, not the source text.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: deploy/helm/scripts/check-otel-env-parity.sh

Run from the repository root. Requires helm.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

umbrella_helpers="deploy/helm/nvidia-recovery/templates/_helpers.tpl"
controller_helpers="deploy/helm/slurm-advisor-controller/templates/_helpers.tpl"

if ! command -v helm >/dev/null 2>&1; then
  printf 'error: helm is required to compare OTLP environment helpers\n' >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
# One scratch chart per helper: Helm renders every template even with
# --show-only, so a failure in one helper would otherwise mask the other.
for helper in umbrella:heimdall.otelEnv controller:slurm-advisor-controller.otelEnv; do
  chart="$work/${helper%%:*}"
  mkdir -p "$chart/templates"
  printf 'apiVersion: v2\nname: otel-env-parity\nversion: 0.1.0\n' > "$chart/Chart.yaml"
  cp "$umbrella_helpers" "$chart/templates/_umbrella.tpl"
  cp "$controller_helpers" "$chart/templates/_controller.tpl"
  # Helm parses rendered templates as Kubernetes objects, so wrap the
  # environment list in the same minimal object for both helpers.
  cat > "$chart/templates/env.yaml" <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: parity
env:
  {{- include "${helper#*:}" . | nindent 2 }}
EOF
done

# Prints the helper's rendered environment, or "render failed" when Helm
# rejects the values (for example a missing required field).
render() {
  local helper="$1"
  local values="$2"
  if ! helm template parity "$work/$helper" --values "$values" 2>/dev/null | sed '/^# Source:/d; /^---$/d'; then
    printf 'render failed\n'
  fi
}

# Each case is a global.otelcollector configuration an operator can set.
cat > "$work/disabled.yaml" <<'EOF'
global:
  otelcollector:
    enabled: false
EOF
cat > "$work/unauthenticated.yaml" <<'EOF'
global:
  otelcollector:
    enabled: true
    endpoint: http://collector:4317
    protocol: grpc
EOF
cat > "$work/authenticated.yaml" <<'EOF'
global:
  otelcollector:
    enabled: true
    endpoint: https://collector.example:4318
    protocol: http/protobuf
    auth:
      enabled: true
      secretName: otlp
      secretKey: token
EOF
cat > "$work/missing-endpoint.yaml" <<'EOF'
global:
  otelcollector:
    enabled: true
    protocol: grpc
EOF
cat > "$work/missing-secret.yaml" <<'EOF'
global:
  otelcollector:
    enabled: true
    endpoint: http://collector:4317
    protocol: grpc
    auth:
      enabled: true
EOF

status=0
for case_file in "$work"/*.yaml; do
  case_name="$(basename "$case_file" .yaml)"
  umbrella="$(render umbrella "$case_file")"
  controller="$(render controller "$case_file")"
  # A broken scratch chart would make every case fail identically.
  if [[ "$case_name" == *authenticated && "$umbrella" == "render failed" ]]; then
    printf 'error: heimdall.otelEnv did not render valid %s values\n' "$case_name" >&2
    status=1
  fi
  if [[ "$umbrella" != "$controller" ]]; then
    printf 'error: OTLP environment differs for %s values\n' "$case_name" >&2
    diff <(printf '%s\n' "$umbrella") <(printf '%s\n' "$controller") \
      --label "heimdall.otelEnv ($umbrella_helpers)" \
      --label "slurm-advisor-controller.otelEnv ($controller_helpers)" >&2 || true
    status=1
  fi
done

if (( status == 0 )); then
  echo "ok: slurm-advisor-controller.otelEnv matches heimdall.otelEnv"
fi
exit "$status"
