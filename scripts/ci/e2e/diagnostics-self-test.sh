#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Cluster-free contract test for failure artifact collection and cleanup order.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT

mkdir -p "$test_root/bin" "$test_root/repo"
cat >"$test_root/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  cluster-info)
    exit 0
    ;;
  get)
    if [[ "${2:-}" == "--raw" ]]; then
      printf '{"request":"%s"}\n' "${3:-}"
    elif [[ "${2:-}" == "pod" ]]; then
      printf 'slurm-login-0'
    elif [[ "${2:-}" == "deployment" ]]; then
      exit 0
    else
      printf 'get %s\n' "$*"
    fi
    ;;
  logs)
    printf 'logs %s\n' "$*"
    ;;
  exec)
    printf 'exec %s\n' "$*"
    ;;
  *)
    printf 'unexpected kubectl invocation: %s\n' "$*" >&2
    exit 1
    ;;
esac
EOF
chmod +x "$test_root/bin/kubectl"

export PATH="$test_root/bin:$PATH"
export REPO_ROOT="$test_root/repo"
export E2E_PROFILE="advisor-act"
export E2E_LOG_BACKEND="victorialogs"
export E2E_STACK_NAMESPACE="slurm"
export E2E_STACK_DEPLOYMENTS="nvidia-recovery-job-manager slurm-advisor-controller"
export E2E_RUN_STARTED_AT="2026-08-04T00:00:00Z"
export E2E_XUNIT_PATH="$test_root/reports/e2e-real-fact/advisor-act/junit.xml"

# shellcheck source=diagnostics.sh
source "$SCRIPT_DIR/diagnostics.sh"

output="$(collect_failure_diagnostics)"
[[ -z "$output" ]] || {
  printf 'error: diagnostics polluted stdout: %s\n' "$output" >&2
  exit 1
}

diagnostics_dir="$test_root/reports/e2e-real-fact/advisor-act/diagnostics"
for artifact in \
  collector.log pods.txt events.txt jobs.json advice.json \
  nvidia-recovery-job-manager.log slurm-advisor-controller.log nvidia-recovery-api-gateway.log \
  otel-collector.log tempo.log squeue.json sacct.json; do
  [[ -f "$diagnostics_dir/$artifact" ]] || {
    printf 'error: missing diagnostic artifact %s\n' "$artifact" >&2
    exit 1
  }
done
grep -F '/jobs?limit=1000' "$diagnostics_dir/jobs.json" >/dev/null
grep -F 'sacct -X --duplicates --json' "$diagnostics_dir/sacct.json" >/dev/null
grep -F -- '--since-time=2026-08-04T00:00:00Z' "$diagnostics_dir/slurm-advisor-controller.log" >/dev/null

unset E2E_XUNIT_PATH
collect_failure_diagnostics
[[ -f "$test_root/repo/reports/e2e/advisor-act-victorialogs/job_state/diagnostics/jobs.json" ]] || {
  printf 'error: default E2E report tree did not receive diagnostics\n' >&2
  exit 1
}

# Source the harness without executing main, replace its external actions, and
# verify that a failing cleanup captures artifacts before teardown while
# preserving the original status.
# shellcheck source=e2e.sh
source "$SCRIPT_DIR/e2e.sh"
events="$test_root/cleanup-events"
collect_failure_diagnostics() { printf 'collect\n' >>"$events"; }
cluster_teardown() { printf 'teardown\n' >>"$events"; }
banner() { :; }
warn() { :; }

# A failure before cluster setup must not delete preexisting resources.
unset E2E_CLUSTER_SETUP_STARTED
set +e
false
_cleanup
cleanup_status=$?
set -e
[[ "$cleanup_status" -eq 1 && "$(cat "$events")" == "collect" ]] || {
  printf 'error: preflight failure must collect evidence without teardown\n' >&2
  exit 1
}

: >"$events"
E2E_CLUSTER_SETUP_STARTED=true
set +e
false
_cleanup
cleanup_status=$?
set -e
[[ "$cleanup_status" -eq 1 ]] || {
  printf 'error: cleanup replaced failure status with %s\n' "$cleanup_status" >&2
  exit 1
}
[[ "$(cat "$events")" == $'collect\nteardown' ]] || {
  printf 'error: failure cleanup order was:\n%s\n' "$(cat "$events")" >&2
  exit 1
}

: >"$events"
_cleanup
[[ $? -eq 0 && "$(cat "$events")" == "teardown" ]] || {
  printf 'error: successful cleanup unexpectedly collected failure diagnostics\n' >&2
  exit 1
}

printf 'ok: E2E failure diagnostics are silent, ordered, and artifact-backed\n'
