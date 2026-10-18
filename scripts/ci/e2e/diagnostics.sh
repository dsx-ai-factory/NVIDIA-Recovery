#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Failure-only diagnostics for the Kind E2E harness. The public entry point
# writes directly into the existing GitHub artifact tree and emits nothing to
# the caller's stdout or stderr.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

_e2e_diagnostic_report_directory() {
  local report
  report="${E2E_XUNIT_PATH:-$REPO_ROOT/reports/e2e/${E2E_PROFILE}-${E2E_LOG_BACKEND}/job_state/junit.xml}"
  if [[ "$report" != /* ]]; then
    report="$REPO_ROOT/$report"
  fi
  dirname "$report"
}

_e2e_capture_diagnostic() {
  local output="$1"
  shift

  if ! "$@" >"$output" 2>&1; then
    printf 'diagnostic command failed; inspect %s\n' "$(basename "$output")"
  fi
}

_e2e_collect_deployment_logs() {
  local diagnostics_dir="$1"
  local namespace="$2"
  local since_time="$3"
  local deployment
  local seen=" "

  # E2E_STACK_DEPLOYMENTS owns the recovery services. The API gateway is also
  # useful for distinguishing transport failures from Job Manager failures,
  # but is not part of the readiness gate.
  for deployment in ${E2E_STACK_DEPLOYMENTS:-} "${HELM_RELEASE:-nvidia-recovery}-api-gateway"; do
    [[ "$deployment" =~ ^[a-zA-Z0-9.-]+$ ]] || continue
    [[ "$seen" == *" $deployment "* ]] && continue
    seen+="$deployment "

    if ! kubectl get deployment -n "$namespace" "$deployment" --request-timeout=10s >/dev/null 2>&1; then
      continue
    fi

    _e2e_capture_diagnostic "$diagnostics_dir/$deployment.log" \
      kubectl logs -n "$namespace" "deployment/$deployment" \
      --all-containers=true --prefix=true --timestamps=true \
      "--since-time=$since_time" --tail=2000
    _e2e_capture_diagnostic "$diagnostics_dir/$deployment.previous.log" \
      kubectl logs -n "$namespace" "deployment/$deployment" \
      --all-containers=true --prefix=true --timestamps=true --previous=true \
      "--since-time=$since_time" --tail=500
  done
}

_e2e_collect_slurm_state() {
  local diagnostics_dir="$1"
  local namespace="${SLURM_NS:-slurm}"
  local login_pod

  if ! login_pod="$(kubectl get pod -n "$namespace" \
    -l app.kubernetes.io/component=login \
    -o 'jsonpath={.items[0].metadata.name}' \
    --request-timeout=10s 2>/dev/null)" || [[ -z "$login_pod" ]]; then
    printf 'Slurm login pod unavailable; scheduler snapshots skipped\n'
    return 0
  fi

  _e2e_capture_diagnostic "$diagnostics_dir/squeue.json" \
    kubectl exec -n "$namespace" "$login_pod" -- squeue --json
  _e2e_capture_diagnostic "$diagnostics_dir/sacct.json" \
    kubectl exec -n "$namespace" "$login_pod" -- sacct -X --duplicates --json
}

_e2e_collect_failure_diagnostics() {
  local diagnostics_dir="$1"
  local namespace="${E2E_STACK_NAMESPACE:-${HELM_NAMESPACE:-slurm}}"
  local since_time="${E2E_RUN_STARTED_AT:-1970-01-01T00:00:00Z}"
  local job_manager_proxy="/api/v1/namespaces/$namespace/services/http:job-manager:8080/proxy"

  if ! command -v kubectl >/dev/null 2>&1; then
    printf 'kubectl unavailable; cluster diagnostics skipped\n'
    return 0
  fi
  if ! kubectl cluster-info --request-timeout=10s >/dev/null 2>&1; then
    printf 'cluster unreachable; cluster diagnostics skipped\n'
    return 0
  fi

  _e2e_capture_diagnostic "$diagnostics_dir/pods.txt" \
    kubectl get pods -A -o wide --request-timeout=10s
  _e2e_capture_diagnostic "$diagnostics_dir/events.txt" \
    kubectl get events -A --sort-by=.metadata.creationTimestamp --request-timeout=10s
  _e2e_capture_diagnostic "$diagnostics_dir/jobs.json" \
    kubectl get --raw "$job_manager_proxy/jobs?limit=1000"
  _e2e_capture_diagnostic "$diagnostics_dir/advice.json" \
    kubectl get --raw "$job_manager_proxy/advice?page_size=1000&include_hidden=true"

  _e2e_collect_deployment_logs "$diagnostics_dir" "$namespace" "$since_time"
  _e2e_collect_slurm_state "$diagnostics_dir"
}

collect_failure_diagnostics() {
  local report_directory diagnostics_dir
  report_directory="$(_e2e_diagnostic_report_directory)"
  diagnostics_dir="$report_directory/diagnostics"

  # Cleanup must never mask the test result or pollute its log. Even directory
  # creation errors are intentionally silent; GitHub will still upload any
  # reports pytest produced before the failure.
  mkdir -p "$diagnostics_dir" >/dev/null 2>&1 || return 0
  _e2e_collect_failure_diagnostics "$diagnostics_dir" \
    >"$diagnostics_dir/collector.log" 2>&1 || true
}
