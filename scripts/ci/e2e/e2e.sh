#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# e2e.sh — Heimdall e2e harness entry point.
#
# Orchestrates the full pipeline:
#   prepare → cluster → tilt ci → run-tests
#
# The EXIT trap runs cleanup on every exit path (success, failure, Ctrl-C),
# tearing down the cluster and removing the kubeconfig artifact — unless
# KEEP_CLUSTER is set, which leaves the cluster up for inspection.
#
# Usage:
#   bash scripts/ci/e2e/e2e.sh
#
# Common overrides (boolean toggles: empty = off, any non-empty value = on):
#   DEPLOY_KIND_CLUSTER= KUBECONFIG=/path/to/kubeconfig bash scripts/ci/e2e/e2e.sh
#   CLUSTER_NAME=heimdall-dev-2 bash scripts/ci/e2e/e2e.sh
#   KEEP_CLUSTER=1 bash scripts/ci/e2e/e2e.sh   # leave cluster up to inspect
#
# See scripts/ci/e2e/env.sh for the full list of configuration variables.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source shared configuration and the phases retained by the Tilt-backed suite.
source "$SCRIPT_DIR/env.sh"
source "$SCRIPT_DIR/lib.sh"
source "$SCRIPT_DIR/prepare.sh"
source "$SCRIPT_DIR/cluster.sh"
source "$SCRIPT_DIR/run-tests.sh"
source "$SCRIPT_DIR/diagnostics.sh"
E2E_CLUSTER_SETUP_STARTED=false

# env.sh resolves defaults as shell variables. Export graph inputs that Tilt
# reads directly so defaulted and explicitly empty values keep the same meaning.
export CLUSTER_NAME DEPLOY_INFRA DEPLOY_STACK E2E_ATTRIBUTION_MODE E2E_PROFILE
export E2E_LOG_BACKEND E2E_KUBERNETES_SERVICE_AUTH_SMOKE
export ATTRIBUTION_SOURCE_DIR ATTRIBUTION_SOURCE_REF ATTRIBUTION_SOURCE_URL
export SLURM_SHARED_HOME

# ---------------------------------------------------------------------------
# Cleanup — called by the EXIT trap regardless of exit code.
# cluster_teardown() is defined in cluster.sh and handles both the Kind
# cluster deletion and the standalone kubeconfig artifact removal.
# ---------------------------------------------------------------------------
_cleanup() {
  local exit_code=$?
  if (( exit_code != 0 )); then
    collect_failure_diagnostics
  fi
  banner "Cleanup"
  if [[ "${E2E_CLUSTER_SETUP_STARTED:-}" == true ]]; then
    cluster_teardown || warn "Cleanup incomplete; retained resources need inspection"
  fi
  if [[ $exit_code -ne 0 ]]; then
    warn "Pipeline exited with code $exit_code"
  fi
  return "$exit_code"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
main() {
  local start_time
  start_time="$(date +%s)"
  E2E_RUN_STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  export E2E_RUN_STARTED_AT

  banner "Heimdall e2e harness"
  log "Configuration:"
  log "  E2E_PROFILE        = $E2E_PROFILE"
  log "  ATTRIBUTION_MODE   = $E2E_ATTRIBUTION_MODE"
  if [[ "$E2E_ATTRIBUTION_MODE" == "available" ]]; then
    log "  ATTRIBUTION_SOURCE = $ATTRIBUTION_SOURCE_REF"
    log "  ATTRIBUTION_CACHE  = $ATTRIBUTION_SOURCE_DIR"
  fi
  log "  K8S_SERVICE_AUTH   = $(enabled "$E2E_KUBERNETES_SERVICE_AUTH_SMOKE" && echo on || echo off)"
  log "  DEPLOY_KIND_CLUSTER = $(enabled "$DEPLOY_KIND_CLUSTER" && echo on || echo off)"
  log "  CLUSTER_NAME        = $CLUSTER_NAME"
  log "  ARCH                = $ARCH"
  log "  DEPLOY_INFRA        = $(enabled "$DEPLOY_INFRA" && echo on || echo off)"
  log "  E2E_LOG_BACKEND     = $E2E_LOG_BACKEND"
  log "  E2E_TEMPO_SERVICE   = ${E2E_TEMPO_SERVICE:-${E2E_TEMPO_URL:-<disabled>}}"
  log "  E2E_OTLP_LOGS       = $(enabled "$E2E_OTLP_LOGS" && echo on || echo off)"
  log "  DEPLOY_GPU_SCRAPE   = $(enabled "$DEPLOY_GPU_SCRAPE" && echo on || echo off)"
  log "  DEPLOY_STACK        = $(enabled "$DEPLOY_STACK" && echo on || echo off)"
  log "  DEV_CI_TIMEOUT      = $DEV_CI_TIMEOUT"
  log "  TILT_LOG_SOURCE     = ${TILT_LOG_SOURCE:-all}"
  log "  KEEP_CLUSTER        = $(enabled "$KEEP_CLUSTER" && echo on || echo off)"
  log "  RUN_TESTS           = $(enabled "$RUN_TESTS" && echo on || echo off)"
  log "  KIND_CONFIG         = ${KIND_CONFIG:-(default)}"
  log "  KUBECONFIG          = $KUBECONFIG"

  # Phase 1: ensure required tools are available / downloaded.
  prepare

  # Only available mode consumes the upstream checkout. Remove the optional
  # credential before image builds and scenarios inherit the environment.
  if [[ -n "$DEPLOY_STACK" && "$E2E_ATTRIBUTION_MODE" == "available" ]]; then
    "$REPO_ROOT/deploy/tilt/prepare-attribution-source.sh" \
      "$ATTRIBUTION_SOURCE_DIR" \
      "$ATTRIBUTION_SOURCE_URL" \
      "$ATTRIBUTION_SOURCE_REF"
    unset ATTRIBUTION_SOURCE_DEPLOY_KEY ATTRIBUTION_SOURCE_DEPLOY_KEY_FILE
  fi

  # Phase 2: create Kind cluster or validate existing KUBECONFIG.
  cluster_up

  # Phase 3: Tilt owns the complete deployment graph. Wait for convergence
  # before scenarios run so they never observe a partial stack.
  local tilt_rc=0
  local -a tilt_ci_flags=(--timeout "$DEV_CI_TIMEOUT")
  if [[ -n "${TILT_LOG_SOURCE:-}" ]]; then
    tilt_ci_flags+=(--log-source "$TILT_LOG_SOURCE")
  fi
  (cd "$REPO_ROOT" && tilt ci "${tilt_ci_flags[@]}") || tilt_rc=$?
  if (( tilt_rc != 0 )); then
    dump_unready_pod_diagnostics
    return "$tilt_rc"
  fi

  # Phase 4: discover and run test scenarios from tests/e2e/.
  local tests_rc=0
  run_tests || tests_rc=$?
  (( tests_rc == 0 )) || return "$tests_rc"

  local elapsed=$(( $(date +%s) - start_time ))
  banner "Pipeline complete (${elapsed}s)"
  success "All phases passed"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  # Cleanup runs once, on EXIT. INT/TERM convert to an exit, which fires the
  # EXIT trap — so we never run _cleanup twice.
  trap _cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  main "$@"
fi
