#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# run-tests.sh — thin, sourceable adapter from the e2e lifecycle to pytest.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
source "$SCRIPT_DIR/lib.sh"

run_tests() {
  banner "Phase: Run e2e tests"

  if ! enabled "$RUN_TESTS"; then
    log "RUN_TESTS is empty — skipping test phase"
    return 0
  fi

  local report report_root
  report_root="$REPO_ROOT/reports/e2e/${E2E_PROFILE}-${E2E_LOG_BACKEND}/job_state"
  report="${E2E_XUNIT_PATH:-$report_root/junit.xml}"
  if [[ "$report" != /* ]]; then
    report="$REPO_ROOT/$report"
  fi
  mkdir -p "$(dirname "$report")"

  # This is a production-interface check, not a scenario fixture: it validates
  # the deployed controller's real gRPC calls through the deployed gateway.
  if ! bash "$SCRIPT_DIR/kubernetes-service-auth-smoke.sh"; then
    return 1
  fi

  (
    # env.sh resolves these as shell-local profile values. Export only that
    # bridge here; already-exported caller knobs pass through unchanged.
    export E2E_ATTRIBUTION_MODE E2E_LOG_BACKEND E2E_PROFILE E2E_STACK_NAMESPACE E2E_STACK_DEPLOYMENTS
    export E2E_STACK_STATEFULSETS E2E_STACK_TIMEOUT E2E_SCENARIOS
    export DEPLOY_SLURM_ADVISOR_STACK LOKI_URL VICTORIALOGS_URL E2E_TEMPO_SERVICE E2E_TEMPO_URL
    cd "$REPO_ROOT"
    if ! uv run --locked --package recovery-libs --extra e2e e2e-stack-gate; then
      dump_unready_pod_diagnostics
      exit 1
    fi
    # run_tests can be called where errexit is ignored, so check each step's status explicitly.
    if ! PYTHONPATH="$REPO_ROOT/tests" uv run --locked --group e2e python -m e2e_harness.conformance; then
      exit 1
    fi
    uv run --locked --group e2e pytest -v -ra --durations=10 \
      -n "$E2E_PYTEST_WORKERS" --dist worksteal \
      --capture=tee-sys -o junit_logging=all \
      --junitxml "$report" \
      --trace-output "${E2E_TRACE_OUTPUT:-$(dirname "$report")/trace.json}" \
      "$REPO_ROOT/tests/e2e"
  )
}
