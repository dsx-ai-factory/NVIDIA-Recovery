#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONTROLLER_ENV="$PACKAGE_DIR/slurm-advisor-controller.env.example"
CONTROLLER_UNIT="$PACKAGE_DIR/slurm-advisor-controller.service"
WATCHDOG_ENV="$PACKAGE_DIR/slurm-watchdog.env.example"
WATCHDOG_UNIT="$PACKAGE_DIR/slurm-watchdog.service"
LOG_LINKS_DROP_IN="$PACKAGE_DIR/slurm-advisor-controller-log-links.conf.example"
README="$PACKAGE_DIR/README.md"

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  local file="$1"
  local expected="$2"
  grep -F -- "$expected" "$file" >/dev/null ||
    fail "expected $(basename "$file") to contain: $expected"
}

assert_not_contains() {
  local file="$1"
  local unexpected="$2"
  if grep -F -- "$unexpected" "$file" >/dev/null; then
    fail "expected $(basename "$file") not to contain: $unexpected"
  fi
}

assert_line() {
  local file="$1"
  local expected="$2"
  local count
  count="$(grep -F -x -c -- "$expected" "$file" || true)"
  [[ "$count" == 1 ]] ||
    fail "expected one exact line in $(basename "$file"): $expected"
}

assert_setting_once() {
  local name="$1"
  local count
  count="$(grep -E -c "^#? ${name}=|^${name}=" "$CONTROLLER_ENV" || true)"
  [[ "$count" == 1 ]] ||
    fail "expected exactly one environment example for $name, found $count"
}

for file in \
  "$CONTROLLER_ENV" \
  "$CONTROLLER_UNIT" \
  "$WATCHDOG_ENV" \
  "$WATCHDOG_UNIT" \
  "$LOG_LINKS_DROP_IN" \
  "$README"; do
  [[ -f "$file" ]] || fail "missing contract file: $file"
done

assert_line "$CONTROLLER_ENV" "SLURM_ADVISOR_LOG_LINKS_ENABLED=false"
assert_line "$CONTROLLER_ENV" \
  "SLURM_ADVISOR_AUDIT_TRAIL_DESTINATION=logging"
assert_line "$CONTROLLER_ENV" "SLURM_ADVISOR_AUDIT_TRAIL_MAX_BATCH_SIZE=100"
assert_line "$CONTROLLER_ENV" "SLURM_ADVISOR_AUDIT_TRAIL_BUFFER_SIZE=1000"
assert_line "$CONTROLLER_ENV" "SLURM_ADVISOR_AUDIT_TRAIL_FLUSH_INTERVAL=30s"
assert_line "$WATCHDOG_ENV" \
  "SLURM_WATCHDOG_AUDIT_TRAIL_DESTINATION=logging"
assert_line "$WATCHDOG_ENV" "SLURM_WATCHDOG_AUDIT_TRAIL_MAX_BATCH_SIZE=100"
assert_line "$WATCHDOG_ENV" "SLURM_WATCHDOG_AUDIT_TRAIL_BUFFER_SIZE=1000"
assert_line "$WATCHDOG_ENV" "SLURM_WATCHDOG_AUDIT_TRAIL_FLUSH_INTERVAL=30s"
assert_line "$CONTROLLER_ENV" \
  "# SLURM_ADVISOR_LOG_LINKS_DESTINATION=/var/lib/heimdall-oss/log-links"
assert_line "$CONTROLLER_ENV" \
  "# The destination must not equal, overlap, contain, be contained by, or alias"
assert_line "$CONTROLLER_ENV" \
  "# Source writers must be trusted not to substitute symlinks; a following"
assert_line "$CONTROLLER_ENV" \
  "# collector must expose no unrelated mounts or secrets that such a link can reach."
assert_line "$CONTROLLER_ENV" \
  "# SLURM_ADVISOR_LOG_LINKS_SOURCE_ROOTS_JSON='[\"/var/log/slurm/jobs\"]'"
assert_line "$CONTROLLER_ENV" \
  "# SLURM_ADVISOR_LOG_LINKS_APPLICATION_PATTERNS_JSON='[\"^app-{job_id}[.]log$\",\"^worker-{job_id}[.]log$\"]'"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_SCAN_INTERVAL=1m"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_CLEANUP_INTERVAL=1h"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_RETENTION=24h"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_SCAN_TIMEOUT=5s"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_MAX_PATH_LENGTH=4096"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_MAX_PATHS_PER_SYNC=4096"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_MAX_PATTERNS=32"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_MAX_SCAN_DIRECTORIES=64"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_MAX_ENTRIES_PER_DIRECTORY=4096"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_MAX_MATCHES_PER_SCAN=512"
assert_line "$CONTROLLER_ENV" "# SLURM_ADVISOR_LOG_LINKS_MAX_OWNED_LINKS=16384"

for name in \
  SLURM_ADVISOR_LOG_LINKS_ENABLED \
  SLURM_ADVISOR_LOG_LINKS_DESTINATION \
  SLURM_ADVISOR_LOG_LINKS_SOURCE_ROOTS_JSON \
  SLURM_ADVISOR_LOG_LINKS_APPLICATION_PATTERNS_JSON \
  SLURM_ADVISOR_LOG_LINKS_SCAN_INTERVAL \
  SLURM_ADVISOR_LOG_LINKS_CLEANUP_INTERVAL \
  SLURM_ADVISOR_LOG_LINKS_RETENTION \
  SLURM_ADVISOR_LOG_LINKS_SCAN_TIMEOUT \
  SLURM_ADVISOR_LOG_LINKS_MAX_PATH_LENGTH \
  SLURM_ADVISOR_LOG_LINKS_MAX_PATHS_PER_SYNC \
  SLURM_ADVISOR_LOG_LINKS_MAX_PATTERNS \
  SLURM_ADVISOR_LOG_LINKS_MAX_SCAN_DIRECTORIES \
  SLURM_ADVISOR_LOG_LINKS_MAX_ENTRIES_PER_DIRECTORY \
  SLURM_ADVISOR_LOG_LINKS_MAX_MATCHES_PER_SCAN \
  SLURM_ADVISOR_LOG_LINKS_MAX_OWNED_LINKS; do
  assert_setting_once "$name"
done
log_link_setting_count="$(
  grep -E -c '^(# )?SLURM_ADVISOR_LOG_LINKS_[A-Z_]+=' "$CONTROLLER_ENV" || true
)"
[[ "$log_link_setting_count" == 15 ]] ||
  fail "expected exactly 15 log-link settings and no async queue setting"

# The stock unit remains write-denying. Only the opt-in drop-in grants the
# prepared destination, and the watchdog never receives this capability.
assert_line "$CONTROLLER_UNIT" "TimeoutStopSec=45s"
assert_line "$CONTROLLER_UNIT" "KillMode=control-group"
assert_line "$CONTROLLER_UNIT" "ProtectSystem=strict"
assert_line "$CONTROLLER_UNIT" "ProtectHome=true"
assert_not_contains "$CONTROLLER_UNIT" "ReadWritePaths="
assert_not_contains "$WATCHDOG_UNIT" "ReadWritePaths="
assert_not_contains "$WATCHDOG_ENV" "SLURM_ADVISOR_LOG_LINKS_"
assert_line "$WATCHDOG_ENV" "SLURM_WATCHDOG_ENVIRONMENT_PATTERN=unspecified"
assert_line "$LOG_LINKS_DROP_IN" "[Service]"
assert_line "$LOG_LINKS_DROP_IN" \
  "# This grants advisor writes only; separately sandbox the following collector"
assert_line "$LOG_LINKS_DROP_IN" \
  "# and use only source roots whose writers are trusted not to substitute links."
assert_line "$LOG_LINKS_DROP_IN" \
  "# The destination must not overlap or alias any configured source root."
assert_line "$LOG_LINKS_DROP_IN" \
  "ReadWritePaths=/var/lib/heimdall-oss/log-links"
read_write_count="$(grep -F -c -- "ReadWritePaths=" "$LOG_LINKS_DROP_IN" || true)"
[[ "$read_write_count" == 1 ]] ||
  fail "drop-in must contain exactly one ReadWritePaths grant"

# Keep the operator contract discoverable alongside the files it governs.
assert_contains "$README" "same absolute paths as the advisor"
assert_contains "$README" "Audit buffering"
assert_contains "$README" "drops new events instead of waiting for Notifications delivery"
assert_contains "$README" "Run only one advisor materializer against a destination."
assert_contains "$README" "verify the effective mounts rather than relying on path spelling alone"
assert_contains "$README" "separate from scheduler-action safety"
assert_contains "$README" "ProtectHome=true"
assert_contains "$README" "Treat every configured source root as trusted."
assert_contains "$README" \
  "periodic validation cannot close"
assert_contains "$README" \
  "not expose service credentials, secret files, home directories, the host root"
assert_contains "$README" "one latest-wins pending"
assert_contains "$README" \
  "Stopping the service signals cancellation to in-flight work and drops pending"
assert_contains "$README" \
  "No additional unit, queue storage, environment"
assert_contains "$README" \
  "not a promise to drain"
assert_contains "$README" "To roll back"

if command -v systemd-analyze >/dev/null 2>&1; then
  UNIT_ROOT="$(mktemp -d)"
  trap 'rm -rf "$UNIT_ROOT"' EXIT
  mkdir -p "$UNIT_ROOT/slurm-advisor-controller.service.d"
  cp "$CONTROLLER_UNIT" "$UNIT_ROOT/slurm-advisor-controller.service"
  cp "$WATCHDOG_UNIT" "$UNIT_ROOT/slurm-watchdog.service"
  cp "$LOG_LINKS_DROP_IN" \
    "$UNIT_ROOT/slurm-advisor-controller.service.d/log-links.conf"
  SYSTEMD_UNIT_PATH="$UNIT_ROOT:" systemd-analyze verify \
    slurm-advisor-controller.service \
    slurm-watchdog.service
fi

printf 'ok: Slurm advisor controller systemd contract\n'

assert_line "$WATCHDOG_ENV" "# SLURM_WATCHDOG_AUDIT_GATEWAY_ENDPOINT=heimdall-api-gateway-internal:9090"
assert_line "$WATCHDOG_ENV" "# SLURM_WATCHDOG_SERVICE_ACCOUNT_TOKEN_PATH=/var/run/secrets/heimdall/service-account-token"
