#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# provision.sh — shared-infrastructure provisioning for the Heimdall e2e harness.
#
# Provides one function consumed by e2e.sh:
#
#   provision — discover the service install scripts under services/, run them
#               in filename order, and install each into the active cluster.
#               Gated by DEPLOY_INFRA; a no-op when empty or when services/ has
#               no scripts.
#
# Each services/*.sh is self-contained (adds its own helm repo, namespace, and
# `helm upgrade --install --wait`). Ordering is controlled by numeric filename
# prefixes (01-, 02-, ...). Idempotency is Helm's: unchanged installs are
# no-ops, value changes reconcile.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
source "$SCRIPT_DIR/lib.sh"

# ---------------------------------------------------------------------------
# provision — entry point called by e2e.sh
# ---------------------------------------------------------------------------
provision() {
  banner "Phase: Provision infrastructure"

  if ! enabled "$DEPLOY_INFRA"; then
    log "DEPLOY_INFRA is empty — skipping infrastructure provisioning"
    return 0
  fi

  _provision_preflight

  local -a scripts=()
  local f
  while IFS= read -r -d '' f; do
    scripts+=("$f")
  done < <(_discover_services)

  if [[ ${#scripts[@]} -eq 0 ]]; then
    log "No service scripts under services/ — nothing to provision"
    return 0
  fi

  log "Installing ${#scripts[@]} service(s)"
  local s
  for s in "${scripts[@]}"; do
    _install_service "$s"
  done

  success "Provisioned ${#scripts[@]} service(s)"
}

# ---------------------------------------------------------------------------
# _provision_preflight
# Fails fast if the tools or cluster needed for installs are not ready.
# ---------------------------------------------------------------------------
_provision_preflight() {
  require_cmd helm
  require_cmd kubectl
  kubectl cluster-info >/dev/null 2>&1 \
    || error "cluster not reachable — run cluster_up before provisioning"

  # These values are substituted into chart values with `sed s|...|...|`, so a
  # literal '|' would corrupt the expression. Reject it early with a clear msg.
  local v
  for v in CLUSTER_LABEL SLURM_SHARED_HOME; do
    case "${!v}" in
      *"|"*) error "$v must not contain '|' (used as a sed delimiter): ${!v}" ;;
    esac
  done
}

# ---------------------------------------------------------------------------
# _discover_services
# Prints services/*.sh in execution order, NUL-separated. Numeric filename
# prefixes control ordering via `sort -V`. (NUL output + a read loop in the
# caller keeps this portable — no bash 4 namerefs/mapfile.)
# ---------------------------------------------------------------------------
_discover_services() {
  find "$SCRIPT_DIR/services" -maxdepth 1 -name '*.sh' -print0 | sort -zV
}

# ---------------------------------------------------------------------------
# _install_service <script>
# Installs one service via its script, with timing; fails the phase with a
# clear message if the install errors.
# ---------------------------------------------------------------------------
_install_service() {
  local script="$1"
  local name start
  name="$(basename "$script")"
  start="$(date +%s)"

  log "--- Installing: $name ---"
  if ! bash "$script"; then
    # The install failed (often a `helm --wait` timeout whose only signal is
    # "context deadline exceeded"). Dump cluster state so CI logs show which
    # pod hung and why before the phase aborts.
    dump_unready_pod_diagnostics
    error "service install failed: $name"
  fi
  success "Installed: $name ($(( $(date +%s) - start ))s)"
}
