#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# lib.sh — shared logging, color, and utility functions.
#
# No side effects. Safe to source multiple times.

[[ -n "${_HEIMDALL_LIB_LOADED:-}" ]] && return 0
_HEIMDALL_LIB_LOADED=1

# ---------------------------------------------------------------------------
# Colors — on when stdout is a tty, or when FORCE_COLOR is set (e.g. GitHub CI,
# which renders ANSI in its log viewer despite stdout not being a tty).
# Otherwise stripped so piped/redirected output stays clean.
# ---------------------------------------------------------------------------
if [[ -t 1 || -n "${FORCE_COLOR:-}" ]]; then
  C_RESET='\033[0m'
  C_RED='\033[0;31m'
  C_GREEN='\033[0;32m'
  C_YELLOW='\033[0;33m'
  C_BLUE='\033[0;34m'
  C_CYAN='\033[0;36m'
else
  C_RESET='' C_RED='' C_GREEN='' C_YELLOW='' C_BLUE='' C_CYAN=''
fi

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

_ts() { date '+%Y-%m-%d %H:%M:%S'; }

# log — informational, goes to stdout
log() {
  echo -e "${C_BLUE}[$(_ts)] [INFO]${C_RESET}  $*"
}

# success — positive outcome, goes to stdout
success() {
  echo -e "${C_GREEN}[$(_ts)] [OK]${C_RESET}    $*"
}

# warn — non-fatal issue, goes to stderr
warn() {
  echo -e "${C_YELLOW}[$(_ts)] [WARN]${C_RESET}   $*" >&2
}

# error — fatal, prints caller location and exits 1
error() {
  echo -e "${C_RED}[$(_ts)] [ERROR]${C_RESET}  $* (${BASH_SOURCE[1]:-unknown}:${BASH_LINENO[0]:-0})" >&2
  exit 1
}

# banner — section header, helps scan CI logs
banner() {
  echo -e "\n${C_CYAN}=== $* ===${C_RESET}"
}

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

# require_cmd <cmd> [install-hint]
# Exits with an error if <cmd> is not found in PATH.
require_cmd() {
  local cmd="$1"
  local hint="${2:-}"
  if ! command -v "$cmd" &>/dev/null; then
    local msg="Required command '$cmd' not found in PATH."
    [[ -n "$hint" ]] && msg+=" $hint"
    error "$msg"
  fi
}

# enabled <value>
# Returns success when <value> is non-empty. Single source of truth for the
# harness boolean convention (empty = OFF, any non-empty value = ON), so all
# toggles behave the same regardless of true/True/1/yes spelling.
enabled() {
  [[ -n "${1:-}" ]]
}

# dump_unready_pod_diagnostics
# Print cluster state to help diagnose an install/wait failure. A `helm --wait`
# timeout surfaces only "context deadline exceeded" with no indication of which
# pod hung or why — this fills that gap by dumping, cluster-wide:
#   - every pod (so the overall picture is visible), then
#   - for each pod that is NOT Ready and NOT Completed: its describe (events,
#     image-pull/scheduling/probe failures) and recent container logs, and
#   - recent Warning events across all namespaces.
# Namespace-agnostic on purpose so it is useful for any failing service, not
# just slurm. Best-effort: never fails the caller (always returns 0), and is a
# no-op when kubectl is unavailable or the cluster is unreachable.
dump_unready_pod_diagnostics() {
  command -v kubectl >/dev/null 2>&1 || return 0
  kubectl cluster-info >/dev/null 2>&1 || { warn "cluster unreachable — skipping diagnostics"; return 0; }

  banner "Diagnostics: cluster state after failure"

  # banner marks the section; the "--- ... ---" lines are lightweight dividers
  # within the dump. Plain echo on purpose — log/warn would stamp every divider
  # with a timestamp/level, which is just noise inside a single block of output.
  echo "--- Pods (all namespaces) ---"
  kubectl get pods -A -o wide 2>&1 || true

  # Columns from `kubectl get pods -A --no-headers`: NAMESPACE NAME READY STATUS ...
  # A pod is "ready" when READY ($3) is n/n and STATUS ($4) is Running, or when
  # it has Completed (a finished job: READY shows 0/n but that is expected).
  # Everything else is worth describing. NB: awk has no regex backreferences, so
  # split READY on "/" and compare the counts numerically rather than matching.
  local notready
  notready="$(kubectl get pods -A --no-headers 2>/dev/null \
    | awk '{ split($3, a, "/");
             ready = ((a[1] == a[2] && $4 == "Running") || $4 == "Completed");
             if (!ready) print $1" "$2 }')"

  if [[ -z "$notready" ]]; then
    echo "--- All pods are Ready or Completed; no per-pod detail to dump ---"
  else
    local ns pod
    while read -r ns pod; do
      [[ -z "$ns" ]] && continue
      echo "--- describe pod $ns/$pod ---"
      kubectl describe pod -n "$ns" "$pod" 2>&1 || true
      echo "--- logs $ns/$pod (current, last 50 lines, all containers) ---"
      kubectl logs -n "$ns" "$pod" --all-containers --tail=50 2>&1 || true
      echo "--- logs $ns/$pod (previous, last 50 lines, all containers) ---"
      kubectl logs -n "$ns" "$pod" --all-containers --previous --tail=50 2>&1 || true
    done <<< "$notready"
  fi

  echo "--- Warning events (all namespaces, last 40) ---"
  kubectl get events -A --field-selector type=Warning 2>&1 | tail -40 || true
}

# dump_node_and_system_diagnostics
# Dump node and kube-system state so a cluster bring-up timeout is diagnosable
# instead of a bare error. Scoped to the bring-up phase (before any application
# namespace exists), where the interesting failures are node-level — e.g. the
# common kind-on-Docker case of kube-proxy CrashLoopBackOff with "too many open
# files" (raise the host's fs.inotify.max_user_{instances,watches}). Use
# dump_unready_pod_diagnostics instead once workloads are being installed.
# Best-effort: never fails the caller.
dump_node_and_system_diagnostics() {
  warn "Cluster did not reach the expected state — dumping diagnostics:"
  kubectl get nodes -o wide 2>&1 || true
  kubectl get pods -n kube-system -o wide 2>&1 || true
}
