#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.common.sh
. "${scripts_dir}/lib.common.sh"
ensure_tools
ensure_textfile_dir

backend="${LOG_QUERY_BACKEND:-loki}"
if [[ "$backend" != "loki" && "$backend" != "victorialogs" ]]; then
  log "Unsupported LOG_QUERY_BACKEND '${backend}'"
  exit 1
fi
if [[ "$backend" == "loki" && ( -z "${LOKI_APP_URL:-}" || -z "${LOKI_SYS_URL:-}" ) ]]; then
  log "Loki application and system provider query URLs are required"
  exit 1
fi
if [[ "$backend" == "victorialogs" && ( -z "${VICTORIALOGS_APP_URL:-}" || -z "${VICTORIALOGS_SYS_URL:-}" ) ]]; then
  log "VictoriaLogs application and system provider query URLs are required"
  exit 1
fi
if [[ "$backend" == "victorialogs" ]]; then
  app_account_id="${VICTORIALOGS_APP_ACCOUNT_ID:-}"
  app_project_id="${VICTORIALOGS_APP_PROJECT_ID:-}"
  sys_account_id="${VICTORIALOGS_SYS_ACCOUNT_ID:-}"
  sys_project_id="${VICTORIALOGS_SYS_PROJECT_ID:-}"
  if [[ -n "$app_account_id" && -z "$app_project_id" ]] || [[ -z "$app_account_id" && -n "$app_project_id" ]]; then
    log "VictoriaLogs application tenant headers must be configured as a pair"
    exit 1
  fi
  if [[ -n "$sys_account_id" && -z "$sys_project_id" ]] || [[ -z "$sys_account_id" && -n "$sys_project_id" ]]; then
    log "VictoriaLogs system tenant headers must be configured as a pair"
    exit 1
  fi
  if [[ "${VICTORIALOGS_APP_REQUIRE_TENANT:-false}" == "true" && -z "$app_account_id" ]]; then
    log "VictoriaLogs application tenant headers are required but incomplete"
    exit 1
  fi
  if [[ "${VICTORIALOGS_SYS_REQUIRE_TENANT:-false}" == "true" && -z "$sys_account_id" ]]; then
    log "VictoriaLogs system tenant headers are required but incomplete"
    exit 1
  fi
fi

queries_names=()
queries_domains=()
queries_vals=()
if [[ -z "${LOG_QUERY_SYS_HEALTH_QUERY_B64:-}" || -z "${LOG_QUERY_APP_HEALTH_QUERY_B64:-}" ]]; then
  log "Dedicated application and system health queries are required"
  exit 1
fi
system_health_query=$(printf '%s' "${LOG_QUERY_SYS_HEALTH_QUERY_B64}" | base64 -d 2>/dev/null || true)
application_health_query=$(printf '%s' "${LOG_QUERY_APP_HEALTH_QUERY_B64}" | base64 -d 2>/dev/null || true)
if [[ -z "$system_health_query" || -z "$application_health_query" ]]; then
  log "Dedicated application and system health queries must be valid non-empty base64 values"
  exit 1
fi
queries_names+=("system_health")
queries_domains+=("system")
queries_vals+=("$system_health_query")
queries_names+=("application_health")
queries_domains+=("application")
queries_vals+=("$application_health_query")
if [[ -n "${WCH_QUERY_PREFIX_B64:-}" ]]; then
  q=$(printf '%s' "${WCH_QUERY_PREFIX_B64}" | base64 -d 2>/dev/null || true)
  [[ -n "$q" ]] && queries_names+=("watcher_prefix") && queries_domains+=("application") && queries_vals+=("$q")
fi

window_seconds="${LOG_QUERY_RECENT_WINDOW_SECONDS:-300}"
end_seconds=$(date +%s)
start_seconds=$((end_seconds - window_seconds))
start_ns="${start_seconds}000000000"
end_ns="${end_seconds}000000000"
total_q=${#queries_vals[@]}
healthy_q=0
recent_q=0
tmpfile=$(mktemp)
{
  echo "# HELP dependency_checker_log_query_health Query API health for the selected backend"
  echo "# TYPE dependency_checker_log_query_health gauge"
  echo "# HELP dependency_checker_log_query_query_health Per-query protocol health"
  echo "# TYPE dependency_checker_log_query_query_health gauge"
  echo "# HELP dependency_checker_log_query_recent_data Recent matching data presence; empty can still be healthy"
  echo "# TYPE dependency_checker_log_query_recent_data gauge"
  echo "# HELP dependency_checker_log_query_http_code Last HTTP status for a provider-native query"
  echo "# TYPE dependency_checker_log_query_http_code gauge"
  for i in "${!queries_vals[@]}"; do
    name="${queries_names[$i]}"
    domain="${queries_domains[$i]}"
    query="${queries_vals[$i]}"
    if log_query_request "$backend" "$domain" "$query" "$start_ns" "$end_ns"; then
      echo "dependency_checker_log_query_query_health{backend=\"${backend}\",source=\"${name}\"} 1"
      healthy_q=$((healthy_q + 1))
    else
      echo "dependency_checker_log_query_query_health{backend=\"${backend}\",source=\"${name}\"} 0"
      if [[ "${DEPCHK_DEBUG:-}" == "1" ]]; then
        log "Log-query check '${name}' backend=${backend} http=${LOG_QUERY_LAST_HTTP_CODE:-0} body='${LOG_QUERY_LAST_BODY_SNIPPET:-}'"
      fi
    fi
    echo "dependency_checker_log_query_recent_data{backend=\"${backend}\",source=\"${name}\"} ${LOG_QUERY_LAST_HAS_RESULTS}"
    echo "dependency_checker_log_query_http_code{backend=\"${backend}\",source=\"${name}\"} ${LOG_QUERY_LAST_HTTP_CODE:-0}"
    recent_q=$((recent_q + LOG_QUERY_LAST_HAS_RESULTS))

    # Preserve the original Loki recent-data series for one compatibility
    # window; backend-neutral health above is the new correctness signal.
    if [[ "$backend" == "loki" ]]; then
      echo "dependency_checker_loki_query_status{source=\"${name}\"} ${LOG_QUERY_LAST_HAS_RESULTS}"
      echo "dependency_checker_loki_http_code{source=\"${name}\"} ${LOG_QUERY_LAST_HTTP_CODE:-0}"
    fi
  done
  overall=$((total_q > 0 && healthy_q == total_q ? 1 : 0))
  echo "dependency_checker_log_query_health{backend=\"${backend}\"} ${overall}"
  echo "dependency_checker_log_query_queries_total{backend=\"${backend}\"} ${total_q}"
  echo "dependency_checker_log_query_queries_healthy{backend=\"${backend}\"} ${healthy_q}"
  echo "dependency_checker_log_query_queries_with_recent_data{backend=\"${backend}\"} ${recent_q}"
  echo "dependency_checker_test_status{test=\"log_query\"} ${overall}"
  if [[ "$backend" == "loki" ]]; then
    legacy_overall=$((total_q > 0 && recent_q == total_q ? 1 : 0))
    echo "dependency_checker_loki_queries_total ${total_q}"
    echo "dependency_checker_loki_queries_passed ${recent_q}"
    echo "dependency_checker_test_status{test=\"loki\"} ${legacy_overall}"
  fi
} >"$tmpfile"

write_prom_file dependency_checker_log_query.prom <"$tmpfile"
rm -f "$tmpfile" || true
[[ ${overall:-0} -eq 1 ]]
