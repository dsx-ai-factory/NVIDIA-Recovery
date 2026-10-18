#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
log() { echo "[$(date -Is)] $*" >&2; }
ensure_tools() {
  if [[ "${DEPCHK_SKIP_INSTALL:-0}" == "1" || -f /tmp/.depcheck_tools_installed ]]; then return 0; fi
  missing_pkgs=()
  command -v mysql >/dev/null 2>&1 || missing_pkgs+=(mysql-client)
  command -v curl >/dev/null 2>&1 || missing_pkgs+=(curl)
  command -v base64 >/dev/null 2>&1 || missing_pkgs+=(coreutils)
  command -v jq >/dev/null 2>&1 || missing_pkgs+=(jq)
  if [[ ${#missing_pkgs[@]} -gt 0 ]] && command -v apt-get >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq && apt-get install -y -qq "${missing_pkgs[@]}"
  fi
  touch /tmp/.depcheck_tools_installed || true
}
ensure_textfile_dir() {
  umask 022
  : "${TEXTFILE_DIR:=/var/node_exporter/textfile}"
  mkdir -p "${TEXTFILE_DIR}" && chmod 0777 "${TEXTFILE_DIR}" || true
}
write_prom_file() {
  local tmpfile; tmpfile=$(mktemp "${TEXTFILE_DIR}/.depcheck.XXXXXX")
  cat >"${tmpfile}"
  chmod 0644 "${tmpfile}" || true
  mv "${tmpfile}" "${TEXTFILE_DIR}/$1"
  chmod 0644 "${TEXTFILE_DIR}/$1" || true
}
epoch_to_rfc3339() {
  local epoch_seconds="$1"
  if date -u -d "@${epoch_seconds}" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null; then
    return 0
  fi
  date -u -r "$epoch_seconds" '+%Y-%m-%dT%H:%M:%SZ'
}
sanitize_loki_selector() {
  local raw; raw=$(printf '%s' "$1" | sed 's/^\s*//;s/\s*$//')
  # For pipeline queries, only adjust the left-hand selector
  if [[ "$raw" == *"|"* ]]; then
    local left right
    left=${raw%%|*}; right=${raw#*|}
    left=$(printf '%s' "$left" | sed 's/^\s*//;s/\s*$//')
    # If double-wrapped like {{...}}, remove exactly one pair -> {...}
    if [[ "$left" == \{\{* && "$left" == *\}\} ]]; then
      left=$(printf '%s' "$left" | sed -E 's/^\{\{(.*)\}\}$/\{\1\}/')
    fi
    printf '%s |%s' "$left" "$right"
    return 0
  fi
  # No pipeline: if double-wrapped, remove exactly one pair; otherwise leave as-is
  if [[ "$raw" == \{\{* && "$raw" == *\}\} ]]; then
    printf '%s' "$raw" | sed -E 's/^\{\{(.*)\}\}$/\{\1\}/'
  else
    printf '%s' "$raw"
  fi
}

# Execute one bounded provider-native row query. A clean empty response is
# healthy; callers inspect LOG_QUERY_LAST_HAS_RESULTS separately for ingestion
# freshness. The function buffers the body before validation so a partial
# VictoriaLogs stream cannot be reported as success.
log_query_request() {
  local backend="$1" domain="$2" query="$3" start_ns="$4" end_ns="$5"
  local tmp_body tmp_headers http_code curl_rc url
  tmp_body=$(mktemp)
  tmp_headers=$(mktemp)
  LOG_QUERY_LAST_HEALTH=0
  LOG_QUERY_LAST_HAS_RESULTS=0
  LOG_QUERY_LAST_HTTP_CODE=""
  LOG_QUERY_LAST_BODY_SNIPPET=""

  if [[ "$backend" == "loki" ]]; then
    if [[ "$domain" == "system" ]]; then
      url="${LOKI_SYS_URL:-}"
      local loki_tenants="${LOKI_SYS_TENANTS:-}"
      local loki_org_id="${LOKI_SYS_ORG_ID:-}"
      local loki_token="${LOKI_SYS_TOKEN:-${LOKI_TOKEN:-}}"
    else
      url="${LOKI_APP_URL:-}"
      local loki_tenants="${LOKI_APP_TENANTS:-}"
      local loki_org_id="${LOKI_APP_ORG_ID:-}"
      local loki_token="${LOKI_APP_TOKEN:-${LOKI_TOKEN:-}}"
    fi
    if [[ -z "$url" ]]; then
      log "Missing Loki ${domain} provider query URL"
      rm -f "$tmp_body" "$tmp_headers" || true
      return 1
    fi
    local selector
    selector=$(sanitize_loki_selector "$query")
    local -a loki_args=(--max-time 5 --connect-timeout 5 -sS -L --get)
    if [[ -n "$loki_token" && -n "$loki_tenants" ]]; then
      loki_args+=(-u "${loki_tenants}:${loki_token}")
    elif [[ -n "$loki_token" ]]; then
      loki_args+=(-H "Authorization: Bearer ${loki_token}")
    fi
    [[ -n "$loki_org_id" ]] && loki_args+=(-H "X-Scope-OrgID: ${loki_org_id}")
    set +e
    http_code=$(curl "${loki_args[@]}" \
      --data-urlencode "query=${selector}" \
      --data "start=${start_ns}" --data "end=${end_ns}" \
      --data "limit=1" --data "direction=backward" \
      -D "$tmp_headers" -o "$tmp_body" -w "%{http_code}" \
      "${url}/loki/api/v1/query_range")
    curl_rc=$?
    set -e
    if [[ $curl_rc -eq 0 && "$http_code" == "200" ]] &&
      jq -e '.status == "success" and .data.resultType == "streams" and
        (.data.result | type == "array") and
        (.data.result | all(.[];
          (.stream | type == "object") and
          (.values | type == "array") and
          (.values | all(.[];
            (type == "array") and (length == 2) and
            (.[0] | type == "string") and (.[1] | type == "string")))))' "$tmp_body" >/dev/null 2>&1; then
      LOG_QUERY_LAST_HEALTH=1
      if jq -e '.data.result | any(.[]; .values | length > 0)' "$tmp_body" >/dev/null 2>&1; then
        LOG_QUERY_LAST_HAS_RESULTS=1
      fi
    fi
  elif [[ "$backend" == "victorialogs" ]]; then
    if [[ "$domain" == "system" ]]; then
      url="${VICTORIALOGS_SYS_URL:-}"
      local account_id="${VICTORIALOGS_SYS_ACCOUNT_ID:-}"
      local project_id="${VICTORIALOGS_SYS_PROJECT_ID:-}"
      local require_tenant="${VICTORIALOGS_SYS_REQUIRE_TENANT:-false}"
      local victoria_token="${VICTORIALOGS_SYS_TOKEN:-${VICTORIALOGS_TOKEN:-}}"
      local victoria_username="${VICTORIALOGS_SYS_USERNAME:-${VICTORIALOGS_USERNAME:-}}"
      local victoria_password="${VICTORIALOGS_SYS_PASSWORD:-${VICTORIALOGS_PASSWORD:-}}"
    else
      url="${VICTORIALOGS_APP_URL:-}"
      local account_id="${VICTORIALOGS_APP_ACCOUNT_ID:-}"
      local project_id="${VICTORIALOGS_APP_PROJECT_ID:-}"
      local require_tenant="${VICTORIALOGS_APP_REQUIRE_TENANT:-false}"
      local victoria_token="${VICTORIALOGS_APP_TOKEN:-${VICTORIALOGS_TOKEN:-}}"
      local victoria_username="${VICTORIALOGS_APP_USERNAME:-${VICTORIALOGS_USERNAME:-}}"
      local victoria_password="${VICTORIALOGS_APP_PASSWORD:-${VICTORIALOGS_PASSWORD:-}}"
    fi
    if [[ -z "$url" ]]; then
      log "Missing VictoriaLogs ${domain} provider query URL"
      rm -f "$tmp_body" "$tmp_headers" || true
      return 1
    fi
    if [[ -n "$account_id" && -z "$project_id" ]] || [[ -z "$account_id" && -n "$project_id" ]]; then
      rm -f "$tmp_body" "$tmp_headers" || true
      return 1
    fi
    if [[ "$require_tenant" == "true" && -z "$account_id" ]]; then
      rm -f "$tmp_body" "$tmp_headers" || true
      return 1
    fi
    if [[ -n "$victoria_token" && ( -n "$victoria_username" || -n "$victoria_password" ) ]]; then
      rm -f "$tmp_body" "$tmp_headers" || true
      return 1
    fi
    if [[ -n "$victoria_username" && -z "$victoria_password" ]] ||
      [[ -z "$victoria_username" && -n "$victoria_password" ]]; then
      rm -f "$tmp_body" "$tmp_headers" || true
      return 1
    fi
    if [[ -n "${VICTORIALOGS_CERT_FILE:-}" && -z "${VICTORIALOGS_KEY_FILE:-}" ]] ||
      [[ -z "${VICTORIALOGS_CERT_FILE:-}" && -n "${VICTORIALOGS_KEY_FILE:-}" ]]; then
      rm -f "$tmp_body" "$tmp_headers" || true
      return 1
    fi
    local start_seconds=$((start_ns / 1000000000))
    local end_seconds=$((end_ns / 1000000000))
    local start_rfc end_rfc
    start_rfc=$(epoch_to_rfc3339 "$start_seconds")
    end_rfc=$(epoch_to_rfc3339 "$end_seconds")
    local -a victoria_args=(--max-time 5 --connect-timeout 5 -sS -L --request POST)
    if [[ -n "$victoria_username" ]]; then
      victoria_args+=(-u "${victoria_username}:${victoria_password}")
    elif [[ -n "$victoria_token" ]]; then
      victoria_args+=(-H "Authorization: Bearer ${victoria_token}")
    fi
    [[ -n "${VICTORIALOGS_CA_CERT:-}" ]] && victoria_args+=(--cacert "${VICTORIALOGS_CA_CERT}")
    if [[ -n "${VICTORIALOGS_CERT_FILE:-}" ]]; then
      victoria_args+=(--cert "${VICTORIALOGS_CERT_FILE}" --key "${VICTORIALOGS_KEY_FILE}")
    fi
    [[ -n "$account_id" ]] && victoria_args+=(-H "AccountID: ${account_id}")
    [[ -n "$project_id" ]] && victoria_args+=(-H "ProjectID: ${project_id}")
    set +e
    http_code=$(curl "${victoria_args[@]}" \
      --data-urlencode "query=${query}" \
      --data-urlencode "start=${start_rfc}" \
      --data-urlencode "end=${end_rfc}" \
      --data "limit=1" --data "timeout=5s" \
      --data "allow_partial_response=false" \
      -D "$tmp_headers" -o "$tmp_body" -w "%{http_code}" \
      "${url}/select/logsql/query")
    curl_rc=$?
    set -e
    local tenant_ok=1
    if [[ -n "$account_id" ]]; then
      grep -i -E -q "^AccountID:[[:space:]]*${account_id}[[:space:]]*$" "$tmp_headers" || tenant_ok=0
      grep -i -E -q "^ProjectID:[[:space:]]*${project_id}[[:space:]]*$" "$tmp_headers" || tenant_ok=0
    fi
    if [[ $curl_rc -eq 0 && "$http_code" == "200" && $tenant_ok -eq 1 ]] &&
      jq -e -s 'all(.[]; type == "object" and (._time | type == "string") and (._msg | type == "string"))' "$tmp_body" >/dev/null 2>&1; then
      LOG_QUERY_LAST_HEALTH=1
      [[ -s "$tmp_body" ]] && LOG_QUERY_LAST_HAS_RESULTS=1
    fi
  else
    http_code="0"
  fi

  LOG_QUERY_LAST_HTTP_CODE="$http_code"
  if [[ "${DEPCHK_DEBUG:-}" == "1" ]]; then
    LOG_QUERY_LAST_BODY_SNIPPET=$(head -c 512 "$tmp_body" | tr '\0' ' ')
  fi
  rm -f "$tmp_body" "$tmp_headers" || true
  [[ "$LOG_QUERY_LAST_HEALTH" -eq 1 ]]
}
