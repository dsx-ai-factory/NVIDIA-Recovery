#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../files/checker-scripts/lib.common.sh"

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

curl() {
  local body_file="" headers_file=""
  local auth_mode="none" org_mode="none" tls_mode="none"
  local query=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -o)
        body_file="$2"
        shift 2
        ;;
      -D)
        headers_file="$2"
        shift 2
        ;;
      -u)
        [[ "$2" == "tenant-a:test-token" ]] && auth_mode="basic"
        [[ "$2" == "tenant-system:system-token" ]] && auth_mode="system-basic"
        [[ "$2" == "vl-user:vl-password" ]] && auth_mode="victorialogs-basic"
        [[ "$2" == "vl-system:vl-system-password" ]] && auth_mode="victorialogs-system-basic"
        shift 2
        ;;
      --cacert)
        [[ "$2" == "/tls/ca.crt" ]] && tls_mode="ca"
        shift 2
        ;;
      --cert)
        [[ "$2" == "/tls/client.crt" && "$tls_mode" == "ca" ]] && tls_mode="ca-cert"
        shift 2
        ;;
      --key)
        [[ "$2" == "/tls/client.key" && "$tls_mode" == "ca-cert" ]] && tls_mode="ca-cert-key"
        shift 2
        ;;
      -H)
        [[ "$2" == "Authorization: Bearer test-token" ]] && auth_mode="bearer"
        [[ "$2" == "Authorization: Bearer vl-app-token" ]] && auth_mode="victorialogs-app-bearer"
        [[ "$2" == "X-Scope-OrgID: org-a" ]] && org_mode="org"
        [[ "$2" == "X-Scope-OrgID: org-system" ]] && org_mode="system-org"
        shift 2
        ;;
      --data-urlencode)
        if [[ "$2" == query=* ]]; then
          query="${2#query=}"
        fi
        shift 2
        ;;
      *)
        shift
        ;;
    esac
  done
  printf '%s' "${FAKE_BODY:-}" >"$body_file"
  printf '%b' "${FAKE_HEADERS:-}" >"$headers_file"
  if [[ -n "${FAKE_AUTH_MARKERS_FILE:-}" ]]; then
    printf '%s %s %s\n' "$auth_mode" "$org_mode" "$tls_mode" >"$FAKE_AUTH_MARKERS_FILE"
  fi
  if [[ -n "${FAKE_QUERY_MARKERS_FILE:-}" ]]; then
    printf '%s\n' "$query" >>"$FAKE_QUERY_MARKERS_FILE"
  fi
  printf '%s' "${FAKE_HTTP_CODE:-200}"
  return "${FAKE_CURL_RC:-0}"
}

command -v jq >/dev/null 2>&1 || fail "jq is required"

LOKI_APP_URL=https://loki.example.com
LOKI_SYS_URL=https://loki.example.com
LOKI_TOKEN=test-token
LOKI_APP_TENANTS=tenant-a
LOKI_APP_ORG_ID=org-a
FAKE_AUTH_MARKERS_FILE=$(mktemp)
RUNTIME_TMP_DIR=$(mktemp -d)
trap 'rm -f "$FAKE_AUTH_MARKERS_FILE"; rm -rf "$RUNTIME_TMP_DIR"' EXIT
FAKE_BODY='{"status":"success","data":{"resultType":"streams","result":[]}}'
FAKE_HEADERS=$'HTTP/1.1 200 OK\r\n'
log_query_request loki application '{appName="slurm-app-log"}' 1000000000 2000000000 ||
  fail "clean Loki empty response must be healthy"
[[ "$LOG_QUERY_LAST_HAS_RESULTS" -eq 0 ]] || fail "clean Loki empty response must not claim recent data"
grep -qx 'basic org none' "$FAKE_AUTH_MARKERS_FILE" || fail "Loki tenant credentials must use Basic auth and a distinct org header"

LOKI_SYS_TENANTS=tenant-system
LOKI_SYS_ORG_ID=org-system
LOKI_SYS_TOKEN=system-token
log_query_request loki system '{component="journal"}' 1000000000 2000000000 ||
  fail "clean system-domain Loki response must be healthy"
grep -qx 'system-basic system-org none' "$FAKE_AUTH_MARKERS_FILE" ||
  fail "Loki system-domain auth and org settings must remain distinct"
unset LOKI_SYS_TENANTS LOKI_SYS_ORG_ID LOKI_SYS_TOKEN
log_query_request loki system '{component="journal"}' 1000000000 2000000000 ||
  fail "Loki system domain may explicitly omit tenant identity"
grep -qx 'bearer none none' "$FAKE_AUTH_MARKERS_FILE" ||
  fail "Loki application tenant identity must not bleed into the system domain"

LOKI_URL=https://legacy-loki.example.com
unset LOKI_SYS_URL
if log_query_request loki system '{component="journal"}' 1000000000 2000000000 2>"$RUNTIME_TMP_DIR/missing-loki-url.log"; then
  fail "Loki system URL must not fall back to a legacy shared URL"
fi
LOKI_SYS_URL=https://loki.example.com
unset LOKI_URL

unset LOKI_APP_TENANTS LOKI_APP_ORG_ID
log_query_request loki application '{appName="slurm-app-log"}' 1000000000 2000000000 ||
  fail "clean bearer-authenticated Loki response must be healthy"
grep -qx 'bearer none none' "$FAKE_AUTH_MARKERS_FILE" || fail "Loki token without tenant username must use Bearer auth"

FAKE_BODY='{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["1","message"]]}]}}'
log_query_request loki application '{appName="slurm-app-log"}' 1000000000 2000000000 ||
  fail "clean Loki record response must be healthy"
[[ "$LOG_QUERY_LAST_HAS_RESULTS" -eq 1 ]] || fail "Loki record response must report recent data"

FAKE_HTTP_CODE=206
if log_query_request loki application '{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "partial Loki HTTP response must fail health"
fi
FAKE_HTTP_CODE=200

FAKE_BODY='{"status":"success","data":{"resultType":"streams","result":[{}]}}'
if log_query_request loki application '{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "malformed Loki stream shape must fail health"
fi

FAKE_BODY='{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[]}]}}'
log_query_request loki application '{appName="slurm-app-log"}' 1000000000 2000000000 ||
  fail "well-formed empty Loki stream must be healthy"
[[ "$LOG_QUERY_LAST_HAS_RESULTS" -eq 0 ]] || fail "empty Loki values must not claim recent data"

VICTORIALOGS_APP_URL=https://victorialogs.example.com
VICTORIALOGS_APP_ACCOUNT_ID=42
VICTORIALOGS_APP_PROJECT_ID=7
VICTORIALOGS_APP_REQUIRE_TENANT=true
if log_query_request victorialogs system '_stream:{component="journal"}' 1000000000 2000000000 2>"$RUNTIME_TMP_DIR/missing-victorialogs-url.log"; then
  fail "VictoriaLogs system URL must not fall back to its application URL"
fi
FAKE_BODY=''
FAKE_HEADERS=$'HTTP/1.1 200 OK\r\nAccountID: 42\r\nProjectID: 7\r\n'
log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000 ||
  fail "clean VictoriaLogs empty response must be healthy"
[[ "$LOG_QUERY_LAST_HAS_RESULTS" -eq 0 ]] || fail "clean VictoriaLogs empty response must not claim recent data"

FAKE_BODY=$'{"_time":"1970-01-01T00:00:01Z","_msg":"message"}\n'
log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000 ||
  fail "clean VictoriaLogs record response must be healthy"
[[ "$LOG_QUERY_LAST_HAS_RESULTS" -eq 1 ]] || fail "VictoriaLogs record response must report recent data"

VICTORIALOGS_USERNAME=vl-user
VICTORIALOGS_PASSWORD=vl-password
VICTORIALOGS_CA_CERT=/tls/ca.crt
VICTORIALOGS_CERT_FILE=/tls/client.crt
VICTORIALOGS_KEY_FILE=/tls/client.key
log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000 ||
  fail "VictoriaLogs Basic auth and mTLS query must be healthy"
grep -qx 'victorialogs-basic none ca-cert-key' "$FAKE_AUTH_MARKERS_FILE" ||
  fail "VictoriaLogs Basic auth and TLS settings must reach curl"

VICTORIALOGS_TOKEN=test-token
if log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "VictoriaLogs bearer and Basic auth must be mutually exclusive"
fi
unset VICTORIALOGS_TOKEN VICTORIALOGS_USERNAME VICTORIALOGS_PASSWORD

unset VICTORIALOGS_KEY_FILE
if log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "VictoriaLogs client certificate requires a client key"
fi
unset VICTORIALOGS_CA_CERT VICTORIALOGS_CERT_FILE

VICTORIALOGS_APP_TOKEN=vl-app-token
FAKE_BODY=$'{"_time":"1970-01-01T00:00:01Z","_msg":"message"}\n'
log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000 ||
  fail "VictoriaLogs application-domain bearer override must be healthy"
grep -qx 'victorialogs-app-bearer none none' "$FAKE_AUTH_MARKERS_FILE" ||
  fail "VictoriaLogs application-domain token must override shared credentials"
unset VICTORIALOGS_APP_TOKEN

VICTORIALOGS_SYS_URL=https://victorialogs-system.example.com
VICTORIALOGS_SYS_ACCOUNT_ID=84
VICTORIALOGS_SYS_PROJECT_ID=0
VICTORIALOGS_SYS_REQUIRE_TENANT=true
VICTORIALOGS_SYS_USERNAME=vl-system
VICTORIALOGS_SYS_PASSWORD=vl-system-password
FAKE_HEADERS=$'HTTP/1.1 200 OK\r\nAccountID: 84\r\nProjectID: 0\r\n'
log_query_request victorialogs system '_stream:{component="journal"}' 1000000000 2000000000 ||
  fail "VictoriaLogs system-domain Basic override must be healthy"
grep -qx 'victorialogs-system-basic none none' "$FAKE_AUTH_MARKERS_FILE" ||
  fail "VictoriaLogs system-domain Basic credentials must be selected independently"
unset VICTORIALOGS_SYS_USERNAME VICTORIALOGS_SYS_PASSWORD

FAKE_BODY='not-json'
FAKE_HEADERS=$'HTTP/1.1 200 OK\r\nAccountID: 42\r\nProjectID: 7\r\n'
if log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "malformed VictoriaLogs stream must fail health"
fi

FAKE_BODY=''
FAKE_HTTP_CODE=204
if log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "empty-body VictoriaLogs HTTP 204 must fail health"
fi
FAKE_HTTP_CODE=200

VICTORIALOGS_APP_REQUIRE_TENANT=false
FAKE_HEADERS=$'HTTP/1.1 200 OK\r\nAccountID: 999\r\nProjectID: 7\r\n'
if log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "configured VictoriaLogs tenant mismatch must fail even when require_tenant=false"
fi

FAKE_HEADERS=$'HTTP/1.1 200 OK\r\n'
if log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "configured VictoriaLogs tenant echo is required even when require_tenant=false"
fi

unset VICTORIALOGS_APP_ACCOUNT_ID
if log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "half-configured VictoriaLogs tenant pair must be rejected"
fi

unset VICTORIALOGS_APP_PROJECT_ID
log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000 ||
  fail "explicitly anonymous VictoriaLogs query must be allowed when require_tenant=false"

VICTORIALOGS_APP_REQUIRE_TENANT=true
if log_query_request victorialogs application '_stream:{appName="slurm-app-log"}' 1000000000 2000000000; then
  fail "required VictoriaLogs tenant pair must be validated before querying"
fi

# Execute the checker entrypoint with both new health-query inputs and legacy
# Attribution inputs. Only the dedicated probes and Watcher prefix may reach
# the provider request helper.
export -f curl
FAKE_QUERY_MARKERS_FILE="$RUNTIME_TMP_DIR/queries.txt"
FAKE_BODY='{"status":"success","data":{"resultType":"streams","result":[]}}'
FAKE_HEADERS=$'HTTP/1.1 200 OK\r\n'
app_health_query='{probe="dependency-checker-app"}'
system_health_query='{probe="dependency-checker-system"}'
watcher_query='{watcher="prefix"}'
legacy_app_query='{job_id="{job_id}",source="attribution-app"}'
legacy_system_query='{job_id="{job_id}",source="attribution-system"}'
DEPCHK_SKIP_INSTALL=1 \
TEXTFILE_DIR="$RUNTIME_TMP_DIR/textfiles" \
LOG_QUERY_BACKEND=loki \
LOKI_APP_URL=https://loki.example.com \
LOKI_SYS_URL=https://loki.example.com \
LOG_QUERY_APP_HEALTH_QUERY_B64="$(printf '%s' "$app_health_query" | base64 | tr -d '\n')" \
LOG_QUERY_SYS_HEALTH_QUERY_B64="$(printf '%s' "$system_health_query" | base64 | tr -d '\n')" \
WCH_QUERY_PREFIX_B64="$(printf '%s' "$watcher_query" | base64 | tr -d '\n')" \
ADI_APPLOG_QUERY_B64="$(printf '%s' "$legacy_app_query" | base64 | tr -d '\n')" \
ADI_SYSLOG_QUERY_B64="$(printf '%s' "$legacy_system_query" | base64 | tr -d '\n')" \
FAKE_QUERY_MARKERS_FILE="$FAKE_QUERY_MARKERS_FILE" \
FAKE_BODY="$FAKE_BODY" \
FAKE_HEADERS="$FAKE_HEADERS" \
bash "$SCRIPT_DIR/../files/checker-scripts/check-log-query.sh" ||
  fail "checker entrypoint must accept dedicated provider-native health queries"
grep -Fx -- "$app_health_query" "$FAKE_QUERY_MARKERS_FILE" >/dev/null ||
  fail "application health query must be sent"
grep -Fx -- "$system_health_query" "$FAKE_QUERY_MARKERS_FILE" >/dev/null ||
  fail "system health query must be sent"
grep -Fx -- "$watcher_query" "$FAKE_QUERY_MARKERS_FILE" >/dev/null ||
  fail "Watcher query prefix must remain checked"
if grep -F -- '{job_id}' "$FAKE_QUERY_MARKERS_FILE" >/dev/null; then
  fail "Attribution workload query placeholders must never be sent"
fi
metrics_file="$RUNTIME_TMP_DIR/textfiles/dependency_checker_log_query.prom"
grep -Fx 'dependency_checker_test_status{test="log_query"} 1' "$metrics_file" >/dev/null ||
  fail "clean empty responses must keep backend-neutral query health healthy"
grep -Fx 'dependency_checker_test_status{test="loki"} 0' "$metrics_file" >/dev/null ||
  fail "legacy Loki status must preserve its recent-data compatibility semantics"

echo "ok: dependency checker log-query semantics"
