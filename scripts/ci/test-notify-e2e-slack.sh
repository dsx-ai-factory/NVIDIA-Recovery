#!/bin/sh

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

ROOT="$(git rev-parse --show-toplevel)"
NOTIFIER="$ROOT/scripts/ci/notify-e2e-slack.sh"
TMP="$(mktemp -d)"
ORIGINAL_PATH="$PATH"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/bin"
cat >"$TMP/bin/curl" <<'STUB'
#!/bin/sh
set -eu
: >"$CURL_ARGS_FILE"
for arg in "$@"; do
  printf '%s\n' "$arg" >>"$CURL_ARGS_FILE"
done
printf '%s' "$CURL_RESPONSE"
exit "$CURL_EXIT_CODE"
STUB
chmod +x "$TMP/bin/curl"

fail() {
  printf 'notifier test failed: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  grep -Fq -- "$2" "$1" || fail "missing '$2' in $1"
}

assert_not_contains() {
  if grep -Fq -- "$2" "$1"; then
    fail "unexpected '$2' in $1"
  fi
}

reset_env() {
  export PATH="$TMP/bin:$ORIGINAL_PATH"
  export CURL_ARGS_FILE="$TMP/curl-args"
  export CURL_RESPONSE=ok
  export CURL_EXIT_CODE=0
  export SLACK_WEBHOOK_URL=https://example.invalid/webhook
  export E2E_PROVIDER=GitHub
  export E2E_OUTCOME=passed
  export E2E_REPOSITORY=example/heimdall
  export E2E_SHA=01234567
  export E2E_REF=main
  export E2E_EVENT=schedule
  export E2E_RUN_URL=https://ci.example.invalid/runs/1
  rm -f "$CURL_ARGS_FILE"
}

reset_env
sh "$NOTIFIER"
assert_contains "$CURL_ARGS_FILE" '{"text":"✅ Heimdall E2E passed on GitHub for example/heimdall@01234567 on main (schedule): https://ci.example.invalid/runs/1"}'
assert_not_contains "$CURL_ARGS_FILE" '<!here>'

reset_env
E2E_OUTCOME=failed sh "$NOTIFIER"
assert_contains "$CURL_ARGS_FILE" '{"text":"❌ Heimdall E2E failed on GitHub for example/heimdall@01234567 on main (schedule): https://ci.example.invalid/runs/1"}'
assert_not_contains "$CURL_ARGS_FILE" '<!here>'

reset_env
if E2E_OUTCOME=invalid sh "$NOTIFIER" >"$TMP/out" 2>"$TMP/error"; then
  fail 'invalid outcome succeeded'
fi
assert_contains "$TMP/error" 'E2E_OUTCOME must be passed or failed.'

reset_env
unset E2E_PROVIDER
if sh "$NOTIFIER" >"$TMP/out" 2>"$TMP/error"; then
  fail 'missing provider succeeded'
fi
assert_contains "$TMP/error" 'E2E_PROVIDER is required.'

reset_env
CURL_EXIT_CODE=22
if sh "$NOTIFIER" >"$TMP/out" 2>"$TMP/error"; then
  fail 'curl failure succeeded'
fi
assert_contains "$TMP/error" 'Slack webhook request failed.'

reset_env
CURL_RESPONSE=unexpected
if sh "$NOTIFIER" >"$TMP/out" 2>"$TMP/error"; then
  fail 'unexpected response succeeded'
fi
assert_contains "$TMP/error" 'Slack webhook returned an unexpected response.'

printf 'ok: E2E Slack notifier behavior\n'
