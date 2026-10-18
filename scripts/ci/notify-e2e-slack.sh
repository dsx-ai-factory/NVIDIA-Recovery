#!/bin/sh

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

: "${SLACK_WEBHOOK_URL:?SLACK_WEBHOOK_URL is required.}"
: "${E2E_PROVIDER:?E2E_PROVIDER is required.}"
: "${E2E_OUTCOME:?E2E_OUTCOME is required.}"
: "${E2E_REPOSITORY:?E2E_REPOSITORY is required.}"
: "${E2E_SHA:?E2E_SHA is required.}"
: "${E2E_REF:?E2E_REF is required.}"
: "${E2E_EVENT:?E2E_EVENT is required.}"
: "${E2E_RUN_URL:?E2E_RUN_URL is required.}"

case "$E2E_OUTCOME" in
  passed)
    status_emoji="✅"
    ;;
  failed)
    status_emoji="❌"
    ;;
  *)
    echo "E2E_OUTCOME must be passed or failed." >&2
    exit 1
    ;;
esac

payload="{\"text\":\"${status_emoji} Heimdall E2E ${E2E_OUTCOME} on ${E2E_PROVIDER} for ${E2E_REPOSITORY}@${E2E_SHA} on ${E2E_REF} (${E2E_EVENT}): ${E2E_RUN_URL}\"}"
if ! response="$(curl --silent --show-error --fail-with-body --max-time 10 \
  --header 'Content-Type: application/json' \
  --data-raw "$payload" \
  "$SLACK_WEBHOOK_URL")"; then
  echo "Slack webhook request failed." >&2
  exit 1
fi
if [ "$response" != "ok" ]; then
  echo "Slack webhook returned an unexpected response." >&2
  exit 1
fi
echo "Slack E2E notification sent."
