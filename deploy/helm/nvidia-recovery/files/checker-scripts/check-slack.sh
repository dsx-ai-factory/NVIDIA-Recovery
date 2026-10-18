#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
. /scripts/lib.common.sh
ensure_tools; ensure_textfile_dir
if [[ "${SLACK_ENABLED:-false}" != "true" || -z "${SLACK_TOKEN:-}" ]]; then exit 1; fi
if curl --max-time 5 --connect-timeout 5 -sSfL -H "Authorization: Bearer ${SLACK_TOKEN}" https://slack.com/api/auth.test | grep -q '"ok":true'; then
  write_prom_file dependency_checker_slack.prom <<<'dependency_checker_test_status{test="slack"} 1'
  exit 0
else
  write_prom_file dependency_checker_slack.prom <<<'dependency_checker_test_status{test="slack"} 0'
  exit 1
fi
