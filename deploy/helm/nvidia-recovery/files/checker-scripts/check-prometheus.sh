#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
. /scripts/lib.common.sh
ensure_tools; ensure_textfile_dir
if [[ -z "${PROM_URL:-}" ]]; then exit 1; fi
if curl --max-time 5 --connect-timeout 5 -sSfL ${PROM_TOKEN:+-H "Authorization: Bearer ${PROM_TOKEN}"} ${PROM_TENANT:+-H "X-Scope-OrgID: ${PROM_TENANT}"} \
  --get --data-urlencode "query=up" "${PROM_URL}/api/v1/query" >/dev/null; then
  write_prom_file dependency_checker_prometheus.prom <<<'dependency_checker_test_status{test="prometheus"} 1'
  exit 0
else
  write_prom_file dependency_checker_prometheus.prom <<<'dependency_checker_test_status{test="prometheus"} 0'
  exit 1
fi
