#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
. /scripts/lib.common.sh
ensure_tools; ensure_textfile_dir
if mysql --connect-timeout=5 -h"${DB_HOST}" -P"${DB_PORT}" -u"${DB_USER}" -p"${DB_PASSWORD:-}" -e "SELECT 1" "${DB_NAME}" >/dev/null 2>&1; then
  write_prom_file dependency_checker_db.prom <<<'dependency_checker_test_status{test="db"} 1'
  exit 0
else
  write_prom_file dependency_checker_db.prom <<<'dependency_checker_test_status{test="db"} 0'
  exit 1
fi
