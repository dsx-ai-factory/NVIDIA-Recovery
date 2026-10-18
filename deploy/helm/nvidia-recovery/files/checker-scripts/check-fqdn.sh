#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
. /scripts/lib.common.sh
ensure_tools; ensure_textfile_dir
if [[ -z "${FQDN_HOST:-}" ]]; then exit 1; fi
url="https://${FQDN_HOST}${FQDN_PATH:-/}"
if curl --max-time 5 --connect-timeout 5 -skfL "$url" >/dev/null; then
  write_prom_file dependency_checker_fqdn.prom <<<'dependency_checker_test_status{test="fqdn"} 1'
  exit 0
else
  write_prom_file dependency_checker_fqdn.prom <<<'dependency_checker_test_status{test="fqdn"} 0'
  exit 1
fi
