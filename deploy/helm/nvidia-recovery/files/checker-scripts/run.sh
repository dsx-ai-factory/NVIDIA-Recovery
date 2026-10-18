#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
. /scripts/lib.common.sh
ensure_tools; ensure_textfile_dir
tests=(db log-query prometheus slack fqdn)
ok=0; total=0
for t in "${tests[@]}"; do
  total=$((total+1))
  if "/scripts/check-${t}.sh"; then ok=$((ok+1)); fi
done
ts=$(date +%s)
cat <<EOF | write_prom_file dependency_checker_summary.prom
# HELP dependency_checker_last_run_timestamp_seconds Unix time of last check run
# TYPE dependency_checker_last_run_timestamp_seconds gauge
dependency_checker_last_run_timestamp_seconds ${ts}
# HELP dependency_checker_tests_total Total number of checks in last run
# TYPE dependency_checker_tests_total gauge
dependency_checker_tests_total ${total}
# HELP dependency_checker_passed_tests Number of successful checks in last run
# TYPE dependency_checker_passed_tests gauge
dependency_checker_passed_tests ${ok}
# HELP dependency_checker_failed_tests Number of failed checks in last run
# TYPE dependency_checker_failed_tests gauge
dependency_checker_failed_tests $((total-ok))
EOF
