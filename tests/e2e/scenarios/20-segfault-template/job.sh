#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:10:00
#SBATCH --job-name=segfault-template-requeue
set -euo pipefail

: "${JOB_MIN_RUNTIME:=20}"
: "${JITTER_SEC:=0.2}"
: "${APP_LOG_PATH:?APP_LOG_PATH is required}"

sleep "$JOB_MIN_RUNTIME"
echo "Heimdall fault simulation: replay then segfault"
while IFS= read -r line; do
  echo "$line"
  sleep "$JITTER_SEC"
done < "$APP_LOG_PATH"
echo "HEIMDALL_E2E_CRASH job_id=${SLURM_JOB_ID} attempt=${SLURM_RESTART_COUNT:-0}: Rank0 segmentation fault"
exit 139
