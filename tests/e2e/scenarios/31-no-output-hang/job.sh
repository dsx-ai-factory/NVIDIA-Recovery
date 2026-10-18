#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:05:00
#SBATCH --job-name=no-output-hang-requeue
set -euo pipefail

: "${JOB_MIN_RUNTIME:=20}"
: "${JITTER_SEC:=0.3}"
: "${APP_LOG_PATH:?APP_LOG_PATH is required}"

sleep "$JOB_MIN_RUNTIME"
echo "Heimdall fault simulation: application stops producing output"
while IFS= read -r line; do
  echo "$line"
  sleep "$JITTER_SEC"
done < "$APP_LOG_PATH"
echo "HEIMDALL_E2E_NOOUTPUT job_id=${SLURM_JOB_ID} attempt=${SLURM_RESTART_COUNT:-0}: application output stopped"
while true; do
  sleep 60
done
