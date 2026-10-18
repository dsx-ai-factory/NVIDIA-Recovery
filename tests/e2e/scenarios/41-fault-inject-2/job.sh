#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:10:00
#SBATCH --job-name=fault-inject-2-requeue
set -euo pipefail

: "${JOB_MIN_RUNTIME:=20}"
: "${JITTER_SEC:=0.2}"
: "${RECOVERY_ATTEMPT_HOLD_SEC:=120}"
: "${SYSLOG_INGESTION_DELAY_SEC:=30}"
: "${APP_LOG_PATH:?APP_LOG_PATH is required}"
: "${KERNEL_LOG_PATH:?KERNEL_LOG_PATH is required}"
: "${SYSLOG_INJECTOR:?SYSLOG_INJECTOR is required}"
: "${SYSLOG_MARKER:?SYSLOG_MARKER is required}"

sleep "$JOB_MIN_RUNTIME"
if (( ${SLURM_RESTART_COUNT:-0} > 0 )); then
  echo "Heimdall recovery attempt running on ${SLURM_JOB_NODELIST:-<unknown>}"
  sleep "$RECOVERY_ATTEMPT_HOLD_SEC"
  exit 0
fi

echo "Heimdall fault injection: GPU fell off the bus, with kernel logs"
bash "$SYSLOG_INJECTOR"
sleep "$SYSLOG_INGESTION_DELAY_SEC"
while IFS= read -r line; do
  echo "$line"
  sleep "$JITTER_SEC"
done < "$APP_LOG_PATH"

echo "Rank0: GPU fell off the bus, aborting"
exit 1
