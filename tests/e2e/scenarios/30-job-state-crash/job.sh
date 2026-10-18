#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:05:00
#SBATCH --job-name=job-state-crash-requeue
set -euo pipefail

: "${JOB_MIN_RUNTIME:=20}"
: "${JITTER_SEC:=0.2}"
: "${FAILURE_DELAY:?FAILURE_DELAY is required}"
: "${APP_LOG_PATH:?APP_LOG_PATH is required}"

sleep "$JOB_MIN_RUNTIME"
echo "Heimdall state-based recovery fixture: fail without a recognized crash line"
while IFS= read -r line; do
  echo "$line"
  sleep "$JITTER_SEC"
done < "$APP_LOG_PATH"
echo "fixture exiting with an unclassified application failure"
sleep "$FAILURE_DELAY"
exit 1
