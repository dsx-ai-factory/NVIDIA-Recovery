#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:20:00
set -euo pipefail

: "${JOB_MIN_RUNTIME:=20}"
: "${FAILURE_GATE_PREFIX:?FAILURE_GATE_PREFIX is required}"
: "${SLURM_ARRAY_TASK_ID:?this workload must be submitted as an array}"

attempt="${SLURM_RESTART_COUNT:-0}"
sleep "$JOB_MIN_RUNTIME"
if [[ "$SLURM_ARRAY_TASK_ID" == 0 && "$attempt" == 1 ]]; then
  echo "Done saving distributed checkpoint for job-array-recovery"
fi

# Each task/attempt has its own gate. The sibling remains alive while this task fails.
while [[ ! -f "${FAILURE_GATE_PREFIX}.${SLURM_ARRAY_TASK_ID}.${attempt}" ]]; do
  echo "array fixture task=${SLURM_ARRAY_TASK_ID} attempt=${attempt}"
  sleep 2
done
echo "array fixture exiting with an unclassified application failure"
exit 1
