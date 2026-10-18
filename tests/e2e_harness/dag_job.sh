#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:10:00
set -euo pipefail

: "${DAG_JOB_BEHAVIOR:=success}"
: "${DAG_JOB_RUNTIME_SECONDS:=10}"

attempt="${SLURM_RESTART_COUNT:-0}"
echo "HEIMDALL_E2E_DAG job_id=${SLURM_JOB_ID} attempt=${attempt} behavior=${DAG_JOB_BEHAVIOR}"
sleep "$DAG_JOB_RUNTIME_SECONDS"

case "$DAG_JOB_BEHAVIOR" in
  success | long-running)
    exit 0
    ;;
  fail-once)
    if (( attempt == 0 )); then
      exit 1
    fi
    exit 0
    ;;
  fail-always)
    exit 1
    ;;
  *)
    echo "unsupported DAG_JOB_BEHAVIOR=${DAG_JOB_BEHAVIOR}" >&2
    exit 64
    ;;
esac
