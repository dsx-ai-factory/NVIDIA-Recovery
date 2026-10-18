#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:05:00
#SBATCH --job-name=simple-segfault-requeue
#SBATCH --output=simple-segfault-%j.log
set -euo pipefail

: "${JOB_MIN_RUNTIME:=20}"
: "${FAULT_DELAY:=30}"
: "${POST_FAULT_DELAY:=60}"

sleep "$JOB_MIN_RUNTIME"
export FAULT_DELAY POST_FAULT_DELAY

srun --export=PATH,FAULT_DELAY,POST_FAULT_DELAY /bin/bash -c '
  set -euo pipefail
  echo "Heimdall fault simulation test"
  sleep "$FAULT_DELAY"
  echo "HEIMDALL_E2E_CRASH job_id=${SLURM_JOB_ID} attempt=${SLURM_RESTART_COUNT:-0}: Rank0 segmentation fault"
  sleep "$POST_FAULT_DELAY"
  exit 1
'
