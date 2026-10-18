#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:05:00
#SBATCH --job-name=eager-requeue
set -euo pipefail

: "${JOB_MIN_RUNTIME:=20}"

sleep "$JOB_MIN_RUNTIME"
if (( ${SLURM_RESTART_COUNT:-0} == 0 )); then
  echo "Eager recovery fixture exits unsuccessfully on its first attempt"
  exit 1
fi

echo "Eager recovery successor completed successfully"
