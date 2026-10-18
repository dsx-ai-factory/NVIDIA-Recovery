#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:03:00
#SBATCH --job-name=healthy-negative-control
set -euo pipefail

: "${JOB_MIN_RUNTIME:=20}"

echo "HEIMDALL_E2E_HEALTHY job_id=${SLURM_JOB_ID} attempt=${SLURM_RESTART_COUNT:-0}: starting"
sleep "$JOB_MIN_RUNTIME"
echo "HEIMDALL_E2E_HEALTHY job_id=${SLURM_JOB_ID} attempt=${SLURM_RESTART_COUNT:-0}: completed"
