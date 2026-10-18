#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:10:00
#SBATCH --job-name=checkpoint-stop-guardrail
set -euo pipefail

: "${JOB_MIN_RUNTIME:=20}"
: "${FAILURE_GATE_PREFIX:?FAILURE_GATE_PREFIX is required}"
: "${CHECKPOINT_ATTEMPT:=-1}"

attempt="${SLURM_RESTART_COUNT:-0}"
sleep "$JOB_MIN_RUNTIME"
if [[ "$attempt" == "$CHECKPOINT_ATTEMPT" ]]; then
  # Ordinary application output follows the collector -> Watcher -> JM path.
  echo "Done saving distributed checkpoint for checkpoint-stop-guardrail"
fi

# Let the test observe the exact running attempt and, where requested, wait for
# checkpoint persistence before this application failure becomes actionable.
while [[ ! -f "${FAILURE_GATE_PREFIX}.${attempt}" ]]; do
  echo "checkpoint-stop fixture running attempt=${attempt}"
  sleep 2
done
echo "checkpoint-stop fixture exiting with an unclassified application failure"
exit 1
