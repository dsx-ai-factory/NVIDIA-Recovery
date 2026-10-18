#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:05:00
#SBATCH --job-name=collector-file-lifecycle
#SBATCH --output=collector-lifecycle-%j.log
set -euo pipefail

: "${MESSAGES_DIRECTORY:?MESSAGES_DIRECTORY is required}"
output="/home/logs/collector-lifecycle-${SLURM_JOB_ID}.log"

wait_for_phase() {
    while [[ ! -f "${MESSAGES_DIRECTORY}/$1.ready" ]]; do sleep 1; done
}

cat "${MESSAGES_DIRECTORY}/initial.txt"
wait_for_phase append
cat "${MESSAGES_DIRECTORY}/append.txt" >> "$output"

# Shrink below the collected offset before writing the next marker. Open each
# write with append mode instead of reusing Slurm's original stdout offset.
wait_for_phase truncate
: > "$output"
cat "${MESSAGES_DIRECTORY}/truncate.txt" >> "$output"

# The Advisor link keeps the same target path while the job replaces its inode.
wait_for_phase rotate
mv -- "$output" "${output}.previous"
cat "${MESSAGES_DIRECTORY}/rotate.txt" > "$output"
sleep 240
