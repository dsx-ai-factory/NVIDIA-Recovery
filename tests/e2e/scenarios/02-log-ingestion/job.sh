#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:05:00
#SBATCH --job-name=log-ingestion-qualification
set -euo pipefail

: "${APPLICATION_MESSAGES:?APPLICATION_MESSAGES is required}"
: "${SYSTEM_MESSAGES:?SYSTEM_MESSAGES is required}"

# The collector reads ordinary Slurm output and the same node syslog mount as
# hardware-fault scenarios. There is no direct connection to either log store.
cat "$APPLICATION_MESSAGES"
cat "$SYSTEM_MESSAGES" >> /var/log/syslog
# Keep allocation identity observable while the collector and evidence queries
# converge; managed_job cleanup cancels only this captured allocation.
sleep 240
