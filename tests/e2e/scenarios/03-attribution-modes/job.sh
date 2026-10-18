#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:10:00
#SBATCH --job-name=attribution-mode-recovery
#SBATCH --output=attribution-mode-%j.log
set -euo pipefail

: "${ATTRIBUTION_GATE:?owned staged gate path is required}"
: "${ATTRIBUTION_CRASH_JOB:?staged crash fixture is required}"

# The runner opens this gate only after the selected mode is established.
while [[ ! -f "$ATTRIBUTION_GATE" ]]; do sleep 1; done
exec bash "$ATTRIBUTION_CRASH_JOB"
