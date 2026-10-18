#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ "${SLURM_RESTART_COUNT:-0}" -eq 0 ]]; then
  exit 1
fi

sleep 300
