#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#SBATCH --time=00:05:00
set -euo pipefail

: "${MARKER:?MARKER is required}"
printf '%s\n' "$MARKER"
# Keep the allocation active while asynchronous path publication and collection converge.
sleep 240
