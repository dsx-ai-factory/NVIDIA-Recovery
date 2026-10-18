#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Compatibility entry point. The per-dependency runner owns the current
# implementation, including backend-neutral log-query checks.
set -euo pipefail
exec /scripts/run.sh
