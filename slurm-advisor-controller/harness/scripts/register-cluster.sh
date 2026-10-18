#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
HARNESS_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)

cd "${HARNESS_DIR}"
docker compose exec -T slurmctld sacctmgr --immediate add cluster name=linux || true
docker compose restart slurmdbd slurmctld
