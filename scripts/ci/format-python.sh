#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
ci_configure_uv
ci_parse_python_check_args "$@"
if ! ci_resolve_python_targets; then
    exit 0
fi

if [[ "$ci_fix" == true ]]; then
    uv run --locked ruff format "${ci_targets[@]}"
else
    uv run --locked ruff format --check "${ci_targets[@]}"
fi
