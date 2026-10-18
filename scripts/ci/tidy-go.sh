#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
ci_parse_go_packages "$@"

for package in "${ci_selected_go_packages[@]}"; do
    package_dir="$(ci_go_package_dir "$package")"
    ci_require_command go
    ci_log "tidying Go package $package in $package_dir"
    (
        cd "$package_dir"
        go mod tidy
    )
done
