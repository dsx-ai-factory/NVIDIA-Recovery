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
    case "$package" in
        api-gateway)
            buf="$(ci_buf_path)" || {
                echo "buf is required; run make install-go-tools" >&2
                exit 127
            }
            ci_verify_buf_version "$buf"
            ci_log "generating Go protobuf and OpenAPI artifacts for $package"
            (
                cd "$package_dir"
                "$buf" generate --include-imports
            )
            ;;
        slurm-advisor-controller)
            ci_require_command go
            ci_log "running go generate for $package"
            (
                cd "$package_dir"
                go generate ./...
            )
            ;;
    esac
done
