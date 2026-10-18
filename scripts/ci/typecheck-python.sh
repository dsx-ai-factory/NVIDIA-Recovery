#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
ci_configure_uv

packages=()
while (($# > 0)); do
    case "$1" in
        --package)
            if (($# < 2)); then
                echo "missing value for --package" >&2
                exit 2
            fi
            packages+=("$2")
            shift 2
            ;;
        *)
            echo "unexpected argument: $1" >&2
            exit 2
            ;;
    esac
done

if ((${#packages[@]} == 0)); then
    while IFS= read -r package; do
        packages+=("$package")
    done < <(ci_python_packages)
fi

failed=()
for package in "${packages[@]}"; do
    if ! package_dir="$(ci_package_dir "$package")"; then
        exit 2
    fi

    ci_log "type checking Python package $package"
    if ! (cd "$package_dir" && uv run --locked --package "$package" mypy "$package_dir"); then
        failed+=("$package")
    fi
done

if ((${#failed[@]} > 0)); then
    echo "type check failed for: ${failed[*]}" >&2
    exit 1
fi
