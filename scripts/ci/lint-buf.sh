#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

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
        --)
            shift
            if (($# > 0)); then
                echo "unexpected positional arguments: $*" >&2
                exit 2
            fi
            ;;
        -*)
            echo "unsupported option: $1" >&2
            exit 2
            ;;
        *)
            echo "unexpected positional argument: $1" >&2
            exit 2
            ;;
    esac
done

ci_cd_root

if ((${#packages[@]} == 0)); then
    while IFS= read -r package; do
        packages+=("$package")
    done < <(ci_go_packages)
fi

buf="$(ci_buf_path)" || {
    echo "buf is required; run make install-go-tools" >&2
    exit 127
}
ci_verify_buf_version "$buf"

for package in "${packages[@]}"; do
    package_dir="$(ci_go_package_dir "$package")"
    ci_log "running buf lint for Go package $package in $package_dir"
    (
        cd "$package_dir"
        "$buf" lint
    )
done
