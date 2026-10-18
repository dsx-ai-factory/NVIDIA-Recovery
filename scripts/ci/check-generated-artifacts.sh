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

check_api_gateway() {
    local package_dir
    package_dir="$(ci_go_package_dir api-gateway)" || return 1

    ci_log "checking generated protobuf, gRPC, gateway, and OpenAPI artifacts for api-gateway"
    "$SCRIPT_DIR/generate-go.sh" --package api-gateway || return 1

    ci_check_no_generated_drift \
        "api-gateway" \
        "$package_dir/gen/go" \
        "$package_dir/gen/openapiv2" || return 1
}

check_slurm_advisor_controller() {
    local package_dir generated_file
    local generated_files=()
    package_dir="$(ci_go_package_dir slurm-advisor-controller)" || return 1

    ci_require_command go || return 1
    ci_log "checking generated Go artifacts for slurm-advisor-controller"
    "$SCRIPT_DIR/generate-go.sh" --package slurm-advisor-controller || return 1

    while IFS= read -r generated_file; do
        generated_files+=("$generated_file")
    done < <(ci_go_generated_files "$package_dir")

    if ((${#generated_files[@]} == 0)); then
        echo "no generated Go artifacts found for slurm-advisor-controller" >&2
        return 1
    fi

    ci_check_no_generated_drift \
        "slurm-advisor-controller" \
        "${generated_files[@]}" || return 1
}

failed=0
for package in "${packages[@]}"; do
    case "$package" in
        api-gateway)
            check_api_gateway || failed=1
            ;;
        slurm-advisor-controller)
            check_slurm_advisor_controller || failed=1
            ;;
        *)
            echo "generated-artifact check does not support package: $package" >&2
            failed=1
            ;;
    esac
done

exit "$failed"
