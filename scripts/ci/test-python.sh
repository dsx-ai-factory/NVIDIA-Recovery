#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
ci_configure_uv

packages=()
ci_reports=false
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
        --ci-reports)
            ci_reports=true
            shift
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

if ((${#packages[@]} == 0)); then
    while IFS= read -r package; do
        packages+=("$package")
    done < <(ci_python_packages)
fi

run_package_tests() {
    local package="$1"
    local package_dir
    local report_dir
    local repo_root

    if ! package_dir="$(ci_package_dir "$package")"; then
        exit 2
    fi

    repo_root="$(ci_repo_root)"

    ci_log "testing Python package $package"
    if [[ "$ci_reports" == true ]]; then
        report_dir="$(ci_report_dir "$package")"
        (
            cd "$package_dir"
            uv run --locked --package "$package" pytest -q \
                --ignore=tests/integration/ \
                --junitxml "$repo_root/$report_dir/junit.xml" \
                --cov \
                --cov-report term \
                --cov-report "xml:$repo_root/$report_dir/coverage.xml"
        )
    else
        (
            cd "$package_dir"
            uv run --locked --package "$package" pytest -q \
                --ignore=tests/integration/
        )
    fi
}

for package in "${packages[@]}"; do
    run_package_tests "$package"
done
