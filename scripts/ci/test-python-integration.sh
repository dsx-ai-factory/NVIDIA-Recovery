#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
ci_configure_uv

package=""
ci_reports=false

while (($# > 0)); do
    case "$1" in
        --package)
            if (($# < 2)); then
                echo "missing value for --package" >&2
                exit 2
            fi
            package="$2"
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

case "$package" in
    recovery-libs)
        test_path="recovery_libs/tests/integration"
        report_name="recovery-libs-integration"
        ;;
    notifications)
        test_path="notifications/tests/integration"
        report_name="notifications-integration"
        ;;
    "")
        echo "missing required --package (recovery-libs or notifications)" >&2
        exit 2
        ;;
    *)
        echo "unsupported integration test package: $package" >&2
        exit 2
        ;;
esac

repo_root="$(ci_repo_root)"
args=("$repo_root/$test_path" -v)

if [[ "$ci_reports" == true ]]; then
    report_dir="$(ci_report_dir "$report_name")"
    args+=(--junitxml "$repo_root/$report_dir/junit.xml")
fi

ci_log "testing Python package $package integration suite"
uv run --locked --package "$package" pytest "${args[@]}"
