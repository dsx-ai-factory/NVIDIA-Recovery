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
export GOLANGCI_LINT_CACHE="${GOLANGCI_LINT_CACHE:-$(ci_repo_root)/.cache/golangci-lint}"
mkdir -p "$GOLANGCI_LINT_CACHE"
golangci_lint="$(ci_golangci_lint_path)" || {
    echo "golangci-lint is required; run make install-go-tools" >&2
    exit 127
}
ci_verify_golangci_lint_version "$golangci_lint"
golangci_config="$(ci_repo_root)/.golangci.yml"

if ((${#packages[@]} == 0)); then
    while IFS= read -r package; do
        packages+=("$package")
    done < <(ci_go_packages)
fi

for package in "${packages[@]}"; do
    package_dir="$(ci_go_package_dir "$package")"
    ci_require_command gofmt
    ci_log "running golangci-lint for Go package $package in $package_dir"
    (
        cd "$package_dir"
        go_files=()
        while IFS= read -r -d '' go_file; do
            go_files+=("$go_file")
        done < <(find . -type f -name '*.go' -not -path './vendor/*' -not -path './gen/*' -print0)
        if ((${#go_files[@]} > 0)); then
            unformatted="$(gofmt -l "${go_files[@]}")"
            if [[ -n "$unformatted" ]]; then
                echo "Go files need gofmt in $package_dir:" >&2
                echo "$unformatted" >&2
                exit 1
            fi
        fi
        "$golangci_lint" run --config "$golangci_config" ./...
        ci_assert_lint_left_tracked_files_clean "Go package $package" .
    )
done
