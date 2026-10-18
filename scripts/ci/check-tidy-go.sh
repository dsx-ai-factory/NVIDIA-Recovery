#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
ci_parse_go_packages "$@"

failed=0
for package in "${ci_selected_go_packages[@]}"; do
    package_dir="$(ci_go_package_dir "$package")"
    before="$(mktemp)"
    after="$(mktemp)"
    git diff --binary HEAD -- "$package_dir/go.mod" "$package_dir/go.sum" >"$before"

    "$SCRIPT_DIR/tidy-go.sh" --package "$package" || {
        rm -f "$before" "$after"
        failed=1
        continue
    }

    git diff --binary HEAD -- "$package_dir/go.mod" "$package_dir/go.sum" >"$after"
    if ! cmp -s "$before" "$after"; then
        echo "go mod tidy changed dependency files for $package:" >&2
        git status --short -- "$package_dir/go.mod" "$package_dir/go.sum" >&2
        rm -f "$before" "$after"
        failed=1
        continue
    fi
    rm -f "$before" "$after"
    printf 'ok: go.mod and go.sum are tidy for %s\n' "$package"
done

exit "$failed"
