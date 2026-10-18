#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

packages=()
go_test_args=()
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
            go_test_args+=("$@")
            break
            ;;
        -*)
            go_test_args+=("$1")
            shift
            ;;
        *)
            echo "unexpected positional argument: $1" >&2
            exit 2
            ;;
    esac
done

if [[ -n "${GO_TEST_ARGS:-}" ]]; then
    ci_require_command python3
    parsed_args_file="$(mktemp)"
    trap 'rm -f "$parsed_args_file"' EXIT
    if ! python3 - "$GO_TEST_ARGS" >"$parsed_args_file" <<'PY'; then
import shlex
import sys

try:
    args = shlex.split(sys.argv[1])
except ValueError as exc:
    print(f"invalid GO_TEST_ARGS: {exc}", file=sys.stderr)
    raise SystemExit(2) from exc

for arg in args:
    sys.stdout.buffer.write(arg.encode("utf-8") + b"\0")
PY
        exit 2
    fi
    while IFS= read -r -d '' test_arg; do
        go_test_args+=("$test_arg")
    done <"$parsed_args_file"
fi

ci_cd_root

if ((${#packages[@]} == 0)); then
    while IFS= read -r package; do
        packages+=("$package")
    done < <(ci_go_packages)
fi

for package in "${packages[@]}"; do
    package_dir="$(ci_go_package_dir "$package")"
    ci_require_command go
    ci_log "testing Go package $package in $package_dir"
    (
        cd "$package_dir"
        if ((${#go_test_args[@]} > 0)); then
            go test ./... "${go_test_args[@]}"
        else
            go test ./...
        fi
    )
done
