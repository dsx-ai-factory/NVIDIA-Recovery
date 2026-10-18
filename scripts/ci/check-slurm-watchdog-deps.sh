#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd)
MODULE_DIR="${REPO_ROOT}/slurm-advisor-controller"
MODULE_PATH="github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller"

deps_file=
imports_file=
while (($# > 0)); do
    case "$1" in
        --deps-file)
            deps_file=$2
            shift 2
            ;;
        --imports-file)
            imports_file=$2
            shift 2
            ;;
        *)
            echo "unexpected argument: $1" >&2
            exit 2
            ;;
    esac
done

temp_dir=$(mktemp -d)
trap 'rm -rf "$temp_dir"' EXIT

if [[ -z "$deps_file" ]]; then
    deps_file="${temp_dir}/deps.txt"
    (
        cd "$MODULE_DIR"
        go list -deps ./cmd/slurm-watchdog
    ) >"$deps_file"
fi

if [[ -z "$imports_file" ]]; then
    imports_file="${temp_dir}/imports.txt"
    (
        cd "$MODULE_DIR"
        go list -deps -f "{{if .Module}}{{if eq .Module.Path \"${MODULE_PATH}\"}}{{join .Imports \"\\n\"}}{{end}}{{end}}" ./internal/watchdog ./internal/slurm
    ) >"$imports_file"
fi

failed=false
while IFS= read -r dependency; do
    [[ -n "$dependency" ]] || continue
    case "$dependency" in
        */api-gateway/gen/go/heimdall/v1)
            # The process uses generated AuditService bindings for delivery only.
            ;;
        */api-gateway | */api-gateway/* | */internal/heimdall | */internal/heimdall/* | */internal/reconcile | */internal/reconcile/*)
            echo "forbidden slurm-watchdog dependency: $dependency" >&2
            failed=true
            ;;
    esac
done <"$deps_file"

while IFS= read -r dependency; do
    [[ -n "$dependency" ]] || continue
    case "$dependency" in
        */api-gateway | */api-gateway/* | */internal/heimdall | */internal/heimdall/* | */internal/reconcile | */internal/reconcile/* | google.golang.org/grpc | google.golang.org/grpc/* | google.golang.org/protobuf | google.golang.org/protobuf/* | github.com/gogo/protobuf | github.com/gogo/protobuf/*)
            echo "forbidden watchdog scheduler-core import: $dependency" >&2
            failed=true
            ;;
    esac
done <"$imports_file"

if [[ "$failed" == true ]]; then
    exit 1
fi

echo "slurm-watchdog scheduler core is independent of Advisor and gateway transport"
