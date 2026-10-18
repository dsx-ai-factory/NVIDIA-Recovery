#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

if (($# != 1)); then
    echo "usage: scripts/ci/convert-go-coverage.sh <coverage.out>" >&2
    exit 2
fi

coverage_path="$1"
if [[ "$coverage_path" != *.out ]]; then
    echo "coverage profile must end in .out: $coverage_path" >&2
    exit 2
fi
if [[ ! -f "$coverage_path" ]]; then
    echo "coverage profile does not exist: $coverage_path" >&2
    exit 1
fi

ci_require_command gocover-cobertura

coverage_dir="$(cd "$(dirname "$coverage_path")" && pwd)"
coverage_file="$(basename "$coverage_path")"
cobertura_file="${coverage_file%.out}.xml"

ci_log "converting Go coverage profile $coverage_path to $coverage_dir/$cobertura_file"
(
    cd "$coverage_dir"
    gocover-cobertura < "$coverage_file" > "$cobertura_file"
)
