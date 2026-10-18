#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
typescript_package_dir="$(ci_typescript_package_dir)"

ci_require_command pnpm
ci_log "linting TypeScript package $typescript_package_dir"
pnpm --dir "$typescript_package_dir" lint
ci_assert_lint_left_tracked_files_clean "TypeScript package $typescript_package_dir" "$typescript_package_dir"
