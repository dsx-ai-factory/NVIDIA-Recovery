#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
typescript_package_dir="$(ci_typescript_package_dir)"

ci_require_command pnpm
ci_log "testing TypeScript package $typescript_package_dir"
pnpm --dir "$typescript_package_dir" test
