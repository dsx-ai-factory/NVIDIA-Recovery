#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
package_dir="$(ci_go_package_dir api-gateway)"

ci_log "checking all component-owned generated artifacts for api-gateway"
ci_check_no_generated_drift \
    "api-gateway bindings" \
    "$package_dir/gen/go" \
    "$package_dir/gen/openapiv2" \
    "$package_dir/gen/jobmanager" \
    "$package_dir/gen/notifications"
