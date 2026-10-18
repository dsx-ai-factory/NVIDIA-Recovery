#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
ci_require_command go
ci_load_go_tool_versions

tools_bin="$(ci_go_tools_bin)"
mkdir -p "$tools_bin"

buf="$tools_bin/buf"
if [[ -x "$buf" ]] && ci_verify_buf_version "$buf" >/dev/null 2>&1; then
    ci_log "buf $BUF_VERSION is already installed"
else
    ci_log "installing buf $BUF_VERSION"
    GOBIN="$tools_bin" go install -p=1 \
        "github.com/bufbuild/buf/cmd/buf@v${BUF_VERSION#v}"
fi
ci_verify_buf_version "$buf"

ci_log "installing golangci-lint $GOLANGCI_LINT_VERSION"
GOBIN="$tools_bin" go install -p=1 \
    "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$GOLANGCI_LINT_VERSION"
ci_verify_golangci_lint_version "$tools_bin/golangci-lint"
