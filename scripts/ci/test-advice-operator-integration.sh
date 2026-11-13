#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Run controller/syncer integration tests against an isolated envtest API server.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

ci_cd_root
KUBEBUILDER_ASSETS="$("$SCRIPT_DIR/setup-advice-operator-envtest.sh")"
export KUBEBUILDER_ASSETS
cd advice-operator
exec go test -race -tags=integration -timeout=5m ./test/integration "$@"
