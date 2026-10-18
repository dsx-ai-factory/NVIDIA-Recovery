#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

revision=""
if command -v git >/dev/null 2>&1; then
  revision="$(git rev-parse HEAD 2>/dev/null || true)"
fi
if [[ -n "$revision" ]]; then
  printf '%s\n' "$revision"
  exit 0
fi

if command -v jj >/dev/null 2>&1; then
  revision="$(
    jj log \
      -r 'coalesce(@ & ~empty(), @-)' \
      --no-graph \
      -T 'commit_id' \
      2>/dev/null || true
  )"
fi
if [[ -n "$revision" ]]; then
  printf '%s\n' "$revision"
  exit 0
fi

printf 'unknown\n'
