#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if (( $# != 1 )); then
  echo "usage: $0 ATTRIBUTION_SOURCE_DIR" >&2
  exit 2
fi

source_dir="$1"
dockerfile="$source_dir/service/service-app/Dockerfile"

: "${EXPECTED_REF:?Tilt must provide EXPECTED_REF}"
if [[ ! -f "$dockerfile" || ! -f "$source_dir/LICENSE" || ! -f "$source_dir/service/service-app/uv.lock" || ! -d "$source_dir/service/attribution-lib" ]]; then
  echo "error: upstream attribution service source is incomplete: $source_dir" >&2
  exit 1
fi

exec docker buildx build \
  --load \
  --platform linux/amd64 \
  --file "$dockerfile" \
  --ulimit nofile=65536:65536 \
  --progress=plain \
  --tag "$EXPECTED_REF" \
  "$source_dir"
