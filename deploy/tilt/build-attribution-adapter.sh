#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if (( $# != 1 )); then
  echo "usage: $0 ATTRIBUTION_SOURCE_DIR" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
client_dir="$1/service/service-app/nvidia_attribution_client"

: "${EXPECTED_REF:?Tilt must provide EXPECTED_REF}"
if [[ ! -f "$client_dir/pyproject.toml" || ! -d "$client_dir/nvidia_attribution_client" ]]; then
  echo "error: upstream generated client is missing from $client_dir" >&2
  exit 1
fi

exec docker buildx build \
  --load \
  --platform linux/amd64 \
  --file "$repo_root/attribution_adapter/Dockerfile" \
  --build-context "attribution-client=$client_dir" \
  --ulimit nofile=65536:65536 \
  --progress=plain \
  --tag "$EXPECTED_REF" \
  "$repo_root"
