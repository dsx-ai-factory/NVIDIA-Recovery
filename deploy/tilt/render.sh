#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if (( $# < 4 || $# % 2 != 0 )); then
  echo "usage: $0 SOURCE DEST PLACEHOLDER VALUE [PLACEHOLDER VALUE ...]" >&2
  exit 2
fi

source_file="$1"
dest_file="$2"
shift 2

sed_args=()
while (( $# > 0 )); do
  placeholder="$1"
  replacement="$2"
  shift 2

  replacement="${replacement//\\/\\\\}"
  replacement="${replacement//&/\\&}"
  replacement="${replacement//|/\\|}"
  sed_args+=(-e "s|${placeholder}|${replacement}|g")
done

mkdir -p "$(dirname "$dest_file")"
tmp_file="${dest_file}.tmp"
trap 'rm -f "$tmp_file"' EXIT
sed "${sed_args[@]}" "$source_file" > "$tmp_file"
mv "$tmp_file" "$dest_file"
trap - EXIT
