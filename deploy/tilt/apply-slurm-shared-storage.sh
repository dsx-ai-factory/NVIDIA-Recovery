#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if (( $# != 3 )); then
  echo "usage: $0 SOURCE DEST SLURM_SHARED_HOME" >&2
  exit 2
fi

source_file="$1"
dest_file="$2"
slurm_shared_home="$3"

"$(dirname "$0")/render.sh" \
  "$source_file" \
  "$dest_file" \
  '__SLURM_SHARED_HOME__' \
  "$slurm_shared_home"

kubectl create namespace slurm --dry-run=client -o yaml \
  | kubectl apply -f -
kubectl apply -f "$dest_file"
