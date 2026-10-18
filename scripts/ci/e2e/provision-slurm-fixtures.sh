#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Publish e2e fixtures to the Slurm shared home, mirroring their paths under
# tests/e2e/scenarios and tests/e2e_harness, and replace any earlier copy.
set -euo pipefail

namespace="${1:-slurm}"
fixtures_root="${2:-/home/e2e/fixtures}"
tests_dir="$(cd "$(dirname "$0")/../../../tests/e2e" && pwd)"

login_pod="$(kubectl get pod \
  --namespace "$namespace" \
  --selector app.kubernetes.io/component=login \
  --output 'jsonpath={.items[0].metadata.name}')"
if [[ -z "$login_pod" ]]; then
  printf 'error: Slurm login pod was not found in namespace %s\n' "$namespace" >&2
  exit 1
fi

# shellcheck disable=SC2016 # expanded by the remote shell
replace_root='set -euo pipefail
mkdir -p -- "$(dirname -- "$1")"
staging="$(mktemp -d "$1.XXXXXX")"
tar -C "$staging" --no-same-owner -xf -
chmod -R a+rX "$staging"
rm -rf -- "$1"
mv -- "$staging" "$1"'

# Jobs and the Python harness are not fixtures; everything else is data a job may read.
(cd "$tests_dir" && COPYFILE_DISABLE=1 tar --no-xattrs \
  --exclude='*.py' --exclude='__pycache__' --exclude='job.sh' \
  -cf - scenarios -C .. e2e_harness) \
  | kubectl exec -i --namespace "$namespace" "$login_pod" -- bash -c "$replace_root" _ "$fixtures_root"
printf 'published e2e fixtures to %s:%s\n' "$login_pod" "$fixtures_root"
