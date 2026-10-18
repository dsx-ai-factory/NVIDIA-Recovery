#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DOCKERFILE="$REPO_ROOT/slurm-advisor-controller/Dockerfile"
BUILD_IMAGE_TAG="${BUILD_IMAGE_TAG:-dev}"
IMAGE="slurm-advisor-controller:${BUILD_IMAGE_TAG}"
SLURM_IMAGE="${SLURM_ADVISOR_CONTROLLER_SLURM_IMAGE:-ghcr.io/slinkyproject/sackd:25.05-ubuntu24.04@sha256:dd1f0b6030229b3d602e3d80e6d50cc4f9fd17b325312521dfa07376fffc7569}"

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is required"
[[ -f "$DOCKERFILE" ]] || fail "missing production Dockerfile: $DOCKERFILE"

if [[ "${BUILD_IMAGE:-1}" != "0" ]]; then
  IMAGE_PREFIX='' \
    IMAGE_TAG="$BUILD_IMAGE_TAG" \
    SLURM_ADVISOR_CONTROLLER_SLURM_IMAGE="$SLURM_IMAGE" \
    "$REPO_ROOT/scripts/build-images.sh" --load slurm-advisor-controller
fi

docker image inspect "$IMAGE" >/dev/null 2>&1 || fail "missing image: $IMAGE"

user="$(docker image inspect --format '{{.Config.User}}' "$IMAGE")"
[[ "$user" == "401:401" ]] || fail "expected image user 401:401, found: $user"

entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "$IMAGE")"
[[ "$entrypoint" == '["/usr/local/bin/slurm-advisor-controller"]' ]] ||
  fail "unexpected entrypoint: $entrypoint"

for label in \
  org.opencontainers.image.source \
  org.opencontainers.image.revision \
  org.opencontainers.image.version; do
  value="$(docker image inspect --format "{{index .Config.Labels \"$label\"}}" "$IMAGE")"
  [[ -n "$value" && "$value" != "<no value>" ]] || fail "missing OCI label: $label"
done

docker run --rm --entrypoint /bin/sh "$IMAGE" -ec '
  printf "ClusterName=contract\nSlurmctldHost=localhost\nAuthType=auth/slurm\nCredType=cred/slurm\n" >/tmp/slurm.conf
  export SLURM_CONF=/tmp/slurm.conf
  test "$(id -u)" = 401
  test "$(id -g)" = 401
  test -x /usr/local/bin/slurm-advisor-controller
  test ! -e /usr/local/bin/mock-heimdall-gateway
  test ! -e /usr/bin/sbatch
  test -f /usr/lib/*/slurm/accounting_storage_slurmdbd.so
  test -f /usr/lib/*/slurm/data_parser_v0_0_40.so
  for command in squeue sacct scontrol scancel; do
    test -x "/usr/bin/$command"
    "$command" --version
    if ldd "/usr/bin/$command" | grep -F "not found"; then
      exit 1
    fi
  done
  for plugin in /usr/lib/*/slurm/*.so; do
    if ldd "$plugin" | grep -F "not found"; then
      exit 1
    fi
  done
'

controller_versions="$(docker run --rm --entrypoint /bin/sh "$IMAGE" -ec '
  printf "ClusterName=contract\nSlurmctldHost=localhost\nAuthType=auth/slurm\nCredType=cred/slurm\n" >/tmp/slurm.conf
  export SLURM_CONF=/tmp/slurm.conf
  squeue --version
  sacct --version
  scontrol --version
  scancel --version
')"
if [[ "$(printf '%s\n' "$controller_versions" | sort -u | wc -l)" -ne 1 ]]; then
  fail "packaged Slurm commands report different versions: $controller_versions"
fi

donor_version="$(docker run --rm --entrypoint /bin/sh "$SLURM_IMAGE" -ec '
  printf "ClusterName=contract\nSlurmctldHost=localhost\nAuthType=auth/slurm\nCredType=cred/slurm\n" >/tmp/slurm.conf
  SLURM_CONF=/tmp/slurm.conf scontrol --version
')"
packaged_version="$(printf '%s\n' "$controller_versions" | head -n 1)"
[[ "$packaged_version" == "$donor_version" ]] ||
  fail "packaged Slurm version '$packaged_version' does not match donor '$donor_version'"

printf 'ok: %s runtime contract (%s)\n' "$IMAGE" "$packaged_version"
