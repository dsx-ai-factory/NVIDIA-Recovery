#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_IMAGE_TAG="${BUILD_IMAGE_TAG:-dev}"
IMAGE="advice-operator:${BUILD_IMAGE_TAG}"

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is required"

if [[ "${BUILD_IMAGE:-1}" != "0" ]]; then
  IMAGE_PREFIX='' \
    IMAGE_TAG="$BUILD_IMAGE_TAG" \
    "$REPO_ROOT/scripts/build-images.sh" --load advice-operator
fi

docker image inspect "$IMAGE" >/dev/null 2>&1 || fail "missing image: $IMAGE"

user="$(docker image inspect --format '{{.Config.User}}' "$IMAGE")"
[[ "$user" == "65532:65532" ]] || fail "expected image user 65532:65532, found: $user"

entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "$IMAGE")"
[[ "$entrypoint" == '["/manager"]' ]] || fail "unexpected entrypoint: $entrypoint"

for label in \
  org.opencontainers.image.source \
  org.opencontainers.image.revision \
  org.opencontainers.image.version; do
  value="$(docker image inspect --format "{{index .Config.Labels \"$label\"}}" "$IMAGE")"
  [[ -n "$value" && "$value" != "<no value>" ]] || fail "missing OCI label: $label"
done

# Exercise the production executable without Kubernetes or backend connectivity.
docker run --rm --network none "$IMAGE" --help

printf 'ok: %s runtime contract\n' "$IMAGE"
