#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# load-images.sh — load a set of Docker images into the cluster.
#
# Controlled by LOAD_IMAGES (default on). Loads exactly the repo:tags in
# E2E_IMAGES_TO_LOAD (see env.sh); e2e.sh is responsible for populating that set
# from the build phase, the caller, or both. This script holds no service list
# of its own, so there is nothing to keep in sync with the build tooling.
#
# Images not present locally are skipped with a warning rather than failing the
# run, so a partial build does not abort the pipeline.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
source "$SCRIPT_DIR/lib.sh"

# ---------------------------------------------------------------------------
# load_images — entry point called by e2e.sh
# ---------------------------------------------------------------------------
load_images() {
  banner "Phase: Load images"

  if ! enabled "$LOAD_IMAGES"; then
    log "LOAD_IMAGES is empty — skipping image load"
    return
  fi

  # The caller supplies the image set via E2E_IMAGES_TO_LOAD; nothing to do otherwise.
  local -a images=()
  read -ra images <<< "${E2E_IMAGES_TO_LOAD:-}"
  if [[ ${#images[@]} -eq 0 ]]; then
    warn "LOAD_IMAGES is set but E2E_IMAGES_TO_LOAD is empty — no images to load"
    return
  fi

  require_cmd docker "Install: https://docs.docker.com/get-docker/"
  require_cmd kind   "Run prepare.sh first"

  local loaded=0 skipped=0 image
  for image in "${images[@]}"; do
    # A bare name (no tag) gets the default BUILD_IMAGE_TAG.
    [[ "$image" == *:* ]] || image="${image}:${BUILD_IMAGE_TAG}"

    if docker image inspect "$image" &>/dev/null; then
      log "Loading $image → cluster '$CLUSTER_NAME' ..."
      kind load docker-image "$image" --name "$CLUSTER_NAME"
      (( loaded++ )) || true
    else
      warn "Image $image not found locally — skipping"
      (( skipped++ )) || true
    fi
  done

  success "Images loaded: $loaded  /  Skipped (not present): $skipped"
}
