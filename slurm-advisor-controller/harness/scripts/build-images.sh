#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
HARNESS_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)
SERVICE_DIR=$(cd -- "${HARNESS_DIR}/.." && pwd)
REPO_ROOT=$(cd -- "${HARNESS_DIR}/../.." && pwd)

IMAGE_TAG=${IMAGE_TAG:-dev}
SLURM_TAG=${SLURM_TAG:-slurm-25-05-8-1}
SLURM_BASE_IMAGE=slurm-advisor-controller-slurm-base:${IMAGE_TAG}
CONTROLLER_IMAGE=slurm-advisor-controller-controller:${IMAGE_TAG}
WATCHDOG_IMAGE=slurm-watchdog:${IMAGE_TAG}

docker_build() {
    local docker_args=(buildx build)
    if [[ -n "${DOCKER_BUILD_PLATFORM:-}" ]]; then
        docker_args+=(--platform "${DOCKER_BUILD_PLATFORM}")
    fi
    if [[ -n "${DOCKER_BUILD_NETWORK:-}" ]]; then
        docker_args+=(--network "${DOCKER_BUILD_NETWORK}")
    fi
    local proxy_env
    for proxy_env in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
        if [[ -n "${!proxy_env:-}" ]]; then
            docker_args+=(--build-arg "${proxy_env}=${!proxy_env}")
        fi
    done

    docker "${docker_args[@]}" "$@"
}

docker_build \
    --build-arg "SLURM_TAG=${SLURM_TAG}" \
    --progress=plain \
    --load \
    -t "${SLURM_BASE_IMAGE}" \
    -f "${HARNESS_DIR}/Dockerfile.slurm-base" \
    "${REPO_ROOT}"

docker_build \
    --build-arg "BASE_IMAGE=${SLURM_BASE_IMAGE}" \
    --progress=plain \
    --load \
    -t "${CONTROLLER_IMAGE}" \
    -f "${HARNESS_DIR}/Dockerfile.controller" \
    "${REPO_ROOT}"

docker_build \
    --build-arg "BASE_IMAGE=${SLURM_BASE_IMAGE}" \
    --progress=plain \
    --load \
    -t "${WATCHDOG_IMAGE}" \
    -f "${SERVICE_DIR}/Dockerfile.watchdog" \
    "${REPO_ROOT}"
