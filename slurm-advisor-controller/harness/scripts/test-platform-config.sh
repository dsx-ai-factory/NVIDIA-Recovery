#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
HARNESS_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)

TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT

cat >"${TMP_DIR}/docker" <<'EOF'
#!/usr/bin/env bash
printf '%q ' "$@" >>"${DOCKER_CALL_LOG}"
printf '\n' >>"${DOCKER_CALL_LOG}"
EOF
chmod +x "${TMP_DIR}/docker"

fail() {
    echo "test-platform-config: $*" >&2
    exit 1
}

run_build_images() {
    PATH="${TMP_DIR}:${PATH}" \
        DOCKER_CALL_LOG="${DOCKER_CALL_LOG}" \
        IMAGE_TAG=test-platform-config \
        SLURM_TAG=test-slurm-tag \
        "$@"
}

DOCKER_CALL_LOG="${TMP_DIR}/default-platform.log"
run_build_images "${SCRIPT_DIR}/build-images.sh"

if grep -q -- '--platform' "${DOCKER_CALL_LOG}"; then
    fail "build-images.sh should not force a Docker platform by default"
fi
if ! grep -q -- 'Dockerfile.watchdog' "${DOCKER_CALL_LOG}"; then
    fail "build-images.sh should build the standalone watchdog image"
fi
if ! grep -q -- 'BASE_IMAGE=slurm-advisor-controller-slurm-base:test-platform-config' "${DOCKER_CALL_LOG}"; then
    fail "watchdog harness build should override the runtime with the local Slurm base"
fi

DOCKER_CALL_LOG="${TMP_DIR}/explicit-platform.log"
run_build_images env DOCKER_BUILD_PLATFORM=linux/test-platform "${SCRIPT_DIR}/build-images.sh"

platform_args=$(grep -c -- '--platform linux/test-platform' "${DOCKER_CALL_LOG}" || true)
if [[ "${platform_args}" -ne 3 ]]; then
    fail "build-images.sh should pass explicit DOCKER_BUILD_PLATFORM to all three image builds"
fi

if grep -q 'platform: linux/amd64' "${HARNESS_DIR}/docker-compose.yml"; then
    fail "docker-compose.yml should not hard-code linux/amd64 service platforms"
fi

if ! grep -q '^  watchdog:' "${HARNESS_DIR}/docker-compose.yml"; then
    fail "docker-compose.yml should define an independent watchdog service"
fi

if ! grep -q '^  watchdog-peer:' "${HARNESS_DIR}/docker-compose.yml"; then
    fail "docker-compose.yml should define an environment-isolation watchdog peer"
fi

readonly_sources=$(grep -c -- '      - slurm_jobdir:/data:ro' "${HARNESS_DIR}/docker-compose.yml" || true)
if [[ "${readonly_sources}" -ne 2 ]]; then
    fail "both controllers should mount the shared Slurm source tree read-only"
fi

metadata_timeout_pattern="      SLURM_ADVISOR_LOG_METADATA_ENRICHMENT_TIMEOUT: \${SLURM_ADVISOR_LOG_METADATA_ENRICHMENT_TIMEOUT:-2s}"
metadata_enrichment_timeouts=$(grep -F -c -- "${metadata_timeout_pattern}" "${HARNESS_DIR}/docker-compose.yml" || true)
if [[ "${metadata_enrichment_timeouts}" -ne 2 ]]; then
    fail "both controllers should inherit the log metadata enrichment timeout"
fi

destination_mounts=$(grep -c -- '      - slurm_loglinks:/log-links' "${HARNESS_DIR}/docker-compose.yml" || true)
if [[ "${destination_mounts}" -ne 2 ]]; then
    fail "controller and assertion host should share only the dedicated log-link destination"
fi

if ! grep -q '^  slurm_loglinks:' "${HARNESS_DIR}/docker-compose.yml"; then
    fail "docker-compose.yml should define a dedicated log-link destination volume"
fi
