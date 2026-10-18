#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
HARNESS_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)
SERVICE_DIR=$(cd -- "${HARNESS_DIR}/.." && pwd)
MOCK_DEBUG_URL=${MOCK_DEBUG_URL:-http://127.0.0.1:${MOCK_HEIMDALL_DEBUG_PORT:-18081}}
MOCK_PEER_DEBUG_URL=${MOCK_PEER_DEBUG_URL:-http://127.0.0.1:${MOCK_HEIMDALL_PEER_DEBUG_PORT:-18082}}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-90}

cd "${HARNESS_DIR}"

cleanup() {
    if [ -z "${KEEP_SLURM_ADVISOR_HARNESS:-}" ]; then
        docker compose --profile environment-isolation down -v --remove-orphans
    fi
}
trap cleanup EXIT

"${SCRIPT_DIR}/build-images.sh"
docker compose up -d mysql mock-heimdall slurmdbd slurmctld c1 c2 c3

wait_for_http() {
    local url=$1
    local deadline=$((SECONDS + TIMEOUT_SECONDS))
    until curl -fsS "${url}" >/dev/null; do
        if [ "${SECONDS}" -ge "${deadline}" ]; then
            echo "Timed out waiting for ${url}" >&2
            return 1
        fi
        sleep 2
    done
}

wait_for_slurm() {
    local deadline=$((SECONDS + TIMEOUT_SECONDS))
    until docker compose exec -T slurmctld sinfo -h >/dev/null 2>&1; do
        if [ "${SECONDS}" -ge "${deadline}" ]; then
            echo "Timed out waiting for Slurm" >&2
            docker compose logs --tail=100 slurmctld c1 c2 c3 >&2 || true
            return 1
        fi
        sleep 2
    done
}

wait_for_http "${MOCK_DEBUG_URL}/healthz"
wait_for_slurm
"${SCRIPT_DIR}/register-cluster.sh"
wait_for_slurm

(
    cd "${SERVICE_DIR}"
    SLURM_HARNESS_DIR="${HARNESS_DIR}" MOCK_DEBUG_URL="${MOCK_DEBUG_URL}" MOCK_PEER_DEBUG_URL="${MOCK_PEER_DEBUG_URL}" \
        go test -tags harness ./harness/scenarios -run TestSafetyMatrix -count=1 -v
)

echo "Slurm advisor controller smoke passed"
