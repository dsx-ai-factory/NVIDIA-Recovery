#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Common utilities for OpenAPI binding generation scripts

# Set strict bash mode
set -euo pipefail

# Check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "Docker is not installed. Please install Docker to run the openapi-generator."
        exit 1
    fi

    if ! docker info &> /dev/null; then
        echo "Docker daemon is not running. Please start Docker and try again."
        exit 1
    fi
}

# Run openapi-generator using Docker
# Parameters: input_volume input_file output_volume output_path generator [config_file]
run_openapi_generator() {
    local input_volume="$1"
    local input_file="$2"
    local output_volume="$3"
    local output_path="$4"
    local generator="$5"
    local config_file="${6:-}"  # Use default empty string if not provided

    local docker_args=(
        --rm
        -v "$input_volume:/input"
        -v "$output_volume:/output"
        openapitools/openapi-generator-cli:v7.14.0 generate
        -i "/input/$input_file"
        -g "$generator"
        -o "/output/$output_path"
        --skip-validate-spec
    )

    # Add config file if provided (mount current directory for config access)
    if [[ -n "$config_file" ]]; then
        docker_args=(-v "$(pwd):/local" "${docker_args[@]}")
        docker_args+=(--config "/local/$config_file")
    fi

    docker run "${docker_args[@]}"
}

# Check if pre-commit is installed
check_precommit() {
    if [ -x "$(command -v pre-commit)" ]; then
        echo "pre-commit is installed"
    else
        echo "pre-commit is not installed. Please install it to run the pre-commit hooks."
        exit 1
    fi
}

# Run pre-commit hooks on generated files
run_precommit_hooks() {
    echo "Running pre-commit hooks..."
    # Run pre-commit, but don't fail if it exits nonzero (e.g., due to reformatting)
    find gen -type f -print0 | xargs -0 pre-commit run --files || true
    # Files should pass now
    find gen -type f -print0 | xargs -0 pre-commit run --files
}

# Print completion message
print_done() {
    echo "Done"
}
