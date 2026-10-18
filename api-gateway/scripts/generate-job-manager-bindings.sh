#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Source common utilities
source "$(dirname "$0")/common-bindings.sh"

check_docker
check_precommit

rm -rf ./gen/jobmanager/query

mkdir -p ./gen/jobmanager/query

cp .openapi-generator-ignore ./gen/jobmanager/.openapi-generator-ignore

run_openapi_generator \
    "$(pwd)/../job_manager" \
    "job-manager-openapi-3-1-schema.json" \
    "$(pwd)" \
    "gen/jobmanager" \
    "go" \
    "job-manager-gen-config.yaml"

# Preserve the checked-in Go formatting when regenerating these bindings.
gofmt -w ./gen/jobmanager

run_precommit_hooks

print_done
