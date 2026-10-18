#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

source "$(dirname "$0")/common-bindings.sh"

check_docker
check_precommit

rm -rf ./gen/notifications
mkdir -p ./gen/notifications

cp .openapi-generator-ignore ./gen/notifications/.openapi-generator-ignore

run_openapi_generator \
    "$(pwd)/../notifications" \
    "openapi.yaml" \
    "$(pwd)" \
    "gen/notifications" \
    "go" \
    "notifications-gen-config.yaml"

run_precommit_hooks

print_done
