# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

REPO_ROOT := $(abspath $(dir $(firstword $(MAKEFILE_LIST)))/..)

.PHONY: build fmt generate lint test test-integration vet verify

build:
	go build -trimpath -o bin/manager ./cmd

fmt:
	go fmt ./...

generate:
	+$(MAKE) -C "$(REPO_ROOT)" generate-go GO_PACKAGES=advice-operator

lint:
	+$(MAKE) -C "$(REPO_ROOT)" lint-go GO_PACKAGES=advice-operator

test:
	+$(MAKE) -C "$(REPO_ROOT)" test-go GO_PACKAGES=advice-operator

test-integration:
	+$(MAKE) -C "$(REPO_ROOT)" test-advice-operator-integration

vet:
	+$(MAKE) -C "$(REPO_ROOT)" vet-go GO_PACKAGES=advice-operator

verify:
	+$(MAKE) -C "$(REPO_ROOT)" verify-go GO_PACKAGES=advice-operator
