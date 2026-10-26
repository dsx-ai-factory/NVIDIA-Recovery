# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

REPO_ROOT := $(abspath $(dir $(firstword $(MAKEFILE_LIST)))/..)

.PHONY: build fmt lint test vet

build:
	go build -o bin/ ./cmd

fmt:
	go fmt ./...

lint:
	$(MAKE) -C "$(REPO_ROOT)" lint-go GO_PACKAGES=advice-operator

test:
	go test ./...

vet:
	go vet ./...
