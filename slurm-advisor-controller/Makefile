# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

.PHONY: fmt test vet harness-build harness-up harness-down harness-smoke test-harness test-harness-config

fmt:
	go fmt ./...

test:
	go test ./...

vet:
	go vet ./...

harness-build:
	harness/scripts/build-images.sh

harness-up:
	harness/scripts/build-images.sh
	cd harness && docker compose up -d mysql mock-heimdall slurmdbd slurmctld c1 c2 c3 controller

harness-down:
	cd harness && docker compose down -v --remove-orphans

harness-smoke:
	harness/scripts/run-smoke.sh

test-harness: harness-smoke

test-harness-config:
	bash harness/scripts/test-platform-config.sh
