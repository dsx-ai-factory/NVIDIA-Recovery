# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

REPO_ROOT := $(abspath $(dir $(firstword $(MAKEFILE_LIST)))/..)

build: ## Build application executables.
	go build -o bin/ ./...

build-and-run: build
	./bin/api-gateway

install-deps: ## Install required developer dependencies.
	brew install bufbuild/buf/buf

_gen-proto:
	echo "Generating protobuf files..."
	buf generate --include-imports

_gen-job-manager-bindings:
	echo "Generating job manager bindings..."
	./scripts/generate-job-manager-bindings.sh

_gen-notifications-bindings:
	echo "Generating notifications bindings..."
	./scripts/generate-notifications-bindings.sh

gen: _gen-job-manager-bindings _gen-notifications-bindings ## Generate downstream HTTP service bindings
	echo "Done"

kind-up: ## Bring up a new kind cluster.
	kind create cluster --name pnp --config=hack/kind-config.yaml

kind-down: ## Tear down the kind cluster.
	kind delete cluster --name pnp

test: ## Run the tests
	go test -v ./...

tests-watch: ## Run the tests in watch mode.
	watchexec -e go -w . -- go test -v ./...

lint: ## Run the linter
	$(MAKE) -C "$(REPO_ROOT)" lint-go GO_PACKAGES=api-gateway

clean: ## Clean the build artifacts
	rm -rf bin

run:
	HEIMDALL_INSECURE=true HEIMDALL_JOB_SOURCE=synthetic ./bin/api-gateway

# From https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
help:
	#
	# Heimdall API Server
	#
	# Below are list of available commands that can be run:
	#
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build build-and-run install-deps gen _gen-proto _gen-job-manager-bindings _gen-notifications-bindings kind-up kind-down test lint help
