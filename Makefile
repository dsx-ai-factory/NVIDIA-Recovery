# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Docker build configuration
DOCKER_CONFIG ?= $(HOME)/.docker
BUILD_IMAGE_TAG ?= latest
IMAGE_PREFIX ?=
IMAGE_TAG ?= $(BUILD_IMAGE_TAG)
PLATFORMS ?= linux/amd64
E2E_PROFILE ?= advisor-act
SLINKY_SLURM_IMAGE ?= ghcr.io/slinkyproject/sackd:25.05-ubuntu24.04@sha256:dd1f0b6030229b3d602e3d80e6d50cc4f9fd17b325312521dfa07376fffc7569
SLURM_ADVISOR_CONTROLLER_SLURM_IMAGE ?= $(SLINKY_SLURM_IMAGE)
# Verify both supported Go modules by default. GO_PACKAGES can select either.
GO_PACKAGES ?= api-gateway slurm-advisor-controller
GO_PACKAGE_ARGS = $(foreach package,$(GO_PACKAGES),--package $(package))
GO_TEST_ARGS ?=
GO_TOOLS_BIN ?= $(CURDIR)/.cache/go-tools/bin
BUF_PACKAGES ?= api-gateway
BUF_PACKAGE_ARGS = $(foreach package,$(BUF_PACKAGES),--package $(package))

# Export DOCKER_CONFIG so buildx picks up registry credentials
export DOCKER_CONFIG
export GO_TEST_ARGS
export GO_TOOLS_BIN
export IMAGE_PREFIX
export IMAGE_TAG
export PLATFORMS

.PHONY: check-generated check-generated-api-gateway-bindings check-generated-go check-slurm-watchdog-deps check-tidy-go format-python generate-api-gateway-bindings generate-go install-go-tools lint lint-buf lint-go lint-python lint-typescript test test-go test-go-race test-nightly-selection-contract test-python test-typescript test-unit test-slurm-advisor-controller-chart-e2e test-slurm-advisor-controller-image test-slurm-advisor-controller-systemd test-slurm-advisor-harness tidy-go verify-go vet-go

lint: format-python lint-python lint-go lint-buf check-generated lint-typescript

check-generated: check-generated-go

check-slurm-watchdog-deps:
	scripts/ci/check-slurm-watchdog-deps.sh

test-slurm-advisor-controller-chart-e2e:
	BUILD_IMAGE_TAG=$(BUILD_IMAGE_TAG) bash scripts/ci/e2e/slurm-advisor-controller-chart-smoke.sh

test-slurm-advisor-controller-image:
	BUILD_IMAGE_TAG=$(BUILD_IMAGE_TAG) \
	SLURM_ADVISOR_CONTROLLER_SLURM_IMAGE=$(SLURM_ADVISOR_CONTROLLER_SLURM_IMAGE) \
		scripts/ci/test-slurm-advisor-controller-image.sh

test-slurm-advisor-controller-systemd:
	deploy/systemd/slurm-advisor-controller/tests/contract.sh

install-go-tools:
	scripts/ci/install-go-tools.sh

format-python:
	scripts/ci/format-python.sh

lint-python:
	scripts/ci/lint-python.sh

lint-go:
	scripts/ci/lint-go.sh $(GO_PACKAGE_ARGS)

lint-buf:
	scripts/ci/lint-buf.sh $(BUF_PACKAGE_ARGS)

lint-typescript:
	scripts/ci/lint-typescript.sh

test: test-python test-go test-typescript

test-nightly-selection-contract:
	bash scripts/ci/test-nightly-selection-contract.sh

test-python:
	scripts/ci/test-python.sh

test-go:
	scripts/ci/test-go.sh $(GO_PACKAGE_ARGS)

test-go-race:
	scripts/ci/test-go.sh $(GO_PACKAGE_ARGS) -- -race

vet-go:
	scripts/ci/vet-go.sh $(GO_PACKAGE_ARGS)

generate-go:
	scripts/ci/generate-go.sh $(GO_PACKAGE_ARGS)

check-generated-go:
	scripts/ci/check-generated-artifacts.sh $(GO_PACKAGE_ARGS)

generate-api-gateway-bindings:
	+$(MAKE) --no-print-directory -C api-gateway gen

check-generated-api-gateway-bindings:
	+$(MAKE) --no-print-directory generate-api-gateway-bindings
	scripts/ci/check-api-gateway-bindings.sh

tidy-go:
	scripts/ci/tidy-go.sh $(GO_PACKAGE_ARGS)

check-tidy-go:
	scripts/ci/check-tidy-go.sh $(GO_PACKAGE_ARGS)

verify-go:
	+$(MAKE) --no-print-directory install-go-tools GO_TEST_ARGS=
	+$(MAKE) --no-print-directory check-generated-go GO_TEST_ARGS=
	+$(MAKE) --no-print-directory check-tidy-go GO_TEST_ARGS=
	+$(MAKE) --no-print-directory lint-go GO_TEST_ARGS=
	+$(MAKE) --no-print-directory vet-go GO_TEST_ARGS=
	+$(MAKE) --no-print-directory test-go GO_TEST_ARGS=
	+$(MAKE) --no-print-directory test-go-race GO_TEST_ARGS=

test-typescript:
	scripts/ci/test-typescript.sh

test-unit: test

test-slurm-advisor-harness:
	$(MAKE) -C slurm-advisor-controller test-harness

# Customer-facing image builds. With no SERVICES override, the script builds
# all ten stack services, including Attribution. Set ATTRIBUTION_ENABLED=false
# for a degraded build without provider source. Advice Operator is excluded.
# build-publish pushes to the consumer's registry; it is not a project release.
.PHONY: build-local build-publish

build-local:
	scripts/build-images.sh --load $(SERVICES)

build-publish:
	scripts/build-images.sh --publish $(SERVICES)

# ---------------------------------------------------------------------------
# E2E (Kind) harness — scripts/ci/e2e
#
# Configure via environment variables (see scripts/ci/e2e/env.sh), set before
# make so they are inherited by the recipe, e.g.:
#   KEEP_CLUSTER=1 make e2e
#   DEPLOY_KIND_CLUSTER= KUBECONFIG=/path make e2e   # use an existing cluster
#   CLUSTER_NAME=heimdall-dev-2 make e2e e2e-down
# ---------------------------------------------------------------------------
E2E_DIR := scripts/ci/e2e
DEV_CI_TIMEOUT ?= 90m
E2E_LOG_BACKEND ?= loki
E2E_REPORT ?= reports/e2e/$(E2E_PROFILE)-$(E2E_LOG_BACKEND)/job_state/junit.xml

E2E_TRACE ?= $(or $(E2E_TRACE_OUTPUT),$(dir $(E2E_REPORT))trace.json)

.PHONY: e2e e2e-up e2e-dev e2e-down e2e-lint e2e-harness-test e2e-list e2e-failed e2e-rerun e2e-rerun-failed e2e-trace-view dev dev-ci dev-down dev-validate

# Full pipeline: converge the environment through Tilt, run tests, then tear down.
e2e:
	bash $(E2E_DIR)/e2e.sh

# Bring the cluster up and leave it running for inspection (runs tests, no teardown).
e2e-up:
	KEEP_CLUSTER=1 $(MAKE) --no-print-directory e2e

# Local dev: stand up the selected Slurm/Slinky recovery stack through Tilt,
# leave it running, and skip the test phase. E2E_PROFILE selects the Advisor
# controller safety mode. Re-running this target recreates the Kind
# cluster with the same name. Tear down later with `make dev-down`.
e2e-dev: dev

# Tilt-backed local dev with an inspectable resource graph and support for
# partial reruns.
dev:
	deploy/tilt/cluster.sh up tilt up

# Headless Tilt run for CI, agents, and non-interactive verification.
dev-ci:
	deploy/tilt/cluster.sh up tilt ci --timeout "$(DEV_CI_TIMEOUT)"

# Evaluate the Tiltfile without starting the stack.
dev-validate:
	tilt alpha tiltfile-result >/dev/null

# Stop Tilt-managed resources and remove the owned local Kind cluster.
dev-down:
	deploy/tilt/cluster.sh down

# Tear down the Kind cluster, kubeconfig, and owned registry. Honors
# CLUSTER_NAME from the environment (default heimdall-e2e), matching the harness.
e2e-down:
	CLUSTER_NAME="$${CLUSTER_NAME:-heimdall-e2e}" KEEP_CLUSTER= bash -c 'source scripts/ci/e2e/cluster.sh; cluster_teardown'

# Lint the shell and Python scenario-runner surfaces.
e2e-lint:
	cd $(E2E_DIR) && shellcheck -x --source-path=SCRIPTDIR *.sh
	scripts/ci/e2e/diagnostics-self-test.sh
	for job in tests/e2e/scenarios/[0-9][0-9]-*/*.sh; do bash -n "$$job" || exit; done
	shellcheck tests/e2e/scenarios/[0-9][0-9]-*/*.sh
	uv run --locked --group e2e ruff format --check tests/e2e tests/e2e_harness tests/tooling
	uv run --locked --group e2e ruff check tests/e2e tests/e2e_harness tests/tooling

# Run only cluster-free tests for the E2E harness and its tooling.
e2e-harness-test:
	uv run --locked --group e2e pytest -q tests/e2e_harness tests/tooling

# List scenario names accepted by E2E_SCENARIOS.
e2e-list:
	@python3 $(E2E_DIR)/scenarios.py list

# List scenarios that failed in the last report (E2E_REPORT).
e2e-failed:
	@python3 $(E2E_DIR)/scenarios.py failed "$(E2E_REPORT)"

# Run only the test phase against the retained CLUSTER_NAME cluster. Select
# scenarios with E2E_SCENARIOS and pass pytest options with PYTEST_ADDOPTS.
e2e-rerun:
	cluster="$${CLUSTER_NAME:-heimdall-e2e}"; \
	CLUSTER_NAME="$$cluster" DEPLOY_KIND_CLUSTER= KUBECONFIG="/tmp/kind-$$cluster.kubeconfig" \
	PYTEST_ADDOPTS="$${PYTEST_ADDOPTS---instafail}" \
	bash -c 'source $(E2E_DIR)/run-tests.sh; run_tests'

# Rerun only the scenarios that failed in the last report.
e2e-rerun-failed:
	@failed="$$(python3 $(E2E_DIR)/scenarios.py failed "$(E2E_REPORT)")" || exit $$?; \
	failed="$$(printf '%s' "$$failed" | tr '\n' ' ')"; \
	test -n "$${failed% }" || { echo "No failed scenarios in $(E2E_REPORT)"; exit 0; }; \
	E2E_SCENARIOS="$$failed" $(MAKE) --no-print-directory e2e-rerun

# Open a run's trace (written with --trace-output) in speedscope on this machine.
# npm ci installs exactly the digests in scripts/trace-viewer/package-lock.json.
e2e-trace-view:
	npm ci --prefix scripts/trace-viewer --ignore-scripts --no-audit --no-fund
	scripts/trace-viewer/node_modules/.bin/speedscope "$(E2E_TRACE)"
