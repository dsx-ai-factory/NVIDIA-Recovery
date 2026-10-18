#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Contract checks for the opt-in Kubernetes ServiceAccount authentication smoke.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  grep -F -- "$2" "$1" >/dev/null || fail "expected ${1#$REPO_ROOT/} to contain: $2"
}

assert_not_contains() {
  ! grep -F -- "$2" "$1" >/dev/null || fail "expected ${1#$REPO_ROOT/} not to contain: $2"
}

umbrella_values="$SCRIPT_DIR/values/stack-local-kubernetes-service-auth.yaml"
advisor_values="$REPO_ROOT/deploy/tilt/services/slurm-advisor-controller/kubernetes-service-auth.yaml"
smoke="$SCRIPT_DIR/kubernetes-service-auth-smoke.sh"
run_tests="$SCRIPT_DIR/run-tests.sh"
tiltfile="$REPO_ROOT/Tiltfile"

[[ -x "$smoke" ]] || fail "missing executable smoke: ${smoke#$REPO_ROOT/}"

# The normal local stack deliberately remains unauthenticated so existing E2E
# profiles retain their historical behavior. The dedicated overlay enables the
# production TokenReview path instead of using HEIMDALL_INSECURE.
assert_contains "$umbrella_values" "auth:"
assert_contains "$umbrella_values" "insecure: false"
assert_contains "$umbrella_values" "kubernetes:"
assert_contains "$umbrella_values" "audience: nvidia-recovery-api-gateway"
assert_contains "$umbrella_values" "expectedServiceAccount: system:serviceaccount:slurm:slurm-advisor-controller"
assert_contains "$umbrella_values" "serviceAccount:"
assert_contains "$umbrella_values" "name: nvidia-recovery-api-gateway-tokenreviewer"
assert_not_contains "$umbrella_values" "insecure: true"

assert_contains "$advisor_values" "kubernetesAuth:"
assert_contains "$advisor_values" "enabled: true"
assert_contains "$advisor_values" "audience: nvidia-recovery-api-gateway"
assert_contains "$advisor_values" "expirationSeconds: 3600"
assert_contains "$advisor_values" "logLevel: debug"

# Tilt must select the overlay only when the explicit smoke switch is present.
assert_contains "$tiltfile" "E2E_KUBERNETES_SERVICE_AUTH_SMOKE"
assert_contains "$tiltfile" "stack-local-kubernetes-service-auth.yaml"
assert_contains "$tiltfile" "kubernetes-service-auth.yaml"

# The runtime assertion uses live workload state, RBAC, and the controller's
# real sync. The HTTP check reads the projected bearer only inside the Pod.
assert_contains "$smoke" "kubectl auth can-i"
assert_contains "$smoke" "controller sync completed"
assert_contains "$smoke" "HEIMDALL_KUBERNETES_AUTH_ENABLED"
assert_contains "$smoke" "HEIMDALL_INSECURE"
assert_contains "$smoke" "serviceAccountToken"
assert_contains "$smoke" 'SMOKE_NAMESPACE="slurm"'
assert_contains "$run_tests" 'if ! bash "$SCRIPT_DIR/kubernetes-service-auth-smoke.sh"; then'
assert_not_contains "$smoke" '{"\\n"}'

printf 'ok: Kubernetes service authentication smoke contract\n'
