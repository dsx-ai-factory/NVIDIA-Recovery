#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Assert the opt-in, real Kubernetes ServiceAccount auth path in a deployed stack.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
source "$SCRIPT_DIR/lib.sh"

if ! enabled "$E2E_KUBERNETES_SERVICE_AUTH_SMOKE"; then
  return 0 2>/dev/null || exit 0
fi

# Tilt's local stack and its Slurm integration are intentionally deployed to
# this fixed namespace. HELM_NAMESPACE names the umbrella release's historical
# default namespace and does not describe these workloads.
SMOKE_NAMESPACE="slurm"
GATEWAY_DEPLOYMENT="${HELM_RELEASE}-api-gateway"
CONTROLLER_DEPLOYMENT="${SLURM_ADVISOR_RELEASE}"
GATEWAY_SERVICE_ACCOUNT="nvidia-recovery-api-gateway-tokenreviewer"
CONTROLLER_SERVICE_ACCOUNT="slurm-advisor-controller"
CONTROLLER_USERNAME="system:serviceaccount:${SMOKE_NAMESPACE}:${CONTROLLER_SERVICE_ACCOUNT}"
AUTH_AUDIENCE="nvidia-recovery-api-gateway"
TIMEOUT="${E2E_KUBERNETES_SERVICE_AUTH_TIMEOUT:-180}"

[[ "$E2E_PROFILE" == "advisor-observe" ]] ||
  error "E2E_KUBERNETES_SERVICE_AUTH_SMOKE requires E2E_PROFILE=advisor-observe"
[[ "$HELM_RELEASE" == "nvidia-recovery" ]] ||
  error "E2E_KUBERNETES_SERVICE_AUTH_SMOKE requires HELM_RELEASE=nvidia-recovery"
[[ "$CONTROLLER_DEPLOYMENT" == "$CONTROLLER_SERVICE_ACCOUNT" ]] ||
  error "E2E_KUBERNETES_SERVICE_AUTH_SMOKE requires SLURM_ADVISOR_RELEASE=$CONTROLLER_SERVICE_ACCOUNT"

banner "E2E smoke: Kubernetes ServiceAccount authentication"
kubectl rollout status -n "$SMOKE_NAMESPACE" "deployment/${GATEWAY_DEPLOYMENT}" --timeout="${TIMEOUT}s"
kubectl rollout status -n "$SMOKE_NAMESPACE" "deployment/${CONTROLLER_DEPLOYMENT}" --timeout="${TIMEOUT}s"

# The gateway must be running the Kubernetes authenticator, not the historical
# insecure fallback. The inspection is limited to manifest-derived env names
# and values; it never reads bearer-token data.
gateway_env="$(kubectl get deployment -n "$SMOKE_NAMESPACE" "$GATEWAY_DEPLOYMENT" \
  -o jsonpath='{range .spec.template.spec.containers[*].env[*]}{.name}={.value}{"\n"}{end}')"
grep -Fx 'HEIMDALL_KUBERNETES_AUTH_ENABLED=true' <<<"$gateway_env" >/dev/null ||
  error "gateway Kubernetes authentication is not enabled"
grep -Fx "HEIMDALL_KUBERNETES_AUTH_AUDIENCE=${AUTH_AUDIENCE}" <<<"$gateway_env" >/dev/null ||
  error "gateway Kubernetes authentication audience is not ${AUTH_AUDIENCE}"
grep -Fx "HEIMDALL_KUBERNETES_AUTH_EXPECTED_SERVICE_ACCOUNT=${CONTROLLER_USERNAME}" <<<"$gateway_env" >/dev/null ||
  error "gateway expected ServiceAccount is not ${CONTROLLER_USERNAME}"
! grep -Fq 'HEIMDALL_INSECURE=' <<<"$gateway_env" ||
  error "gateway insecure authentication fallback is enabled"

gateway_service_account="$(kubectl get deployment -n "$SMOKE_NAMESPACE" "$GATEWAY_DEPLOYMENT" \
  -o jsonpath='{.spec.template.spec.serviceAccountName}')"
[[ "$gateway_service_account" == "$GATEWAY_SERVICE_ACCOUNT" ]] ||
  error "gateway ServiceAccount is $gateway_service_account, want $GATEWAY_SERVICE_ACCOUNT"
kubectl auth can-i --as="system:serviceaccount:${SMOKE_NAMESPACE}:${GATEWAY_SERVICE_ACCOUNT}" \
  create tokenreviews.authentication.k8s.io | grep -Fx yes >/dev/null ||
  error "gateway ServiceAccount cannot create TokenReview resources"

controller_pod="$(kubectl get pod -n "$SMOKE_NAMESPACE" \
  -l "app.kubernetes.io/instance=${CONTROLLER_DEPLOYMENT},app.kubernetes.io/name=slurm-advisor-controller,app.kubernetes.io/component=controller" \
  -o jsonpath='{.items[0].metadata.name}')"
[[ -n "$controller_pod" ]] || error "authenticated controller Pod not found"
controller_service_account="$(kubectl get pod -n "$SMOKE_NAMESPACE" "$controller_pod" -o jsonpath='{.spec.serviceAccountName}')"
[[ "$controller_service_account" == "$CONTROLLER_SERVICE_ACCOUNT" ]] ||
  error "controller ServiceAccount is $controller_service_account, want $CONTROLLER_SERVICE_ACCOUNT"

# This reads only Pod specification metadata, never the projected file itself.
pod_spec="$(kubectl get pod -n "$SMOKE_NAMESPACE" "$controller_pod" -o yaml)"
grep -F 'serviceAccountToken:' <<<"$pod_spec" >/dev/null ||
  error "controller Pod has no projected ServiceAccount token volume"
grep -F "audience: ${AUTH_AUDIENCE}" <<<"$pod_spec" >/dev/null ||
  error "controller projected token does not use audience ${AUTH_AUDIENCE}"
grep -F 'expirationSeconds: 3600' <<<"$pod_spec" >/dev/null ||
  error "controller projected token does not use the configured lifetime"

# A completed controller sync proves that the running controller presented its
# projected token over its production gRPC client and that the real gateway
# accepted it after TokenReview. A Ready Pod alone would not prove this.
deadline=$((SECONDS + TIMEOUT))
until kubectl logs -n "$SMOKE_NAMESPACE" "$controller_pod" -c slurm-advisor-controller \
    | grep -F '"msg":"controller sync completed"' >/dev/null; do
  if (( SECONDS >= deadline )); then
    kubectl logs -n "$SMOKE_NAMESPACE" "$controller_pod" -c slurm-advisor-controller --tail=100 >&2 || true
    error "controller did not complete an authenticated gateway sync within ${TIMEOUT}s"
  fi
  sleep 3
done

# Exercise the HTTP route on the ServiceAccount-authenticated internal listener.
# The controller image has Bash but no HTTP client; keep the projected bearer
# inside the Pod and print only the status.
for credentials in missing projected; do
  expected_status=401
  if [[ "$credentials" == projected ]]; then
    expected_status=200
  fi
  # shellcheck disable=SC2016 # Expand the request and token only inside the Pod.
  status="$(kubectl exec -n "$SMOKE_NAMESPACE" "$controller_pod" -c slurm-advisor-controller -- \
    timeout "$TIMEOUT" bash -ceu '
      exec 3<>"/dev/tcp/$1/$2"
      printf "GET /v1/audit/events?limit=1 HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n" "$1" >&3
      if [[ "$4" == projected ]]; then
        token=$(<"$3")
        [[ -n "$token" ]]
        printf "Authorization: Bearer %s\r\n" "$token" >&3
      fi
      printf "\r\n" >&3
      IFS=" " read -r protocol status remainder <&3
      [[ "$protocol" == HTTP/1.* && "$status" =~ ^[0-9]{3}$ ]]
      printf "%s\n" "$status"
    ' -- "${GATEWAY_DEPLOYMENT}-internal" 8080 /var/run/secrets/heimdall/service-account-token "$credentials")"
  [[ "$status" == "$expected_status" ]] ||
    error "gateway audit HTTP read with $credentials bearer returned $status, want $expected_status"
done

success "controller authenticated its gateway sync and audit HTTP read; unauthenticated audit read was rejected"
