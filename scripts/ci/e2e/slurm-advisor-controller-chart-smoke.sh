#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Focused source-built image and Helm smoke for the Slurm advisor controller.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
source "$SCRIPT_DIR/lib.sh"
source "$SCRIPT_DIR/prepare.sh"
source "$SCRIPT_DIR/cluster.sh"
source "$SCRIPT_DIR/load-images.sh"
source "$SCRIPT_DIR/provision.sh"

SMOKE_NAMESPACE="${SLURM_ADVISOR_SMOKE_NAMESPACE:-slurm}"
SMOKE_RELEASE="${SLURM_ADVISOR_SMOKE_RELEASE:-slurm-advisor-controller-smoke}"
MOCK_NAME="${SLURM_ADVISOR_SMOKE_MOCK_NAME:-slurm-advisor-mock-heimdall}"
SMOKE_TIMEOUT="${SLURM_ADVISOR_SMOKE_TIMEOUT:-300}"
SMOKE_PHASE_LOG="${SLURM_ADVISOR_SMOKE_PHASE_LOG:-}"
SMOKE_TEST_MODE="${SLURM_ADVISOR_SMOKE_TEST_MODE:-}"
SMOKE_FAIL_PHASE="${SLURM_ADVISOR_SMOKE_FAIL_PHASE:-}"
CONTROLLER_METRICS_LOCAL_PORT="${SLURM_ADVISOR_CONTROLLER_METRICS_LOCAL_PORT:-19090}"
WATCHDOG_METRICS_LOCAL_PORT="${SLURM_ADVISOR_WATCHDOG_METRICS_LOCAL_PORT:-19091}"
SMOKE_TMP_DIR=""
CONTROLLER_PORT_FORWARD_PID=""
WATCHDOG_PORT_FORWARD_PID=""
CONTROLLER_POD=""
WATCHDOG_POD=""

record_phase() {
  local phase="$1"
  if [[ -n "$SMOKE_PHASE_LOG" ]]; then
    printf '%s\n' "$phase" >>"$SMOKE_PHASE_LOG"
  fi
}

run_phase() {
  local phase="$1"
  shift
  record_phase "$phase"
  if [[ "$SMOKE_FAIL_PHASE" == "$phase" ]]; then
    return 97
  fi
  if enabled "$SMOKE_TEST_MODE"; then
    return 0
  fi
  "$@"
}

validate_configuration() {
  local expected_sackd="ghcr.io/slinkyproject/sackd:${E2E_SLINKY_SLURM_TAG}"
  local donor_suffix="${E2E_SLINKY_SACKD_IMAGE#"$expected_sackd"}"
  enabled "$DEPLOY_KIND_CLUSTER" ||
    error "the focused smoke requires its disposable Kind cluster; DEPLOY_KIND_CLUSTER must be enabled"
  [[ -n "$E2E_SLINKY_SLURM_TAG" ]] || error "E2E_SLINKY_SLURM_TAG must not be empty"
  # The donor may pin the matching tag's bytes without applying that digest to
  # every other Slinky component that shares E2E_SLINKY_SLURM_TAG.
  if [[ "$E2E_SLINKY_SACKD_IMAGE" != "$expected_sackd" ]]; then
    [[ "$E2E_SLINKY_SACKD_IMAGE" == "$expected_sackd"@sha256:* && "$donor_suffix" =~ ^@sha256:[a-f0-9]{64}$ ]] ||
      error "E2E_SLINKY_SACKD_IMAGE must be $expected_sackd with an optional sha256 digest, found $E2E_SLINKY_SACKD_IMAGE"
  fi
  [[ -n "$BUILD_IMAGE_TAG" ]] || error "BUILD_IMAGE_TAG must not be empty"
}

configure_smoke() {
  LOAD_IMAGES=1
  E2E_IMAGES_TO_LOAD=""
  export E2E_SLINKY_SLURM_TAG E2E_SLINKY_SACKD_IMAGE BUILD_IMAGE_TAG
}

build_smoke_images() {
  local controller_image="slurm-advisor-controller:${BUILD_IMAGE_TAG}"
  local watchdog_image="slurm-watchdog:${BUILD_IMAGE_TAG}"
  local mock_image="slurm-advisor-mock-heimdall:${BUILD_IMAGE_TAG}"

  IMAGE_TAG="$BUILD_IMAGE_TAG" \
    SLURM_ADVISOR_CONTROLLER_SLURM_IMAGE="$E2E_SLINKY_SACKD_IMAGE" \
    SLURM_WATCHDOG_BASE_IMAGE="$E2E_SLINKY_SACKD_IMAGE" \
    "$REPO_ROOT/scripts/build-images.sh" --load \
      slurm-advisor-controller \
      slurm-watchdog

  log "Building smoke-only image: $mock_image"
  docker buildx build \
    --file "$REPO_ROOT/slurm-advisor-controller/harness/Dockerfile.controller" \
    --build-arg "BASE_IMAGE=$controller_image" \
    --ulimit nofile=65536:65536 \
    --progress=plain \
    --tag "$mock_image" \
    --load \
    "$REPO_ROOT"

  E2E_IMAGES_TO_LOAD="$controller_image $watchdog_image $mock_image"
}

cluster_up_for_smoke() {
  local proxy="${HTTPS_PROXY:-${https_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}"
  if [[ "$proxy" == *"127.0.0.1:"* || "$proxy" == *"localhost:"* ]]; then
    warn "Not forwarding the host loopback proxy into the Kind node"
    (
      unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy
      cluster_up
    )
    return
  fi
  cluster_up
}

load_support_images() {
  local -a images=(
    "quay.io/jetstack/cert-manager-cainjector:${CERT_MANAGER_CHART_VERSION}"
    "quay.io/jetstack/cert-manager-controller:${CERT_MANAGER_CHART_VERSION}"
    "quay.io/jetstack/cert-manager-startupapicheck:${CERT_MANAGER_CHART_VERSION}"
    "quay.io/jetstack/cert-manager-webhook:${CERT_MANAGER_CHART_VERSION}"
  )
  local image
  for image in "${images[@]}"; do
    if ! docker image inspect "$image" >/dev/null 2>&1; then
      log "Pulling support image: $image"
      docker pull "$image"
    fi
    log "Loading support image into Kind: $image"
    kind load docker-image "$image" --name "$CLUSTER_NAME"
  done
  success "Pinned cert-manager images loaded into Kind"
}

provision_smoke_infrastructure() {
  _provision_preflight
  _install_service "$SCRIPT_DIR/services/00-install-cert-manager.sh"
  _install_service "$SCRIPT_DIR/services/04-install-slurm.sh"
}

verify_slinky_images() {
  local images_file="$SMOKE_TMP_DIR/slinky-images.txt"
  kubectl get pods -n "$SMOKE_NAMESPACE" \
    -o jsonpath='{range .items[*]}{range .spec.containers[*]}{.image}{"\n"}{end}{end}' \
    >"$images_file"

  local image
  while IFS= read -r image; do
    case "$image" in
      ghcr.io/slinkyproject/*)
        [[ "$image" == *":${E2E_SLINKY_SLURM_TAG}" ]] ||
          error "running Slinky image does not use ${E2E_SLINKY_SLURM_TAG}: $image"
        ;;
    esac
  done <"$images_file"

  local repository
  for repository in sackd slurmctld login slurmd slurmdbd slurmrestd; do
    grep -Fx "ghcr.io/slinkyproject/${repository}:${E2E_SLINKY_SLURM_TAG}" "$images_file" >/dev/null ||
      error "missing running Slinky component image: $repository:${E2E_SLINKY_SLURM_TAG}"
  done

  success "Slinky component images use ${E2E_SLINKY_SLURM_TAG}"
}

deploy_mock_heimdall() {
  kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${MOCK_NAME}
  namespace: ${SMOKE_NAMESPACE}
  labels:
    app.kubernetes.io/name: ${MOCK_NAME}
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ${MOCK_NAME}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ${MOCK_NAME}
    spec:
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 401
        runAsGroup: 401
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: mock-heimdall-gateway
          image: slurm-advisor-mock-heimdall:${BUILD_IMAGE_TAG}
          imagePullPolicy: Never
          command:
            - /usr/local/bin/mock-heimdall-gateway
          env:
            - name: MOCK_HEIMDALL_GRPC_ADDR
              value: :8080
            - name: MOCK_HEIMDALL_DEBUG_ADDR
              value: :8081
          ports:
            - name: grpc
              containerPort: 8080
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop:
                - ALL
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              cpu: 100m
              memory: 64Mi
---
apiVersion: v1
kind: Service
metadata:
  name: ${MOCK_NAME}
  namespace: ${SMOKE_NAMESPACE}
spec:
  selector:
    app.kubernetes.io/name: ${MOCK_NAME}
  ports:
    - name: grpc
      port: 8080
      targetPort: grpc
EOF
  kubectl rollout status -n "$SMOKE_NAMESPACE" "deployment/${MOCK_NAME}" --timeout="${SMOKE_TIMEOUT}s"
}

install_chart() {
  helm upgrade --install "$SMOKE_RELEASE" \
    "$REPO_ROOT/deploy/helm/slurm-advisor-controller" \
    --namespace "$SMOKE_NAMESPACE" \
    --set-string image.repository=slurm-advisor-controller \
    --set-string image.tag="$BUILD_IMAGE_TAG" \
    --set image.pullPolicy=Never \
    --set-string watchdog.image.repository=slurm-watchdog \
    --set-string watchdog.image.tag="$BUILD_IMAGE_TAG" \
    --set watchdog.image.pullPolicy=Never \
    --set-string sackd.image.repository=ghcr.io/slinkyproject/sackd \
    --set-string sackd.image.tag="$E2E_SLINKY_SLURM_TAG" \
    --set sackd.image.pullPolicy=IfNotPresent \
    --set-string sackd.confServer=slurm-controller:6817 \
    --set-string controller.heimdallEndpoint="${MOCK_NAME}:8080" \
    --set controller.mode=observe \
    --set controller.acknowledgeSchedulerActions=false \
    --set controller.logLevel=debug \
    --set watchdog.mode=observe \
    --set watchdog.acknowledgeSchedulerActions=false \
    --set watchdog.logLevel=debug \
    --wait \
    --timeout="${SMOKE_TIMEOUT}s"
}

wait_for_workloads() {
  kubectl rollout status -n "$SMOKE_NAMESPACE" "deployment/${SMOKE_RELEASE}" --timeout="${SMOKE_TIMEOUT}s"
  kubectl rollout status -n "$SMOKE_NAMESPACE" "deployment/${SMOKE_RELEASE}-watchdog" --timeout="${SMOKE_TIMEOUT}s"
  CONTROLLER_POD="$(kubectl get pods -n "$SMOKE_NAMESPACE" \
    -l "app.kubernetes.io/instance=${SMOKE_RELEASE},app.kubernetes.io/name=slurm-advisor-controller,app.kubernetes.io/component=controller" \
    -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "$CONTROLLER_POD" ]] || error "controller Pod not found"
  WATCHDOG_POD="$(kubectl get pods -n "$SMOKE_NAMESPACE" \
    -l "app.kubernetes.io/instance=${SMOKE_RELEASE},app.kubernetes.io/name=slurm-advisor-controller,app.kubernetes.io/component=watchdog" \
    -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "$WATCHDOG_POD" ]] || error "watchdog Pod not found"

  local controller_images watchdog_images
  controller_images="$(kubectl get pod -n "$SMOKE_NAMESPACE" "$CONTROLLER_POD" \
    -o jsonpath='{range .spec.containers[*]}{.name}={.image}{"\n"}{end}')"
  watchdog_images="$(kubectl get pod -n "$SMOKE_NAMESPACE" "$WATCHDOG_POD" \
    -o jsonpath='{range .spec.containers[*]}{.name}={.image}{"\n"}{end}')"

  local expected_controller_images expected_watchdog_images
  expected_controller_images="$(printf '%s\n%s' \
    "slurm-advisor-controller=slurm-advisor-controller:${BUILD_IMAGE_TAG}" \
    "sackd=ghcr.io/slinkyproject/sackd:${E2E_SLINKY_SLURM_TAG}")"
  expected_watchdog_images="$(printf '%s\n%s' \
    "slurm-watchdog=slurm-watchdog:${BUILD_IMAGE_TAG}" \
    "sackd=ghcr.io/slinkyproject/sackd:${E2E_SLINKY_SLURM_TAG}")"
  [[ "$controller_images" == "$expected_controller_images" ]] ||
    error "controller Pod has unexpected containers or images: $controller_images"
  [[ "$watchdog_images" == "$expected_watchdog_images" ]] ||
    error "watchdog Pod has unexpected containers or images: $watchdog_images"
  success "Helm release $SMOKE_RELEASE is Ready with isolated controller and watchdog Pods"
}

wait_for_squeue() {
  local pod="$1"
  local container="$2"
  local output_file="$3"
  local error_file="$4"
  local deadline=$((SECONDS + SMOKE_TIMEOUT))
  until kubectl exec -n "$SMOKE_NAMESPACE" "$pod" \
      -c "$container" -- squeue --json >"$output_file" 2>"$error_file"; do
    if ((SECONDS >= deadline)); then
      cat "$error_file" >&2
      error "squeue --json did not succeed in $pod/$container through sackd within ${SMOKE_TIMEOUT}s"
    fi
    sleep 3
  done
  [[ -s "$output_file" ]] || error "squeue --json returned empty output in $pod/$container"
}

verify_slurm() {
  local login_pod
  login_pod="$(kubectl get pods -n "$SMOKE_NAMESPACE" \
    -o custom-columns='NAME:.metadata.name,IMAGES:.spec.containers[*].image' --no-headers |
    awk -v image="ghcr.io/slinkyproject/login:${E2E_SLINKY_SLURM_TAG}" 'index($0, image) { print $1; exit }')"
  [[ -n "$login_pod" ]] || error "Slinky login Pod not found"

  local cluster_version controller_version watchdog_version
  cluster_version="$(kubectl exec -n "$SMOKE_NAMESPACE" "$login_pod" -- scontrol --version)"
  controller_version="$(kubectl exec -n "$SMOKE_NAMESPACE" "$CONTROLLER_POD" -c slurm-advisor-controller -- scontrol --version)"
  watchdog_version="$(kubectl exec -n "$SMOKE_NAMESPACE" "$WATCHDOG_POD" -c slurm-watchdog -- scontrol --version)"
  [[ "$cluster_version" == "$controller_version" && "$cluster_version" == "$watchdog_version" ]] ||
    error "Slurm version mismatch: cluster=$cluster_version controller=$controller_version watchdog=$watchdog_version"

  wait_for_squeue "$CONTROLLER_POD" slurm-advisor-controller \
    "$SMOKE_TMP_DIR/controller-squeue.json" "$SMOKE_TMP_DIR/controller-squeue.err"
  wait_for_squeue "$WATCHDOG_POD" slurm-watchdog \
    "$SMOKE_TMP_DIR/watchdog-squeue.json" "$SMOKE_TMP_DIR/watchdog-squeue.err"
  success "Slurm CLI contract: $cluster_version; both Pods used their local sackd"
}

wait_for_log_line() {
  local pod="$1"
  local container="$2"
  local message="$3"
  local deadline=$((SECONDS + SMOKE_TIMEOUT))
  until kubectl logs -n "$SMOKE_NAMESPACE" "$pod" -c "$container" | grep -F "$message" >/dev/null; do
    if ((SECONDS >= deadline)); then
      kubectl logs -n "$SMOKE_NAMESPACE" "$pod" -c "$container" >&2 || true
      error "$pod/$container did not log '$message' within ${SMOKE_TIMEOUT}s"
    fi
    sleep 3
  done
}

verify_polls() {
  wait_for_log_line "$CONTROLLER_POD" slurm-advisor-controller '"msg":"controller sync completed"'
  wait_for_log_line "$WATCHDOG_POD" slurm-watchdog '"msg":"watchdog poll completed"'
  success "Controller reconciliation and standalone watchdog polling succeeded in observe mode"
}

verify_metrics() {
  kubectl port-forward -n "$SMOKE_NAMESPACE" "service/${SMOKE_RELEASE}" \
    "${CONTROLLER_METRICS_LOCAL_PORT}:9090" \
    >"$SMOKE_TMP_DIR/controller-port-forward.log" 2>&1 &
  CONTROLLER_PORT_FORWARD_PID=$!
  kubectl port-forward -n "$SMOKE_NAMESPACE" "service/${SMOKE_RELEASE}-watchdog" \
    "${WATCHDOG_METRICS_LOCAL_PORT}:9091" \
    >"$SMOKE_TMP_DIR/watchdog-port-forward.log" 2>&1 &
  WATCHDOG_PORT_FORWARD_PID=$!

  local deadline=$((SECONDS + 30))
  until curl --fail --silent "http://127.0.0.1:${CONTROLLER_METRICS_LOCAL_PORT}/metrics" \
      >"$SMOKE_TMP_DIR/controller.metrics"; do
    ((SECONDS < deadline)) || error "controller metrics port-forward did not become ready"
    sleep 1
  done
  until curl --fail --silent "http://127.0.0.1:${WATCHDOG_METRICS_LOCAL_PORT}/metrics" \
      >"$SMOKE_TMP_DIR/watchdog.metrics"; do
    ((SECONDS < deadline)) || error "watchdog metrics port-forward did not become ready"
    sleep 1
  done

  grep -F 'slurm_advisor_controller_reconcile_loops_total' "$SMOKE_TMP_DIR/controller.metrics" >/dev/null ||
    error "controller metrics are missing reconciliation output"
  grep -F 'slurm_advisor_controller_reconcile_stage_duration_seconds' "$SMOKE_TMP_DIR/controller.metrics" >/dev/null ||
    error "controller metrics are missing reconcile stage duration output"
  grep -F 'slurm_watchdog_polls_total' "$SMOKE_TMP_DIR/watchdog.metrics" >/dev/null ||
    error "watchdog metrics are missing poll output"
  success "Controller and watchdog Prometheus metrics are non-empty"
}

cleanup() {
  local exit_code=$?
  record_phase cleanup
  if enabled "$SMOKE_TEST_MODE"; then
    return 0
  fi

  if [[ -n "$CONTROLLER_PORT_FORWARD_PID" ]]; then
    kill "$CONTROLLER_PORT_FORWARD_PID" >/dev/null 2>&1 || true
    wait "$CONTROLLER_PORT_FORWARD_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$WATCHDOG_PORT_FORWARD_PID" ]]; then
    kill "$WATCHDOG_PORT_FORWARD_PID" >/dev/null 2>&1 || true
    wait "$WATCHDOG_PORT_FORWARD_PID" >/dev/null 2>&1 || true
  fi

  if enabled "${KEEP_CLUSTER:-}"; then
    cluster_teardown
    warn "KEEP_CLUSTER set — retaining smoke Helm release and mock service for inspection"
  elif command -v kubectl >/dev/null 2>&1 && kubectl cluster-info >/dev/null 2>&1; then
    helm uninstall "$SMOKE_RELEASE" -n "$SMOKE_NAMESPACE" >/dev/null 2>&1 || true
    kubectl delete deployment,service -n "$SMOKE_NAMESPACE" "$MOCK_NAME" --ignore-not-found >/dev/null 2>&1 || true
    cluster_teardown
  else
    cluster_teardown
  fi

  if [[ -n "$SMOKE_TMP_DIR" ]]; then
    rm -rf "$SMOKE_TMP_DIR"
  fi
  if [[ $exit_code -ne 0 ]]; then
    warn "Slurm advisor chart smoke exited with code $exit_code"
  fi
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

main() {
  record_phase validate
  validate_configuration
  if enabled "$SMOKE_TEST_MODE"; then
    run_phase prepare true
    run_phase build-images true
    run_phase cluster true
    run_phase load-images true
    run_phase load-support-images true
    run_phase provision true
    run_phase verify-slinky-images true
    run_phase deploy-mock-heimdall true
    run_phase install-chart true
    run_phase wait-ready true
    run_phase verify-slurm true
    run_phase verify-polls true
    run_phase verify-metrics true
    return 0
  fi

  SMOKE_TMP_DIR="$(mktemp -d)"
  configure_smoke
  run_phase prepare prepare
  run_phase build-images build_smoke_images
  run_phase cluster cluster_up_for_smoke
  run_phase load-images load_images
  run_phase load-support-images load_support_images
  run_phase provision provision_smoke_infrastructure
  run_phase verify-slinky-images verify_slinky_images
  run_phase deploy-mock-heimdall deploy_mock_heimdall
  run_phase install-chart install_chart
  run_phase wait-ready wait_for_workloads
  run_phase verify-slurm verify_slurm
  run_phase verify-polls verify_polls
  run_phase verify-metrics verify_metrics
  success "Slurm advisor controller chart smoke passed"
}

main "$@"
