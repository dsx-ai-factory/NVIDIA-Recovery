#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# 04-install-slurm.sh — install the slinky slurm-operator and a Slurm cluster.
#
# Run by provision.sh. Both charts are public OCI charts; idempotent via
# `helm upgrade --install`. Login and compute share claim-backed `/home`.
# Kind's host backing is confined to the explicit PersistentVolume manifest.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../env.sh"
source "$SCRIPT_DIR/../lib.sh"

banner "Service: slurm-operator ${SLURM_OPERATOR_CHART_VERSION} + slurm ${SLURM_CHART_VERSION}"

# Helm 4's watcher readiness strategy waits indefinitely on Slinky's NodeSet,
# whose controller does not populate status. Use the Helm 3 readiness behavior
# on Helm 4 while retaining the boolean flag accepted by Helm 3.
helm_wait_args=(--wait)
if helm upgrade --help | grep -Fq 'WaitStrategy'; then
  helm_wait_args=(--wait=legacy)
fi

# Operator (controller).
helm upgrade --install slurm-operator \
  oci://ghcr.io/slinkyproject/charts/slurm-operator \
  --version "$SLURM_OPERATOR_CHART_VERSION" \
  --namespace slurm-system --create-namespace \
  --cleanup-on-fail \
  "${helm_wait_args[@]}" --timeout "${SERVICE_WAIT_TIMEOUT}s"

# PriorityClass referenced by the compute nodeset (the NodeSet CRD rejects an
# empty priorityClassName). Cluster-scoped; idempotent via apply.
kubectl apply -f "$SCRIPT_DIR/values/slurm-priorityclass.yaml"

# Cluster (controller, login, compute) on the shared home.
values_rendered="$(mktemp)"
storage_rendered="$(mktemp)"
trap 'rm -f "$values_rendered" "$storage_rendered"' EXIT
kubectl create namespace slurm --dry-run=client -o yaml | kubectl apply -f -
sed "s|__SLURM_SHARED_HOME__|${SLURM_SHARED_HOME}|g" \
  "$SCRIPT_DIR/values/slurm-shared-storage.yaml" > "$storage_rendered"
kubectl apply -f "$storage_rendered"
sed \
  -e "s|__SLINKY_SLURM_TAG__|${E2E_SLINKY_SLURM_TAG}|g" \
  "$SCRIPT_DIR/values/slurm.yaml" > "$values_rendered"

helm upgrade --install slurm \
  oci://ghcr.io/slinkyproject/charts/slurm \
  --version "$SLURM_CHART_VERSION" \
  --namespace slurm --create-namespace \
  --values "$values_rendered" \
  --cleanup-on-fail \
  "${helm_wait_args[@]}" --timeout "${SERVICE_WAIT_TIMEOUT}s"

"$SCRIPT_DIR/../provision-slurm-log-directories.sh" slurm

success "slurm installed (namespaces: slurm-system, slurm)"
