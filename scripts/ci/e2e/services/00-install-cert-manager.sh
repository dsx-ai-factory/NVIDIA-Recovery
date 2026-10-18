#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# 00-install-cert-manager.sh — install cert-manager (runs first).
#
# Run by provision.sh. Prerequisite for the slurm-operator webhook: its chart
# ships a selfSigned cert-manager Issuer + Certificates, so cert-manager's CRDs
# and controller must exist before slurm installs. Idempotent via
# `helm upgrade --install`.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../env.sh"
source "$SCRIPT_DIR/../lib.sh"

banner "Service: cert-manager ${CERT_MANAGER_CHART_VERSION}"

helm repo add jetstack https://charts.jetstack.io --force-update >/dev/null
helm repo update jetstack >/dev/null

helm upgrade --install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --version "$CERT_MANAGER_CHART_VERSION" \
  --set crds.enabled=true \
  --cleanup-on-fail \
  --wait --timeout "${SERVICE_WAIT_TIMEOUT}s"

success "cert-manager installed (namespace: cert-manager)"
