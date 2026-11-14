#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

context="kind-${KUBERNETES_DEV_CLUSTER:-nvr-kubernetes-dev}"
version=v0.12.0
kubectl --context "$context" apply --server-side \
  -f "https://github.com/kubernetes-sigs/jobset/releases/download/$version/manifests.yaml"
kubectl --context "$context" wait --for=condition=Established \
  crd/jobsets.jobset.x-k8s.io --timeout=120s
kubectl --context "$context" -n jobset-system rollout status \
  deployment/jobset-controller-manager --timeout=300s
kubectl --context "$context" create namespace recovery-dev --dry-run=client -o yaml |
  kubectl --context "$context" apply -f -
