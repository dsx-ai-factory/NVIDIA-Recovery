#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
: "${KUBECONFIG:?Select an explicit kubeconfig}"
: "${LAUNCHER_KUBERNETES_JOBSET_CONTEXT:?Select an explicit test-cluster context}"
: "${LAUNCHER_KUBERNETES_JOBSET_NAMESPACE:?Select an explicit test namespace}"
export WORKLOAD_MAP_PROBE="${1:?Pass the built probe path}"
shift
mkdir -p reports/workload-map
report=$(mktemp -d reports/workload-map/run-XXXXXXXX)
kubectl --context "$LAUNCHER_KUBERNETES_JOBSET_CONTEXT" --request-timeout=30s version -o json > "$report/kubernetes-version.json"
# Record the installed controller image independently of the SDK's own report.
kubectl --context "$LAUNCHER_KUBERNETES_JOBSET_CONTEXT" --request-timeout=30s -n jobset-system \
  get deployment jobset-controller-manager -o json > "$report/jobset-controller.json"
uv run --locked --group e2e pytest tests/workload_map_live \
  --launcher kubernetes-jobset --basetemp "$report/pytest" \
  --trace-output "$report/trace.json" --junitxml "$report/junit.xml" "$@"
