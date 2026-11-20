#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
: "${KUBECONFIG:?Select an explicit kubeconfig}"
export WORKLOAD_MAP_PROBE="${1:?Pass the built probe path}"
shift
launcher=${WORKLOAD_MAP_LAUNCHER:-kubernetes-jobset}
case "$launcher" in
  kubernetes-jobset)
    suite=tests/workload_map_live/test_jobset.py
    context=${LAUNCHER_KUBERNETES_JOBSET_CONTEXT:-}
    namespace=${LAUNCHER_KUBERNETES_JOBSET_NAMESPACE:-}
    controller_namespace=${WORKLOAD_MAP_CONTROLLER_NAMESPACE:-jobset-system}
    controller_deployment=${WORKLOAD_MAP_CONTROLLER_DEPLOYMENT:-jobset-controller-manager}
    ;;
  kubernetes-pytorch)
    suite=tests/workload_map_live/test_pytorch.py
    context=${LAUNCHER_KUBERNETES_PYTORCH_CONTEXT:-${LAUNCHER_KUBERNETES_JOBSET_CONTEXT:-}}
    namespace=${LAUNCHER_KUBERNETES_PYTORCH_NAMESPACE:-${LAUNCHER_KUBERNETES_JOBSET_NAMESPACE:-}}
    controller_namespace=${WORKLOAD_MAP_CONTROLLER_NAMESPACE:-kubeflow}
    controller_deployment=${WORKLOAD_MAP_CONTROLLER_DEPLOYMENT:-training-operator}
    ;;
  *) echo "Unsupported qualification launcher: $launcher" >&2; exit 2 ;;
esac
: "${context:?Select an explicit test-cluster context for the selected launcher}"
: "${namespace:?Select an explicit test namespace for the selected launcher}"
# Provenance and the launcher's fallback selection must describe the same cluster.
export LAUNCHER_KUBERNETES_JOBSET_CONTEXT="$context"
export LAUNCHER_KUBERNETES_JOBSET_NAMESPACE="$namespace"
mkdir -p reports/workload-map
report=$(mktemp -d reports/workload-map/run-XXXXXXXX)
kubectl --context "$context" --request-timeout=30s version -o json > "$report/kubernetes-version.json"
# Capture configuration, controller ownership and actual running container imageIDs.
kubectl --context "$context" --request-timeout=30s -n "$controller_namespace" \
  get deployment "$controller_deployment" -o json > "$report/workload-controller.json"
kubectl --context "$context" --request-timeout=30s -n "$controller_namespace" \
  get replicasets,pods -o json > "$report/controller-instances.json"
if [[ ${WORKLOAD_MAP_SCHEDULER:-default-scheduler} == kai-scheduler ]]; then
  : "${WORKLOAD_MAP_QUEUE:?Select an explicit KAI queue}"
  kai_namespace=${WORKLOAD_MAP_KAI_NAMESPACE:-kai-scheduler}
  kubectl --context "$context" --request-timeout=30s -n "$kai_namespace" \
    get deployments,replicasets,pods -o json > "$report/kai-components.json"
  kubectl --context "$context" --request-timeout=30s \
    get queues.scheduling.run.ai "$WORKLOAD_MAP_QUEUE" -o json > "$report/kai-queue.json"
fi
uv run --locked python - "$context" "$namespace" "$launcher" "${WORKLOAD_MAP_SCHEDULER:-default-scheduler}" <<'PY' > "$report/selection.json"
import json
import sys

print(json.dumps(dict(zip(("context", "namespace", "launcher", "scheduler"), sys.argv[1:], strict=True)), indent=2))
PY
uv run --locked --group e2e pytest "$suite" \
  --launcher "$launcher" --basetemp "$report/pytest" \
  --trace-output "$report/trace.json" --junitxml "$report/junit.xml" "$@"
