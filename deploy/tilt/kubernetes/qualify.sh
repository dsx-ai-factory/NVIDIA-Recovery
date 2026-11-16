#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Install optional qualification controllers only into this profile's owned Kind cluster.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
: "${KUBERNETES_DEV_CLUSTER:?Select the disposable qualification cluster}"
[[ "$KUBERNETES_DEV_CLUSTER" =~ ^[a-z0-9][a-z0-9-]*$ ]] || {
  echo 'KUBERNETES_DEV_CLUSTER must be a lowercase DNS label' >&2; exit 2;
}
state="$repo_root/deploy/tilt/tmp/$KUBERNETES_DEV_CLUSTER"
export KUBECONFIG="$state/kubeconfig"
context="kind-$KUBERNETES_DEV_CLUSTER"
[[ -s "$state/owned" && -f "$KUBECONFIG" ]] || {
  echo 'Create the owned Kind profile with make dev-k8s-ci first' >&2; exit 1;
}
uid="$(kubectl --context "$context" --request-timeout=15s get namespace kube-system -o jsonpath='{.metadata.uid}')"
[[ "$uid" == "$(cat "$state/owned")" ]] || {
  echo 'Cluster identity changed; refusing to install qualification controllers' >&2; exit 1;
}

kubectl --context "$context" apply --server-side -k \
  'https://github.com/kubeflow/training-operator/manifests/overlays/standalone?ref=v1.9.0'
kubectl --context "$context" wait --for=condition=Established \
  crd/pytorchjobs.kubeflow.org --timeout=120s
kubectl --context "$context" -n kubeflow rollout status \
  deployment/training-operator --timeout=300s

helm upgrade --install kai-scheduler oci://ghcr.io/nvidia/kai-scheduler/kai-scheduler \
  --kube-context "$context" --namespace kai-scheduler --create-namespace \
  --version v0.12.0 --timeout 10m
# Wait on native deployments explicitly: generic Helm readiness treats KAI
# Reconciling=True as unfinished even when its Available condition is True.
# The operator creates these deployments after Helm has installed the release.
for deployment in kai-operator pod-grouper podgroup-controller binder queue-controller admission kai-scheduler-default; do
  kubectl --context "$context" -n kai-scheduler wait --for=create "deployment/$deployment" --timeout=120s
  if [[ "$deployment" == kai-scheduler-default ]]; then
    # v0.12.0 recreates its Config hook and owned ServiceAccount on upgrade,
    # but retains the SchedulingShard-owned scheduler Pod. Refresh the Pod's
    # bound token after the account UID changes; other operands are recreated.
    kubectl --context "$context" -n kai-scheduler rollout restart "deployment/$deployment"
  fi
  kubectl --context "$context" -n kai-scheduler rollout status "deployment/$deployment" --timeout=300s
done
kubectl --context "$context" wait --for=condition=Available schedulingshards.kai.scheduler/default --timeout=300s
kubectl --context "$context" get queues.scheduling.run.ai default-queue
