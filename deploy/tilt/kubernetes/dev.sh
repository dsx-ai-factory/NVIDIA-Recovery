#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
export KUBERNETES_DEV_CLUSTER="${KUBERNETES_DEV_CLUSTER:-nvr-kubernetes-dev}"
[[ "$KUBERNETES_DEV_CLUSTER" =~ ^[a-z0-9][a-z0-9-]*$ ]] || {
  echo 'KUBERNETES_DEV_CLUSTER must be a lowercase DNS label' >&2; exit 2;
}
state="$repo_root/deploy/tilt/tmp/$KUBERNETES_DEV_CLUSTER"
export KUBECONFIG="$state/kubeconfig"
export LAUNCHER_KUBERNETES_JOBSET_CONTEXT="kind-$KUBERNETES_DEV_CLUSTER"

require_owned() {
  [[ -f "$state/owned" && -f "$KUBECONFIG" ]] || {
    echo "No owned development cluster; run $0 up first" >&2; exit 1;
  }
  uid="$(kubectl --context "$LAUNCHER_KUBERNETES_JOBSET_CONTEXT" --request-timeout=15s get namespace kube-system -o jsonpath='{.metadata.uid}')"
  [[ "$uid" == "$(cat "$state/owned")" ]] || {
    echo 'Cluster identity changed; refusing to use or delete it' >&2; exit 1;
  }
}

case "${1:-}" in
  up)
    shift
    mkdir -p "$state"
    # Never replace an unrelated cluster merely because its name matches.
    clusters="$(kind get clusters)"
    if printf '%s\n' "$clusters" | grep -Fxq "$KUBERNETES_DEV_CLUSTER"; then
      require_owned
    else
      kind create cluster --name "$KUBERNETES_DEV_CLUSTER" \
        --kubeconfig "$KUBECONFIG" --image kindest/node:v1.35.0 \
        --config "${KUBERNETES_DEV_KIND_CONFIG:-$script_dir/kind.yaml}" --wait 180s
      kubectl --context "$LAUNCHER_KUBERNETES_JOBSET_CONTEXT" get namespace kube-system \
        -o jsonpath='{.metadata.uid}' > "$state/owned"
    fi
    # Tilt retains the same build/deploy model as the existing local workflow.
    if (( $# == 0 )); then set -- up; fi
    exec tilt "$@" --file "$script_dir/Tiltfile"
    ;;
  test)
    shift
    require_owned
    kubectl --context "$LAUNCHER_KUBERNETES_JOBSET_CONTEXT" -n jobset-system \
      rollout status deployment/jobset-controller-manager --timeout=120s
    report="$(mktemp -d "$state/run-XXXXXXXX")"
    cd "$repo_root"
    exec uv run --locked --group e2e pytest tests/e2e_kubernetes \
      --launcher kubernetes-jobset --basetemp "$report/pytest" \
      --trace-output "$report/trace.json" --junitxml "$report/junit.xml" "$@"
    ;;
  down)
    require_owned
    kind delete cluster --name "$KUBERNETES_DEV_CLUSTER" --kubeconfig "$KUBECONFIG"
    rm -f "$state/owned" "$KUBECONFIG"
    ;;
  *)
    echo "usage: $0 {up [up|ci] [tilt options...]|test [pytest options...]|down}" >&2
    exit 2
    ;;
esac
