#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if (( $# < 2 || $# > 4 )); then
  echo "usage: $0 SOURCE DEST [loki|victorialogs] [CONFIGMAP]" >&2
  exit 2
fi

source_file="$1"
dest_file="$2"
log_backend="${3-${E2E_LOG_BACKEND-loki}}"
configmap="${4-alloy-config}"
case "$log_backend" in
  loki) push_url='http://loki-gateway.monitoring.svc/loki/api/v1/push' ;;
  victorialogs)
    # Keep JSON messages opaque, including fields that collide with stream labels.
    push_url='http://victorialogs.monitoring.svc:9428/insert/loki/api/v1/push?disable_message_parsing=1'
    ;;
  *) echo "invalid E2E_LOG_BACKEND=$log_backend; expected loki or victorialogs" >&2; exit 2 ;;
esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$script_dir/render.sh" "$source_file" "$dest_file" __LOG_PUSH_URL__ "$push_url"

kubectl create namespace slurm --dry-run=client -o yaml \
  | kubectl apply -f -
kubectl create configmap "$configmap" \
  --namespace slurm \
  --from-file="config.alloy=$dest_file" \
  --dry-run=client -o yaml \
  | kubectl apply -f -

# These ConfigMaps are externally managed and the chart reloader is disabled.
# Restart an existing owner after applying config so endpoint/pipeline edits
# take effect; the application's Recreate strategy drains its old reader first.
workloads="$(kubectl get daemonset,deployment --namespace slurm \
  --selector "app.kubernetes.io/instance=${configmap%-config}" --output name)"
for workload in $workloads; do
  kubectl rollout restart "$workload" --namespace slurm
  kubectl rollout status "$workload" --namespace slurm \
    --timeout="${ALLOY_WAIT_TIMEOUT:-${SERVICE_WAIT_TIMEOUT:-900}}s"
done
