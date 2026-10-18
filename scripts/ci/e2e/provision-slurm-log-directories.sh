#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Provision the shared source and controller-owned destination used by the
# Advisor log-link reference deployment.
set -euo pipefail

namespace="${1:-slurm}"
login_pod="$(kubectl get pod \
  --namespace "$namespace" \
  --selector app.kubernetes.io/component=login \
  --output 'jsonpath={.items[0].metadata.name}')"
if [[ -z "$login_pod" ]]; then
  printf 'error: Slurm login pod was not found in namespace %s\n' "$namespace" >&2
  exit 1
fi

# The production controller runs as 401:401. Pre-provision its dedicated
# destination instead of granting DirectoryOrCreate or broad shared-home write
# access. Jobs write targets under /home/logs; the collector follows links
# from /home/log-links while mounting both at the same absolute paths. The
# setgid source directory and the submission umask keep future Slurm output in
# the collector's read-only group without granting world write access.
kubectl exec --namespace "$namespace" "$login_pod" -- mkdir -p -- /home/logs /home/log-links
kubectl exec --namespace "$namespace" "$login_pod" -- chown -- 0:401 /home/logs
kubectl exec --namespace "$namespace" "$login_pod" -- chmod -- 2770 /home/logs
kubectl exec --namespace "$namespace" "$login_pod" -- chown -- 401:401 /home/log-links
kubectl exec --namespace "$namespace" "$login_pod" -- chmod -- 0750 /home/log-links

# Keep collector offsets outside both read-only source claims.
kubectl exec --namespace "$namespace" "$login_pod" -- install -d -o 401 -g 401 -m 0750 /home/alloy-application-positions
