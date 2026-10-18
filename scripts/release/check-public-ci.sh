#!/bin/sh
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Require the latest public main push run for this commit, including reruns.
# Reachability or a successful run for a different main commit is insufficient.
set -eu
sha="$(git rev-parse HEAD)"
for workflow in pull-request-and-main.yml docs.yml; do
  result="$(gh api --method GET \
    "repos/${GITHUB_REPOSITORY:?}/actions/workflows/$workflow/runs" \
    -f "head_sha=$sha" -f branch=main -f event=push -f per_page=1 \
    --jq '.workflow_runs[0] | [.head_sha, .status, .conclusion] | join(" ")')"
  if [ "$result" != "$sha completed success" ]; then
    printf 'error: latest %s main CI for %s must complete successfully; got: %s\n' \
      "$workflow" "$sha" "$result" >&2
    exit 1
  fi
  printf '%s main CI succeeded for exact source %s\n' "$workflow" "$sha"
done
