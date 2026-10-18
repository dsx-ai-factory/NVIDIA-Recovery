#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Prepare an immutable, sparse checkout of the attribution service for the
# source-backed E2E lane. The checkout lives outside this repository and is
# never packaged as a Heimdall artifact.
set -euo pipefail

if (( $# != 3 )); then
  echo "usage: $0 DESTINATION SOURCE_URL SOURCE_COMMIT" >&2
  exit 2
fi

destination="$1"
source_url="$2"
source_commit="$3"

if [[ "$destination" != /* ]]; then
  echo "error: DESTINATION must be an absolute path" >&2
  exit 2
fi
if [[ -z "$source_url" || "$source_url" == -* ]]; then
  echo "error: SOURCE_URL must be non-empty and must not begin with '-'" >&2
  exit 2
fi
if [[ "$source_url" =~ ^https?://[^/]*@ ]]; then
  echo "error: credentials must not be embedded in SOURCE_URL" >&2
  exit 2
fi
if [[ ! "$source_commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "error: SOURCE_COMMIT must be a full lowercase Git commit SHA" >&2
  exit 2
fi

git_with_source_auth() {
  local deploy_key="${ATTRIBUTION_SOURCE_DEPLOY_KEY:-}"
  local deploy_key_file="${ATTRIBUTION_SOURCE_DEPLOY_KEY_FILE:-}"
  local key_path
  local known_hosts_path
  local quoted_key_path
  local quoted_known_hosts_path

  if [[ -z "$deploy_key" && -z "$deploy_key_file" ]]; then
    git "$@"
    return
  fi
  if [[ -n "$deploy_key" && -n "$deploy_key_file" ]]; then
    echo "error: set only one attribution deploy-key input" >&2
    return 2
  fi
  if [[ "$source_url" != git@github.com:* && "$source_url" != ssh://git@github.com/* ]]; then
    echo "error: attribution deploy keys require a GitHub SSH source URL" >&2
    return 2
  fi

  # A filtered fetch can defer blob downloads until checkout. Reuse one SSH
  # identity for both operations so the promisor remote remains reachable.
  if [[ -z "$auth_directory" ]]; then
    auth_directory="$(mktemp -d "${TMPDIR:-/tmp}/heimdall-attribution-ssh.XXXXXX")"
  fi
  key_path="$auth_directory/id"
  known_hosts_path="$auth_directory/known_hosts"
  if [[ ! -f "$key_path" ]]; then
    if [[ -n "$deploy_key_file" ]]; then
      [[ -r "$deploy_key_file" ]] || {
        echo "error: attribution deploy-key file is unreadable" >&2
        return 2
      }
      cp "$deploy_key_file" "$key_path"
    else
      printf '%s\n' "$deploy_key" >"$key_path"
    fi
    chmod 0600 "$key_path"
    ssh-keyscan -T 10 github.com >"$known_hosts_path" 2>/dev/null
  fi
  [[ -s "$known_hosts_path" ]] || {
    echo "error: could not obtain GitHub SSH host keys" >&2
    return 1
  }
  printf -v quoted_key_path '%q' "$key_path"
  printf -v quoted_known_hosts_path '%q' "$known_hosts_path"
  GIT_SSH_COMMAND="ssh -i $quoted_key_path -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$quoted_known_hosts_path" \
    git "$@"
}

validate_checkout() {
  local head remote status

  if ! head="$(git -C "$destination" rev-parse HEAD 2>/dev/null)"; then
    echo "error: existing attribution source path is not a Git checkout: $destination" >&2
    return 1
  fi
  if [[ "$head" != "$source_commit" ]]; then
    echo "error: existing attribution source checkout is at $head, expected $source_commit" >&2
    return 1
  fi
  if ! remote="$(git -C "$destination" config --get remote.origin.url 2>/dev/null)"; then
    echo "error: existing attribution source checkout has no origin remote" >&2
    return 1
  fi
  if [[ "$remote" != "$source_url" ]]; then
    echo "error: existing attribution source origin does not match SOURCE_URL" >&2
    return 1
  fi
  status="$(git -C "$destination" status --porcelain)"
  if [[ -n "$status" ]]; then
    echo "error: existing attribution source checkout has local changes: $destination" >&2
    return 1
  fi
  # Earlier cache entries included only /service/. The provider image now also
  # copies the root license; expand verified sparse checkouts without replacing
  # their immutable source or changing a caller-owned full checkout.
  if [[ ! -f "$destination/LICENSE" ]] &&
    [[ "$(git -C "$destination" config --bool core.sparseCheckout || true)" == true ]]; then
    git_with_source_auth -C "$destination" sparse-checkout add /LICENSE
  fi
  for required_path in \
    LICENSE \
    service/service-app/Dockerfile \
    service/service-app/pyproject.toml \
    service/service-app/uv.lock \
    service/service-app/nvidia_attribution_client/pyproject.toml \
    service/service-app/nvidia_attribution_client/nvidia_attribution_client \
    service/attribution-lib; do
    if [[ ! -e "$destination/$required_path" ]]; then
      echo "error: attribution source checkout is missing $required_path" >&2
      return 1
    fi
  done
}

parent_directory="$(dirname "$destination")"
mkdir -p "$parent_directory"

# A commit-addressed destination is shared safely across repeated local runs.
# Serialize its one-time creation, and never replace an existing checkout.
lock_directory="${destination}.lock"
lock_attempt=0
until mkdir "$lock_directory" 2>/dev/null; do
  lock_attempt=$((lock_attempt + 1))
  if (( lock_attempt >= 300 )); then
    echo "error: timed out waiting for attribution source checkout lock: $lock_directory" >&2
    exit 1
  fi
  sleep 0.2
done

staging_directory=""
auth_directory=""
cleanup() {
  if [[ -n "$staging_directory" && -d "$staging_directory" ]]; then
    rm -rf -- "$staging_directory"
  fi
  if [[ -n "$auth_directory" && -d "$auth_directory" ]]; then
    rm -rf -- "$auth_directory"
  fi
  rmdir "$lock_directory" 2>/dev/null || true
}
trap cleanup EXIT

if [[ -e "$destination" ]]; then
  validate_checkout
  exit 0
fi

staging_directory="$(mktemp -d "${destination}.tmp.XXXXXX")"
git init --quiet "$staging_directory"
git -C "$staging_directory" remote add origin "$source_url"
git -C "$staging_directory" sparse-checkout init --no-cone
# Include the gitignore rules for the later git status call, which could otherwise fetch them without authentication.
git -C "$staging_directory" sparse-checkout set --no-cone /service/ /.gitignore /LICENSE
if ! git_with_source_auth -C "$staging_directory" fetch \
  --quiet \
  --depth=1 \
  --filter=blob:none \
  origin \
  "$source_commit"; then
  echo "error: Attribution is enabled but its pinned source could not be fetched; provide authorized source access or explicitly select degraded OFF mode. Public source availability is a release prerequisite." >&2
  exit 1
fi
git_with_source_auth -C "$staging_directory" checkout --quiet --detach FETCH_HEAD

staged_head="$(git -C "$staging_directory" rev-parse HEAD)"
if [[ "$staged_head" != "$source_commit" ]]; then
  echo "error: fetched attribution source resolved to $staged_head, expected $source_commit" >&2
  exit 1
fi

mv "$staging_directory" "$destination"
staging_directory=""
validate_checkout
