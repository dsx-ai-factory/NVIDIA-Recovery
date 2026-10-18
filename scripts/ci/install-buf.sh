#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

BUF_VERSION="${BUF_VERSION:-1.54.0}"

if [[ -z "${GOBIN:-}" ]]; then
    echo "GOBIN is required to install buf" >&2
    exit 2
fi

buf_machine="$(uname -m)"
case "$buf_machine" in
    x86_64)
        buf_arch="x86_64"
        ;;
    aarch64|arm64)
        buf_arch="aarch64"
        ;;
    *)
        echo "unsupported buf architecture: $buf_machine" >&2
        exit 1
        ;;
esac

verify_sha256_manifest_entry() {
    local checksum_file="$1"

    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum -c "$checksum_file"
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 -c "$checksum_file"
    else
        echo "sha256sum or shasum is required to verify buf" >&2
        return 127
    fi
}

mkdir -p "$GOBIN"

buf_asset="buf-Linux-${buf_arch}"
buf_base_url="https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}"
buf_minisign_public_key="RWQ/i9xseZwBVE7pEniCNjlNOeeyp4BQgdZDLQcAohxEAH5Uj5DEKjv6"
buf_work_dir="$(mktemp -d)"
buf_checksum_file="$buf_work_dir/sha256.txt"
buf_asset_checksum_file="$buf_work_dir/${buf_asset}.sha256"

cleanup() {
    rm -rf "$buf_work_dir"
}

trap cleanup EXIT

curl -fsSL -o "$buf_checksum_file" "$buf_base_url/sha256.txt"
curl -fsSL -o "$buf_checksum_file.minisig" "$buf_base_url/sha256.txt.minisig"
minisign -Vm "$buf_checksum_file" -P "$buf_minisign_public_key"

curl -fsSL -o "$buf_work_dir/$buf_asset" "$buf_base_url/$buf_asset"

if ! awk -v asset="$buf_asset" '$2 == asset { print; found = 1 } END { exit found ? 0 : 1 }' \
    "$buf_checksum_file" > "$buf_asset_checksum_file"; then
    echo "signed buf checksum manifest does not contain $buf_asset" >&2
    exit 1
fi

(
    cd "$buf_work_dir"
    verify_sha256_manifest_entry "$buf_asset_checksum_file"
)

chmod +x "$buf_work_dir/$buf_asset"
mv -f "$buf_work_dir/$buf_asset" "$GOBIN/buf"
