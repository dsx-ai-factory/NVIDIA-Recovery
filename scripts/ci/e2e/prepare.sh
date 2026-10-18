#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# prepare.sh — ensure kind, kubectl, helm, and Tilt are available.
#
# Provides one function consumed by e2e.sh:
#
#   prepare — for each tool, use the copy on PATH if present; otherwise download
#             the pinned version into TOOLS_DIR (a persistent cache, see env.sh),
#             verifying its SHA256 against the upstream checksum before
#             installing. Safe to run repeatedly — cached binaries are reused.
#
# Downloads are staged to a temp file and only moved into place after checksum
# verification passes, so a corrupt or tampered artifact never lands on PATH.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
source "$SCRIPT_DIR/lib.sh"

# Resolve OS name in lowercase for download URLs.
_os() { uname -s | tr '[:upper:]' '[:lower:]'; }

# ---------------------------------------------------------------------------
# _sha256 <file> — print the file's SHA256 hex digest using whichever tool is
# available (sha256sum on Linux, shasum on macOS).
# ---------------------------------------------------------------------------
_sha256() {
  local file="$1"
  if command -v sha256sum &>/dev/null; then
    sha256sum "$file" | awk '{print $1}'
  elif command -v shasum &>/dev/null; then
    shasum -a 256 "$file" | awk '{print $1}'
  else
    error "Neither sha256sum nor shasum found — cannot verify downloads."
  fi
}

# ---------------------------------------------------------------------------
# _fetch_verified <dest> <url> <checksum_url> <version>
# Downloads <url> to a temp file, verifies its SHA256 against the first token
# of <checksum_url>'s contents, then atomically moves it to <dest>. <version>
# is encoded into the temp name so a stale/interrupted download is identifiable.
# ---------------------------------------------------------------------------
_fetch_verified() {
  local dest="$1"
  local url="$2"
  local checksum_url="$3"
  local version="$4"

  local tmp expected actual
  tmp="$(mktemp "${dest}.${version}.download.XXXXXX")"

  log "  fetching: $url"
  curl -fsSL -o "$tmp" "$url" \
    || { rm -f "$tmp"; error "Download failed: $url"; }

  log "  verifying checksum: $checksum_url"
  expected="$(curl -fsSL "$checksum_url" | awk '{print $1}')" \
    || { rm -f "$tmp"; error "Checksum download failed: $checksum_url"; }
  [[ -n "$expected" ]] \
    || { rm -f "$tmp"; error "Empty checksum from: $checksum_url"; }

  actual="$(_sha256 "$tmp")"
  if [[ "$actual" != "$expected" ]]; then
    rm -f "$tmp"
    error "Checksum mismatch for $dest — expected $expected, got $actual"
  fi

  chmod +x "$tmp"
  mv -f "$tmp" "$dest"
  success "  verified and installed: $dest"
}

# ---------------------------------------------------------------------------
# _install_kind
# ---------------------------------------------------------------------------
_install_kind() {
  command -v kind &>/dev/null && return 0
  log "Installing kind ${KIND_VERSION} ..."
  local asset
  asset="kind-$(_os)-${ARCH}"
  _fetch_verified \
    "$TOOLS_DIR/kind" \
    "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/${asset}" \
    "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/${asset}.sha256sum" \
    "$KIND_VERSION"
}

# ---------------------------------------------------------------------------
# _install_kubectl
# ---------------------------------------------------------------------------
_install_kubectl() {
  command -v kubectl &>/dev/null && return 0
  log "Installing kubectl ${KUBECTL_VERSION} ..."
  local base
  base="https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/$(_os)/${ARCH}/kubectl"
  _fetch_verified \
    "$TOOLS_DIR/kubectl" \
    "$base" \
    "${base}.sha256" \
    "$KUBECTL_VERSION"
}

# ---------------------------------------------------------------------------
# _install_helm — distributed as a tarball, so verify then extract.
# ---------------------------------------------------------------------------
_install_helm() {
  command -v helm &>/dev/null && return 0
  log "Installing helm ${HELM_VERSION} ..."
  local os pkg tmp expected actual
  os="$(_os)"
  pkg="helm-${HELM_VERSION}-${os}-${ARCH}.tar.gz"
  tmp="$(mktemp "${TOOLS_DIR}/helm.${HELM_VERSION}.download.XXXXXX")"

  log "  fetching: https://get.helm.sh/${pkg}"
  curl -fsSL -o "$tmp" "https://get.helm.sh/${pkg}" \
    || { rm -f "$tmp"; error "Download failed: ${pkg}"; }

  log "  verifying checksum: https://get.helm.sh/${pkg}.sha256sum"
  expected="$(curl -fsSL "https://get.helm.sh/${pkg}.sha256sum" | awk '{print $1}')" \
    || { rm -f "$tmp"; error "Checksum download failed for ${pkg}"; }
  actual="$(_sha256 "$tmp")"
  if [[ "$expected" != "$actual" ]]; then
    rm -f "$tmp"
    error "Checksum mismatch for helm — expected $expected, got $actual"
  fi

  tar -xzf "$tmp" -C "$TOOLS_DIR" --strip-components=1 "${os}-${ARCH}/helm"
  chmod +x "$TOOLS_DIR/helm"
  rm -f "$tmp"
  success "  verified and installed: $TOOLS_DIR/helm"
}

# ---------------------------------------------------------------------------
# Tilt release selection and installation.
# Tilt publishes separate archives for Alpine Linux, GNU Linux, and macOS.
# ---------------------------------------------------------------------------
_tilt_platform() {
  case "$(_os)" in
    darwin) printf 'mac\n' ;;
    linux)
      if [[ -f /etc/alpine-release ]]; then
        printf 'linux-alpine\n'
      else
        printf 'linux\n'
      fi
      ;;
    *) error "Tilt ${TILT_VERSION} is not available for OS: $(_os)" ;;
  esac
}

_tilt_asset() {
  local platform="$1"
  local arch="$2"
  local release_arch
  case "$arch" in
    amd64 | x86_64) release_arch="x86_64" ;;
    arm64 | aarch64) release_arch="arm64" ;;
    *) error "Tilt ${TILT_VERSION} is not available for architecture: $arch" ;;
  esac
  case "$platform" in
    linux | linux-alpine | mac) ;;
    *) error "Unsupported Tilt release platform: $platform" ;;
  esac
  printf 'tilt.%s.%s.%s.tar.gz\n' "${TILT_VERSION#v}" "$platform" "$release_arch"
}

_install_tilt() (
  command -v tilt &>/dev/null && exit 0

  local platform asset base archive checksums staging_dir expected actual
  platform="$(_tilt_platform)"
  asset="$(_tilt_asset "$platform" "$ARCH")"
  base="https://github.com/tilt-dev/tilt/releases/download/${TILT_VERSION}"
  archive="$(mktemp "${TOOLS_DIR}/tilt.${TILT_VERSION}.download.XXXXXX")"
  checksums="$(mktemp "${TOOLS_DIR}/tilt.${TILT_VERSION}.checksums.XXXXXX")"
  staging_dir=''
  trap 'rm -f "$archive" "$checksums"; [[ -z "$staging_dir" ]] || rm -rf "$staging_dir"' EXIT

  log "Installing Tilt ${TILT_VERSION} ..."
  log "  fetching: ${base}/${asset}"
  curl -fsSL -o "$archive" "${base}/${asset}" \
    || error "Download failed: ${asset}"

  log "  verifying checksum: ${base}/checksums.txt"
  curl -fsSL -o "$checksums" "${base}/checksums.txt" \
    || error "Checksum download failed for ${asset}"
  expected="$(awk -v asset="$asset" '$2 == asset { print $1; exit }' "$checksums")"
  [[ -n "$expected" ]] || error "Checksum not found for ${asset}"
  actual="$(_sha256 "$archive")"
  [[ "$actual" == "$expected" ]] \
    || error "Checksum mismatch for Tilt — expected $expected, got $actual"

  staging_dir="$(mktemp -d "${TOOLS_DIR}/tilt.${TILT_VERSION}.extract.XXXXXX")"
  tar -xzf "$archive" -C "$staging_dir" tilt \
    || error "Failed to extract Tilt archive: ${asset}"
  [[ -s "$staging_dir/tilt" ]] \
    || error "Tilt archive did not contain a usable tilt binary: ${asset}"
  chmod +x "$staging_dir/tilt"
  mv -f "$staging_dir/tilt" "$TOOLS_DIR/tilt"
  success "  verified and installed: $TOOLS_DIR/tilt"
)

# ---------------------------------------------------------------------------
# prepare — entry point called by e2e.sh
# ---------------------------------------------------------------------------
prepare() {
  banner "Phase: Prepare tools"

  mkdir -p "$TOOLS_DIR"
  _install_kind
  _install_kubectl
  _install_helm
  _install_tilt

  require_cmd kind    "Install: https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
  require_cmd kubectl "Install: https://kubernetes.io/docs/tasks/tools/"
  require_cmd helm    "Install: https://helm.sh/docs/intro/install/"
  require_cmd tilt    "Install: https://docs.tilt.dev/install.html"
  require_cmd docker  "Install: https://docs.docker.com/get-docker/"

  if [[ -n "${DEPLOY_STACK:-}" && "${E2E_ATTRIBUTION_MODE:-available}" == "available" ]]; then
    require_cmd git    "Install Git to fetch the pinned attribution source"
    if [[ -n "${ATTRIBUTION_SOURCE_DEPLOY_KEY:-}${ATTRIBUTION_SOURCE_DEPLOY_KEY_FILE:-}" ]]; then
      require_cmd ssh         "Install OpenSSH to use the attribution deploy key"
      require_cmd ssh-keyscan "Install OpenSSH to verify the GitHub SSH host"
    fi
  fi

  success "All tools available"
  log "  kind:    $(kind version 2>&1 | head -1)"
  log "  kubectl: $(kubectl version --client 2>&1 | head -1)"
  log "  helm:    $(helm version --short 2>&1 | head -1)"
  log "  tilt:    $(tilt version 2>&1 | head -1)"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  prepare
  (( $# == 0 )) || exec "$@"
fi
