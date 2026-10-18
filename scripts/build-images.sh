#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

readonly DEFAULT_PLATFORMS="linux/amd64"
readonly DEFAULT_SLURM_IMAGE="ghcr.io/slinkyproject/sackd:25.05-ubuntu24.04@sha256:dd1f0b6030229b3d602e3d80e6d50cc4f9fd17b325312521dfa07376fffc7569"
readonly DEFAULT_ATTRIBUTION_SOURCE_URL="https://github.com/dsx-ai-factory/attribution.git"
readonly DEFAULT_ATTRIBUTION_SOURCE_REF="7d6dff168bc282f471828fc8468e02de0df17480"

# Attribution is part of the default stack. Explicit OFF is degraded operation.
readonly -a DEFAULT_SERVICES=(
  anomaly-detection-isolator
  api-gateway
  dashboard
  job-manager
  notifications
  watcher
  slurm-advisor-controller
  slurm-watchdog
)

readonly -a SERVICES=(
  "${DEFAULT_SERVICES[@]}"
  attribution-adapter
  attribution-service
)

list_services() {
  printf '%s\n' "${SERVICES[@]}"
}

service_supported() {
  local candidate="$1"
  local service

  for service in "${SERVICES[@]}"; do
    [[ "$candidate" == "$service" ]] && return 0
  done

  return 1
}

configure_python_service() {
  DOCKERFILE="$REPO_ROOT/$1"
  BUILD_CONTEXT="$REPO_ROOT"
}

prepare_attribution_source() {
  local cache_root
  local source_ref="${ATTRIBUTION_SOURCE_REF:-$DEFAULT_ATTRIBUTION_SOURCE_REF}"
  local source_url="${ATTRIBUTION_SOURCE_URL:-$DEFAULT_ATTRIBUTION_SOURCE_URL}"

  cache_root="${XDG_CACHE_HOME:-${HOME}/.cache}/heimdall-build"
  ATTRIBUTION_SOURCE_DIR="${ATTRIBUTION_SOURCE_DIR:-$cache_root/attribution-source/$source_ref}"
  "$REPO_ROOT/deploy/tilt/prepare-attribution-source.sh" \
    "$ATTRIBUTION_SOURCE_DIR" \
    "$source_url" \
    "$source_ref"
  unset ATTRIBUTION_SOURCE_DEPLOY_KEY ATTRIBUTION_SOURCE_DEPLOY_KEY_FILE
}

configure_attribution_adapter() {
  configure_python_service attribution_adapter/Dockerfile
  BUILD_ARGS+=(
    --build-context
    "attribution-client=$ATTRIBUTION_SOURCE_DIR/service/service-app/nvidia_attribution_client"
  )
}

configure_attribution_service() {
  DOCKERFILE="$ATTRIBUTION_SOURCE_DIR/service/service-app/Dockerfile"
  BUILD_CONTEXT="$ATTRIBUTION_SOURCE_DIR"
  local source_url="${ATTRIBUTION_SOURCE_URL:-$DEFAULT_ATTRIBUTION_SOURCE_URL}"
  local source_ref="${ATTRIBUTION_SOURCE_REF:-$DEFAULT_ATTRIBUTION_SOURCE_REF}"
  BUILD_ARGS+=(
    --label "org.opencontainers.image.title=NVIDIA Attribution Service"
    --label "org.opencontainers.image.documentation="
    --label "org.opencontainers.image.source=$source_url"
    --label "org.opencontainers.image.url=$source_url"
    --label "org.opencontainers.image.revision=$source_ref"
  )
}

configure_service() {
  local service="$1"
  local slurm_image

  DOCKERFILE=
  BUILD_CONTEXT=
  BUILD_ARGS=()

  case "$service" in
    anomaly-detection-isolator)
      configure_python_service anomaly_detection_isolator/Dockerfile
      ;;
    attribution-adapter)
      configure_attribution_adapter
      ;;
    attribution-service)
      configure_attribution_service
      ;;
    api-gateway)
      DOCKERFILE="$REPO_ROOT/api-gateway/Dockerfile"
      BUILD_CONTEXT="$REPO_ROOT/api-gateway"
      ;;
    dashboard)
      DOCKERFILE="$REPO_ROOT/dashboard/Dockerfile"
      BUILD_CONTEXT="$REPO_ROOT/dashboard"
      ;;
    job-manager)
      configure_python_service job_manager/Dockerfile
      ;;
    notifications)
      configure_python_service notifications/Dockerfile
      ;;
    watcher)
      configure_python_service watcher/Dockerfile
      ;;
    slurm-advisor-controller)
      slurm_image="${SLURM_ADVISOR_CONTROLLER_SLURM_IMAGE:-${SLINKY_SLURM_IMAGE:-$DEFAULT_SLURM_IMAGE}}"
      DOCKERFILE="$REPO_ROOT/slurm-advisor-controller/Dockerfile"
      BUILD_CONTEXT="$REPO_ROOT"
      BUILD_ARGS+=(--build-arg "SLURM_IMAGE=$slurm_image")
      ;;
    slurm-watchdog)
      slurm_image="${SLURM_WATCHDOG_BASE_IMAGE:-${SLURM_ADVISOR_CONTROLLER_SLURM_IMAGE:-${SLINKY_SLURM_IMAGE:-$DEFAULT_SLURM_IMAGE}}}"
      DOCKERFILE="$REPO_ROOT/slurm-advisor-controller/Dockerfile.watchdog"
      BUILD_CONTEXT="$REPO_ROOT"
      BUILD_ARGS+=(--build-arg "BASE_IMAGE=$slurm_image")
      ;;
    *)
      return 1
      ;;
  esac
}

run_build() {
  local image="$1"
  shift
  local -a command=(docker buildx build --file "$DOCKERFILE")

  command+=("$@")
  command+=(
    --ulimit nofile=65536:65536
    --progress=plain
    --tag "$image"
    "$BUILD_CONTEXT"
  )

  "${command[@]}"
}

usage() {
  cat <<'USAGE'
Usage:
  scripts/build-images.sh --load [SERVICE ...]
  scripts/build-images.sh --publish [--platform PLATFORM[,PLATFORM...]] [SERVICE ...]
  scripts/build-images.sh --list

Builds every default service when no SERVICE is supplied.
Attribution is enabled by default and requires upstream source access.
Set ATTRIBUTION_ENABLED=false for an explicit degraded build without it.

Modes:
  --load       Load images locally (native except Attribution, which uses amd64).
  --publish    Build and push the resulting image or image index.
  --platform   Publish for the selected platform or platforms. Defaults to
               linux/amd64. Attribution arm64 qualification is pending.
  --list       Print the supported service names.

Environment:
  IMAGE_PREFIX  Registry/repository prefix. Required for --publish.
  IMAGE_TAG     Image tag. Defaults to BUILD_IMAGE_TAG, then latest.
  PLATFORMS     Default comma-separated publish platforms. Overridden by
                --platform.
  ATTRIBUTION_ENABLED
                Include Attribution in the default selection (true or false).
  ATTRIBUTION_SOURCE_URL
                Upstream attribution repository URL.
  ATTRIBUTION_SOURCE_REF
                Full immutable upstream commit SHA.
  ATTRIBUTION_SOURCE_DIR
                Optional existing/cache checkout path outside this repository.
  ATTRIBUTION_SOURCE_DEPLOY_KEY
                Optional read-only GitHub deploy key used only while fetching.
  ATTRIBUTION_SOURCE_DEPLOY_KEY_FILE
                Optional path to that deploy key instead of its contents.

Examples:
  IMAGE_TAG=dev scripts/build-images.sh --load job-manager
  IMAGE_PREFIX=registry.example.com/acme/nvidia-recovery IMAGE_TAG=v1 \
    scripts/build-images.sh --publish
  IMAGE_PREFIX=registry.example.com/acme/nvidia-recovery IMAGE_TAG=v1 \
    scripts/build-images.sh --publish --platform linux/arm64 watcher
USAGE
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

warn() {
  printf 'warning: %s\n' "$*" >&2
}

normalize_platforms() {
  local raw="$1"
  local normalized
  local -a requested=()
  local platform

  normalized="$(printf '%s' "$raw" | tr -d '[:space:]')"
  [[ -n "$normalized" ]] || fail "PLATFORMS must not be empty"

  IFS=',' read -ra requested <<< "$normalized"
  for platform in "${requested[@]}"; do
    [[ "$platform" =~ ^[a-z0-9._-]+/[a-z0-9._-]+(/[a-z0-9._-]+)?$ ]] ||
      fail "invalid platform '$platform'; expected os/architecture[/variant]"
  done

  printf '%s\n' "$normalized"
}

require_publish_platforms() {
  local requested_platforms="$1"
  local inspect_output="$2"
  local available
  local platform
  local -a requested=()

  available="$(
    printf '%s\n' "$inspect_output" |
      awk '/^[[:space:]]*Platforms:/ {
        sub(/^[[:space:]]*Platforms:[[:space:]]*/, "")
        gsub(/[[:space:]*]/, "")
        printf "%s,", $0
      }'
  )"
  [[ -n "$available" ]] || fail "selected Buildx builder did not report any platforms"

  IFS=',' read -ra requested <<< "$requested_platforms"
  for platform in "${requested[@]}"; do
    case ",$available" in
      *",$platform,"*) ;;
      *) fail "selected Buildx builder does not advertise $platform (available: ${available%,})" ;;
    esac
  done
}

warn_for_classic_docker_driver() {
  local requested_platforms="$1"
  local inspect_output="$2"
  local driver

  [[ "$requested_platforms" == *,* ]] || return 0

  driver="$(
    printf '%s\n' "$inspect_output" |
      awk -F: '/^Driver:/ {
        sub(/^[[:space:]]*/, "", $2)
        print $2
        exit
      }'
  )"
  if [[ "$driver" == docker ]]; then
    warn "selected builder uses the docker driver; multi-platform publishing requires Docker's containerd image store or a different builder driver such as docker-container"
  fi
}

image_for_service() {
  local service="$1"
  local prefix="$2"
  local tag="$3"

  if [[ -n "$prefix" ]]; then
    printf '%s/%s:%s\n' "$prefix" "$service" "$tag"
  else
    printf '%s:%s\n' "$service" "$tag"
  fi
}

verify_published_image() {
  local image="$1"
  local requested_platforms="$2"
  local inspect_output
  local published_platforms
  local platform
  local -a requested=()

  inspect_output="$(docker buildx imagetools inspect "$image")" ||
    fail "could not inspect published image $image"
  printf '%s\n' "$inspect_output"

  IFS=',' read -ra requested <<< "$requested_platforms"
  if ((${#requested[@]} < 2)); then
    return 0
  fi

  published_platforms="$(
    printf '%s\n' "$inspect_output" |
      awk '$1 == "Platform:" { print $2 }'
  )"
  for platform in "${requested[@]}"; do
    printf '%s\n' "$published_platforms" | grep -Fx -- "$platform" >/dev/null ||
      fail "published image $image is missing platform $platform"
  done
}

build_service() {
  local service="$1"
  local mode="$2"
  local image_prefix="$3"
  local image_tag="$4"
  local platforms="$5"
  local image
  local -a output_args=()

  # A conditional call would disable errexit throughout source preparation.
  configure_service "$service"
  if [[ "$service" == attribution-service ]]; then
    # The upstream Dockerfile cannot declare these build arguments.
    BUILD_ARGS+=(
      --label "org.opencontainers.image.version=$IMAGE_VERSION"
      --label "org.opencontainers.image.created=$IMAGE_CREATED"
    )
  else
    BUILD_ARGS+=(
      --build-arg "REVISION=$IMAGE_REVISION"
      --build-arg "CREATED=$IMAGE_CREATED"
    )
    if [[ -n "$IMAGE_VERSION" ]]; then
      BUILD_ARGS+=(--build-arg "VERSION=$IMAGE_VERSION")
    fi
  fi
  image="$(image_for_service "$service" "$image_prefix" "$image_tag")"

  if [[ "$mode" == load ]]; then
    output_args+=(--load)
    if [[ "$service" == attribution-* ]]; then
      output_args+=(--platform linux/amd64)
    fi
    printf 'Building image %s\n' "$image"
  else
    output_args+=(--platform "$platforms" --push)
    printf 'Building and publishing %s for %s\n' "$image" "$platforms"
  fi

  # Bash 3.2 treats empty arrays as unset under nounset. Expand no arguments
  # for services without build args, while preserving populated array entries.
  run_build \
    "$image" \
    ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"} \
    "${output_args[@]}"

  if [[ "$mode" == publish ]]; then
    verify_published_image "$image" "$platforms"
  fi
}

main() {
  local mode=
  local attribution_enabled="${ATTRIBUTION_ENABLED:-true}"
  local image_prefix="${IMAGE_PREFIX:-}"
  local image_tag="${IMAGE_TAG:-${BUILD_IMAGE_TAG:-latest}}"
  local platforms="${PLATFORMS:-$DEFAULT_PLATFORMS}"
  local builder_output
  local service
  local -a services=()

  while (($# > 0)); do
    case "$1" in
      --load)
        [[ -z "$mode" ]] || fail "choose exactly one mode"
        mode=load
        ;;
      --publish)
        [[ -z "$mode" ]] || fail "choose exactly one mode"
        mode=publish
        ;;
      --platform)
        shift
        (($# > 0)) || fail "--platform requires a value"
        platforms="$1"
        ;;
      --list)
        list_services
        return 0
        ;;
      --help | -h)
        usage
        return 0
        ;;
      --)
        shift
        services+=("$@")
        break
        ;;
      -* )
        fail "unknown option: $1"
        ;;
      *)
        services+=("$1")
        ;;
    esac
    shift
  done

  [[ -n "$mode" ]] || {
    usage >&2
    fail "choose --load or --publish"
  }
  [[ "$image_tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] ||
    fail "invalid IMAGE_TAG '$image_tag'"

  image_prefix="${image_prefix%/}"
  if [[ "$mode" == publish ]]; then
    [[ -n "$image_prefix" ]] || fail "IMAGE_PREFIX is required for --publish"
    [[ "$image_prefix" != *[[:space:]]* ]] || fail "IMAGE_PREFIX must not contain whitespace"
  fi

  case "$attribution_enabled" in
    true | false) ;;
    *) fail "ATTRIBUTION_ENABLED must be true or false" ;;
  esac
  platforms="$(normalize_platforms "$platforms")"
  if ((${#services[@]} == 0)); then
    services=("${DEFAULT_SERVICES[@]}")
    if [[ "$attribution_enabled" == true ]]; then
      services+=(attribution-adapter attribution-service)
    fi
  fi
  for service in "${services[@]}"; do
    service_supported "$service" ||
      fail "unsupported service '$service'; run scripts/build-images.sh --list"
    if [[ "$service" == attribution-* ]]; then
      [[ "$attribution_enabled" == true ]] || fail "Attribution service selected with ATTRIBUTION_ENABLED=false"
      if [[ "$mode" == publish && "$platforms" != linux/amd64 ]]; then
        fail "Attribution builds currently require --platform linux/amd64; arm64 qualification is pending"
      fi
    fi
  done

  command -v docker >/dev/null 2>&1 || fail "docker is required"
  docker buildx version >/dev/null 2>&1 || fail "Docker Buildx is required"
  builder_output="$(docker buildx inspect --bootstrap)" ||
    fail "could not bootstrap the selected Buildx builder"

  if [[ "$mode" == publish ]]; then
    require_publish_platforms "$platforms" "$builder_output"
    warn_for_classic_docker_driver "$platforms" "$builder_output"
  fi

  # Resolve enabled provider source before any image build can partially succeed.
  for service in "${services[@]}"; do
    if [[ "$service" == attribution-* ]]; then
      prepare_attribution_source
      break
    fi
  done

  for service in "${services[@]}"; do
    build_service "$service" "$mode" "$image_prefix" "$image_tag" "$platforms"
  done
}

IMAGE_SOURCE="${IMAGE_SOURCE:-https://github.com/dsx-ai-factory/NVIDIA-Recovery}"
IMAGE_URL="${IMAGE_URL:-$IMAGE_SOURCE}"
IMAGE_REVISION="${IMAGE_REVISION:-$("$REPO_ROOT/scripts/ci/resolve-source-revision.sh")}"
IMAGE_VERSION="${IMAGE_VERSION:-${IMAGE_TAG:-${BUILD_IMAGE_TAG:-}}}"
IMAGE_CREATED="${IMAGE_CREATED:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

main "$@"
