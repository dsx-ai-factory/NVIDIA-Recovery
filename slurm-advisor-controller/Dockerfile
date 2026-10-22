# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# syntax=docker/dockerfile:1.7

ARG GO_IMAGE=golang:1.27.3-bookworm@sha256:972b4110ba0f47539a975c31e52c64765e1ff19f2d74469ae4a595c6676e80a5
ARG SLURM_IMAGE=ghcr.io/slinkyproject/sackd:25.05-ubuntu24.04@sha256:dd1f0b6030229b3d602e3d80e6d50cc4f9fd17b325312521dfa07376fffc7569
ARG RUNTIME_IMAGE=ubuntu:24.04@sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517

FROM ${SLURM_IMAGE} AS slurm

ARG TARGETARCH

# Normalize the donor's Debian multiarch paths into an architecture-neutral
# staging tree. BuildKit sets TARGETARCH independently for each target in a
# multi-platform build, so one invocation can package both amd64 and arm64.
RUN case "${TARGETARCH}" in \
      amd64) libarch="x86_64-linux-gnu" ;; \
      arm64) libarch="aarch64-linux-gnu" ;; \
      *) echo "unsupported target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && install -d \
      /opt/heimdall-slurm/bin \
      /opt/heimdall-slurm/lib \
      /opt/heimdall-slurm/plugins \
    && cp -L /usr/bin/squeue /usr/bin/sacct /usr/bin/scontrol /usr/bin/scancel /opt/heimdall-slurm/bin/ \
    && cp -L \
      "/usr/lib/${libarch}/slurm/libslurmfull.so" \
      "/usr/lib/${libarch}/slurm/accounting_storage_slurmdbd.so" \
      "/usr/lib/${libarch}/slurm/auth_slurm.so" \
      "/usr/lib/${libarch}/slurm/certgen_script.so" \
      "/usr/lib/${libarch}/slurm/hash_k12.so" \
      "/usr/lib/${libarch}/slurm/data_parser_v0_0_40.so" \
      "/usr/lib/${libarch}/slurm/data_parser_v0_0_43.so" \
      "/usr/lib/${libarch}/slurm/serializer_json.so" \
      "/usr/lib/${libarch}/slurm/select_cons_tres.so" \
      "/usr/lib/${libarch}/slurm/tls_none.so" \
      /opt/heimdall-slurm/plugins/ \
    && cp -L \
      "/usr/lib/${libarch}/libjwt.so.2" \
      "/usr/lib/${libarch}/libjansson.so.4" \
      "/usr/lib/${libarch}/libb64.so.0d" \
      "/usr/lib/${libarch}/libjson-c.so.5" \
      "/usr/lib/${libarch}/libreadline.so.8" \
      "/usr/lib/${libarch}/libtinfo.so.6" \
      /opt/heimdall-slurm/lib/

FROM ${GO_IMAGE} AS controller-build

WORKDIR /src

# Keep module downloads cached independently from source changes. The controller
# module replaces API Gateway with ../api-gateway, so both module manifests are
# present before go mod download and the repository root remains the build context.
COPY api-gateway/go.mod api-gateway/go.sum ./api-gateway/
COPY slurm-advisor-controller/go.mod slurm-advisor-controller/go.sum ./slurm-advisor-controller/

WORKDIR /src/slurm-advisor-controller
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY api-gateway /src/api-gateway
COPY slurm-advisor-controller /src/slurm-advisor-controller

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /out/slurm-advisor-controller \
      ./cmd/slurm-advisor-controller

FROM ${RUNTIME_IMAGE}
ARG RUNTIME_IMAGE
ARG SLURM_IMAGE

ARG TARGETARCH

COPY --from=slurm /opt/heimdall-slurm/bin/ /usr/bin/
COPY --from=slurm /opt/heimdall-slurm/lib/ /usr/local/lib/heimdall-slurm/lib/
COPY --from=slurm /opt/heimdall-slurm/plugins/ /usr/local/lib/heimdall-slurm/plugins/

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 401 slurm \
    && useradd --uid 401 --gid 401 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin slurm \
    && install -d -o 401 -g 401 -m 0750 /etc/slurm /run/slurm \
    && case "${TARGETARCH}" in \
      amd64) libarch="x86_64-linux-gnu" ;; \
      arm64) libarch="aarch64-linux-gnu" ;; \
      *) echo "unsupported target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && install -d "/usr/lib/${libarch}" \
    && ln -s /usr/local/lib/heimdall-slurm/plugins "/usr/lib/${libarch}/slurm" \
    && for library in /usr/local/lib/heimdall-slurm/lib/*; do \
      ln -sfn "$library" "/usr/lib/${libarch}/$(basename "$library")"; \
    done

# Keep the frequently changing application binary after the stable Slurm layer.
COPY --from=controller-build --chown=401:401 /out/slurm-advisor-controller /usr/local/bin/slurm-advisor-controller

ARG REVISION=
ARG VERSION=dev
ARG CREATED=

LABEL org.opencontainers.image.title="NVIDIA Recovery Slurm Advisor Controller" \
      org.opencontainers.image.description="NVIDIA Recovery Advisor API controller for Slurm" \
      org.opencontainers.image.source="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.url="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.documentation="" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.base.name="${RUNTIME_IMAGE}" \
      com.nvidia.recovery.slurm.source-image="${SLURM_IMAGE}"

USER 401:401
ENTRYPOINT ["/usr/local/bin/slurm-advisor-controller"]
