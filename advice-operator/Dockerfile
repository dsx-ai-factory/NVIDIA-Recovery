# syntax=docker/dockerfile:1

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG GO_IMAGE=golang:1.27.3-bookworm@sha256:972b4110ba0f47539a975c31e52c64765e1ff19f2d74469ae4a595c6676e80a5
ARG RUNTIME_IMAGE=gcr.io/distroless/static:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329

FROM ${GO_IMAGE} AS builder

ARG TARGETARCH
ARG TARGETOS=linux

WORKDIR /workspace/advice-operator

COPY api-gateway/go.mod /workspace/api-gateway/go.mod
COPY api-gateway/go.sum /workspace/api-gateway/go.sum
COPY advice-operator/go.mod advice-operator/go.sum ./
RUN go mod download

COPY api-gateway/gen/go/heimdall/v1 /workspace/api-gateway/gen/go/heimdall/v1
COPY advice-operator/ .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -o /manager ./cmd

FROM ${RUNTIME_IMAGE}
ARG RUNTIME_IMAGE

COPY --from=builder /manager /manager
USER 65532:65532

ARG REVISION=
ARG VERSION=dev
ARG CREATED=

LABEL org.opencontainers.image.title="NVIDIA Recovery Advice Operator" \
      org.opencontainers.image.description="Materializes NVIDIA Recovery advice as Kubernetes custom resources." \
      org.opencontainers.image.source="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.url="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.documentation="" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.base.name="${RUNTIME_IMAGE}"

ENTRYPOINT ["/manager"]
