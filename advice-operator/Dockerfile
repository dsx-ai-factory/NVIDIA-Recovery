# syntax=docker/dockerfile:1

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM golang:1.25 AS builder

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

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /manager /manager
USER 65532:65532

ENTRYPOINT ["/manager"]
