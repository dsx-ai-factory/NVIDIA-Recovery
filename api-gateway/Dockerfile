# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG GO_BASEIMAGE="alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc"

# ---- Build Stage ----
FROM golang:1.27.1-alpine@sha256:4cb7ac979db5fcc41cae44b2227ba5ab8a51e8807f40d9ba4dee20a0ad960b5b AS build

WORKDIR /app

# Download Go module dependencies.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o bin/ ./...

FROM ${GO_BASEIMAGE} AS app
ARG GO_BASEIMAGE

# Set the working directory for the final stage.
WORKDIR /app

# Copy the built binary from the 'build' stage to the final image.
COPY --from=build /app/bin/api-gateway /app/api-gateway

EXPOSE 8080
EXPOSE 9090

ARG REVISION=
ARG VERSION=dev
ARG CREATED=

LABEL org.opencontainers.image.title="NVIDIA Recovery API Gateway" \
      org.opencontainers.image.description="NVIDIA Recovery API Gateway provides authentication, authorization, and routing for the NVIDIA Recovery services." \
      org.opencontainers.image.source="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.url="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.documentation="" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.base.name="${GO_BASEIMAGE}"

ENTRYPOINT ["/app/api-gateway"]
