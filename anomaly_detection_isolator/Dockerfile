# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG PYTHON_BASEIMAGE=nvcr.io/nvidia/distroless/python:3.13-v4.1.6@sha256:5be5c1f0b90ff32870e118b13706396924f4a9520b0197e53fd0a02c9da2d8ea
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.10.9@sha256:10902f58a1606787602f303954cea099626a4adb02acbac4c69920fe9d278f82

FROM ${UV_IMAGE} AS uv
FROM busybox:musl@sha256:32b5cdad7cce41dfd53d0ae06baebcf8357a147ee7694dc706911c373bc30c37 AS busybox

FROM ${PYTHON_BASEIMAGE} AS build
COPY --from=uv /uv /usr/local/bin/uv
COPY --from=busybox /bin/busybox /bin/sh
COPY --from=busybox /bin/busybox /bin/rm
USER 0

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=never
ENV UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /app

# Layer 1: Install dependencies (cached unless pyproject.toml/uv.lock change)
COPY pyproject.toml uv.lock ./
COPY recovery_libs/pyproject.toml ./recovery_libs/
COPY anomaly_detection_isolator/pyproject.toml ./anomaly_detection_isolator/
RUN uv sync --frozen --no-dev --package anomaly-detection-isolator --no-install-workspace

# Layer 2: Install app (rebuilds on source changes)
COPY recovery_libs/ ./recovery_libs/
COPY anomaly_detection_isolator/ ./anomaly_detection_isolator/
RUN uv sync --frozen --no-dev --no-editable --package anomaly-detection-isolator

# Clean up build tools before copying to final stage
RUN ["/bin/rm", "-f", "/usr/local/bin/uv", "/bin/sh", "/bin/rm"]

# App stage — no uv, no build deps
FROM ${PYTHON_BASEIMAGE} AS app
ARG PYTHON_BASEIMAGE
COPY --from=build /usr/local/lib/python3.13/site-packages/ /usr/local/lib/python3.13/site-packages/
COPY --from=build /usr/local/bin/ /usr/local/bin/
ENV PYTHONUNBUFFERED=1
ENV CONFIG_FILE_PATH=".env"
EXPOSE 8080

ARG REVISION=
ARG VERSION=dev
ARG CREATED=

LABEL org.opencontainers.image.title="NVIDIA Recovery Anomaly Detection Isolator" \
      org.opencontainers.image.description="NVIDIA Recovery Anomaly Detection Isolator (ADI) analyzes diagnostic data to isolate unhealthy nodes during large-scale AI training jobs." \
      org.opencontainers.image.source="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.url="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.documentation="" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.base.name="${PYTHON_BASEIMAGE}"

ENTRYPOINT ["/usr/local/bin/anomaly-detection-isolator"]
