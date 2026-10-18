# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG NODE_BASEIMAGE="nvcr.io/nvidia/distroless/node:22-v4.0.0@sha256:2639322834b8c309a4cf7005337ea2a73942f423f718f0de806076474f8e1264"

FROM node:23.9.0-alpine3.21@sha256:dc4d20572e425f9d4c68a6f9c382fbcfec3fa2f8ef0b12cb1d96feabdb479a48 AS base

ARG APP_ENV=production
ENV NEXT_PUBLIC_APP_ENV=${APP_ENV}

ARG BASE_PATH=""
ENV BASE_PATH=${BASE_PATH}

ARG APP_VERSION
ENV NEXT_PUBLIC_APP_VERSION=${APP_VERSION}

ARG ASSETS_PATH=https://assets.ngc.nvidia.com
ENV NEXT_PUBLIC_ASSETS_PATH=${ASSETS_PATH}

ENV NEXT_TELEMETRY_DISABLED 1

FROM base AS deps
# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine to understand why libc6-compat might be needed.
RUN apk add --no-cache libc6-compat
WORKDIR /app

RUN corepack enable && corepack prepare pnpm@10.8.0 --activate

# Install dependencies based on the preferred package manager
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml* ./
#COPY /vendor/ /app/vendor/
#COPY /deps/ /app/deps/
RUN pnpm install --frozen-lockfile
COPY . .
RUN SKIP_ENV_VALIDATION=1 pnpm build \
    && mkdir -p .next/standalone/.next/cache \
    && chmod 777 .next/standalone/.next/cache

# Rebuild the source code only when needed
FROM ${NODE_BASEIMAGE} AS app
ARG NODE_BASEIMAGE
ARG BASE_PATH=""
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/next.config.js ./
COPY --from=deps /app/tsconfig.json ./
COPY --from=deps /app/postcss.config.cjs ./
COPY --from=deps /app/public ./public
COPY --from=deps /app/.next ./.next
COPY --from=deps /app/public ./.next/standalone/public
COPY --from=deps /app/.next/static ./.next/standalone/.next/static
COPY --from=deps /app/package.json ./
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED 1
ENV BASE_PATH=${BASE_PATH}

EXPOSE 3001
ENV HOSTNAME=0.0.0.0
ENV PORT=3001

ARG REVISION=
ARG VERSION=dev
ARG CREATED=

LABEL org.opencontainers.image.title="NVIDIA Recovery Dashboard" \
      org.opencontainers.image.description="NVIDIA Recovery Dashboard is the web UI for monitoring workloads and recovery." \
      org.opencontainers.image.source="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.url="https://github.com/dsx-ai-factory/NVIDIA-Recovery" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.documentation="" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.base.name="${NODE_BASEIMAGE}"

# server.js is created by next build from the standalone output
# https://nextjs.org/docs/pages/api-reference/next-config-js/output
CMD ["node", "--env-file", ".env.production", ".next/standalone/server.js"]
