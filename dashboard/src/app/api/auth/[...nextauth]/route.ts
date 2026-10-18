// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type { NextRequest } from "next/server";
import NextAuth from "next-auth";

import { getAuthOptions } from "~/server/auth";
import { getAuthConfiguration } from "~/utils/auth";

type RouteContext = {
  params: Promise<{ nextauth: string[] }>;
};

const handler = getAuthConfiguration().available
  ? NextAuth(getAuthOptions())
  : null;

function notFoundResponse() {
  return new Response("Not Found", { status: 404 });
}

export async function GET(request: NextRequest, context: RouteContext) {
  return handler ? handler(request, context) : notFoundResponse();
}

export async function POST(request: NextRequest, context: RouteContext) {
  return handler ? handler(request, context) : notFoundResponse();
}
