// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";

import {
  apiKeysServiceCreateApiKey,
  apiKeysServiceDeleteApiKey,
  createClient,
  type V1CreateApiKeyRequest,
  type V1CreateApiKeyResponse,
  type V1DeleteApiKeyResponse,
} from "~/gen/api-gateway";
import { getBearerToken } from "~/lib/apiGatewaySession";

export async function createApiKeyAction(
  data: V1CreateApiKeyRequest,
): Promise<V1CreateApiKeyResponse> {
  const bearerToken = await getBearerToken();
  if (!bearerToken) {
    throw new Error("No bearer token found");
  }

  return await apiKeysServiceCreateApiKey({
    body: {
      ...data,
      authorizations: {},
    },
    client: createClient({
      headers: {
        Authorization: `Bearer ${bearerToken}`,
      },
    }),
  }).unwrap();
}

export async function deleteApiKeyAction(
  keyId: string,
): Promise<V1DeleteApiKeyResponse> {
  const bearerToken = await getBearerToken();
  if (!bearerToken) {
    throw new Error("No bearer token found");
  }

  return await apiKeysServiceDeleteApiKey({
    path: { keyId },
    client: createClient({
      headers: {
        Authorization: `Bearer ${bearerToken}`,
      },
    }),
  }).unwrap();
}
