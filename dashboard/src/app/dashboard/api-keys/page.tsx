// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";

import { apiKeysServiceListApiKeys, createClient } from "~/gen/api-gateway";
import { getGatewayBearerToken } from "~/server/gatewayToken";
import { ApiKeyPageContent } from "./components/PageContent";

export default async function ApiKeysPage() {
  const bearerToken = await getGatewayBearerToken();

  const { keys } = await apiKeysServiceListApiKeys({
    client: createClient({
      headers: {
        Authorization: `Bearer ${bearerToken}`,
      },
    }),
  }).unwrap();
  return <ApiKeyPageContent keys={keys ?? []} />;
}
