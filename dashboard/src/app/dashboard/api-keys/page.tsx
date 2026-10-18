// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use server";

import { apiKeysServiceListApiKeys, createClient } from "~/gen/api-gateway";
import { getBearerToken } from "~/lib/apiGatewaySession";
import { ApiKeyPageContent } from "./components/PageContent";

export default async function ApiKeysPage() {
  const bearerToken = await getBearerToken();

  const { keys } = await apiKeysServiceListApiKeys({
    client: createClient({
      headers: {
        Authorization: `Bearer ${bearerToken}`,
      },
    }),
  }).unwrap();
  return <ApiKeyPageContent keys={keys ?? []} />;
}
