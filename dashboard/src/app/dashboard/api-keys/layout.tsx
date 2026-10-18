// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import Link from "next/link";
import { notFound } from "next/navigation";
import { PageLayout } from "~/app/dashboard/components/PageLayout";
import { AuthGateway } from "~/components/AuthGateway";
import { getBearerToken } from "~/lib/apiGatewaySession";
import { getApiGatewayUrl } from "~/lib/baseUrls";
import { getAuthConfiguration } from "~/utils/auth";
import { isApiKeysEnabled } from "~/utils/config";

export const dynamic = "force-dynamic";

export default async function ApiKeysLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  if (!getAuthConfiguration().available || !isApiKeysEnabled()) {
    notFound();
  }

  const bearerToken = await getBearerToken();
  return bearerToken ? (
    <PageLayout
      breadcrumbs={[
        { children: <Link href="/dashboard/api-keys">API Keys</Link> },
      ]}
      title="API Keys"
    >
      {children}
    </PageLayout>
  ) : (
    <AuthGateway
      apiGatewayUrl={getApiGatewayUrl()}
      message="Login to API Gateway to access API Keys"
    />
  );
}
