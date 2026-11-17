// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { PageLayout } from "~/app/dashboard/components/PageLayout";
import { env } from "~/env";
import { withBasePath } from "~/lib/base-path";
import { getGatewayBearerToken } from "~/server/gatewayToken";
import { getAuthConfiguration } from "~/utils/auth";
import { isApiKeysEnabled } from "~/utils/config";

export const dynamic = "force-dynamic";

export default async function ApiKeysLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  if (getAuthConfiguration().provider !== "gateway" || !isApiKeysEnabled()) {
    notFound();
  }

  // The dashboard is already behind login, so a missing bearer means the gateway
  // session lapsed and could not be refreshed: sign in again.
  if (!(await getGatewayBearerToken())) {
    redirect(withBasePath("/auth/signin", env.BASE_PATH));
  }

  return (
    <PageLayout
      breadcrumbs={[
        { children: <Link href="/dashboard/api-keys">API Keys</Link> },
      ]}
      title="API Keys"
    >
      {children}
    </PageLayout>
  );
}
