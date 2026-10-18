// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { PageLayout } from "../components/PageLayout";
import { NotificationsTable } from "./components/NotificationsTable";

export default async function NotificationsPage(props: {
  searchParams?: Promise<{
    recipient?: string;
    page?: string;
  }>;
}) {
  const sp = (await props.searchParams) ?? {};
  const initialPage = Number(sp.page) || undefined;

  return (
    <PageLayout
      breadcrumbs={[{ children: "Notifications" }]}
      title="Notifications"
    >
      <div className="py-4">
        <NotificationsTable
          initialRecipient={sp.recipient}
          initialPage={initialPage}
        />
      </div>
    </PageLayout>
  );
}
