// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import Loading from "~/components/page/Loading";
import { PageLayout } from "../components/PageLayout";

export default async function NotificationsLoading() {
  return (
    <PageLayout
      breadcrumbs={[{ children: "Notifications" }]}
      title="Notifications"
    >
      <Loading />
    </PageLayout>
  );
}
