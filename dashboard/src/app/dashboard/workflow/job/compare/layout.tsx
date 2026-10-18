// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import Link from "next/link";
import type React from "react";
import { PageLayout } from "~/app/dashboard/components/PageLayout";

export default async function JobCompareLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <PageLayout
      breadcrumbs={[
        {
          children: <Link href="/dashboard/workflow">Workflows Overview</Link>,
        },
        { children: "Compare" },
      ]}
      title="Compare"
    >
      {children}
    </PageLayout>
  );
}
