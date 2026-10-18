// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { CircleTick } from "@nvidia/react-gui-icons/icons";
import Loading from "~/components/page/Loading";
import { PageLayout } from "./components/PageLayout";

export default function DashboardLoading() {
  return (
    <PageLayout title={<CircleTick className="animate-spin text-base" />}>
      <Loading />
    </PageLayout>
  );
}
