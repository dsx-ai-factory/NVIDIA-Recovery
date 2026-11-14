// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { notFound } from "next/navigation";
import { getAuthConfiguration } from "~/utils/auth";
import { CompleteSignIn } from "./components/CompleteSignIn";

export const dynamic = "force-dynamic";

export default async function CompleteSignInPage({
  searchParams,
}: {
  searchParams: Promise<{ returnPath?: string }>;
}) {
  if (getAuthConfiguration().provider !== "gateway") {
    notFound();
  }

  const { returnPath } = await searchParams;
  const safeReturnPath =
    returnPath?.startsWith("/") && !returnPath.startsWith("//")
      ? returnPath
      : undefined;
  return <CompleteSignIn returnPath={safeReturnPath} />;
}
