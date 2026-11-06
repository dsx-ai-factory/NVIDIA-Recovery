// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { signIn } from "next-auth/react";
import { useEffect, useRef } from "react";
import { withBasePath } from "~/lib/base-path";

/**
 * Turns the API Gateway cookies set by the login redirect into the NextAuth
 * session, then continues to the page the user originally asked for.
 */
export function CompleteSignIn({ returnPath }: { returnPath?: string }) {
  // Guards against React strict-mode double effects issuing two sign-ins.
  const started = useRef(false);

  useEffect(() => {
    if (started.current) {
      return;
    }
    started.current = true;
    void signIn("nvlogin", {
      callbackUrl: withBasePath(returnPath ?? "/dashboard/workflow"),
    });
  }, [returnPath]);

  return (
    <div className="flex min-h-screen items-center justify-center">
      Signing you in…
    </div>
  );
}
