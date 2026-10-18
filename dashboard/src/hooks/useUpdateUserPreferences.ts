// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { useMutation } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import type { UserPreferences } from "~/app/api/user-preferences/types";
import { withBasePath } from "~/lib/base-path";

export function useUpdateUserPreferences() {
  const router = useRouter();
  return useMutation({
    mutationFn: async (
      preferences: Partial<UserPreferences>,
    ): Promise<void> => {
      const response = await fetch(withBasePath("/api/user-preferences"), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(preferences),
      });

      if (!response.ok) {
        throw new Error("Failed to update user preferences");
      }

      return;
    },
    onSuccess: () => {
      router.refresh();
    },
  });
}
