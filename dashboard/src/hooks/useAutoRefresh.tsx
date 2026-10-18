// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { useRouter } from "next/navigation";
import { useEffect } from "react";
import { DEFAULT_AUTO_REFRESH_INTERVAL } from "~/const/statics";
import { useUserPreferences } from "~/provider/userPreferencesProvider";

export function useAutoRefresh(intervalSecondsOverride?: number) {
  const userPreferences = useUserPreferences();
  const router = useRouter();

  useEffect(() => {
    if (!userPreferences?.autoRefresh) {
      return;
    }
    const interval = setInterval(
      () => {
        router.refresh();
      },
      (intervalSecondsOverride ?? DEFAULT_AUTO_REFRESH_INTERVAL) * 1000,
    );
    return () => clearInterval(interval);
  }, [intervalSecondsOverride, router, userPreferences?.autoRefresh]);
}
