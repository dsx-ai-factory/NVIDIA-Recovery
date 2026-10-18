// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export type AuthSessionStatus = "authenticated" | "loading" | "unauthenticated";

export interface UserMenuModel {
  showApiKeys: boolean;
  showIdentity: boolean;
  showSignIn: boolean;
  showSignOut: boolean;
}

export function getUserMenuModel({
  authAvailable,
  isApiKeysEnabled,
  sessionStatus,
}: {
  authAvailable: boolean;
  isApiKeysEnabled: boolean;
  sessionStatus: AuthSessionStatus;
}): UserMenuModel {
  const authenticated = authAvailable && sessionStatus === "authenticated";

  return {
    showApiKeys: authenticated && isApiKeysEnabled,
    showIdentity: authenticated,
    showSignIn: authAvailable && sessionStatus === "unauthenticated",
    showSignOut: authenticated,
  };
}
