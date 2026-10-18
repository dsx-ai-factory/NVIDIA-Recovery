// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Avatar,
  type CheckedState,
  Dropdown,
  type DropdownCheckboxItemEntry,
  type DropdownDefaultItemEntry,
  type DropdownEntry,
  type DropdownSectionEntry,
  type DropdownSubSection,
  Flex,
  Text,
} from "@nvidia/foundations-react-core";
import {
  Check,
  Key,
  Moon,
  Profile,
  SunLow,
  World,
} from "@nvidia/react-gui-icons/icons";
import Link from "next/link";
import type { Session } from "next-auth";
import { signIn, signOut, useSession } from "next-auth/react";
import type React from "react";
import { useUpdateUserPreferences } from "~/hooks/useUpdateUserPreferences";
import { withBasePath } from "~/lib/base-path";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import { type AuthSessionStatus, getUserMenuModel } from "./model";

export function UserDropdown({
  authAvailable,
  isApiKeysEnabled,
}: {
  authAvailable: boolean;
  isApiKeysEnabled?: boolean;
}) {
  if (!authAvailable) {
    return (
      <UserDropdownContent
        authAvailable={false}
        isApiKeysEnabled={false}
        session={null}
        sessionStatus="unauthenticated"
      />
    );
  }

  return <AuthUserDropdown isApiKeysEnabled={isApiKeysEnabled} />;
}

function AuthUserDropdown({
  isApiKeysEnabled,
}: {
  isApiKeysEnabled?: boolean;
}) {
  const session = useSession();
  return (
    <UserDropdownContent
      authAvailable
      isApiKeysEnabled={isApiKeysEnabled}
      session={session.data}
      sessionStatus={session.status}
    />
  );
}

function UserDropdownContent({
  authAvailable,
  isApiKeysEnabled = false,
  session,
  sessionStatus,
}: {
  authAvailable: boolean;
  isApiKeysEnabled?: boolean;
  session: Session | null;
  sessionStatus: AuthSessionStatus;
}) {
  const {
    autoRefresh: autoRefreshEnabled,
    theme,
    timezone,
  } = useUserPreferences();
  const updateUserPreferences = useUpdateUserPreferences();
  const menuModel = getUserMenuModel({
    authAvailable,
    isApiKeysEnabled,
    sessionStatus,
  });

  const renderLink = (
    item: DropdownDefaultItemEntry & {
      href?: string;
      children?: React.ReactNode;
    },
  ) =>
    item.href ? <Link href={item.href}>{item.children}</Link> : item.children;

  const items: DropdownEntry[] = [
    ...(menuModel.showIdentity
      ? [
          {
            slotHeading: session?.user?.name ?? "User",
            items: [
              ...(menuModel.showApiKeys
                ? [
                    {
                      // Default item: API Keys
                      children: (
                        <Flex direction="row" align="center" gap="1">
                          <Key />
                          <Text>API Keys</Text>
                        </Flex>
                      ),
                      href: `/dashboard/api-keys`,
                    } as DropdownDefaultItemEntry,
                  ]
                : []),
            ],
          } as DropdownSectionEntry,
        ]
      : []),
    {
      kind: "sub" as const,
      slotHeading: "Theme",
      children: (
        <Flex direction="row" align="center" gap="1">
          {theme === "dark" ? <Moon /> : <SunLow />}
          Theme
        </Flex>
      ),
      items: [
        {
          children: (
            <Flex direction="row" align="center" gap="1">
              <SunLow />
              <Text>Light Mode</Text>
              {theme === "light" ? <Check color="green" /> : undefined}
            </Flex>
          ),
          onSelect: () => {
            updateUserPreferences.mutate({
              theme: "light",
            });
          },
        } as DropdownDefaultItemEntry,
        {
          children: (
            <Flex direction="row" align="center" gap="1">
              <Moon />
              <Text>Dark Mode</Text>
              {theme === "dark" ? <Check color="green" /> : undefined}
            </Flex>
          ),
          onSelect: () => {
            updateUserPreferences.mutate({
              theme: "dark",
            });
          },
        } as DropdownDefaultItemEntry,
      ],
    } as DropdownSubSection,
    {
      kind: "sub" as const,
      slotHeading: "Timezone",
      children: (
        <Flex direction="row" align="center" gap="1">
          <World />
          Time zone
        </Flex>
      ),
      items: [
        {
          children: (
            <Flex direction="row" align="center" gap="1">
              <Text>UTC</Text>
              {timezone === "utc" ? <Check color="green" /> : undefined}
            </Flex>
          ),
          onSelect: () => {
            updateUserPreferences.mutate({
              timezone: "utc",
            });
          },
        } as DropdownDefaultItemEntry,
        {
          children: (
            <Flex direction="row" align="center" gap="1">
              <Text>System</Text>
              {timezone === "system" ? <Check color="green" /> : undefined}
            </Flex>
          ),
          onSelect: () => {
            updateUserPreferences.mutate({
              timezone: "system",
            });
          },
        } as DropdownDefaultItemEntry,
      ],
    } as DropdownSubSection,
    {
      kind: "checkbox" as const,
      checked: autoRefreshEnabled,
      children: "Auto Refresh",
      onCheckedChange: (checked: CheckedState) => {
        updateUserPreferences.mutate({
          autoRefresh: checked === true,
        });
      },
    } as DropdownCheckboxItemEntry,
    ...(menuModel.showSignOut
      ? [
          {
            children: "Sign Out",
            onSelect: () => {
              void signOut();
            },
            danger: true,
          } as DropdownDefaultItemEntry,
        ]
      : menuModel.showSignIn
        ? [
            {
              children: "Sign In",
              onSelect: () =>
                void signIn("nvlogin", {
                  callbackUrl: withBasePath("/dashboard/workflow"),
                }),
            } as DropdownDefaultItemEntry,
          ]
        : []),
  ];

  return (
    <Dropdown
      items={items}
      renderLink={renderLink}
      size="small"
      align="end"
      density="spacious"
    >
      <Avatar
        size="small"
        src={
          menuModel.showIdentity
            ? ((session?.user?.image as string | undefined) ?? undefined)
            : undefined
        }
        fallback={
          <Profile className={theme === "light" ? "!text-inverse" : ""} />
        }
      />
    </Dropdown>
  );
}
