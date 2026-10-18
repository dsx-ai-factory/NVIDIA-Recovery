// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Breadcrumbs,
  type BreadcrumbsItemFieldProps,
  Flex,
  PageHeaderContent,
  PageHeaderFooter,
  PageHeaderHeader,
  PageHeaderHeading,
  PageHeaderRoot,
  Text,
} from "@nvidia/foundations-react-core";
import Link from "next/link";
import type { ReactNode } from "react";

/**
 * A page layout to be used inside DashboardLayout
 */
export function PageLayout({
  footerActions,
  headerActions,
  breadcrumbs = [],
  children,
  title,
}: Readonly<{
  footerActions?: ReactNode;
  headerActions?: ReactNode;
  breadcrumbs?: BreadcrumbsItemFieldProps[];
  children: ReactNode;
  title?: ReactNode;
}>) {
  return (
    <>
      <PageHeaderRoot className="border-base bg-surface-base border-b px-8 py-3 shadow-sm">
        <PageHeaderContent className="flex flex-col gap-3">
          <Breadcrumbs
            items={[
              {
                children: <Link href="/">Home</Link>,
              },
              ...breadcrumbs,
            ]}
          />
          <PageHeaderHeader>
            <Flex gap="2" justify="between" align="center">
              {title && (
                <PageHeaderHeading>
                  <Text asChild kind="body/bold/xl">
                    <h3>{title}</h3>
                  </Text>
                </PageHeaderHeading>
              )}
              {headerActions ? headerActions : null}
            </Flex>
          </PageHeaderHeader>
        </PageHeaderContent>
        {footerActions && <PageHeaderFooter>{footerActions}</PageHeaderFooter>}
      </PageHeaderRoot>
      <main className="mx-auto px-8 py-4">{children}</main>
    </>
  );
}
