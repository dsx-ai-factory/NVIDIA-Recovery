// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  Flex,
  Pagination,
  StatusMessage,
  TextInput,
} from "@nvidia/foundations-react-core";
import { Megaphone } from "@nvidia/react-gui-icons/icons";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useSession } from "next-auth/react";
import { useEffect, useState } from "react";
import Loading from "~/components/page/Loading";
import { useAuthAvailability } from "~/provider/apsSessionProvider";
import { useGetNotifications } from "../hooks";
import { NotificationCard } from "./NotificationCard";

const PAGE_SIZE = 10;

export function NotificationsTable({
  initialRecipient,
  initialPage,
}: {
  initialRecipient: string | undefined;
  initialPage: number | undefined;
}) {
  const authAvailable = useAuthAvailability();
  if (!authAvailable) {
    return (
      <NotificationsTableContent
        initialRecipient={initialRecipient}
        initialPage={initialPage}
        fallbackRecipient=""
      />
    );
  }

  return (
    <AuthenticatedNotificationsTable
      initialRecipient={initialRecipient}
      initialPage={initialPage}
    />
  );
}

function AuthenticatedNotificationsTable({
  initialRecipient,
  initialPage,
}: {
  initialRecipient: string | undefined;
  initialPage: number | undefined;
}) {
  const { data: session } = useSession();
  const fallbackRecipient =
    session?.user?.username ?? session?.user?.email?.split("@")[0] ?? "";

  return (
    <NotificationsTableContent
      initialRecipient={initialRecipient}
      initialPage={initialPage}
      fallbackRecipient={fallbackRecipient}
    />
  );
}

function NotificationsTableContent({
  initialRecipient,
  initialPage,
  fallbackRecipient,
}: {
  initialRecipient: string | undefined;
  initialPage: number | undefined;
  fallbackRecipient: string;
}) {
  const [interacted, setInteracted] = useState<boolean>(false);
  const externalRecipient = interacted
    ? (initialRecipient ?? "")
    : (initialRecipient ?? fallbackRecipient);

  const [recipient, setRecipient] = useState<string>(externalRecipient);
  const [draft, setDraft] = useState<string>(externalRecipient);
  const [trackedRecipient, setTrackedRecipient] =
    useState<string>(externalRecipient);
  if (trackedRecipient !== externalRecipient) {
    setTrackedRecipient(externalRecipient);
    setRecipient(externalRecipient);
    setDraft(externalRecipient);
  }

  const initialPageIndex = Math.max(0, (initialPage ?? 1) - 1);
  const [pageIndex, setPageIndex] = useState<number>(initialPageIndex);
  const [trackedInitialPage, setTrackedInitialPage] =
    useState<number>(initialPageIndex);
  if (trackedInitialPage !== initialPageIndex) {
    setTrackedInitialPage(initialPageIndex);
    setPageIndex(initialPageIndex);
  }

  const { data, isLoading, isError } = useGetNotifications({
    pageIndex,
    pageSize: PAGE_SIZE,
    recipient: recipient || undefined,
  });

  usePersistNotificationsQuery({ recipient, page: String(pageIndex + 1) });

  if (isLoading) return <Loading />;

  const items = data?.items ?? [];

  return (
    <div className="flex flex-col gap-2">
      <Flex gap="2" align="end">
        <TextInput
          className="flex-grow"
          value={draft}
          placeholder="Filter by recipient name"
          onValueChange={(next) => setDraft(next ?? "")}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === "NumpadEnter") {
              setRecipient(draft);
              setPageIndex(0);
              setInteracted(true);
            }
          }}
        />
        <Button
          kind="primary"
          color="brand"
          onClick={() => {
            setRecipient(draft);
            setPageIndex(0);
            setInteracted(true);
          }}
        >
          Search
        </Button>
      </Flex>
      <FilterCaption currentFilter={recipient} />
      {items.length === 0 ? (
        <div className="flex w-full items-center justify-center p-6">
          <StatusMessage
            slotHeading="No notifications found"
            slotSubheading="Try refreshing the page to view your notifications."
            slotMedia={<Megaphone className="w-[48px] h-[48px]" />}
          />
        </div>
      ) : (
        <div className="flex flex-col gap-2">
          {items.map((notification) => (
            <NotificationCard
              key={notification.id}
              notification={notification}
            />
          ))}
        </div>
      )}
      {items.length > 0 && (
        <Pagination
          displayControls
          kind="tabs"
          page={pageIndex + 1}
          pageSize={PAGE_SIZE}
          pageSizeOptions={[PAGE_SIZE]}
          totalItems={data?.total_items ?? 0}
          onPageChange={(next) => setPageIndex(next - 1)}
        />
      )}
      {isError && (
        <div className="text-12 text-status-error">
          Failed to load notifications.
        </div>
      )}
    </div>
  );
}

function FilterCaption({ currentFilter }: { currentFilter: string }) {
  return (
    <div>
      {currentFilter ? (
        <>
          Filtering notifications by user <b>{currentFilter}</b>:
        </>
      ) : (
        "Showing all notifications:"
      )}
    </div>
  );
}

function usePersistNotificationsQuery({
  recipient,
  page,
}: {
  recipient: string;
  page: string;
}) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const router = useRouter();

  useEffect(() => {
    const params = new URLSearchParams(searchParams);

    if ((params.get("recipient") ?? "") !== recipient) {
      if (recipient === "") {
        params.delete("recipient");
      } else {
        params.set("recipient", recipient);
      }
    }

    if (params.get("page") !== page) {
      if (page === "1") {
        params.delete("page");
      } else {
        params.set("page", page);
      }
    }

    if (params.toString() !== searchParams.toString()) {
      router.replace(`${pathname}?${params.toString()}`);
    }
  }, [recipient, page, pathname, router, searchParams]);
}
