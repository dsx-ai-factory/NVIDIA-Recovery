// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  NotificationContent,
  NotificationFooter,
  NotificationHeader,
  NotificationHeading,
  NotificationIcon,
  NotificationRoot,
  NotificationSubheading,
} from "@nvidia/foundations-react-core";
import Link from "next/link";
import { getJobLink } from "~/app/dashboard/workflow/utils/links";
import { Datetime } from "~/components/utils/Datetime";
import type { NotificationPublic } from "~/gen/notifications/types";
import { notificationStatusFromType } from "../status";

/** Display a stored notification with a link when its job attempt is known. */
export function NotificationCard({
  notification,
}: {
  notification: NotificationPublic;
}) {
  const status = notificationStatusFromType(notification.type);
  const { message, recipient, title, created_time, template_data } =
    notification;
  const data =
    typeof template_data === "object" && template_data !== null
      ? template_data
      : {};
  const jobId = "job_id" in data ? data.job_id : undefined;
  const attemptIndex =
    "job_attempt_index" in data ? data.job_attempt_index : undefined;
  const jobLink =
    typeof jobId === "string" &&
    jobId.trim() !== "" &&
    jobId !== "." &&
    jobId !== ".." &&
    typeof attemptIndex === "number" &&
    Number.isSafeInteger(attemptIndex) &&
    attemptIndex >= 0
      ? getJobLink({ job_id: jobId, job_attempt_index: attemptIndex })
      : undefined;

  return (
    <NotificationRoot className="!max-w-full min-w-0 pl-3" status={status}>
      <NotificationContent>
        <NotificationIcon
          className="!mr-4 !ml-0 flex-shrink-0 [&_svg]:h-8"
          status={status}
        />
        <NotificationHeader>
          <NotificationHeading>
            <div className="m-0 flex justify-between gap-1">
              <span className="flex-1" title={title}>
                {title}
              </span>
              <span className="whitespace-pre" style={{ flexShrink: 0 }}>
                <Datetime utcIso={created_time} />
              </span>
            </div>

            {recipient && (
              <div className="text-primary m-0 truncate" title={recipient}>
                @{recipient}
              </div>
            )}
          </NotificationHeading>
          {message && (
            <NotificationSubheading className="mt-2 break-all">
              <div>{message}</div>
            </NotificationSubheading>
          )}
        </NotificationHeader>
        {jobLink ? (
          <NotificationFooter>
            <Button asChild kind="secondary" size="small" color="neutral">
              <Link href={jobLink.href}>View Job Details</Link>
            </Button>
          </NotificationFooter>
        ) : null}
      </NotificationContent>
    </NotificationRoot>
  );
}
