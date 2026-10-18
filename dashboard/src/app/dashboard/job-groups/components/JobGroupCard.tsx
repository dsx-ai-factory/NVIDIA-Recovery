// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  Card,
  Flex,
  Spinner,
  Text,
} from "@nvidia/foundations-react-core";
import { ChevronDown, ChevronUp } from "@nvidia/react-gui-icons/icons";
import dynamic from "next/dynamic";
import { useState } from "react";
import type { JobGroupStatus } from "~/gen/job-manager/types";
import { useGetJobs } from "../hooks";

const JobsTimelineChart = dynamic(
  () =>
    import("./JobsTimelineChart").then((m) => ({
      default: m.JobsTimelineChart,
    })),
  { ssr: false },
);

export function JobGroupCard({ jobGroup }: { jobGroup: JobGroupStatus }) {
  const [open, setOpen] = useState(false);
  const [hasOpened, setHasOpened] = useState(false);

  const toggleOpen = () => {
    setOpen((prev) => {
      if (!prev) {
        setHasOpened(true);
      }
      return !prev;
    });
  };

  const { data, isLoading } = useGetJobs({
    jobGroupId: jobGroup.job_group_id,
    enabled: hasOpened,
  });

  return (
    <Card density="compact" className="[&>.nv-card-content]:p-3">
      <Flex direction="col" gap="2">
        <Flex
          direction="row"
          align="center"
          gap="5"
          className="cursor-pointer"
          onClick={toggleOpen}
        >
          <Text kind="body/bold/md">Job Group: {jobGroup.job_group_id}</Text>
          <Text kind="body/regular/sm">
            {jobGroup.unique_job_count} Unique Jobs
          </Text>
          <Text kind="body/regular/sm">
            {jobGroup.total_job_attempt_count} Total Attempts
          </Text>
          <Button kind="tertiary" size="small" className="ml-auto">
            {open ? <ChevronUp /> : <ChevronDown />}
          </Button>
        </Flex>
        {open && (
          <Flex direction="col" gap="1">
            {isLoading ? (
              <Spinner aria-label="Loading jobs..." size="medium" />
            ) : !data || data.statuses.length === 0 ? (
              <Text kind="body/regular/sm">No jobs found.</Text>
            ) : (
              <JobsTimelineChart
                jobGroupId={jobGroup.job_group_id}
                jobs={data.statuses}
              />
            )}
          </Flex>
        )}
      </Flex>
    </Card>
  );
}
