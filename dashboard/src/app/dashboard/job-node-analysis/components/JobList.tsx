// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  Banner,
  Button,
  Card,
  Checkbox,
  Divider,
  Flex,
  Spinner,
  Text,
  TextInput,
  Tooltip,
} from "@nvidia/foundations-react-core";
import {
  Check,
  Circle3Q,
  CloseCircle,
  CopyGeneric,
  MagnifyingGlass,
} from "@nvidia/react-gui-icons/icons";
import {
  type InfiniteData,
  keepPreviousData,
  useInfiniteQuery,
} from "@tanstack/react-query";
import Link from "next/link";
import { useEffect, useState } from "react";
import { fetchJobs } from "~/app/dashboard/workflow/api";
import { ApsRenderState } from "~/components/utils/ApsRenderState";
import type {
  JobExecutionStatuses,
  SlurmJobExecutionStatusOutput,
} from "~/gen/job-manager/types";
import { useFormContext } from "./FormContext";

const isAllDigits = (str: string) => /^\d+$/.test(str);

const isJobIdAttemptFormat = (str: string) => /^(\d+)\/(\d+)$/.test(str);

const MIN_SEARCH_TEXT_LENGTH = 3;

export function JobList() {
  const { searchText, setSearchText, queryBuilderExpanded } = useFormContext();
  const [debouncedSearchText, setDebouncedSearchText] = useState(searchText);

  useEffect(() => {
    const handle = setTimeout(() => {
      const jobId = searchText;
      setDebouncedSearchText(jobId);
    }, 300);
    return () => clearTimeout(handle);
  }, [searchText]);

  const pageSize = 25;
  const {
    data: jobsData,
    isLoading: isLoadingJobs,
    isFetching: isFetchingJobs,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteQuery<
    JobExecutionStatuses,
    Error,
    InfiniteData<JobExecutionStatuses>,
    [
      string,
      {
        debouncedSearchText: string;
        limit: number;
        sortBy: string;
      },
    ],
    number
  >({
    enabled: debouncedSearchText.length >= MIN_SEARCH_TEXT_LENGTH,
    placeholderData: keepPreviousData,
    queryKey: [
      "jobs-search",
      {
        debouncedSearchText,
        limit: pageSize,
        sortBy: "start_time:desc",
      },
    ],
    initialPageParam: 0,
    queryFn: ({ pageParam }) =>
      fetchJobs({
        ...(debouncedSearchText.length >= MIN_SEARCH_TEXT_LENGTH
          ? isJobIdAttemptFormat(debouncedSearchText)
            ? {
                jobId: debouncedSearchText.split("/")[0],
                jobAttemptIndex: debouncedSearchText.split("/")[1],
              }
            : isAllDigits(debouncedSearchText)
              ? { jobId: debouncedSearchText }
              : { jobName: debouncedSearchText }
          : {}),
        // jobId: debouncedSearchText,
        limit: pageSize,
        offset: pageParam,
        sortBy: "start_time:desc,job_name:asc",
      }),
    getNextPageParam: (lastPage, allPages) => {
      const loadedCount = allPages.reduce(
        (total, page) => total + (page.statuses?.length ?? 0),
        0,
      );
      const totalCount = lastPage.item_count ?? loadedCount;
      return loadedCount < totalCount ? loadedCount : undefined;
    },
  });

  const jobs = jobsData?.pages.flatMap((page) => page.statuses ?? []);
  const totalCount = jobsData?.pages?.[0]?.item_count ?? 0;

  return (
    <div>
      <Flex direction="col" gap="4">
        <Banner kind="inline" status="info">
          {queryBuilderExpanded === "guided"
            ? "Select two or more jobs and then choose your operator"
            : "Find jobs and copy the job id/attempt for your custom comparison expression"}
        </Banner>
        <Flex direction="row" gap="2" align="center">
          <TextInput
            placeholder="Search for jobs by id, id/attempt, or workflow"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            slotEnd={
              isFetchingJobs && !isFetchingNextPage ? (
                <Circle3Q className="text-brand animate-spin" />
              ) : null
            }
          />
          <Text className="whitespace-nowrap">
            {debouncedSearchText.length > 0 ? totalCount : 0} results
          </Text>
        </Flex>
      </Flex>

      <Divider className="my-2" />

      {debouncedSearchText.length < MIN_SEARCH_TEXT_LENGTH ? (
        <Flex
          direction="col"
          gap="2"
          align="center"
          justify="center"
          className="my-6 w-full"
        >
          <MagnifyingGlass className="w-[36px] h-[36px]" />
          <Text kind="body/regular/lg">
            Search by job id, id/attempt, or workflow
          </Text>
        </Flex>
      ) : isLoadingJobs ? (
        <Flex justify="center" align="center" className="my-6 w-full">
          <Spinner size="medium" aria-label="Loading" />
        </Flex>
      ) : Array.isArray(jobs) && jobs.length > 0 ? (
        <>
          <div className="mt-4 grid gap-4 sm:grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-3">
            {jobs.map((job) => (
              <JobCard
                key={`${job.job_id}/${job.job_attempt_index}`}
                job={job}
              />
            ))}
          </div>
          <Flex direction="col" align="center" gap="2" className="my-4">
            <Text>
              Showing {jobs.length}
              {totalCount ? ` of ${totalCount}` : ""} jobs
            </Text>
            {hasNextPage ? (
              <Button
                kind="primary"
                color="brand"
                disabled={isFetchingNextPage}
                onClick={() => fetchNextPage()}
              >
                {isFetchingNextPage ? "Loading more..." : "Show more"}
              </Button>
            ) : null}
          </Flex>
        </>
      ) : (
        <div className="col-span-full my-6 w-full text-center">
          <Flex direction="col" gap="2" align="center" justify="center">
            <CloseCircle className="w-[36px] h-[36px]" />
            <Text kind="body/regular/lg">No jobs found</Text>
          </Flex>
        </div>
      )}
    </div>
  );
}

function JobCard({ job }: { job: SlurmJobExecutionStatusOutput }) {
  const {
    selectedJobs,
    addSelectedJob,
    removeSelectedJob,
    queryBuilderExpanded,
  } = useFormContext();
  const isSelected = selectedJobs.some(
    ({ job_id, job_attempt_index }) =>
      job_id === job.job_id && job_attempt_index === job.job_attempt_index,
  );
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    void navigator.clipboard.writeText(
      `${job.job_id}/${job.job_attempt_index}`,
    );
    setCopied(true);
    setTimeout(() => {
      setCopied(false);
    }, 2000);
  };
  return (
    <Card
      key={`${job.job_id}/${job.job_attempt_index}`}
      className={`relative ${isSelected && queryBuilderExpanded === "guided" ? "border border-green-500" : ""}`}
      density="compact"
    >
      {queryBuilderExpanded === "manual" ? (
        <div
          className="absolute top-3 right-2 h-fit w-fit"
          onClick={handleCopy}
        >
          <Tooltip slotContent="Copy job id/attempt">
            {copied ? (
              <Check className="text-green-500" />
            ) : (
              <CopyGeneric className="cursor-pointer" />
            )}
          </Tooltip>
        </div>
      ) : (
        <Checkbox
          className="absolute top-3 right-2"
          checked={isSelected}
          onCheckedChange={() =>
            isSelected ? removeSelectedJob(job) : addSelectedJob(job)
          }
        />
      )}
      {job.job_name && (
        <Text
          fontWeight="semibold"
          className="overflow-hidden pr-4 break-words"
          title={job.job_name}
        >
          {job.job_name}
        </Text>
      )}
      <Link
        className="w-fit underline"
        href={`/dashboard/workflow/job/${job.job_id}/${job.job_attempt_index}`}
      >
        {job.job_id}/{job.job_attempt_index}
      </Link>
      <Flex direction="row" gap="2" justify="between" align="center">
        <ApsRenderState state={job.state} />
        <Text className="overflow-hidden text-sm overflow-ellipsis whitespace-nowrap">
          {job.node_count ? `Nodes: ${job.node_count}` : ""}
        </Text>
      </Flex>
    </Card>
  );
}
