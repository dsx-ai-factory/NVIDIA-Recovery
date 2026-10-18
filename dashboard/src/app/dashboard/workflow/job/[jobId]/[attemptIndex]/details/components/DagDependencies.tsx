// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Badge,
  Banner,
  Button,
  CodeSnippet,
  ModalCloseButton,
  ModalContent,
  ModalDialog,
  ModalFooter,
  ModalHeading,
  ModalMain,
  ModalRoot,
  ModalTrigger,
  TableBody,
  TableDataCell,
  TableHead,
  TableHeaderCell,
  TableRoot,
  TableRow,
  Text,
} from "@nvidia/foundations-react-core";
import { ArrowUpDown, CheckCircle, Code } from "@nvidia/react-gui-icons/icons";
import Link from "next/link";
import { useMemo } from "react";
import { utcToLocal } from "~/components/utils/Datetime";
import type {
  DagActionEnumKey,
  DagDependencyStep,
  SlurmJobExecutionStatusOutput,
} from "~/gen/job-manager/types";
import {
  buildDagConfiguration,
  type DagDependencyDisplayRow,
  formatDagValue,
  getDagDependencyStatusColor,
} from "./dagDependencyUtils";

function SortableHeading({ children }: { children: React.ReactNode }) {
  return (
    <span className="flex items-center gap-1">
      {children}
      <ArrowUpDown className="text-secondary h-3.5 w-3.5" />
    </span>
  );
}

export function DagDependencies({
  dependencies,
  dependencyJobs,
  schedulerDependency,
  configuredDependencies,
  dagAction,
  timezone,
}: {
  dependencies: DagDependencyDisplayRow[];
  dependencyJobs: SlurmJobExecutionStatusOutput[];
  schedulerDependency?: string | null;
  configuredDependencies?: DagDependencyStep[] | null;
  dagAction?: DagActionEnumKey;
  timezone: "system" | "utc";
}) {
  const configuration = useMemo(
    () =>
      buildDagConfiguration({
        schedulerDependency,
        dagDependencies: configuredDependencies,
        dagAction,
      }),
    [configuredDependencies, dagAction, schedulerDependency],
  );
  const dependencyJobsById = new Map(
    dependencyJobs.map((job) => [job.job_id, job]),
  );
  const dependencyJobsByName = new Map(
    dependencyJobs
      .filter((job) => job.job_name)
      .map((job) => [job.job_name!, job]),
  );
  const hasUnregisteredDependencies = dependencies.some(
    (dependency) => !dependency.registered,
  );

  return (
    <div className="flex flex-col gap-4">
      <div>
        <ModalRoot>
          <ModalTrigger asChild>
            <Button kind="secondary" size="small">
              View Configuration
            </Button>
          </ModalTrigger>
          <ModalDialog>
            <ModalContent className="!w-[560px] max-w-[calc(100vw-2rem)]">
              <ModalHeading className="flex items-center gap-2">
                <Code />
                DAG configuration
              </ModalHeading>
              <ModalMain>
                <CodeSnippet value={configuration} language="json" rows={12} />
              </ModalMain>
              <ModalFooter>
                <ModalCloseButton color="brand">Close</ModalCloseButton>
              </ModalFooter>
            </ModalContent>
          </ModalDialog>
        </ModalRoot>
      </div>

      {hasUnregisteredDependencies && (
        <Banner kind="inline" status="warning">
          This job declares DAG dependencies that have no runtime status. NVIDIA
          Recovery may not be managing them.
        </Banner>
      )}

      <div className="overflow-x-auto">
        <TableRoot className="w-full min-w-[840px]">
          <TableHead>
            <TableRow>
              <TableHeaderCell>
                <SortableHeading>Reference Type</SortableHeading>
              </TableHeaderCell>
              <TableHeaderCell>
                <SortableHeading>Identifier</SortableHeading>
              </TableHeaderCell>
              <TableHeaderCell>
                <SortableHeading>Trigger Condition</SortableHeading>
              </TableHeaderCell>
              <TableHeaderCell>
                <SortableHeading>Action Status</SortableHeading>
              </TableHeaderCell>
              <TableHeaderCell>
                <SortableHeading>Action Time</SortableHeading>
              </TableHeaderCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {dependencies.length === 0 ? (
              <TableRow>
                <TableDataCell colSpan={5}>
                  <Text className="text-secondary py-3 text-center">
                    No DAG dependencies configured for this job.
                  </Text>
                </TableDataCell>
              </TableRow>
            ) : (
              dependencies.map((dependency) => {
                const dependencyJob =
                  dependency.referenceType === "id"
                    ? dependencyJobsById.get(dependency.identifier)
                    : dependencyJobsByName.get(dependency.identifier);
                const identifier =
                  dependency.referenceType === "name"
                    ? (dependencyJob?.job_name ?? dependency.identifier)
                    : dependency.identifier;

                return (
                  <TableRow key={dependency.key}>
                    <TableDataCell>
                      {formatDagValue(dependency.referenceType)}
                    </TableDataCell>
                    <TableDataCell>
                      {dependencyJob ? (
                        <Link
                          className="text-primary underline underline-offset-2"
                          href={`/dashboard/workflow/job/${dependencyJob.job_id}/${dependencyJob.job_attempt_index}/details`}
                        >
                          {identifier}
                        </Link>
                      ) : (
                        <Text fontFamily="mono">{identifier}</Text>
                      )}
                    </TableDataCell>
                    <TableDataCell>
                      <Badge color="green" kind="solid">
                        <CheckCircle className="h-3.5 w-3.5" />
                        {formatDagValue(dependency.trigger)}
                      </Badge>
                    </TableDataCell>
                    <TableDataCell>
                      {dependency.actionStatus ? (
                        <Badge
                          color={getDagDependencyStatusColor(
                            dependency.actionStatus,
                          )}
                          kind="solid"
                        >
                          {formatDagValue(dependency.actionStatus)}
                        </Badge>
                      ) : (
                        <Badge color="yellow" kind="solid">
                          Not Registered
                        </Badge>
                      )}
                    </TableDataCell>
                    <TableDataCell>
                      {dependency.actionTime
                        ? utcToLocal(dependency.actionTime, timezone)
                        : "-"}
                    </TableDataCell>
                  </TableRow>
                );
              })
            )}
          </TableBody>
        </TableRoot>
      </div>
    </div>
  );
}
