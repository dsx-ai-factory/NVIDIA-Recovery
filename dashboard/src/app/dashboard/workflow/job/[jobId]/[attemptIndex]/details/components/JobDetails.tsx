// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Badge, Banner, Button, Tooltip } from "@nvidia/foundations-react-core";
import {
  CopyDoc,
  Fullscreen,
  FullscreenExit,
  InfoCircle,
} from "@nvidia/react-gui-icons/icons";
import { useQueries } from "@tanstack/react-query";
import { notFound } from "next/navigation";
import React, { type ReactNode, useReducer, useState } from "react";
import {
  getJobErrorAttributions,
  type JobKey,
} from "~/app/dashboard/workflow/api";
import {
  useGetDagDependencies,
  useGetJob,
  useGetJobErrorAttributions,
  useGetJobs,
} from "~/app/dashboard/workflow/hooks";
import { getJobLink } from "~/app/dashboard/workflow/utils/links";
import { ApsDuration } from "~/components/utils/ApsDuration";
import { ApsRenderState } from "~/components/utils/ApsRenderState";
import { ApsRenderStateReason } from "~/components/utils/apsJobRender";
import { ApsRenderCheckpointChart } from "~/components/utils/apsRenderCheckpointChart";
import { utcToLocal } from "~/components/utils/Datetime";
import { DEFAULT_AUTO_REFRESH_INTERVAL } from "~/const/statics";
import {
  autoResumeModeEnum,
  type JobAnomaly,
  type NodeEvents,
} from "~/gen/job-manager/types";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import { GetFACTUrl } from "~/utils/fact";
import { AnomalyCard } from "./AnomalyCard";
import { DagDependencies } from "./DagDependencies";
import { buildDagDependencyDisplayRows } from "./dagDependencyUtils";
import { ExpandableLogFileList } from "./ExpandableLogFileList";
import { Section } from "./Section";

function CopyInput({
  className,
  slotLeft,
  slotRight,
  value,
  label,
  ellipsizeValue = true,
  copyValue,
}: {
  className?: string;
  slotLeft?: ReactNode;
  slotRight?: ReactNode;
  value?: string | null;
  label: string;
  ellipsizeValue?: boolean;
  copyValue?: string | null;
}) {
  const copyClickValue = String(copyValue ?? value ?? "");
  return (
    <div className="min-w-0">
      <div className="text-12 text-secondary block font-medium">{label}</div>
      <div
        className={`border-base flex items-center justify-between gap-1 rounded-md border p-2 ${className ? className : ""}`}
      >
        {slotLeft && (
          <div className="text-12 text-secondary block min-w-0 font-medium">
            {slotLeft}
          </div>
        )}
        <div
          className={`text-14 text-secondary min-h-[21px] min-w-0 break-words ${ellipsizeValue ? "truncate" : ""}`}
        >
          {slotLeft ? undefined : (value ?? "-")}
        </div>
        <div className="text-14 text-secondary flex items-center gap-1">
          {slotRight}
          {value && (
            <Tooltip slotContent={`Copy "${copyClickValue}" to clipboard`}>
              <Button
                className="text-14 hover:text-strong cursor-pointer p-0 text-inherit"
                onClick={() => {
                  void navigator.clipboard.writeText(copyClickValue);
                }}
                kind="tertiary"
                size="tiny"
              >
                <CopyDoc />
              </Button>
            </Tooltip>
          )}
        </div>
      </div>
    </div>
  );
}

function ExpandableNodeList({ nodeList }: { nodeList: string[] }) {
  const MAX_NODE_LIST_LENGTH = 5;
  const [expanded, setExpanded] = useState(false);
  return (
    <div className="flex flex-row justify-between">
      <div className="flex flex-wrap items-center gap-1">
        {expanded ? (
          nodeList.map((node) => (
            <Badge
              className="whitespace-pre"
              key={node}
              color="blue"
              kind="solid"
            >
              {node}
            </Badge>
          ))
        ) : (
          <>
            {nodeList.slice(0, MAX_NODE_LIST_LENGTH).map((node) => (
              <Badge
                className="whitespace-pre"
                key={node}
                color="blue"
                kind="solid"
              >
                {node}
              </Badge>
            ))}

            {nodeList.length > MAX_NODE_LIST_LENGTH && (
              <Badge
                className="whitespace-pre"
                key="more"
                color="blue"
                kind="solid"
              >
                {`${nodeList.length - MAX_NODE_LIST_LENGTH} more nodes...`}
              </Badge>
            )}
          </>
        )}
      </div>
      {nodeList.length > MAX_NODE_LIST_LENGTH && (
        <div>
          <Button
            kind="tertiary"
            size="tiny"
            onClick={() => setExpanded(!expanded)}
          >
            {expanded ? <FullscreenExit /> : <Fullscreen />}
          </Button>
        </div>
      )}
    </div>
  );
}

export function JobDetails({
  attemptIndex,
  jobId,
  datadogLink,
  factUrl,
  cockpitName,
  grafanaTraceUrlEnabled,
  grafanaDataSourceId,
  grafanaUrl,
  grafanaOrgId,
}: {
  attemptIndex: string;
  jobId: string;
  datadogLink: string | undefined;
  factUrl: string | false | undefined;
  cockpitName: string | undefined;
  grafanaTraceUrlEnabled: boolean;
  grafanaDataSourceId: string | undefined;
  grafanaUrl: string | undefined;
  grafanaOrgId: string | undefined;
}) {
  const { data: job, error, isLoading } = useGetJob({ attemptIndex, jobId });
  const {
    data: dagDependencies,
    error: dagDependenciesError,
    isLoading: dagDependenciesLoading,
  } = useGetDagDependencies({ dependentJobId: jobId });
  const displayedDagDependencies = React.useMemo(
    () =>
      buildDagDependencyDisplayRows({
        configuredDependencies: job?.aps_spec?.dag_dependencies,
        registeredDependencies: dagDependencies?.dag_dependencies ?? [],
      }),
    [dagDependencies?.dag_dependencies, job?.aps_spec?.dag_dependencies],
  );
  const dependencyJobKeys = React.useMemo<JobKey[]>(
    () =>
      displayedDagDependencies.map((dependency) =>
        dependency.referenceType === "id"
          ? { jobId: dependency.identifier }
          : { jobName: dependency.identifier },
      ),
    [displayedDagDependencies],
  );
  const { data: dependencyJobs, isLoading: dependencyJobsLoading } = useGetJobs(
    { jobKeys: dependencyJobKeys },
  );
  const [showInSeconds, toggleShowInSeconds] = useReducer(
    (v: boolean) => !v,
    false,
  );

  const { autoRefresh: autoRefreshEnabled, timezone } = useUserPreferences();

  const anomalyQueries = useQueries({
    queries:
      job?.anomalies?.map((anomaly) => ({
        enabled: !!jobId && !!anomaly?.uuid,
        queryKey: [
          "job-error-attributions",
          { jobId, jobAttemptIndex: attemptIndex, anomalyid: anomaly.uuid },
        ],
        queryFn: () =>
          getJobErrorAttributions({
            jobId,
            jobAttemptIndex: attemptIndex,
            anomalyid: anomaly.uuid,
          }),
        refetchInterval: autoRefreshEnabled
          ? DEFAULT_AUTO_REFRESH_INTERVAL * 1000
          : (false as const),
      })) ?? [],
  });

  const anomalyAttributionsLoading = anomalyQueries.some((q) => q.isLoading);
  const anomalyAttributionsByUuid = React.useMemo(() => {
    return (job?.anomalies ?? []).reduce(
      (acc, anomaly, idx) => {
        const q = anomalyQueries[idx];
        if (anomaly?.uuid && q?.data !== undefined) {
          acc[anomaly.uuid] = {
            ...(acc[anomaly.uuid] ?? {}),
            jobAttributions: q.data,
            anomaly: anomaly,
          };
        }
        return acc;
      },
      {} as Record<
        string,
        { jobAttributions: NodeEvents | undefined; anomaly: JobAnomaly }
      >,
    );
  }, [job?.anomalies, anomalyQueries]);

  const { data: jobAttributions } = useGetJobErrorAttributions({
    jobId,
    jobAttemptIndex: attemptIndex,
  });

  if (!isLoading && !job) {
    return notFound();
  }

  return (
    <div className="flex w-full flex-col gap-4">
      {error && (
        <Banner kind="global" status="error">
          {error.name} - {error.message}
        </Banner>
      )}
      <Section heading="Basic Info" loading={!job}>
        <Section.Row>
          <CopyInput
            label="Job ID"
            slotRight={
              factUrl && (
                <a
                  className="text-12 text-green-700 underline"
                  href={GetFACTUrl({
                    factUrl: factUrl,
                    jobId: jobId,
                    jobName: "",
                    clusterName: job?.cluster ?? "",
                    userName: job?.user ?? "",
                  })}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Efficiency
                </a>
              )
            }
            value={getJobLink(job)?.label}
          />
          <CopyInput label="User" value={job?.user} />
          <CopyInput
            label="Job State"
            copyValue={job?.state}
            slotLeft={<ApsRenderState state={job?.state} />}
          />
          <CopyInput
            label="Job State Reason"
            copyValue={job?.state_reason}
            slotLeft={<ApsRenderStateReason reason={job?.state_reason} />}
          />
        </Section.Row>
        <Section.Row>
          <CopyInput
            label="Job Name"
            value={job?.job_name}
            ellipsizeValue={false}
            slotRight={
              job?.job_name &&
              factUrl && (
                <a
                  className="text-12 text-green-700 underline"
                  href={GetFACTUrl({
                    factUrl: factUrl,
                    jobName: job.job_name,
                    jobId: "",
                    clusterName: job.cluster ?? "",
                    userName: job.user ?? "",
                  })}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Efficiency
                </a>
              )
            }
          />
        </Section.Row>
        <Section.Row>
          <CopyInput
            label="Command"
            value={job?.command}
            ellipsizeValue={false}
          />
        </Section.Row>
        <Section.Row>
          <CopyInput
            label="Log File Paths"
            value={job?.log_file_paths?.join("\n")}
            slotLeft={
              <ExpandableLogFileList
                key={`${jobId}-${attemptIndex}`}
                paths={job?.log_file_paths ?? []}
              />
            }
            ellipsizeValue={false}
          />
        </Section.Row>
        {
          <Section.Row>
            <CopyInput label="Cluster" value={job?.cluster} />
            <CopyInput label="Partition" value={job?.partition} />
            <CopyInput label="Reservation" value={job?.reservation} />
            <CopyInput label="Dependency" value={job?.dependency} />
          </Section.Row>
        }
      </Section>
      <Section heading="Hardware Info" loading={!job}>
        <Section.Row>
          <CopyInput label="Node Count" value={job?.node_count?.toString()} />
          <CopyInput
            label="GPU Count"
            value={job?.total_gpu_count?.toString()}
          />
          <CopyInput
            label="CPU Count"
            value={job?.total_cpu_count?.toString()}
          />
          <CopyInput
            label="Memory (MB)"
            value={job?.total_memory_mb?.toString()}
          />
        </Section.Row>
        <Section.Row>
          <Section.Item label="Allocated Node List">
            <ExpandableNodeList nodeList={job?.allocated_node_list ?? []} />
          </Section.Item>
        </Section.Row>
        <Section.Row>
          <Section.Item label="Inclusion Node List">
            <ExpandableNodeList nodeList={job?.inclusion_node_list ?? []} />
          </Section.Item>
        </Section.Row>
        <Section.Row>
          <Section.Item label="Exclusion Node List">
            <ExpandableNodeList nodeList={job?.exclusion_node_list ?? []} />
          </Section.Item>
        </Section.Row>
      </Section>
      <Section heading="Performance Info" loading={!job}>
        <Section.Row>
          <CopyInput
            label="Submit Time"
            value={utcToLocal(job?.submit_time, timezone)}
          />
          <CopyInput
            label="Start Time"
            value={utcToLocal(job?.start_time, timezone)}
          />
          <CopyInput
            label="End Time"
            value={utcToLocal(job?.end_time, timezone)}
          />
          <CopyInput
            label={`Duration (${showInSeconds ? "s" : "hh:mm:ss"})`}
            slotLeft={
              <ApsDuration
                job={job}
                inSeconds={showInSeconds}
                onClick={toggleShowInSeconds}
              />
            }
          />
          <CopyInput
            label="Last Updated"
            value={utcToLocal(job?.last_updated_at, timezone)}
          />
        </Section.Row>
        <Section.Row>
          <CopyInput
            label="GPU Throughput"
            slotLeft={
              datadogLink && (
                <a
                  className="text-12 text-green-700 underline"
                  href={datadogLink}
                >
                  Datadog
                </a>
              )
            }
          />
        </Section.Row>
        <Section.Row>
          <Section.Item label="Checkpoints">
            <ApsRenderCheckpointChart job={job} key={job?.job_id} />
          </Section.Item>
        </Section.Row>
      </Section>
      <Section
        heading={`${cockpitName ?? "NVIDIA Recovery"} Info`}
        loading={!job}
      >
        <Section.Row>
          <CopyInput
            label="Environment"
            value={job?.aps_spec?.environment ?? "None"}
          />
        </Section.Row>
        <Section.Row>
          <Section.Item label="Resume Mode">
            {job?.aps_spec?.auto_resume_mode === autoResumeModeEnum.requeue
              ? autoResumeModeEnum.requeue
              : autoResumeModeEnum.singleton_dependency}
          </Section.Item>
          {job?.aps_spec?.auto_resume_mode === autoResumeModeEnum.requeue && (
            <Section.Item label="Resume Max Requeue Times">
              {job?.aps_spec?.max_requeue_times ?? "None"}
            </Section.Item>
          )}
        </Section.Row>
      </Section>
      <Section
        heading={
          <span className="flex items-center gap-2">
            Dag Dependency
            <Tooltip slotContent="Jobs that must satisfy their trigger before this job's configured action can run.">
              <span className="text-secondary inline-flex cursor-help">
                <InfoCircle />
              </span>
            </Tooltip>
          </span>
        }
        loading={!job || dagDependenciesLoading || dependencyJobsLoading}
      >
        {dagDependenciesError && (
          <Banner kind="inline" status="error">
            Unable to load DAG dependency status.
          </Banner>
        )}
        {job && (
          <DagDependencies
            dependencies={displayedDagDependencies}
            dependencyJobs={dependencyJobs?.statuses ?? []}
            schedulerDependency={job.dependency}
            configuredDependencies={job.aps_spec?.dag_dependencies}
            dagAction={job.aps_spec?.dag_action}
            timezone={timezone}
          />
        )}
      </Section>

      <Section
        heading="Anomalies & Attribution"
        loading={!job || !jobAttributions || anomalyAttributionsLoading}
      >
        {Object.values(anomalyAttributionsByUuid).map(
          ({ anomaly, jobAttributions }, index) => (
            <Section.Row key={`${anomaly.uuid}-${index}`} className="mb-2">
              <Section.Item innerClassName="!p-0">
                <AnomalyCard
                  anomaly={anomaly}
                  jobAttributions={jobAttributions}
                  grafanaTraceUrlEnabled={grafanaTraceUrlEnabled}
                  grafanaDataSourceId={grafanaDataSourceId}
                  grafanaUrl={grafanaUrl}
                  grafanaOrgId={grafanaOrgId}
                />
              </Section.Item>
            </Section.Row>
          ),
        )}
      </Section>
    </div>
  );
}
