// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Button, Tabs, Text } from "@nvidia/foundations-react-core";
import { ChevronDown, ChevronUp } from "@nvidia/react-gui-icons/icons";
import { useMemo, useState } from "react";
import {
  ApsRenderAnomaly,
  ApsRenderAttribution,
  ApsRenderAttributionExclusionType,
  ApsRenderAttributionSource,
  ApsRenderAttributionType,
  ApsRenderGlobalEventAttribution,
  ApsRenderOriginalExclusion,
} from "~/components/utils/apsJobRender";
import { Datetime } from "~/components/utils/Datetime";
import type { JobAnomaly, NodeEvents } from "~/gen/job-manager/types";
import { buildGrafanaTraceUrl } from "~/utils/grafana";
import { SimpleAccordion } from "./SimpleAccordion";

// Deduplicated node events type: identical items except for error_attribution_type
// are merged and their types accumulated as error_attribution_types.
type NodeEventDecorated = Omit<
  NodeEvents["events"][number],
  "error_attribution_type"
> & {
  error_attribution_types: NodeEvents["events"][number]["error_attribution_type"][];
};

type NodeEventsDecorated = NodeEventDecorated[];

// Deduplicated global events type: identical items except for error_attribution_type
// are merged and their types accumulated as error_attribution_types.
type GlobalEventBase = NonNullable<NodeEvents["global_events"]>[number];
type GlobalEventDecorated = Omit<GlobalEventBase, "error_attribution_type"> & {
  error_attribution_types: GlobalEventBase["error_attribution_type"][];
};
type GlobalEventsDecorated = GlobalEventDecorated[];

function deduplicateNodeEvents(
  events: NodeEvents["events"] | undefined,
): NodeEventsDecorated {
  const list = events ?? [];
  if (!list.length) return [];
  const grouped = new Map<string, NodeEventDecorated>();
  for (const event of list) {
    const { error_attribution_type, ...rest } = event;
    const key = JSON.stringify(rest);
    const existing = grouped.get(key);
    if (existing) {
      if (!existing.error_attribution_types.includes(error_attribution_type)) {
        existing.error_attribution_types.push(error_attribution_type);
      }
    } else {
      grouped.set(key, {
        ...rest,
        error_attribution_types: [error_attribution_type],
      });
    }
  }
  return Array.from(grouped.values());
}

function deduplicateGlobalEvents(
  events: NodeEvents["global_events"] | undefined,
): GlobalEventsDecorated {
  const list = events ?? [];
  if (!list.length) return [];
  const grouped = new Map<string, GlobalEventDecorated>();
  for (const event of list) {
    const { error_attribution_type, ...rest } = event;
    const key = JSON.stringify(rest);
    const existing = grouped.get(key);
    if (existing) {
      if (!existing.error_attribution_types.includes(error_attribution_type)) {
        existing.error_attribution_types.push(error_attribution_type);
      }
    } else {
      grouped.set(key, {
        ...rest,
        error_attribution_types: [error_attribution_type],
      });
    }
  }
  return Array.from(grouped.values());
}

export function AnomalyCard({
  anomaly,
  jobAttributions,
  grafanaTraceUrlEnabled,
  grafanaDataSourceId,
  grafanaUrl,
  grafanaOrgId,
}: {
  anomaly: JobAnomaly;
  jobAttributions: NodeEvents | undefined;
  grafanaTraceUrlEnabled: boolean;
  grafanaDataSourceId: string | undefined;
  grafanaUrl: string | undefined;
  grafanaOrgId: string | undefined;
}) {
  const grafanaTraceUrl = anomaly.trace_id
    ? buildGrafanaTraceUrl(
        anomaly.trace_id,
        grafanaDataSourceId,
        grafanaUrl,
        grafanaOrgId,
      )
    : "";
  const [tabOpen, setTabOpen] = useState<
    "nodeAttributions" | "globalAttributions"
  >("globalAttributions");
  const [moreOpen, setMoreOpen] = useState(false);
  const dedupedNodeEvents: NodeEventsDecorated = useMemo(
    () => deduplicateNodeEvents(jobAttributions?.events),
    [jobAttributions?.events],
  );

  const dedupedGlobalEvents: GlobalEventsDecorated = useMemo(
    () => deduplicateGlobalEvents(jobAttributions?.global_events),
    [jobAttributions?.global_events],
  );

  return (
    <div>
      <div className="flex flex-row flex-wrap gap-6 px-5 py-2">
        <div className="flex items-center gap-2">
          <Text className="block" fontSize="16" fontWeight="semibold">
            Anomaly
          </Text>
          <ApsRenderAnomaly key={anomaly.uuid} anomaly={anomaly} />
        </div>
        <div className="flex items-center gap-2">
          <Text className="block" fontSize="16" fontWeight="semibold">
            Time
          </Text>
          <Datetime utcIso={anomaly.timestamp} />
        </div>

        <Button
          kind="tertiary"
          size="small"
          type="button"
          onClick={() => setMoreOpen(!moreOpen)}
        >
          More
          {moreOpen ? <ChevronUp /> : <ChevronDown />}
        </Button>
      </div>
      {moreOpen && (
        <div className="flex flex-row flex-wrap gap-4 px-5 py-1">
          <div className="flex items-center gap-2">
            <Text className="block" fontSize="16" fontWeight="semibold">
              UUID
            </Text>
            <div>{anomaly.uuid ?? "N/A"}</div>
          </div>
          <div className="flex items-center gap-2">
            <Text className="block" fontSize="16" fontWeight="semibold">
              Handled By
            </Text>
            <div>{anomaly.handled_by ?? "N/A"}</div>
          </div>
          {grafanaTraceUrlEnabled ? (
            <div className="flex items-center gap-2">
              <Text className="block" fontSize="16" fontWeight="semibold">
                Trace ID
              </Text>
              {grafanaTraceUrl ? (
                <a
                  className="underline"
                  href={grafanaTraceUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  {anomaly.trace_id}
                </a>
              ) : (
                "N/A"
              )}
            </div>
          ) : null}
        </div>
      )}
      <div className="mt-1">
        <Tabs
          className="[&>.nv-tabs-content]:w-full [&>.nv-tabs-content]:gap-0 [&>.nv-tabs-content]:p-0"
          items={[
            {
              children: <>Global Attributions</>,
              slotContent: (
                <GlobalAttributions globalEvents={dedupedGlobalEvents} />
              ),
              value: "globalAttributions",
            },
            {
              children: <>Node Attributions</>,
              slotContent: <NodeAttributions nodeEvents={dedupedNodeEvents} />,
              value: "nodeAttributions",
            },
          ]}
          value={tabOpen}
          onValueChange={(value) =>
            setTabOpen(value as "nodeAttributions" | "globalAttributions")
          }
        />
      </div>
    </div>
  );
}

function NodeAttributions({ nodeEvents }: { nodeEvents: NodeEventsDecorated }) {
  if (!nodeEvents || nodeEvents.length === 0) {
    return <div className="px-3 py-4">No node attributions found.</div>;
  }

  return (
    <>
      {nodeEvents.map((nodeEvent, i) => (
        <SimpleAccordion
          key={`${nodeEvent.node}-${i}`}
          className="w-full border-t-0 border-r-0 border-b border-l-0 last:border-b-0"
          title={
            <div className="flex items-center gap-10">
              <div>{`Node Event: ${nodeEvent.node}`}</div>
              <div className="flex items-center gap-2">
                {`Exclusion Type:`}
                <ApsRenderAttributionExclusionType
                  exclusionType={nodeEvent.exclusion_type}
                />
              </div>
              {nodeEvent.error_attribution && (
                <div className="flex items-center gap-2">
                  {`Error Attribution:`}
                  <ApsRenderAttribution
                    errorAttribution={nodeEvent.error_attribution}
                    timestamp={nodeEvent.timestamp}
                  />
                </div>
              )}
              {nodeEvent.original_exclusion_type && (
                <div className="flex items-center gap-2">
                  {`Original Exclusion:`}
                  <ApsRenderOriginalExclusion
                    originalExclusion={nodeEvent.original_exclusion_type}
                  />
                </div>
              )}
            </div>
          }
        >
          <div className="flex flex-col gap-4">
            <div className="flex w-full flex-wrap items-center gap-6">
              <div className="flex items-center gap-2">
                <Text fontWeight="semibold">Error Attribution Types</Text>
                <div className="flex gap-2">
                  {nodeEvent.error_attribution_types.map(
                    (errorAttributionType) => (
                      <ApsRenderAttributionType
                        key={errorAttributionType}
                        attributionType={errorAttributionType}
                      />
                    ),
                  )}
                </div>
              </div>

              <div className="flex items-center gap-2">
                <Text fontWeight="semibold">Error Attribution Source</Text>
                <div className="flex gap-2">
                  {nodeEvent.error_attribution_sources.map(
                    (errorAttributionSource) => (
                      <ApsRenderAttributionSource
                        key={errorAttributionSource}
                        attributionSource={errorAttributionSource}
                      />
                    ),
                  )}
                </div>
              </div>
            </div>

            <div className="w-full">
              <Text fontWeight="semibold">Job Execution Error</Text>
              <div className="flex flex-col gap-2">
                <div className="flex flex-row gap-0">
                  <code className="leading-body text-14 overflow-auto bg-gray-700 p-1 text-blue-400">
                    {`${nodeEvent.timestamp}`}
                  </code>
                  <code className="leading-body text-14 text-gray-025 flex-1 overflow-auto bg-gray-700 p-1">
                    {`${nodeEvent.job_execution_error.message}`}
                  </code>
                </div>
                {nodeEvent.job_execution_error.signature}
              </div>
            </div>
          </div>
        </SimpleAccordion>
      ))}
    </>
  );
}

function GlobalAttributions({
  globalEvents,
}: {
  globalEvents: GlobalEventsDecorated;
}) {
  if (!globalEvents || globalEvents.length === 0) {
    return <div className="px-3 py-4">No global attributions found.</div>;
  }
  return (
    <>
      {globalEvents.map((globalEvent, i) => (
        <SimpleAccordion
          key={`${globalEvent.timestamp}-${i}`}
          className="w-full border-t-0 border-r-0 border-b border-l-0 last:border-b-0"
          title={
            <div className="flex items-center gap-10">
              <div>Global Event</div>
              <div className="flex items-center gap-2">
                {`Error Attribution:`}
                <ApsRenderGlobalEventAttribution
                  errorAttribution={globalEvent.error_attribution}
                  timestamp={globalEvent.timestamp}
                />
              </div>
            </div>
          }
        >
          <div className="flex flex-col gap-4">
            <div className="flex w-full flex-wrap items-center gap-6">
              <div className="flex items-center gap-2">
                <Text fontWeight="semibold">Error Attribution Types</Text>
                <div className="flex gap-2">
                  {globalEvent.error_attribution_types.map(
                    (
                      errorAttributionType: GlobalEventBase["error_attribution_type"],
                    ) => (
                      <ApsRenderAttributionType
                        key={errorAttributionType}
                        attributionType={errorAttributionType}
                      />
                    ),
                  )}
                </div>
              </div>
              <div className="flex items-center gap-2">
                <Text fontWeight="semibold">Error Attribution Source</Text>
                <div className="flex gap-2">
                  {globalEvent.error_attribution_sources.map(
                    (errorAttributionSource) => (
                      <ApsRenderAttributionSource
                        key={errorAttributionSource}
                        attributionSource={errorAttributionSource}
                      />
                    ),
                  )}
                </div>
              </div>
            </div>

            <div className="w-full">
              <Text fontWeight="semibold">Job Execution Error</Text>
              <div className="flex flex-col gap-2">
                <div className="flex flex-row gap-0">
                  <code className="leading-body text-14 overflow-auto bg-gray-700 p-1 text-blue-400">
                    {`${globalEvent.timestamp}`}
                  </code>
                  <code className="leading-body text-14 text-gray-025 flex-1 overflow-auto bg-gray-700 p-1">
                    {`${globalEvent.job_execution_error.message}`}
                  </code>
                </div>
                {globalEvent.job_execution_error.signature}
              </div>
            </div>
          </div>
        </SimpleAccordion>
      ))}
    </>
  );
}
