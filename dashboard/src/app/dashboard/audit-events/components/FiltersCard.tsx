// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  Card,
  Flex,
  Select,
  Text,
  TextInput,
} from "@nvidia/foundations-react-core";
import { Filter } from "@nvidia/react-gui-icons/icons";
import { useEffect, useState } from "react";
import {
  type AuditEventLevelKey,
  type AuditEventsGetQuery,
  auditEventLevel,
} from "~/gen/notifications/types";
import {
  formatJobAttempt,
  generateHeimdallJobId,
  jobAttemptValidationError,
  parseJobAttempt,
} from "../jobIdentity";

export type AuditEventsFilters = Omit<AuditEventsGetQuery, "cursor" | "limit">;

const SHOW_TIMESTAMP_FILTERS = false;

const EVENT_LEVEL_OPTIONS = [
  { children: "Any level", value: "" },
  ...Object.values(auditEventLevel).map((level) => ({
    children: level,
    value: level,
  })),
];

// TODO - get these from an API eventually
const EVENT_PRODUCER_OPTIONS = [
  { children: "Any component", value: "" },
  { children: "watcher", value: "watcher" },
  {
    children: "anomaly-detection-isolator",
    value: "anomaly-detection-isolator",
  },
  { children: "job-manager", value: "job-manager" },
  {
    children: "slurm-advisor-controller",
    value: "slurm-advisor-controller",
  },
  { children: "slurm-watchdog", value: "slurm-watchdog" },
];

const EMPTY_DRAFT = {
  jobAttempt: "",
  heimdallJobId: "",
  eventLevel: "",
  eventProducer: "",
  eventType: "",
  minEventProducedAt: "",
  maxEventProducedAt: "",
};

function toDraft(filters: AuditEventsFilters) {
  return {
    jobAttempt: formatJobAttempt(filters.job_id, filters.attempt_index),
    heimdallJobId: filters.heimdall_job_id ?? "",
    eventLevel: filters.event_level ?? "",
    eventProducer: filters.event_producer ?? "",
    eventType: filters.event_type ?? "",
    minEventProducedAt: filters.min_event_produced_at?.toString() ?? "",
    maxEventProducedAt: filters.max_event_produced_at?.toString() ?? "",
  };
}

export function FiltersCard({
  filters,
  onApply,
  isFetching,
  expanded,
  onExpandedChange,
}: {
  filters: AuditEventsFilters;
  onApply: (next: AuditEventsFilters) => void;
  isFetching: boolean;
  expanded: boolean;
  onExpandedChange: (expanded: boolean) => void;
}) {
  const [draft, setDraft] = useState(() => toDraft(filters));
  const [computedIdentity, setComputedIdentity] = useState<{
    jobId: string;
    attemptIndex: number;
    heimdallJobId?: string;
    error?: string;
  }>();
  // Preserve job-only deep links containing '/' while other filters change.
  const parsedJobAttempt =
    filters.job_id &&
    draft.jobAttempt === formatJobAttempt(filters.job_id, filters.attempt_index)
      ? {
          jobId: filters.job_id,
          attemptIndex: filters.attempt_index ?? undefined,
        }
      : parseJobAttempt(draft.jobAttempt);
  const jobAttemptError = parsedJobAttempt
    ? undefined
    : jobAttemptValidationError(draft.jobAttempt);
  const matchingComputedIdentity =
    computedIdentity?.jobId === parsedJobAttempt?.jobId &&
    computedIdentity?.attemptIndex === parsedJobAttempt?.attemptIndex
      ? computedIdentity
      : undefined;

  useEffect(() => {
    if (parsedJobAttempt?.attemptIndex === undefined) {
      return;
    }
    const { jobId, attemptIndex } = parsedJobAttempt;

    let active = true;
    void generateHeimdallJobId(jobId, attemptIndex)
      .then((heimdallJobId) => {
        if (active) {
          setComputedIdentity({ jobId, attemptIndex, heimdallJobId });
        }
      })
      .catch(() => {
        if (active) {
          setComputedIdentity({
            jobId,
            attemptIndex,
            error: "Unable to calculate the NVIDIA Recovery Job ID.",
          });
        }
      });
    return () => {
      active = false;
    };
  }, [parsedJobAttempt?.attemptIndex, parsedJobAttempt?.jobId]);

  function handleApply() {
    if (jobAttemptError) {
      return;
    }
    onApply({
      heimdall_job_id: parsedJobAttempt
        ? undefined
        : draft.heimdallJobId || undefined,
      job_id: parsedJobAttempt?.jobId,
      attempt_index: parsedJobAttempt?.attemptIndex,
      event_level:
        draft.eventLevel.trim() === ""
          ? undefined
          : (draft.eventLevel.trim() as AuditEventLevelKey),
      event_producer:
        draft.eventProducer.trim() === ""
          ? undefined
          : draft.eventProducer.trim(),
      event_type: draft.eventType.trim() || undefined,
      min_event_produced_at:
        draft.minEventProducedAt === ""
          ? undefined
          : Number(draft.minEventProducedAt),
      max_event_produced_at:
        draft.maxEventProducedAt === ""
          ? undefined
          : Number(draft.maxEventProducedAt),
    });
  }

  function handleReset() {
    setDraft(EMPTY_DRAFT);
    onApply({});
  }

  if (!expanded) {
    return (
      <Card className="w-fit" density="compact">
        <Flex direction="row" align="center" gap="2">
          <Filter className="size-4" />
          <Button
            kind="tertiary"
            size="small"
            aria-label="Show filters"
            onClick={() => onExpandedChange(true)}
          >
            Show
          </Button>
        </Flex>
      </Card>
    );
  }

  return (
    <Card density="compact">
      <Flex direction="col" gap="4">
        <Flex direction="row" align="center" justify="between">
          <Flex direction="row" align="center" gap="2">
            <Filter className="size-4" />
            <Text kind="body/bold/lg">Filters</Text>
          </Flex>
          <Flex direction="row" align="center" gap="2">
            <Button
              kind="tertiary"
              size="small"
              aria-label="Hide filters"
              onClick={() => onExpandedChange(false)}
            >
              Hide
            </Button>
            <Button kind="tertiary" size="small" onClick={handleReset}>
              Reset
            </Button>
          </Flex>
        </Flex>
        <form
          className="flex flex-col gap-4"
          onSubmit={(e) => {
            e.preventDefault();
            handleApply();
          }}
        >
          <Text kind="body/regular/sm" className="text-muted">
            Enter a job ID to see all attempts, or add an attempt index to
            narrow the events. Filter further by component, event type, or
            level.
          </Text>

          <div className="flex flex-col gap-1">
            <label htmlFor="job-attempt-filter">
              Job ID / optional Attempt
            </label>
            <TextInput
              id="job-attempt-filter"
              placeholder="12345 or 12345/2"
              value={draft.jobAttempt}
              onValueChange={(value) =>
                setDraft((prev) => ({
                  ...prev,
                  jobAttempt: value,
                  heimdallJobId: "",
                }))
              }
            />
            {jobAttemptError ? (
              <Text kind="body/regular/xs" className="text-red-700">
                {jobAttemptError}
              </Text>
            ) : (
              (parsedJobAttempt?.attemptIndex !== undefined ||
                draft.heimdallJobId) && (
                <div className="mt-1 flex flex-col gap-0.5">
                  <Text kind="body/bold/xs">NVIDIA Recovery Job ID</Text>
                  <Text
                    kind="body/regular/xs"
                    className={`break-all ${matchingComputedIdentity?.error ? "text-red-700" : "text-muted"}`}
                  >
                    {matchingComputedIdentity?.error ||
                      matchingComputedIdentity?.heimdallJobId ||
                      draft.heimdallJobId ||
                      "Calculating…"}
                  </Text>
                </div>
              )
            )}
          </div>

          <div className="flex flex-col gap-1">
            <div>Component (Event Producer)</div>
            <Select
              items={EVENT_PRODUCER_OPTIONS}
              value={draft.eventProducer}
              onValueChange={(value) =>
                setDraft((prev) => ({ ...prev, eventProducer: value }))
              }
            />
          </div>

          <div className="flex flex-col gap-1">
            <label htmlFor="event-type-filter">Event Type</label>
            <TextInput
              id="event-type-filter"
              placeholder="job_attempt_observed"
              value={draft.eventType}
              onValueChange={(value) =>
                setDraft((prev) => ({ ...prev, eventType: value }))
              }
            />
          </div>

          <div className="flex flex-col gap-1">
            <div>Event Level</div>
            <Select
              items={EVENT_LEVEL_OPTIONS}
              value={draft.eventLevel}
              onValueChange={(value) =>
                setDraft((prev) => ({ ...prev, eventLevel: value }))
              }
            />
          </div>

          {SHOW_TIMESTAMP_FILTERS && (
            <Flex direction="col" gap="1">
              <Text kind="body/bold/md">Timestamp</Text>
              <Flex direction="row" gap="2">
                <div className="flex flex-1 flex-col gap-1">
                  <div>Min</div>
                  <TextInput
                    id="min-event-produced-at-filter"
                    placeholder="1719859200000000"
                    value={draft.minEventProducedAt}
                    onValueChange={(value) =>
                      setDraft((prev) => ({
                        ...prev,
                        minEventProducedAt: value,
                      }))
                    }
                  />
                </div>
                <div className="flex flex-1 flex-col gap-1">
                  <div>Max</div>
                  <TextInput
                    id="max-event-produced-at-filter"
                    placeholder="1719862800000000"
                    value={draft.maxEventProducedAt}
                    onValueChange={(value) =>
                      setDraft((prev) => ({
                        ...prev,
                        maxEventProducedAt: value,
                      }))
                    }
                  />
                </div>
              </Flex>
            </Flex>
          )}

          <Button
            kind="primary"
            color="brand"
            className="w-full"
            type="submit"
            disabled={isFetching || jobAttemptError !== undefined}
          >
            {isFetching ? "Applying..." : "Apply Filters"}
          </Button>
        </form>
      </Flex>
    </Card>
  );
}
