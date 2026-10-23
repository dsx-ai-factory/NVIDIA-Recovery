// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Badge,
  Button,
  Card,
  CodeSnippet,
  Flex,
  SidePanel,
  TableBody,
  TableDataCell,
  TableHead,
  TableHeaderCell,
  TableRoot,
  TableRow,
  Text,
  Tooltip,
} from "@nvidia/foundations-react-core";
import { ArrowDown, ArrowUp, Filter } from "@nvidia/react-gui-icons/icons";
import { useState } from "react";
import Loading from "~/components/page/Loading";
import { Datetime, epochMicrosecondsToIso } from "~/components/utils/Datetime";
import type { AuditEvent } from "~/gen/notifications/types";
import type { AuditEventSort } from "../sort";
import { AUDIT_EVENT_SORT } from "../sort";
import { AuditTraceLink } from "./AuditTraceLink";
import { EventColumnsMenu } from "./EventColumnsMenu";
import {
  getAuditEventDescription,
  getAuditEventJobIdentity,
  getAuditEventOccurredAt,
  getAuditEventTitle,
  getDefaultAuditEventColumnVisibility,
  JOB_DETAILS_EVENT_COLUMN_IDS,
} from "./eventPresentation";
import {
  getNextSelectedAuditEvent,
  isAuditEventRowInteraction,
} from "./eventSelection";

function formatRemoveUnderscores(value: string): string {
  return value.replace(/_/g, " ");
}

function safeStringifyPayload(payload: unknown, space?: number): string {
  if (payload === null || payload === undefined) {
    return "--";
  }
  try {
    return JSON.stringify(payload, null, space) ?? "--";
  } catch {
    return "--";
  }
}

function LevelBadge({ level }: { level: string }) {
  return (
    <Badge
      color="gray"
      kind="outline"
      className="block max-w-full truncate"
      title={level}
    >
      {level}
    </Badge>
  );
}

/** Truncates cell text to fit the column, revealing the full value in a tooltip. */
function TruncatedCellText({ text }: { text: string }) {
  return (
    <Tooltip slotContent={text} side="top">
      <span className="block truncate">{text}</span>
    </Tooltip>
  );
}

interface EventColumn {
  id: string;
  header: string;
  className?: string;
  render: (event: AuditEvent) => React.ReactNode;
  /** Overrides `render` for the table cell only; the side panel still shows the full value. */
  renderForCell?: (event: AuditEvent) => React.ReactNode;
  renderForSidePanel?: (event: AuditEvent) => React.ReactNode;
}

type EventsTableMode = "audit-page" | "job-details";

const EVENT_PRODUCED_AT_COLUMN: EventColumn = {
  id: "produced_at",
  header: "Occurred At",
  className: "w-[180px]",
  render: (event) => (
    <Text className="whitespace-nowrap">
      <Datetime utcIso={epochMicrosecondsToIso(event.event_produced_at)} />
    </Text>
  ),
};

const EVENT_COLUMNS: EventColumn[] = [
  EVENT_PRODUCED_AT_COLUMN,
  {
    id: "job_id",
    header: "Job ID",
    className: "w-[120px]",
    render: getAuditEventJobIdentity,
    renderForCell: (event) => (
      <TruncatedCellText text={getAuditEventJobIdentity(event)} />
    ),
  },
  {
    id: "component",
    header: "Component",
    render: (event) => event.event_producer,
    renderForCell: (event) => <TruncatedCellText text={event.event_producer} />,
  },
  {
    id: "event",
    header: "Event",
    render: getAuditEventTitle,
    renderForCell: (event) => (
      <>
        <TruncatedCellText text={getAuditEventTitle(event)} />
        <AuditTraceLink payload={event.payload} />
      </>
    ),
  },
  {
    id: "description",
    header: "Description",
    render: getAuditEventDescription,
    renderForCell: (event) => (
      <TruncatedCellText text={getAuditEventDescription(event)} />
    ),
  },
  {
    id: "level",
    header: "Level",
    className: "w-[120px]",
    render: (event) => <LevelBadge level={event.event_level} />,
  },
  {
    id: "payload",
    header: "Payload",
    className: "w-[90px] whitespace-nowrap text-right",
    render: (event) => safeStringifyPayload(event.payload),
    renderForCell: () => (
      <Button kind="tertiary" size="small">
        View
      </Button>
    ),
    renderForSidePanel: (event) => (
      <CodeSnippet
        language="json"
        value={safeStringifyPayload(event.payload, 2)}
      />
    ),
  },
];

const JOB_TIMELINE_OCCURRED_AT_COLUMN: EventColumn = {
  ...EVENT_PRODUCED_AT_COLUMN,
  render: (event) => (
    <Text className="whitespace-nowrap">
      <Datetime
        utcIso={epochMicrosecondsToIso(getAuditEventOccurredAt(event))}
      />
    </Text>
  ),
};

const EVENT_TYPE_DETAIL: EventColumn = {
  id: "event_type",
  header: "Event Type",
  render: (event) => formatRemoveUnderscores(event.event_type),
};

const JOB_DETAILS_COLUMN_IDS = new Set<string>(JOB_DETAILS_EVENT_COLUMN_IDS);

function getEventColumns(mode: EventsTableMode): EventColumn[] {
  if (mode === "audit-page") return EVENT_COLUMNS;
  return EVENT_COLUMNS.filter((column) =>
    JOB_DETAILS_COLUMN_IDS.has(column.id),
  ).map((column) =>
    column.id === "produced_at" ? JOB_TIMELINE_OCCURRED_AT_COLUMN : column,
  );
}

function getEventDetailFields(mode: EventsTableMode): EventColumn[] {
  const timestampColumn =
    mode === "job-details"
      ? JOB_TIMELINE_OCCURRED_AT_COLUMN
      : EVENT_PRODUCED_AT_COLUMN;
  return [
    timestampColumn,
    ...EVENT_COLUMNS.slice(1, 4),
    EVENT_TYPE_DETAIL,
    ...EVENT_COLUMNS.slice(4),
  ];
}

export function EventsTable({
  auditEvents,
  isLoading,
  mode = "audit-page",
  sort = AUDIT_EVENT_SORT.descending,
  onSortChange,
  emptyMessage = "No audit events match your filters",
}: {
  auditEvents: AuditEvent[];
  isLoading: boolean;
  mode?: EventsTableMode;
  /** Server query order; only the audit page exposes the sort control. */
  sort?: AuditEventSort;
  /** Requests a new first page in the selected order. */
  onSortChange?: (sort: AuditEventSort) => void;
  emptyMessage?: string;
}) {
  const [visibility, setVisibility] = useState<Record<string, boolean>>(() =>
    mode === "audit-page" ? getDefaultAuditEventColumnVisibility() : {},
  );
  const isColumnVisible = (id: string) => visibility[id] !== false;
  const tableColumns = getEventColumns(mode);
  const eventDetailFields = getEventDetailFields(mode);
  const visibleColumns = tableColumns.filter((col) => isColumnVisible(col.id));
  const [selectedEvent, setSelectedEvent] = useState<AuditEvent | null>(null);

  if (isLoading) {
    return (
      <Card className="rounded-none border-0">
        <Loading />
      </Card>
    );
  }

  if (auditEvents.length === 0) {
    return (
      <Card className="rounded-none border-0">
        <Flex
          direction="col"
          align="center"
          justify="center"
          className="pt-4 pb-6"
        >
          <Filter className="size-12" />
          <Text kind="body/regular/2xl" fontWeight="bold">
            No results
          </Text>
          <div className="text-muted text-center">{emptyMessage}</div>
        </Flex>
      </Card>
    );
  }

  return (
    <div>
      {mode === "audit-page" && (
        <Flex direction="row" align="center" className="bg-surface-base p-3">
          <Text fontSize="18" fontWeight="bold" className="mr-auto">
            Audit Events
          </Text>
          <EventColumnsMenu
            columns={tableColumns}
            isVisible={isColumnVisible}
            onSetVisible={(id, visible) =>
              setVisibility((prev) => ({ ...prev, [id]: visible }))
            }
          />
        </Flex>
      )}
      <TableRoot className="w-full" hoverableRows>
        <TableHead>
          <TableRow>
            {visibleColumns.map((col) => (
              <TableHeaderCell
                key={col.id}
                className={col.className}
                aria-sort={
                  col.id === "produced_at" && mode === "audit-page"
                    ? sort === AUDIT_EVENT_SORT.ascending
                      ? "ascending"
                      : "descending"
                    : undefined
                }
              >
                {col.id === "produced_at" &&
                mode === "audit-page" &&
                onSortChange ? (
                  <button
                    type="button"
                    className="flex cursor-pointer items-center gap-1"
                    onClick={() =>
                      onSortChange(
                        sort === AUDIT_EVENT_SORT.ascending
                          ? AUDIT_EVENT_SORT.descending
                          : AUDIT_EVENT_SORT.ascending,
                      )
                    }
                  >
                    <span>{col.header}</span>
                    {sort === AUDIT_EVENT_SORT.ascending ? (
                      <ArrowUp />
                    ) : (
                      <ArrowDown />
                    )}
                  </button>
                ) : (
                  col.header
                )}
              </TableHeaderCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {auditEvents.map((event, index) => (
            <TableRow
              key={`${event.heimdall_job_id}-${event.event_produced_at}-${index}`}
              data-audit-event-row
              className={
                selectedEvent === event
                  ? "cursor-pointer bg-interaction-hover"
                  : "cursor-pointer"
              }
              onClick={() =>
                setSelectedEvent((current) =>
                  getNextSelectedAuditEvent(current, event),
                )
              }
            >
              {visibleColumns.map((col) => (
                <TableDataCell key={col.id} className={col.className}>
                  {(col.renderForCell ?? col.render)(event)}
                </TableDataCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </TableRoot>
      <SidePanel
        className="w-[500px]"
        bordered
        closeOnClickOutside={false}
        density="spacious"
        open={selectedEvent !== null}
        onInteractOutside={(event) => {
          if (!isAuditEventRowInteraction(event.target)) {
            setSelectedEvent(null);
          }
        }}
        onOpenChange={(open) => {
          if (!open) setSelectedEvent(null);
        }}
        slotHeading="Audit Event Details"
      >
        {selectedEvent && (
          <Flex direction="col" gap="4">
            <AuditTraceLink payload={selectedEvent.payload} />
            {eventDetailFields.map((col) => (
              <div key={col.id} className="flex flex-col gap-1">
                <Text kind="body/bold/sm" className="text-muted">
                  {col.header}
                </Text>
                <div>
                  {col.renderForSidePanel
                    ? col.renderForSidePanel(selectedEvent)
                    : col.render(selectedEvent)}
                </div>
              </div>
            ))}
          </Flex>
        )}
      </SidePanel>
    </div>
  );
}
