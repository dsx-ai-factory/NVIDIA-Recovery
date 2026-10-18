// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  Flex,
  Tooltip as KuiTooltip,
  Popover,
  Text,
} from "@nvidia/foundations-react-core";
import {
  BarController,
  BarElement,
  CategoryScale,
  type ChartData,
  Chart as ChartJS,
  type ChartOptions,
  Interaction,
  type InteractionModeFunction,
  Legend,
  LinearScale,
  TimeScale,
  Tooltip,
  type TooltipPositionerFunction,
} from "chart.js";
import "chartjs-adapter-date-fns";
import { HelpCircle, TagLabel } from "@nvidia/react-gui-icons/icons";
import { getRelativePosition } from "chart.js/helpers";
import zoomPlugin from "chartjs-plugin-zoom";
import Link from "next/link";
import { useLayoutEffect, useMemo, useRef, useState } from "react";
import { Bar } from "react-chartjs-2";
import { createPortal } from "react-dom";
import { ApsRenderState } from "~/components/utils/ApsRenderState";
import { SLURM_JOB_STATE } from "~/const/slurm_job";
import type { SlurmJobExecutionStatusOutput } from "~/gen/job-manager/types";
import { useUserPreferences } from "~/provider/userPreferencesProvider";

declare module "chart.js" {
  interface InteractionModeMap {
    rowY: InteractionModeFunction;
  }
  interface TooltipPositionerMap {
    cursor: TooltipPositionerFunction<"bar">;
  }
}

ChartJS.register(
  BarController,
  BarElement,
  CategoryScale,
  LinearScale,
  TimeScale,
  Tooltip,
  Legend,
  zoomPlugin,
);

Interaction.modes.rowY = (chart, e, _options, _useFinalPosition) => {
  const position = getRelativePosition(e, chart);
  const yScale = chart.scales.y;
  if (!yScale) return [];

  const rawIndex = yScale.getValueForPixel(position.y);
  if (rawIndex === undefined) return [];
  const labels = yScale.getLabels?.() ?? [];
  const index = Math.round(rawIndex);
  if (index < 0 || index >= labels.length) return [];
  const label = yScale.getLabelForValue(index);

  const items: ReturnType<InteractionModeFunction> = [];
  for (const meta of chart.getSortedVisibleDatasetMetas()) {
    const data = chart.data.datasets[meta.index]?.data ?? [];
    for (let i = 0; i < data.length; i++) {
      const dp = data[i] as { y?: string } | null | undefined;
      const element = meta.data[i];
      if (!element || dp?.y !== label) continue;
      items.push({ element, datasetIndex: meta.index, index: i });
    }
  }
  items.sort((a, b) => {
    const aStart =
      (
        chart.data.datasets[a.datasetIndex]?.data[a.index] as
          | { x?: [number, number] }
          | undefined
      )?.x?.[0] ?? 0;
    const bStart =
      (
        chart.data.datasets[b.datasetIndex]?.data[b.index] as
          | { x?: [number, number] }
          | undefined
      )?.x?.[0] ?? 0;
    return aStart - bStart;
  });

  const xScale = chart.scales.x;
  const cursorX = xScale ? xScale.getValueForPixel(position.x) : undefined;
  let activeKey: string | null = null;
  if (cursorX !== undefined) {
    for (const it of items) {
      const dp = chart.data.datasets[it.datasetIndex]?.data[it.index] as
        | { x?: [number, number] }
        | undefined;
      const range = dp?.x;
      if (range && cursorX >= range[0] && cursorX <= range[1]) {
        activeKey = `${it.datasetIndex}/${it.index}`;
        break;
      }
    }
  }
  (chart as unknown as { $rowYActiveKey?: string | null }).$rowYActiveKey =
    activeKey;

  return items;
};

Tooltip.positioners.cursor = (_items, eventPosition) => ({
  x: eventPosition.x,
  y: eventPosition.y,
});

const RUNNING_TAIL_MS = 5 * 60 * 1000;

type FloatingPoint = {
  x: [number, number];
  y: string;
  attemptIndex?: number;
  fromIndex?: number;
  toIndex?: number;
};

type UnrenderableAttempt = {
  job_id: string;
  job_attempt_index: number;
  reason: "no timing data";
};

type SegmentKind = "Completed" | "Running" | "Pending" | "Cancelled" | "Gap";

type TooltipRow = {
  kind: SegmentKind;
  attemptLabel: string;
  startMs: number;
  endMs: number;
  durationMs: number;
  isActive: boolean;
};

type TooltipSnapshot = {
  title: string;
  rows: TooltipRow[];
  caretX: number;
  caretY: number;
};

const SEGMENT_KIND_TO_SLURM_STATE: Record<SegmentKind, SLURM_JOB_STATE> = {
  Completed: SLURM_JOB_STATE.COMPLETED,
  Running: SLURM_JOB_STATE.RUNNING,
  Pending: SLURM_JOB_STATE.PENDING,
  Cancelled: SLURM_JOB_STATE.CANCELLED,
  Gap: SLURM_JOB_STATE.REQUEUED,
};

function hasNum(v: number | null): v is number {
  return v !== null && !Number.isNaN(v);
}

export function JobsTimelineChart({
  jobGroupId,
  jobs,
}: {
  jobGroupId: string;
  jobs: SlurmJobExecutionStatusOutput[];
}) {
  const chartRef = useRef<ChartJS<"bar", FloatingPoint[]> | null>(null);
  const tooltipKeyRef = useRef<string>("");
  const [tooltipSnapshot, setTooltipSnapshot] =
    useState<TooltipSnapshot | null>(null);
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const { theme } = useUserPreferences();

  // TODO - switch back to kaizen getColor once its available in open source
  const textColor = theme === "dark" ? "#FFFFFF" : "#000";
  const gridColor = theme === "dark" ? "#313131" : "#ccc";
  const tooltipBg = theme === "dark" ? "#222222" : "#fff";
  const tooltipBorder = theme === "dark" ? "#4b4b4b" : "#a7a7a7";

  const view = useMemo(() => {
    const COLOR_COMPLETED = theme === "dark" ? "#002050" : "#cbf5ff";
    const STROKE_COMPLETED = theme === "dark" ? "#0046a4" : "#7cd7fe";
    const COLOR_RUNNING = theme === "dark" ? "#142700" : "#dafb7d";
    const STROKE_RUNNING = theme === "dark" ? "#265600" : "#a5de15";
    const COLOR_GAP = theme === "dark" ? "#4b4b4b" : "#e0e0e0";
    const STROKE_GAP = theme === "dark" ? "#636363" : "#ccc";
    const COLOR_PENDING = theme === "dark" ? "#331344" : "#f0b9fd";
    const STROKE_PENDING = theme === "dark" ? "#741d9d" : "#c359ef";
    const COLOR_CANCELLED = theme === "dark" ? "#441000" : "#fcde7b";
    const STROKE_CANCELLED = theme === "dark" ? "#8d2600" : "#ef9100";

    const parsed = jobs.map((job) => ({
      job,
      startMs: job.start_time ? Date.parse(job.start_time) : null,
      endMs: job.end_time ? Date.parse(job.end_time) : null,
      submitMs: job.submit_time ? Date.parse(job.submit_time) : null,
    }));

    const noDataParsed = parsed.filter(
      (p) => !hasNum(p.startMs) && !hasNum(p.submitMs),
    );

    const unrenderable: UnrenderableAttempt[] = noDataParsed.map((p) => ({
      job_id: p.job.job_id,
      job_attempt_index: p.job.job_attempt_index,
      reason: "no timing data" as const,
    }));

    const renderable = parsed.filter(
      (p) => hasNum(p.startMs) || hasNum(p.submitMs),
    );

    if (renderable.length === 0) {
      return { isEmpty: true as const, unrenderable };
    }

    const now = Date.now();

    const leftAnchors: number[] = [];
    for (const p of renderable) {
      if (hasNum(p.startMs)) leftAnchors.push(p.startMs);
      else if (hasNum(p.submitMs)) leftAnchors.push(p.submitMs);
    }
    const chartMin = Math.min(...leftAnchors);

    const completedEnds = renderable.map((p) => p.endMs).filter(hasNum);
    const latestEnd =
      completedEnds.length > 0 ? Math.max(...completedEnds) : now;
    const hasActive = renderable.some((p) => !hasNum(p.endMs));
    const chartMax = Math.max(
      latestEnd + RUNNING_TAIL_MS,
      hasActive ? now + RUNNING_TAIL_MS : 0,
    );

    type AttemptSeg = {
      kind: "completed" | "running" | "pending" | "cancelled";
      startMs: number;
      endMs: number;
      index: number;
    };

    const groups = new Map<string, AttemptSeg[]>();
    for (const p of renderable) {
      const list = groups.get(p.job.job_id) ?? [];
      if (hasNum(p.startMs)) {
        const isRunning = !hasNum(p.endMs);
        list.push({
          kind: isRunning ? "running" : "completed",
          startMs: p.startMs,
          endMs: isRunning ? now : (p.endMs as number),
          index: p.job.job_attempt_index,
        });
      } else if (hasNum(p.submitMs) && hasNum(p.endMs)) {
        list.push({
          kind: "cancelled",
          startMs: p.submitMs,
          endMs: p.endMs,
          index: p.job.job_attempt_index,
        });
      } else if (hasNum(p.submitMs)) {
        list.push({
          kind: "pending",
          startMs: p.submitMs,
          endMs: now,
          index: p.job.job_attempt_index,
        });
      }
      groups.set(p.job.job_id, list);
    }

    const completedAttempts: FloatingPoint[] = [];
    const runningAttempts: FloatingPoint[] = [];
    const pendingAttempts: FloatingPoint[] = [];
    const cancelledAttempts: FloatingPoint[] = [];
    const gaps: FloatingPoint[] = [];

    const rows: Array<{ job_id: string; anchor: number }> = [];

    for (const [job_id, attempts] of groups) {
      attempts.sort((a, b) => a.index - b.index);
      const first = attempts[0];
      if (!first) continue;
      rows.push({ job_id, anchor: first.startMs });

      for (let i = 0; i < attempts.length; i++) {
        const a = attempts[i];
        if (!a) continue;
        const point: FloatingPoint = {
          x: [a.startMs, a.endMs],
          y: job_id,
          attemptIndex: a.index,
        };

        if (a.kind === "pending") {
          pendingAttempts.push(point);
        } else if (a.kind === "cancelled") {
          cancelledAttempts.push(point);
        } else if (a.kind === "running") {
          runningAttempts.push(point);
        } else {
          completedAttempts.push(point);
        }

        const next = attempts[i + 1];
        if (next) {
          const gap = next.startMs - a.endMs;
          if (gap > 0) {
            gaps.push({
              x: [a.endMs, next.startMs],
              y: job_id,
              fromIndex: a.index,
              toIndex: next.index,
            });
          }
        }
      }
    }

    rows.sort((a, b) => a.anchor - b.anchor);
    const labels = rows.map((r) => r.job_id);

    const data: ChartData<"bar", FloatingPoint[], string> = {
      labels,
      datasets: [
        {
          label: "Completed",
          data: completedAttempts,
          backgroundColor: COLOR_COMPLETED,
          borderColor: STROKE_COMPLETED,
          borderWidth: 1,
          borderSkipped: false,
          grouped: false,
        },
        {
          label: "Running",
          data: runningAttempts,
          backgroundColor: COLOR_RUNNING,
          borderColor: STROKE_RUNNING,
          borderWidth: 1,
          borderSkipped: false,
          grouped: false,
        },
        {
          label: "Pending",
          data: pendingAttempts,
          backgroundColor: COLOR_PENDING,
          borderColor: STROKE_PENDING,
          borderWidth: 1,
          borderSkipped: false,
          grouped: false,
        },
        {
          label: "Cancelled",
          data: cancelledAttempts,
          backgroundColor: COLOR_CANCELLED,
          borderColor: STROKE_CANCELLED,
          borderWidth: 1,
          borderSkipped: false,
          grouped: false,
        },
        {
          label: "Gap",
          data: gaps,
          backgroundColor: COLOR_GAP,
          borderColor: STROKE_GAP,
          borderWidth: 1,
          borderSkipped: false,
          grouped: false,
        },
      ],
    };

    const options: ChartOptions<"bar"> = {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      color: textColor,
      scales: {
        x: {
          type: "time",
          min: chartMin,
          max: chartMax,
          time: { tooltipFormat: "PPpp" },
          afterBuildTicks: (axis) => {
            const lo = axis.min;
            const hi = axis.max;
            if (lo === undefined || hi === undefined || lo >= hi) return;
            const count = 5;
            axis.ticks = Array.from({ length: count }, (_, i) => ({
              value: lo + ((hi - lo) * i) / (count - 1),
              label: "",
            }));
          },
          ticks: {
            color: textColor,
            autoSkip: false,
            maxRotation: 0,
            callback: (value) => new Date(value as number).toLocaleString(),
          },
          grid: { color: gridColor },
          border: { color: gridColor },
        },
        y: {
          type: "category",
          labels,
          ticks: { color: textColor },
          grid: { color: gridColor },
          border: { color: gridColor },
        },
      },
      plugins: {
        legend: {
          position: "top",
          display: false,
          labels: {
            color: textColor,
            generateLabels: (chart) =>
              chart.data.datasets.map((ds, i) => {
                const baseLabel = ds.label ?? "";
                const count = (ds.data as unknown[]).length;
                const labeled =
                  baseLabel === "Completed" ||
                  baseLabel === "Running" ||
                  baseLabel === "Pending" ||
                  baseLabel === "Cancelled";
                const text = labeled ? `${baseLabel} (${count})` : baseLabel;
                return {
                  text,
                  fillStyle: ds.backgroundColor as string,
                  strokeStyle: ds.borderColor as string,
                  lineWidth: (ds.borderWidth as number) ?? 0,
                  fontColor: textColor,
                  hidden: !chart.isDatasetVisible(i),
                  datasetIndex: i,
                };
              }),
          },
        },
        tooltip: {
          enabled: false,
          mode: "rowY",
          intersect: false,
          position: "cursor",
          external: (context) => {
            const { chart, tooltip } = context;
            if (tooltip.opacity === 0 || tooltip.dataPoints.length === 0) {
              if (tooltipKeyRef.current !== "") {
                tooltipKeyRef.current = "";
                setTooltipSnapshot(null);
              }
              return;
            }

            const dataPoints = tooltip.dataPoints;
            const activeKey = (
              chart as unknown as { $rowYActiveKey?: string | null }
            ).$rowYActiveKey;

            const key = [
              tooltip.caretX,
              tooltip.caretY,
              activeKey ?? "",
              dataPoints
                .map((dp) => `${dp.datasetIndex}:${dp.dataIndex}`)
                .join(","),
            ].join("|");
            if (key === tooltipKeyRef.current) return;
            tooltipKeyRef.current = key;

            const firstRaw = dataPoints[0]?.raw as FloatingPoint | undefined;
            const jobId = firstRaw?.y ?? "";

            const attemptIndices: number[] = [];
            for (const dp of dataPoints) {
              const r = dp.raw as FloatingPoint;
              if (r.attemptIndex !== undefined) {
                attemptIndices.push(r.attemptIndex);
              }
            }
            let titleSuffix = "";
            if (attemptIndices.length === 1) {
              titleSuffix = ` · Attempt ${attemptIndices[0]}`;
            } else if (attemptIndices.length > 1) {
              const min = Math.min(...attemptIndices);
              const max = Math.max(...attemptIndices);
              titleSuffix =
                min === max
                  ? ` · Attempt ${min}`
                  : ` · Attempts ${min} → ${max}`;
            }
            const title = `${jobId}${titleSuffix}`;

            const rows: TooltipRow[] = dataPoints.map((dp) => {
              const r = dp.raw as FloatingPoint;
              const [startMs, endMs] = r.x;
              const kind = (dp.dataset.label ?? "Completed") as SegmentKind;
              const attemptLabel =
                r.fromIndex !== undefined && r.toIndex !== undefined
                  ? `Attempts ${r.fromIndex} → ${r.toIndex}`
                  : r.attemptIndex !== undefined
                    ? `Attempt ${r.attemptIndex}`
                    : "";
              return {
                kind,
                attemptLabel,
                startMs,
                endMs,
                durationMs: endMs - startMs,
                isActive: activeKey === `${dp.datasetIndex}/${dp.dataIndex}`,
              };
            });

            setTooltipSnapshot({
              title,
              rows,
              caretX: tooltip.caretX,
              caretY: tooltip.caretY,
            });
          },
        },
        zoom: {
          pan: {
            enabled: true,
            mode: "x",
          },
          zoom: {
            wheel: { enabled: true },
            pinch: { enabled: true },
            mode: "x",
          },
          limits: {
            x: { min: chartMin, max: chartMax, minRange: 1000 },
          },
        },
      },
    };

    const height = Math.max(120, rows.length * 28 + 60);

    return {
      isEmpty: false as const,
      unrenderable,
      data,
      options,
      height,
      completedAttempts,
      runningAttempts,
      pendingAttempts,
      cancelledAttempts,
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobs, theme]);

  if (view.isEmpty) {
    return <UnrenderableList items={view.unrenderable} />;
  }

  const {
    unrenderable,
    data,
    options,
    height,
    completedAttempts,
    runningAttempts,
    pendingAttempts,
    cancelledAttempts,
  } = view;

  return (
    <Flex direction="col" gap="2">
      <Flex direction="row" gap="2" justify="end" align="center">
        <Popover
          side="top"
          align="end"
          slotContent={
            <Flex direction="col" gap="3">
              <Text kind="body/bold/md">Graph Legend</Text>
              <Flex direction="row" gap="2" wrap="wrap">
                <ApsRenderState state={SLURM_JOB_STATE.COMPLETED}>
                  {SLURM_JOB_STATE.COMPLETED} ({completedAttempts.length})
                </ApsRenderState>
                <ApsRenderState state={SLURM_JOB_STATE.RUNNING}>
                  {SLURM_JOB_STATE.RUNNING} ({runningAttempts.length})
                </ApsRenderState>
                <ApsRenderState state={SLURM_JOB_STATE.PENDING}>
                  {SLURM_JOB_STATE.PENDING} ({pendingAttempts.length})
                </ApsRenderState>
                <ApsRenderState state={SLURM_JOB_STATE.CANCELLED}>
                  {SLURM_JOB_STATE.CANCELLED} ({cancelledAttempts.length})
                </ApsRenderState>
                <ApsRenderState state={SLURM_JOB_STATE.REQUEUED}>
                  GAP
                </ApsRenderState>
              </Flex>
            </Flex>
          }
        >
          <Button kind="tertiary" size="small">
            <TagLabel /> Legend
          </Button>
        </Popover>

        <KuiTooltip
          slotContent={
            <Flex direction="col" gap="1">
              <Text kind="body/regular/sm">- Zoom with mouse wheel</Text>
              <Text kind="body/regular/sm">- Pan with click+drag</Text>
            </Flex>
          }
        >
          <Button
            kind="tertiary"
            size="small"
            onClick={() => chartRef.current?.resetZoom()}
          >
            <HelpCircle />
            Reset zoom
          </Button>
        </KuiTooltip>

        <Link
          href={`/dashboard/workflow?jobGroupId=${jobGroupId}`}
          className="underline"
        >
          <Button kind="secondary" size="small" type="button">
            View Jobs Details
          </Button>
        </Link>
      </Flex>
      <div
        ref={wrapperRef}
        style={{ width: "100%", height, position: "relative" }}
      >
        <Bar ref={chartRef} data={data} options={options} />
        {tooltipSnapshot && (
          <TimelineTooltipCard
            snapshot={tooltipSnapshot}
            wrapperRef={wrapperRef}
            theme={theme}
            tooltipBg={tooltipBg}
            tooltipBorder={tooltipBorder}
            textColor={textColor}
          />
        )}
      </div>
      <UnrenderableList items={unrenderable} />
    </Flex>
  );
}

function UnrenderableList({ items }: { items: UnrenderableAttempt[] }) {
  if (items.length === 0) return null;
  return (
    <Flex direction="col" gap="1">
      <Text kind="body/bold/sm">Not shown:</Text>
      {items.map((item) => (
        <Text
          key={`${item.job_id}/${item.job_attempt_index}`}
          kind="body/regular/sm"
        >
          {item.job_id} attempt {item.job_attempt_index} — {item.reason}
        </Text>
      ))}
    </Flex>
  );
}

function formatDuration(ms: number) {
  if (ms < 1000) return `${ms}ms`;
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rs = s % 60;
  if (m < 60) return `${m}m ${rs}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function TimelineTooltipCard({
  snapshot,
  wrapperRef,
  theme,
  tooltipBg,
  tooltipBorder,
  textColor,
}: {
  snapshot: TooltipSnapshot;
  wrapperRef: React.RefObject<HTMLDivElement | null>;
  theme: string;
  tooltipBg: string;
  tooltipBorder: string;
  textColor: string;
}) {
  const cardRef = useRef<HTMLDivElement | null>(null);
  const [cardSize, setCardSize] = useState<{
    width: number;
    height: number;
  } | null>(null);

  useLayoutEffect(() => {
    const el = cardRef.current;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    if (
      !cardSize ||
      cardSize.width !== rect.width ||
      cardSize.height !== rect.height
    ) {
      setCardSize({ width: rect.width, height: rect.height });
    }
  });

  const wrapperRect = wrapperRef.current?.getBoundingClientRect();
  const baseLeft = (wrapperRect?.left ?? 0) + snapshot.caretX;
  const baseTop = (wrapperRect?.top ?? 0) + snapshot.caretY;

  const viewportWidth =
    typeof window !== "undefined" ? window.innerWidth : Infinity;
  const viewportHeight =
    typeof window !== "undefined" ? window.innerHeight : Infinity;

  const cardWidth = cardSize?.width ?? 0;
  const cardHeight = cardSize?.height ?? 0;

  const flipHorizontal =
    cardWidth > 0 && baseLeft + 12 + cardWidth > viewportWidth - 12;
  const flipVertical =
    cardHeight > 0 && baseTop + 12 + cardHeight > viewportHeight - 12;

  const left = flipHorizontal ? baseLeft - 12 : baseLeft + 12;
  const top = flipVertical ? baseTop - 12 : baseTop + 12;
  const transform = `translate(${flipHorizontal ? "-100%" : "0"}, ${flipVertical ? "-100%" : "0"})`;

  // TODO - switch back to kaizen getColor once its available in open source
  const mutedColor = theme === "dark" ? "#898989" : "#636363";
  const activeRowBg = theme === "dark" ? "#4b4b4b" : "#eee";
  const dividerColor = theme === "dark" ? "#4b4b4b" : "#ccc";

  const card = (
    <div
      ref={cardRef}
      style={{
        position: "fixed",
        left,
        top,
        transform,
        pointerEvents: "none",
        zIndex: 1000,
        background: tooltipBg,
        border: `1px solid ${tooltipBorder}`,
        borderRadius: 8,
        padding: "8px 10px",
        boxShadow: "0 8px 24px rgba(0,0,0,0.18)",
        color: textColor,
        minWidth: 320,
        maxWidth: 560,
        visibility: cardSize ? "visible" : "hidden",
      }}
    >
      <Flex direction="col" gap="1">
        <Text kind="body/bold/md">{snapshot.title}</Text>
        <div style={{ height: 1, background: dividerColor }} />
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "auto auto 1fr 1fr auto",
            columnGap: 10,
            rowGap: 0,
          }}
        >
          {snapshot.rows.map((row) => (
            <div
              key={`${row.kind}-${row.startMs}-${row.endMs}`}
              style={{
                display: "grid",
                gridTemplateColumns: "subgrid",
                gridColumn: "1 / -1",
                alignItems: "center",
                padding: "3px 6px",
                borderRadius: 4,
                background: row.isActive ? activeRowBg : "transparent",
              }}
            >
              <ApsRenderState state={SEGMENT_KIND_TO_SLURM_STATE[row.kind]}>
                {row.kind === "Gap" ? "Gap" : row.kind}
              </ApsRenderState>
              <Text kind="body/regular/sm">{row.attemptLabel}</Text>
              <TimelineTimestamp ms={row.startMs} mutedColor={mutedColor} />
              <TimelineTimestamp ms={row.endMs} mutedColor={mutedColor} />
              <Text kind="body/bold/sm">{formatDuration(row.durationMs)}</Text>
            </div>
          ))}
        </div>
      </Flex>
    </div>
  );

  if (typeof document === "undefined") {
    return null;
  }

  return createPortal(card, document.body);
}

function TimelineTimestamp({
  ms,
  mutedColor,
}: {
  ms: number;
  mutedColor: string;
}) {
  const d = new Date(ms);
  return (
    <Flex direction="col" gap="0">
      <Text kind="body/bold/sm">{d.toLocaleDateString()}</Text>
      <span style={{ color: mutedColor, fontSize: 12 }}>
        {d.toLocaleTimeString()}
      </span>
    </Flex>
  );
}
