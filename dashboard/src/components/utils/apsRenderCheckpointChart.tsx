// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  Chart as ChartJS,
  Legend,
  LinearScale,
  PointElement,
  ScatterController,
  TimeScale,
  Tooltip,
  type TooltipItem,
} from "chart.js";
import type React from "react";
import "chartjs-adapter-date-fns";
import { Scatter } from "react-chartjs-2";
import type { SlurmJobExecutionStatusOutput } from "~/gen/job-manager/types";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import { utcToLocal } from "./Datetime";

ChartJS.register(
  ScatterController,
  PointElement,
  TimeScale,
  LinearScale,
  Tooltip,
  Legend,
);

export const ApsRenderCheckpointChart = ({
  job = undefined,
}: {
  job: SlurmJobExecutionStatusOutput | undefined;
}): React.ReactNode => {
  const { timezone } = useUserPreferences();

  if (!job?.checkpoints) return <></>;

  const data = {
    datasets: [
      {
        label: "Count",
        data: job.checkpoints.map((chp) => ({
          x: Date.parse(chp.timestamp),
          y: 1,
        })),
        backgroundColor: "green",
        pointRadius: 5,
      },
    ],
  };

  const options = {
    maintainAspectRatio: false,
    responsive: true,
    scales: {
      x: {
        type: "time" as const,
        ticks: {
          maxTicksLimit: 5,
          callback: (
            _value: number | string,
            index: number,
            ticks: { value: number }[],
          ) => {
            const ms = ticks[index]?.value;
            if (ms == null) return "";
            return (
              utcToLocal(new Date(ms).toISOString(), timezone) ?? String(ms)
            );
          },
        },
        grid: { color: "#f5f5f5" },
      },
      y: {
        type: "linear" as const,
        min: 0,
        max: 2,
        ticks: {
          stepSize: 1,
          callback: (value: number | string) => (value === 1 ? "Count" : ""),
        },
        grid: { color: "#f5f5f5" },
      },
    },
    plugins: {
      tooltip: {
        callbacks: {
          label: (ctx: TooltipItem<"scatter">) => {
            const x = ctx.parsed.x;
            if (x == null) return "";
            return utcToLocal(new Date(x).toISOString(), timezone) ?? "";
          },
        },
      },
    },
  };

  return (
    <div style={{ width: "100%", height: "200px" }}>
      <Scatter data={data} options={options} />
    </div>
  );
};
