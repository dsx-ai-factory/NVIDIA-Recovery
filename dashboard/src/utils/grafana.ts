// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/** Build an Explore link only when the operator supplies a Grafana endpoint and datasource. */
export function buildGrafanaTraceUrl(
  traceId: string,
  grafanaDataSourceId: string | undefined,
  grafanaUrl: string | undefined,
  grafanaOrgId?: string,
): string {
  if (!grafanaDataSourceId || !grafanaUrl) {
    return "";
  }

  let url: URL;
  try {
    url = new URL(grafanaUrl);
    url.pathname = `${url.pathname.replace(/\/+$/, "")}/explore`;
    url.search = "";
    url.hash = "";
  } catch {
    return "";
  }
  if (!["https:", "http:"].includes(url.protocol)) return "";

  if (url.username || url.password) return "";

  const panes = {
    otm: {
      datasource: grafanaDataSourceId,
      queries: [
        {
          refId: "A",
          queryType: "traceql",
          query: traceId,
          datasource: {
            type: "tempo",
            uid: grafanaDataSourceId,
          },
        },
      ],
      range: {
        from: "now-1h",
        to: "now",
      },
    },
  };

  url.searchParams.set("schemaVersion", "1");
  url.searchParams.set("panes", JSON.stringify(panes));
  if (grafanaOrgId) url.searchParams.set("orgId", grafanaOrgId);
  return url.toString();
}
