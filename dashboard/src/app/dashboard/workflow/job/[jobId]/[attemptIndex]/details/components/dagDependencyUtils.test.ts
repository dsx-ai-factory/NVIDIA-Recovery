// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";
import {
  buildDagConfiguration,
  buildDagDependencyDisplayRows,
  formatDagValue,
  getDagDependencyStatusColor,
  hasDagConfiguration,
} from "./dagDependencyUtils";

describe("DAG dependency display helpers", () => {
  it("shows configured dependencies without runtime registration", () => {
    expect(
      buildDagDependencyDisplayRows({
        configuredDependencies: [{ target: { type: "id", id: "7140099" } }],
        registeredDependencies: [],
      }),
    ).toEqual([
      {
        key: "id\u00007140099\u0000completed",
        referenceType: "id",
        identifier: "7140099",
        trigger: "completed",
        actionStatus: undefined,
        actionTime: undefined,
        registered: false,
      },
    ]);
  });

  it("overlays runtime status on its configured dependency", () => {
    expect(
      buildDagDependencyDisplayRows({
        configuredDependencies: [
          {
            target: { type: "name", name: "preprocess" },
            trigger: "completed",
          },
        ],
        registeredDependencies: [
          {
            dependent_job_id: "200",
            dependent_job_action: "release",
            dependency_job_identifier: "preprocess",
            dependency_job_identifier_type: "name",
            dependency_trigger_condition: "completed",
            action_status: "successful",
            action_time: "2026-09-14T12:20:38Z",
          },
        ],
      }),
    ).toEqual([
      {
        key: "name\u0000preprocess\u0000completed",
        referenceType: "name",
        identifier: "preprocess",
        trigger: "completed",
        actionStatus: "successful",
        actionTime: "2026-09-14T12:20:38Z",
        registered: true,
      },
    ]);
  });

  it("retains registered history that is absent from current configuration", () => {
    expect(
      buildDagDependencyDisplayRows({
        configuredDependencies: [],
        registeredDependencies: [
          {
            dependent_job_id: "200",
            dependent_job_action: "release",
            dependency_job_identifier: "100",
            dependency_job_identifier_type: "id",
            dependency_trigger_condition: "completed",
            action_status: "not_eligible",
            action_time: null,
          },
        ],
      }),
    ).toEqual([
      {
        key: "id\u0000100\u0000completed",
        referenceType: "id",
        identifier: "100",
        trigger: "completed",
        actionStatus: "not_eligible",
        actionTime: null,
        registered: true,
      },
    ]);
  });

  it("formats API enum values for display", () => {
    expect(formatDagValue("not_eligible")).toBe("Not Eligible");
    expect(formatDagValue("id")).toBe("ID");
  });

  it("maps lifecycle statuses to semantic badge colors", () => {
    expect(getDagDependencyStatusColor("successful")).toBe("green");
    expect(getDagDependencyStatusColor("in_progress")).toBe("blue");
    expect(getDagDependencyStatusColor("aborted")).toBe("red");
    expect(getDagDependencyStatusColor("not_eligible")).toBe("gray");
    expect(getDagDependencyStatusColor("unknown")).toBe("yellow");
  });

  it("builds the configuration shown in the modal", () => {
    const snippet = buildDagConfiguration({
      schedulerDependency: "afterok:123",
      dagAction: "release",
      dagDependencies: [
        {
          target: { type: "id", id: "123" },
          trigger: "completed",
        },
      ],
    });

    expect(JSON.parse(snippet)).toEqual({
      dag_dependencies: [
        {
          target: { type: "id", id: "123" },
          trigger: "completed",
        },
      ],
      dag_action: "release",
      scheduler_dependency: "afterok:123",
    });
  });

  it("only offers configuration when the job has DAG metadata", () => {
    expect(hasDagConfiguration({})).toBe(false);
    expect(hasDagConfiguration({ dagDependencies: [] })).toBe(false);
    expect(hasDagConfiguration({ dagAction: "release" })).toBe(true);
  });
});
