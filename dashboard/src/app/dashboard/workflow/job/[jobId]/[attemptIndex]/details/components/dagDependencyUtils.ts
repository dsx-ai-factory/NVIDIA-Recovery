// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import type {
  DagActionEnumKey,
  DagDependencyResponse,
  DagDependencyStep,
} from "~/gen/job-manager/types";

export type DagDependencyBadgeColor =
  | "blue"
  | "gray"
  | "green"
  | "red"
  | "yellow";

export interface DagDependencyDisplayRow {
  key: string;
  referenceType: string;
  identifier: string;
  trigger: string;
  actionStatus?: string;
  actionTime?: string | null;
  registered: boolean;
}

function dependencyKey(
  referenceType: string,
  identifier: string,
  trigger: string,
): string {
  return `${referenceType}\u0000${identifier}\u0000${trigger}`;
}

/**
 * Builds display rows from declared configuration and overlays Job Manager's
 * runtime records. Configuration remains visible when DAG management is
 * disabled or registration failed, while unmatched runtime history is retained
 * for diagnosis.
 */
export function buildDagDependencyDisplayRows({
  configuredDependencies,
  registeredDependencies,
}: {
  configuredDependencies?: DagDependencyStep[] | null;
  registeredDependencies: DagDependencyResponse[];
}): DagDependencyDisplayRow[] {
  const registeredByKey = new Map(
    registeredDependencies.map((dependency) => [
      dependencyKey(
        dependency.dependency_job_identifier_type,
        dependency.dependency_job_identifier,
        dependency.dependency_trigger_condition,
      ),
      dependency,
    ]),
  );
  const rows: DagDependencyDisplayRow[] = [];
  const displayedKeys = new Set<string>();

  for (const dependency of configuredDependencies ?? []) {
    const referenceType = dependency.target.type;
    const identifier =
      dependency.target.type === "id"
        ? dependency.target.id
        : dependency.target.name;
    const trigger = dependency.trigger ?? "completed";
    const key = dependencyKey(referenceType, identifier, trigger);
    if (displayedKeys.has(key)) continue;

    const registered = registeredByKey.get(key);
    rows.push({
      key,
      referenceType,
      identifier,
      trigger,
      actionStatus: registered?.action_status,
      actionTime: registered?.action_time,
      registered: registered !== undefined,
    });
    displayedKeys.add(key);
  }

  for (const dependency of registeredDependencies) {
    const key = dependencyKey(
      dependency.dependency_job_identifier_type,
      dependency.dependency_job_identifier,
      dependency.dependency_trigger_condition,
    );
    if (displayedKeys.has(key)) continue;

    rows.push({
      key,
      referenceType: dependency.dependency_job_identifier_type,
      identifier: dependency.dependency_job_identifier,
      trigger: dependency.dependency_trigger_condition,
      actionStatus: dependency.action_status,
      actionTime: dependency.action_time,
      registered: true,
    });
    displayedKeys.add(key);
  }

  return rows;
}

export function formatDagValue(value: string): string {
  if (value.toLowerCase() === "id") return "ID";
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

export function getDagDependencyStatusColor(
  status: string,
): DagDependencyBadgeColor {
  switch (status.toLowerCase()) {
    case "successful":
      return "green";
    case "in_progress":
      return "blue";
    case "failed":
    case "aborted":
      return "red";
    case "not_eligible":
      return "gray";
    default:
      return "yellow";
  }
}

export function hasDagConfiguration({
  schedulerDependency,
  dagDependencies,
  dagAction,
}: {
  schedulerDependency?: string | null;
  dagDependencies?: DagDependencyStep[] | null;
  dagAction?: DagActionEnumKey;
}): boolean {
  return Boolean(
    schedulerDependency || dagDependencies?.length || dagAction !== undefined,
  );
}

export function buildDagConfiguration({
  schedulerDependency,
  dagDependencies,
  dagAction,
}: {
  schedulerDependency?: string | null;
  dagDependencies?: DagDependencyStep[] | null;
  dagAction?: DagActionEnumKey;
}): string {
  const configuration: Record<string, unknown> = {
    dag_dependencies: dagDependencies ?? [],
  };
  if (dagAction !== undefined) configuration.dag_action = dagAction;
  if (schedulerDependency) {
    configuration.scheduler_dependency = schedulerDependency;
  }
  return JSON.stringify(configuration, null, 2);
}
