// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  FormField,
  Popover,
  RadioGroup,
  TextInput,
} from "@nvidia/foundations-react-core";
import { Filter } from "@nvidia/react-gui-icons/icons";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useState } from "react";
import type { GetJobsJobNameMatchKey } from "~/gen/job-manager/types";

export function SearchBar({
  defaultValue,
  defaultJobNameMatch,
  paramName = "query",
}: {
  defaultValue?: string;
  defaultJobNameMatch: GetJobsJobNameMatchKey;
  paramName?: string;
}) {
  const searchParams = useSearchParams();
  const pathname = usePathname();
  const router = useRouter();

  const urlQuery = searchParams.get(paramName) ?? defaultValue ?? "";
  const urlJobNameMatch =
    (searchParams.get("jobNameMatch") as GetJobsJobNameMatchKey | null) ??
    defaultJobNameMatch;

  const [val, setVal] = useState<string>(urlQuery);
  const [trackedQuery, setTrackedQuery] = useState<string>(urlQuery);
  if (urlQuery !== trackedQuery) {
    setTrackedQuery(urlQuery);
    setVal(urlQuery);
  }

  const [jobNameMatchFilter, setJobNameMatchFilter] =
    useState<GetJobsJobNameMatchKey>(urlJobNameMatch);
  const [trackedMatch, setTrackedMatch] =
    useState<GetJobsJobNameMatchKey>(urlJobNameMatch);
  if (urlJobNameMatch !== trackedMatch) {
    setTrackedMatch(urlJobNameMatch);
    setJobNameMatchFilter(urlJobNameMatch);
  }

  function handleSearch() {
    const params = new URLSearchParams(searchParams);
    if (val) {
      params.set(paramName, val);
    } else {
      params.delete(paramName);
    }

    if (jobNameMatchFilter) {
      params.set("jobNameMatch", jobNameMatchFilter);
    } else {
      params.delete("jobNameMatch");
    }

    params.delete("page");

    router.replace(`${pathname}?${params.toString()}`);
  }

  return (
    <FormField className="flex-grow">
      <TextInput
        className="flex-grow"
        value={val}
        placeholder="Search for your workflow/job name"
        slotEnd={
          <Popover
            side="bottom"
            align="end"
            slotContent={
              <RadioGroup
                name="job-name-match-filter"
                items={[
                  {
                    children: "Find Jobs that begin with the search value",
                    value: "prefix",
                  },
                  {
                    children: "Find Jobs that match the search value",
                    value: "exact",
                  },
                  {
                    children: "Find Jobs that contain the search value",
                    value: "fuzzy",
                  },
                ]}
                value={jobNameMatchFilter}
                onValueChange={(value) =>
                  setJobNameMatchFilter(value as GetJobsJobNameMatchKey)
                }
              />
            }
          >
            <Button size="small" kind="tertiary" color="neutral">
              <Filter style={{ width: 12, height: 12 }} />
            </Button>
          </Popover>
        }
        onChange={(e) => {
          setVal(e.target.value);
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === "NumpadEnter") {
            handleSearch();
          }
        }}
      />
    </FormField>
  );
}
