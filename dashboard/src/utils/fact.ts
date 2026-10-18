// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export const GetFACTUrl = ({
  factUrl,
  jobName,
  jobId,
  clusterName,
  userName,
}: {
  factUrl: string;
  jobId: string;
  jobName: string;
  clusterName: string;
  userName: string;
}): string => {
  let res = factUrl;
  if (res.includes("#CLUSTER_NAME#"))
    res = res.replace("#CLUSTER_NAME#", clusterName);
  if (res.includes("#JOB_ID#")) res = res.replace("#JOB_ID#", jobId);
  if (res.includes("#JOB_NAME#")) res = res.replace("#JOB_NAME#", jobName);
  if (res.includes("#USER_NAME#")) res = res.replace("#USER_NAME#", userName);
  return res;
};
