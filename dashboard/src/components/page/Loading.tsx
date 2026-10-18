// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Spinner } from "@nvidia/foundations-react-core";

export default function Loading() {
  return (
    <div className="flex h-full w-full items-center justify-center p-3">
      <Spinner size="large" aria-label="Loading" />
    </div>
  );
}
