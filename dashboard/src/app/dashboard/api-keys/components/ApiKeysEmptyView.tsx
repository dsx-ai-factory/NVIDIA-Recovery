// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { StatusMessage } from "@nvidia/foundations-react-core";
import { Key } from "@nvidia/react-gui-icons/icons";

export function ApiKeysEmptyView() {
  return (
    <StatusMessage
      slotHeading="No API keys found"
      slotSubheading="Create your first API key to get started"
      slotMedia={<Key className="w-[48px] h-[48px]" />}
    />
  );
}
