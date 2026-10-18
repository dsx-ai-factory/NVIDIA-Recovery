// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChevronDown, ChevronRight } from "@nvidia/react-gui-icons/icons";
import { useState } from "react";

export function SimpleAccordion({
  children,
  isInitiallyOpen = false,
  title,
  className,
}: {
  children: React.ReactNode;
  isInitiallyOpen?: boolean;
  title: React.ReactNode;
  className?: string;
}) {
  const [isOpen, setOpen] = useState<boolean>(isInitiallyOpen);
  return (
    <div
      className={`border-base flex flex-col gap-1 border p-2${className ? ` ${className}` : ""}`}
    >
      <div
        className="bg-surface-base text-14 hover:bg-interaction-hover flex cursor-pointer items-center gap-1 font-semibold"
        onClick={() => setOpen(!isOpen)}
      >
        {isOpen ? <ChevronDown /> : <ChevronRight />}
        {title}
      </div>
      {isOpen && <div className="p-4">{children}</div>}
    </div>
  );
}
