// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Spinner } from "@nvidia/foundations-react-core";

export function Section({
  children,
  heading,
  loading,
}: {
  children?: React.ReactNode;
  heading?: React.ReactNode;
  loading: boolean;
}) {
  return (
    <div className="bg-surface-base flex flex-col gap-2 p-6">
      {heading && (
        <h2 className="leading-heading tracking-less text-18 font-medium">
          {heading}
        </h2>
      )}
      {loading ? <Spinner size="small" aria-label="Loading" /> : children}
    </div>
  );
}

Section.Row = function Row({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={`flex gap-4 [&>*]:flex-1 ${className ? ` ${className}` : ""}`}
    >
      {children}
    </div>
  );
};

Section.Item = function Item({
  children,
  className,
  innerClassName,
  label,
}: {
  children: React.ReactNode;
  className?: string;
  innerClassName?: string;
  label?: string;
}) {
  return (
    <div className={`flex flex-col gap-1${className ? ` ${className}` : ""}`}>
      {label && (
        <span className="leading-text text-12 text-secondary font-medium">
          {label}
        </span>
      )}
      <div
        className={`border-base min-h-8 rounded-sm border p-2 ${innerClassName ? ` ${innerClassName}` : ""}`}
      >
        {children}
      </div>
    </div>
  );
};
