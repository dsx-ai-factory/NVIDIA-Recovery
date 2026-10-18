#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Append one job-free marker and a node-normalized fixture to the exact syslog
# file mounted into a trusted disposable Slurm compute pod.
set -euo pipefail

: "${KERNEL_LOG_PATH:?KERNEL_LOG_PATH is required}"
: "${SYSLOG_MARKER:?SYSLOG_MARKER is required}"
: "${SLURMD_NODENAME:?SLURMD_NODENAME is required}"
: "${SYSLOG_PATH:=/var/log/syslog}"

if [[ "$SYSLOG_PATH" != /var/log/syslog ]]; then
  printf 'error: syslog fixture may write only /var/log/syslog\n' >&2
  exit 2
fi
if [[ ! -f "$KERNEL_LOG_PATH" ]]; then
  printf 'error: kernel fixture is not a regular file: %s\n' "$KERNEL_LOG_PATH" >&2
  exit 2
fi
if [[ ! "$SYSLOG_MARKER" =~ ^HEIMDALL_E2E_SYSLOG_[[:xdigit:]]{32}$ ]]; then
  printf 'error: system marker must be an opaque E2E UUID and contain no workload identity\n' >&2
  exit 2
fi

month="$(date '+%b')"
day="$(date '+%e')"
clock="$(date '+%H:%M:%S')"

printf '%s %s %s %s kernel: HEIMDALL_E2E_SYSLOG_MARKER=%s\n' \
  "$month" "$day" "$clock" "$SLURMD_NODENAME" "$SYSLOG_MARKER" >> "$SYSLOG_PATH"

# RFC3164-style fixtures carry timestamp and hostname in fields 1-4. Replace
# those fields with the current time and the scheduler-visible node while
# preserving the fault body consumed by Attribution.
awk -v month="$month" -v day="$day" -v clock="$clock" -v node="$SLURMD_NODENAME" \
  'NF >= 5 {$1=month; $2=day; $3=clock; $4=node} {print}' \
  "$KERNEL_LOG_PATH" >> "$SYSLOG_PATH"

printf 'appended system fixture marker %s on node %s\n' "$SYSLOG_MARKER" "$SLURMD_NODENAME"
