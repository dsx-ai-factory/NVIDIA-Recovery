#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Compatibility entry point. The selected backend and backend-neutral metrics
# are owned by check-log-query.sh.
exec /scripts/check-log-query.sh
