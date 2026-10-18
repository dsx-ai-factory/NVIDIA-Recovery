# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Public tests collect without the private FACT client.

Explicit ``pytest tests/provider`` runs the provider qualification suite and
fails normally if its dependencies are missing; no import failures are skipped.
"""

collect_ignore = ["provider"]
