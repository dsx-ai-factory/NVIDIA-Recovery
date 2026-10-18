# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Version constant for the workload attribution HTTP interface (design §12.6).

Both the Anomaly Policy Engine and attribution-adapter pin this single agreed
value so the two images cannot disagree. Schema-version negotiation is out of
scope for v1.
"""

ATTRIBUTION_SCHEMA_VERSION = "0.1.0"
