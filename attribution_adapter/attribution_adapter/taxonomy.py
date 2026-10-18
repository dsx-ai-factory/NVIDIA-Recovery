# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Owned copy of FACT's attribution-outcome taxonomy (no blueprint import).

Source of truth: FACT taxonomy.py SERVICE_ATTRIBUTION_OUTCOMES / TAXONOMY_MAP (13 outcomes).
Maps FACT's verdict.attribution_outcome -> Heimdall ErrorCategory via FACT's sub_domain.
error_category is display-only for the Policy Engine (isolation is symptom-driven), so a
verdict-level category is acceptable. Unknown/unmapped outcomes -> UNATTRIBUTED + a warning
(the vocabulary is open: production may emit outcomes absent from any sample).
"""

from recovery_libs.schema.attribution_interface.enums import ErrorCategory

from attribution_adapter.utils.logger import logger

# FACT attribution_outcome -> Heimdall ErrorCategory (via FACT sub_domain).
OUTCOME_TO_CATEGORY: dict[str, ErrorCategory] = {
    "": ErrorCategory.UNATTRIBUTED,  # FACT default row (unclassified)
    "Pass": ErrorCategory.UNATTRIBUTED,  # sub_domain none
    "Fail InfraHW": ErrorCategory.HARDWARE,  # hardware
    "Fail InfraSW": ErrorCategory.SYSTEM_SOFTWARE,  # system_software
    "Fail User": ErrorCategory.APPLICATION,  # application
    "Fail Multi": ErrorCategory.AMBIGUOUS,  # ambiguous
    "Fail MultiCategory": ErrorCategory.AMBIGUOUS,  # ambiguous
    "Fail Undetermined": ErrorCategory.UNATTRIBUTED,  # unattributed
    "Cancelled by User": ErrorCategory.UNATTRIBUTED,  # cancelled/workload_owner (benign)
    "Cancelled by cluster operators": ErrorCategory.UNATTRIBUTED,  # cancelled/operator (benign)
    "Expected Fail": ErrorCategory.UNATTRIBUTED,  # expected_failure (benign)
    "Artificial": ErrorCategory.UNATTRIBUTED,  # expected_failure/artificial (benign)
    "Incomplete Info": ErrorCategory.INSUFFICIENT_TELEMETRY,  # insufficient_telemetry
}


def outcome_to_category(attribution_outcome: str) -> ErrorCategory:
    """Map a FACT verdict.attribution_outcome to a Heimdall ErrorCategory.

    Unknown outcomes default to UNATTRIBUTED and are logged (the vocabulary is open).

    Args:
        attribution_outcome: The FACT verdict outcome string.

    Returns:
        The corresponding Heimdall ``ErrorCategory``.
    """
    if attribution_outcome not in OUTCOME_TO_CATEGORY:
        logger.warning(f"Unmapped FACT attribution_outcome '{attribution_outcome}' -> UNATTRIBUTED default")
    return OUTCOME_TO_CATEGORY.get(attribution_outcome, ErrorCategory.UNATTRIBUTED)
