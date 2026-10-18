# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from recovery_libs.storage.node_events import ExclusionType

from anomaly_detection_isolator.settings.settings import Settings
from anomaly_detection_isolator.utils.logger import logger


def derive_exclusion(recommended_action: str | None, symptom: str, settings: Settings) -> ExclusionType:
    """Derive the Heimdall exclusion type from a FACT recommended action and symptom.

    Lifted verbatim from the old fact_client convert_fact_recommendation_to_heimdall_exclusion /
    convert_fact_attribution_description_to_heimdall_exclusion, now reading the fact_*_errors
    lists from settings.

    Args:
        recommended_action: The FACT recommendation value (RecommendationEnum.DRAIN.value == "drain");
            case-normalized, short-circuits to CONFIRMED.
        symptom: The FACT symptom string matched against the configured error lists.
        settings: Settings carrying the fact_isolated/ambiguous/non_isolated_errors(+_env) lists.

    Returns:
        ExclusionType: CONFIRMED / TEMPORARY / NONE per the configured policy.
    """
    # RecommendationEnum.DRAIN.value == "drain"; response_builder emits recommendation.value,
    # so normalize case here to preserve today's DRAIN -> CONFIRMED short-circuit (byte-identity).
    if (recommended_action or "").upper() == "DRAIN":
        return ExclusionType.CONFIRMED

    logger.info(f"Deriving exclusion from symptom: {symptom}")

    # These are the errors for which we should isolate the node for the job.
    for error in (settings.fact_isolated_errors if settings.fact_isolated_errors else []) + (
        settings.fact_isolated_errors_env if settings.fact_isolated_errors_env else []
    ):
        if error in symptom:
            logger.info(f"Isolated error found: {error}")
            return ExclusionType.CONFIRMED

    # These are the errors for which we should temporarily isolate the node for the job to shuffle ranks.
    # if the job succeeds in the second run, it will confirm the isolation and node will be isolated from further runs.
    # if the job fails again, the node is brought back for further runs.
    for error in (settings.fact_ambiguous_errors if settings.fact_ambiguous_errors else []) + (
        settings.fact_ambiguous_errors_env if settings.fact_ambiguous_errors_env else []
    ):
        if error in symptom:
            logger.info(f"Ambiguous error found: {error}")
            return ExclusionType.TEMPORARY

    # These are the errors for which we should not isolate the node for the job.
    for error in (settings.fact_non_isolated_errors if settings.fact_non_isolated_errors else []) + (
        settings.fact_non_isolated_errors_env if settings.fact_non_isolated_errors_env else []
    ):
        if error in symptom:
            logger.info(f"Non-isolated error found: {error}")
            return ExclusionType.NONE

    # Default to Temporary if no match is found
    logger.info(f"No match found for symptom: {symptom}, returning TEMPORARY")
    return ExclusionType.TEMPORARY
