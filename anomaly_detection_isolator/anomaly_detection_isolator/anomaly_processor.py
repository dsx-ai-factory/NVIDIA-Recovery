# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from datetime import datetime

from recovery_libs.audit import AuditBuffer, NoOpAuditBuffer, ProducerEvent
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.job_manager.models import JobAnomaly, SlurmJobExecutionStatus
from recovery_libs.storage.node_events import (
    ErrorAttributionType,
    ExclusionType,
)
from recovery_libs.utils.nodeset.topology import group_by_rack

from anomaly_detection_isolator.attribution_client import AttributionClient
from anomaly_detection_isolator.result_builder import build_attribution_result
from anomaly_detection_isolator.settings.settings import Settings
from anomaly_detection_isolator.utils.logger import logger


class AnomalyProcessor:
    """Policy Engine: orchestrates attribution by calling attribution-adapter over HTTP.

    The Policy Engine no longer fetches logs/metrics or talks to FACT inline; it delegates
    attribution to attribution-adapter via :class:`AttributionClient`, reconstructs the
    :class:`AnomalyAttributionResult` from the HTTP response via :func:`build_attribution_result`,
    then applies the local isolation policy (sanitize, per-rack/cluster caps).
    """

    def __init__(
        self,
        settings: Settings,
        audit_event_buffer: AuditBuffer[ProducerEvent] = NoOpAuditBuffer[ProducerEvent](),
    ):
        """Initialize the AnomalyProcessor with required settings and components.

        Args:
            settings (Settings): Configuration settings for the processor
        """
        self.settings = settings
        self.attribution_client = AttributionClient(
            settings,
            audit_event_buffer=audit_event_buffer,
        )

    def process_anomaly_with_fast_signals(
        self,
        anomaly: JobAnomaly,
        job_status: SlurmJobExecutionStatus,
        run_checkers: bool,
    ) -> tuple[AnomalyAttributionResult, str | None]:
        """Process an anomaly via the FAST attribution pass over HTTP.

        Calls attribution-adapter for the FAST window, reconstructs the
        AnomalyAttributionResult from the response, applies the local isolation
        policy (sanitize), and returns the result together with the opaque
        attribution session token used to thread the SLOW pass.

        This NEVER returns ``None`` for the result (design §10 / D7): any failure
        or empty success yields an (empty) AnomalyAttributionResult so the caller's
        publish loop is unchanged and a last/only anomaly never strands the job.

        Args:
            anomaly (JobAnomaly): The anomaly to process
            job_status (SlurmJobExecutionStatus): Status information about the job
            run_checkers (bool): Unused; retained for service.py call-site parity.

        Returns:
            AnomalyAttributionResult: The attribution result (never None).
            str | None: The attribution session id to continue with slow signals,
                or None when no session is available.
        """
        req = self.attribution_client.build_fast_request(job_status, anomaly)
        heimdall_job_id = str(job_status.heimdall_job_id) if job_status.heimdall_job_id is not None else None
        resp = self.attribution_client.attribute(req, heimdall_job_id=heimdall_job_id)
        result = build_attribution_result(
            resp,
            job_status,
            anomaly.anomaly_type,
            anomaly.uuid or "",
            ErrorAttributionType.FAST,
            self.settings,
        )
        # _sanitize_attribution_result mutates and returns the same (non-None) object;
        # coalesce keeps the never-None guarantee while satisfying the type checker.
        result = self._sanitize_attribution_result(result, job_status) or result

        return result, (resp.attribution_session_id or None)

    def process_anomaly_with_slow_signals(
        self,
        job_status: SlurmJobExecutionStatus,
        fact_attribution_id: str,
        anomaly_type: str,
        anomaly_uuid: str,
        epilog_end_time: datetime,
    ) -> AnomalyAttributionResult | None:
        """Process slow signals for a job via the SLOW attribution pass over HTTP.

        Skips (returns ``None``) when there is no session token to continue or when
        the job has no end time (mirrors the legacy slow-skip behavior; service.py
        then publishes nothing). Otherwise calls attribution-adapter for the epilog
        window, reconstructs and sanitizes the result.

        Args:
            job_status (SlurmJobExecutionStatus): Status information about the job
            fact_attribution_id (str): Opaque attribution session token from the FAST pass.
            anomaly_type (str): The type of anomaly
            anomaly_uuid (str): The UUID of the anomaly
            epilog_end_time (datetime): The end time of the epilog query
        Returns:
            AnomalyAttributionResult: The sanitized attribution result, or None when skipped.
        """
        # Preserve the legacy slow-skip: no session to continue, or no job end time.
        if not fact_attribution_id or not job_status.end_time:
            logger.info(
                f"Skipping slow signals processing for job {job_status.job_id}: "
                f"fact_attribution_id={'set' if fact_attribution_id else 'empty'}, "
                f"end_time={'set' if job_status.end_time else 'missing'}"
            )
            return None

        req = self.attribution_client.build_slow_request(
            job_status, anomaly_type, anomaly_uuid, fact_attribution_id, epilog_end_time
        )
        heimdall_job_id = str(job_status.heimdall_job_id) if job_status.heimdall_job_id is not None else None
        resp = self.attribution_client.attribute(req, heimdall_job_id=heimdall_job_id)
        result = build_attribution_result(
            resp,
            job_status,
            anomaly_type,
            anomaly_uuid,
            ErrorAttributionType.SLOW,
            self.settings,
        )
        return self._sanitize_attribution_result(result, job_status)

    def _sanitize_attribution_result(
        self, attribution_result: AnomalyAttributionResult | None, job_status: SlurmJobExecutionStatus
    ) -> AnomalyAttributionResult | None:
        """Sanitize the attribution result.

        Args:
            attribution_result (AnomalyAttributionResult): The attribution result
            job_status (SlurmJobExecutionStatus): Status information about the job
        """

        if not attribution_result or not attribution_result.node_attributions:
            return attribution_result

        # Create a set of nodes that were allocated to the job; if the list is None, use an empty list.
        allocated_nodes = set(job_status.allocated_node_list or [])
        nodes_to_exclude = set()
        confirmed_nodes = set()
        temporary_nodes = set()

        # collect all nodes that have a non-NONE exclusion type
        for node_events in attribution_result.node_attributions.values():
            for node_event in node_events:
                # Store original exclusion types before sanitization
                node_event.original_exclusion_type = node_event.exclusion_type

                # If the node is not in the allocated nodes list, we can't exclude it, so we change the exclusion type to NONE
                if node_event.node not in allocated_nodes:
                    if node_event.exclusion_type != ExclusionType.NONE:
                        node_event.exclusion_type = ExclusionType.NONE

                    logger.info(
                        f"Node {node_event.node} is not in the allocated nodes list for job {job_status.job_id}, changing exclusion type to NONE"
                    )
                    continue
                # If the node is in the allocated nodes list, we can exclude it
                if node_event.exclusion_type == ExclusionType.CONFIRMED:
                    nodes_to_exclude.add(node_event.node)
                    confirmed_nodes.add(node_event.node)
                elif node_event.exclusion_type == ExclusionType.TEMPORARY:
                    nodes_to_exclude.add(node_event.node)
                    temporary_nodes.add(node_event.node)

        # Log warnings for inconsistent configuration
        if self.settings.rack_aware_isolation_enabled and self.settings.max_isolated_nodes_per_rack is None:
            logger.warning(
                f"Job {job_status.job_id}: rack_aware_isolation_enabled=True but max_isolated_nodes_per_rack is not set. "
                f"Falling back to per-cluster isolation limits. "
                f"Set max_isolated_nodes_per_rack to enable rack-aware isolation."
            )
        elif not self.settings.rack_aware_isolation_enabled and self.settings.max_isolated_nodes_per_rack is not None:
            logger.warning(
                f"Job {job_status.job_id}: max_isolated_nodes_per_rack={self.settings.max_isolated_nodes_per_rack} is set "
                f"but rack_aware_isolation_enabled=False. "
                f"The per-rack limit will be ignored. Set rack_aware_isolation_enabled=True to use rack-aware isolation."
            )

        # Determine if we should use per-rack limits or global limits
        use_per_rack_limits = (
            self.settings.rack_aware_isolation_enabled and self.settings.max_isolated_nodes_per_rack is not None
        )

        if use_per_rack_limits:
            logger.info(
                f"Using rack-aware isolation with per-rack limit of {self.settings.max_isolated_nodes_per_rack} "
                f"nodes for job {job_status.job_id}"
            )
            # Per-rack isolation limit enforcement
            self._apply_per_rack_isolation_limits(
                attribution_result, job_status, nodes_to_exclude, confirmed_nodes, temporary_nodes
            )
        else:
            logger.info(
                f"Using per-cluster isolation with limit of {self.settings.max_isolated_nodes} "
                f"nodes for job {job_status.job_id} "
                f"(rack_aware_isolation_enabled={self.settings.rack_aware_isolation_enabled})"
            )
            # Per-cluster isolation limit enforcement (cluster-wide limit)
            self._apply_per_cluster_isolation_limits(
                attribution_result, job_status, nodes_to_exclude, confirmed_nodes, temporary_nodes
            )

        return attribution_result

    def _apply_per_cluster_isolation_limits(
        self,
        attribution_result: AnomalyAttributionResult,
        job_status: SlurmJobExecutionStatus,
        nodes_to_exclude: set[str],
        confirmed_nodes: set[str],
        temporary_nodes: set[str],
    ) -> None:
        """Apply per-cluster isolation limits across the entire cluster.

        This applies a single cluster-wide limit regardless of rack topology.

        Args:
            attribution_result: The attribution result to modify
            job_status: Status information about the job
            nodes_to_exclude: Set of all nodes marked for exclusion
            confirmed_nodes: Set of nodes with confirmed exclusion
            temporary_nodes: Set of nodes with temporary exclusion
        """
        # If the number of nodes to exclude is greater than the preconfigured threshold
        if len(nodes_to_exclude) > self.settings.max_isolated_nodes:
            # If confirmed nodes are within threshold, keep them and change only temporary nodes to NONE
            # but only for nodes that are NOT in the confirmed set
            if len(confirmed_nodes) <= self.settings.max_isolated_nodes:
                # Calculate temporary-only nodes (nodes that have TEMPORARY but NOT CONFIRMED attributions)
                temporary_only_nodes = temporary_nodes - confirmed_nodes
                logger.info(
                    f"Total nodes ({len(nodes_to_exclude)}) exceeds cluster-wide threshold ({self.settings.max_isolated_nodes}), "
                    f"but confirmed nodes ({len(confirmed_nodes)}) are below threshold. "
                    f"Confirmed nodes: {confirmed_nodes}. "
                    f"Keeping confirmed nodes and changing temporary-only nodes to NONE: {temporary_only_nodes} for job {job_status.job_id}"
                )
                for node_events in attribution_result.node_attributions.values():
                    for node_event in node_events:
                        # Only change TEMPORARY exclusion type to NONE for nodes that are NOT in confirmed set
                        if (
                            node_event.exclusion_type == ExclusionType.TEMPORARY
                            and node_event.node in temporary_only_nodes
                        ):
                            node_event.exclusion_type = ExclusionType.NONE
            else:
                # Too many confirmed nodes, change all to NONE to prevent job starvation
                logger.info(
                    f"Total number of confirmed nodes ({len(confirmed_nodes)}) exceeds cluster-wide threshold ({self.settings.max_isolated_nodes}), "
                    f"changing all node exclusion types to NONE: {nodes_to_exclude} for job {job_status.job_id}"
                )
                for node_events in attribution_result.node_attributions.values():
                    for node_event in node_events:
                        node_event.exclusion_type = ExclusionType.NONE

    def _apply_per_rack_isolation_limits(
        self,
        attribution_result: AnomalyAttributionResult,
        job_status: SlurmJobExecutionStatus,
        nodes_to_exclude: set[str],
        confirmed_nodes: set[str],
        temporary_nodes: set[str],
    ) -> None:
        """Apply per-rack isolation limits to prevent job starvation on a per-rack basis.

        Also enforces max_racks_to_isolate limit to prevent taking out too many racks at once.

        If all nodes are grouped under "unknown" rack (no rack structure detected), this method
        will automatically fallback to per-cluster isolation limits.

        Args:
            attribution_result: The attribution result to modify
            job_status: Status information about the job
            nodes_to_exclude: Set of all nodes marked for exclusion
            confirmed_nodes: Set of nodes with confirmed exclusion
            temporary_nodes: Set of nodes with temporary exclusion
        """
        # Group nodes by rack using recovery_libs topology utilities
        rack_to_nodes_to_exclude_lists = group_by_rack(
            list(nodes_to_exclude), custom_pattern=self.settings.rack_extraction_pattern, include_unknown=True
        )

        # Fallback to per-cluster limits if no rack structure detected
        # (all nodes grouped under "unknown" rack or only one rack exists)
        if len(rack_to_nodes_to_exclude_lists) == 1 and "unknown" in rack_to_nodes_to_exclude_lists:
            logger.warning(
                f"Job {job_status.job_id}: rack_aware_isolation_enabled is true but no rack structure detected "
                f"(all {len(nodes_to_exclude)} nodes grouped as 'unknown' rack). "
                f"Falling back to per-cluster isolation limits."
            )
            self._apply_per_cluster_isolation_limits(
                attribution_result, job_status, nodes_to_exclude, confirmed_nodes, temporary_nodes
            )
            return

        # Convert lists to sets and build confirmed/temporary mappings
        rack_to_nodes_to_exclude: dict[str, set[str]] = {}
        rack_to_confirmed: dict[str, set[str]] = {}
        rack_to_temporary: dict[str, set[str]] = {}

        for rack, nodes in rack_to_nodes_to_exclude_lists.items():
            rack_nodes_set = set(nodes)
            rack_to_nodes_to_exclude[rack] = rack_nodes_set
            rack_to_confirmed[rack] = rack_nodes_set & confirmed_nodes
            rack_to_temporary[rack] = rack_nodes_set & temporary_nodes

        # Check if we're exceeding max_racks_to_isolate limit
        max_racks = self.settings.max_racks_to_isolate
        num_racks_with_isolations = len(rack_to_nodes_to_exclude)

        # If we exceed the rack limit, prioritize racks with more confirmed nodes
        # Top N racks get full isolation (no per-rack limits), others get per-rack limits applied
        racks_to_keep_fully: set[str] = set()
        if max_racks is not None and num_racks_with_isolations > max_racks:
            # Sort racks by: 1) number of confirmed nodes (desc), 2) total nodes (desc), 3) rack name (asc for stability)
            sorted_racks = sorted(
                rack_to_nodes_to_exclude.keys(),
                key=lambda r: (len(rack_to_confirmed[r]), len(rack_to_nodes_to_exclude[r]), -hash(r)),
                reverse=True,
            )

            # Keep top N racks fully isolated (don't apply per-rack limits)
            racks_to_keep_fully = set(sorted_racks[:max_racks])
            # Apply per-rack limits to excess racks
            racks_to_apply_limits = set(sorted_racks[max_racks:])

            logger.warning(
                f"Job {job_status.job_id}: Number of racks with isolations ({num_racks_with_isolations}) exceeds "
                f"max_racks_to_isolate limit ({max_racks}). "
                f"Keeping top {max_racks} racks fully isolated: {racks_to_keep_fully}. "
                f"Applying per-rack limits to excess racks: {racks_to_apply_limits}"
            )
        else:
            # All racks are within the max limit, apply per-rack limits to all
            racks_to_apply_limits = set(rack_to_nodes_to_exclude.keys())

        # Track which nodes should have their exclusion type changed to NONE
        nodes_to_change_to_none = set()

        # Process each rack independently
        for rack, rack_nodes_to_exclude in rack_to_nodes_to_exclude.items():
            # If this rack is in the top N (to keep fully), skip per-rack limit checks
            if rack in racks_to_keep_fully:
                logger.info(
                    f"Rack {rack}: Keeping all {len(rack_nodes_to_exclude)} nodes isolated "
                    f"(top priority rack, no per-rack limits applied) for job {job_status.job_id}"
                )
                continue

            rack_confirmed = rack_to_confirmed[rack]
            rack_temporary = rack_to_temporary[rack]
            rack_threshold = self.settings.max_isolated_nodes_per_rack

            # If the number of nodes to exclude in this rack exceeds the per-rack threshold
            if rack_threshold is not None and len(rack_nodes_to_exclude) > rack_threshold:
                # If confirmed nodes are within threshold, keep them and change only temporary nodes to NONE
                if len(rack_confirmed) <= rack_threshold:
                    # Calculate temporary-only nodes for this rack
                    rack_temporary_only = rack_temporary - rack_confirmed
                    logger.info(
                        f"Rack {rack}: Total nodes ({len(rack_nodes_to_exclude)}) exceeds per-rack threshold ({rack_threshold}), "
                        f"but confirmed nodes ({len(rack_confirmed)}) are below threshold. "
                        f"Confirmed nodes: {rack_confirmed}. "
                        f"Keeping confirmed nodes and changing temporary-only nodes to NONE: {rack_temporary_only} for job {job_status.job_id}"
                    )
                    nodes_to_change_to_none.update(rack_temporary_only)
                else:
                    # Too many confirmed nodes in this rack - keep up to threshold, change excess to NONE
                    # Sort confirmed nodes for deterministic behavior (alphabetically)
                    sorted_confirmed = sorted(rack_confirmed)
                    nodes_to_keep = set(sorted_confirmed[:rack_threshold])
                    nodes_to_drop = rack_nodes_to_exclude - nodes_to_keep

                    logger.warning(
                        f"Rack {rack}: Total number of confirmed nodes ({len(rack_confirmed)}) exceeds per-rack threshold ({rack_threshold}). "
                        f"Keeping {rack_threshold} confirmed nodes: {nodes_to_keep}. "
                        f"Changing {len(nodes_to_drop)} excess node exclusion types to NONE: {nodes_to_drop} for job {job_status.job_id}"
                    )
                    nodes_to_change_to_none.update(nodes_to_drop)

        # Apply the changes to the attribution result
        if nodes_to_change_to_none:
            for node_events in attribution_result.node_attributions.values():
                for node_event in node_events:
                    if node_event.node in nodes_to_change_to_none:
                        node_event.exclusion_type = ExclusionType.NONE
