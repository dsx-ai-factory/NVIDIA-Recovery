# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""attribution-adapter orchestration.

Owns telemetry acquisition (configured log backend/Prometheus) and the FACT 3-stage attribution
session. The FAST/SLOW fetch-and-supply loops are ported verbatim (behavior
preserving) from the legacy ``AnomalyProcessor._process_fast_signals_with_fact`` /
``_process_slow_signals_with_fact``, minus the Policy-side
exclusion/result construction. The trailing ``_convert_fact_attribution_to_response``
call is replaced with ``response_builder.build_response``.

The public ``attribute`` entry adapts a documented ``WorkloadAttributionRequest``
into the ``SlurmJobExecutionStatus`` / ``JobAnomaly`` shapes the FACT session and
query getters expect.
"""

import json
import os
import time
from datetime import UTC, datetime, timedelta
from http import HTTPStatus

from nvidia_attribution_client.models.attribution_response_v2 import AttributionResponseV2
from nvidia_attribution_client.models.source import Source
from prometheus_client import Counter, Histogram
from recovery_libs.log_query import BackendName, QueryFailure, QueryFailureKind, QueryOrder
from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import AttributionErrorType, AttributionPhase
from recovery_libs.schema.attribution_interface.request import WorkloadAttributionRequest
from recovery_libs.schema.attribution_interface.response import AttributionError, WorkloadAttributionResponse
from recovery_libs.schema.job_manager.models import JobAnomaly, SlurmJobExecutionStatus
from requests import RequestException

from attribution_adapter.fact_client import FACTClient
from attribution_adapter.log_query import create_attribution_log_fetcher
from attribution_adapter.metrics_fetcher import ADIMetricsFetcher
from attribution_adapter.response_builder import (
    SERVICE_IDENTIFIER,
    _service_version,
    build_response,
)
from attribution_adapter.settings.settings import Settings
from attribution_adapter.utils.logger import logger

# ---- Histograms/counters ported verbatim from anomaly_processor.py (adi_ prefix retained per spec §12.4) ----
FAST_SIGNALS_PROCESS_DURATION_FACT = Histogram(
    "adi_anomaly_processor_fast_signals_duration_fact",
    "Time taken to process an anomaly with fast signals. This includes log-query time, FACT attributor creation time, FACT observations submission time, and FACT attribution retrieval time",
)

SYSLOGS_PROCESS_DURATION_FACT = Histogram(
    "adi_anomaly_processor_syslogs_duration_fact",
    "Time taken to fetch syslogs. This includes syslogs log-query time, FACT attributor creation time, FACT observations submission time",
)

APPLOGS_PROCESS_DURATION_FACT = Histogram(
    "adi_anomaly_processor_applogs_duration_fact",
    "Time taken to fetch applogs. This includes applogs log-query time, FACT attributor creation time, FACT observations submission time",
)

SLOW_SIGNALS_PROCESS_DURATION_FACT = Histogram(
    "adi_anomaly_processor_slow_signals_duration_fact",
    "Time taken to process an anomaly with slow signals. This includes log-query time, FACT observations submission time, and FACT attribution retrieval time",
)

LOG_FETCH_TIMEOUT_TOTAL = Counter(
    "adi_log_fetch_timeout_total", "Log fetch timeouts", ["log_type", "job_id", "job_attempt_index"]
)

LOG_QUERY_LINES_RETURNED_TOTAL = Counter(
    "adi_log_query_lines_returned_total",
    "Number of log lines supplied from each selected backend query",
    ["backend", "log_type", "job_id", "job_attempt_index"],
)


def _record_log_lines_metrics(
    log_streams,
    backend: str,
    log_type: str,
    job_id: str,
    job_attempt_index: int,
) -> None:
    """Record the number of log lines supplied from a backend query."""
    try:
        log_data = json.loads(log_streams) if isinstance(log_streams, str) else log_streams
        num_lines = sum(len(stream.get("values", [])) for stream in log_data)
        LOG_QUERY_LINES_RETURNED_TOTAL.labels(
            backend=backend,
            log_type=log_type,
            job_id=job_id,
            job_attempt_index=job_attempt_index,
        ).inc(num_lines)
    except Exception as e:
        logger.warning(
            f"Failed to record log-query lines metric for {log_type}, job {job_id}",
            exc_info=True,
            extra={"error": str(e)},
        )


class NoErrorsFoundInLogStream(Exception):
    pass


class AttributionService:
    """Owns telemetry acquisition and the FACT 3-stage attribution session."""

    def __init__(self, settings: Settings):
        """Initialize the AttributionService with required settings and components.

        Args:
            settings (Settings): Configuration settings for the service.
        """
        self.settings = settings
        self._closed = False
        # TODO: Make this configurable
        self.max_entries_per_request_syslogs = 1000
        self.max_entries_per_request_applogs = 700
        self.log_fetcher = create_attribution_log_fetcher(
            settings=self.settings,
            max_entries_per_request=self.max_entries_per_request_applogs,
        )
        try:
            self.fact_client = FACTClient(settings=self.settings)

            self._fetch_prometheus_auth()
            self.metrics_fetcher = ADIMetricsFetcher(
                settings=self.settings,
                auth_token=self.prometheus_auth_token,
                cert_file=self.cert_file,
                key_file=self.key_file,
                ca_cert=self.ca_cert,
            )

            # Log fetch optimization settings
            self.log_fetch_optimization_enabled = settings.log_fetch_optimization_enabled
            self.log_fetch_direction = (
                settings.log_fetch_direction if self.log_fetch_optimization_enabled else QueryOrder.FORWARD
            )
            self.log_fetch_timeout_syslogs = (
                settings.log_fetch_timeout_syslogs_seconds if self.log_fetch_optimization_enabled else None
            )
            self.log_fetch_timeout_applogs = (
                settings.log_fetch_timeout_applogs_seconds if self.log_fetch_optimization_enabled else None
            )
            self.log_fetch_timeout_slow_signals = (
                settings.log_fetch_timeout_slow_signals_seconds if self.log_fetch_optimization_enabled else None
            )
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        """Close the process-owned log-query clients once."""
        if self._closed:
            return
        self._closed = True
        self.log_fetcher.close()

    @staticmethod
    def _log_fetch_deadline(timeout_seconds: int | None) -> float | None:
        """Return one monotonic deadline shared by every page in a log fetch."""
        if timeout_seconds is None:
            return None
        return time.monotonic() + timeout_seconds

    @staticmethod
    def _remaining_log_fetch_timeout(deadline: float | None) -> float | None:
        """Return the remaining page/retry budget, if the fetch is bounded."""
        if deadline is None:
            return None
        return deadline - time.monotonic()

    @staticmethod
    def _record_log_fetch_timeout(log_type: str, job_status: SlurmJobExecutionStatus) -> None:
        """Record an exhausted configured log-fetch budget."""
        LOG_FETCH_TIMEOUT_TOTAL.labels(
            log_type=log_type,
            job_id=job_status.job_id,
            job_attempt_index=job_status.job_attempt_index,
        ).inc()

    def _fetch_prometheus_auth(self):
        """Fetch authentication token for Prometheus from secrets file.

        Ported from the legacy ``AnomalyProcessor._fetch_prometheus_auth``.
        """
        logger.info(
            f"Prometheus URL: {self.settings.prometheus_url}, Prometheus Hostname Label: {self.settings.prometheus_hostname_label}"
        )
        if os.environ.get("PROMETHEUS_API_TOKEN_FILE"):
            with open(os.environ.get("PROMETHEUS_API_TOKEN_FILE"), "r") as token:
                self.prometheus_auth_token = token.read().strip()
        else:
            logger.warning("PROMETHEUS_API_TOKEN_FILE is not set")
            self.prometheus_auth_token = None

        self.cert_file = os.environ.get("PROMETHEUS_CERT_FILE")
        self.key_file = os.environ.get("PROMETHEUS_KEY_FILE")
        self.ca_cert = os.environ.get("PROMETHEUS_CA_CERT")

    def _next_pagination_boundary(
        self, continuation_timestamp: float, direction: str, start_time: datetime, end_time: datetime
    ) -> tuple[datetime, datetime]:
        """Advance the pagination window based on the continuation timestamp and direction.

        For backward: retreats end_time. For forward: advances start_time.
        The selected backend adapter handles boundary convergence and stall detection internally.

        Returns:
            tuple of (start_time, end_time) with the appropriate boundary updated.
        """
        boundary = datetime.fromtimestamp(timestamp=continuation_timestamp, tz=UTC)
        if direction is QueryOrder.BACKWARD:
            return start_time, boundary
        else:
            return boundary, end_time

    def _next_log_query_page(
        self,
        continuation: str | float,
        direction: str,
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[datetime, datetime, str | None]:
        """Advance neutral opaque pagination or the legacy Lepton Loki boundary."""
        if isinstance(continuation, str):
            return start_time, end_time, continuation
        next_start, next_end = self._next_pagination_boundary(continuation, direction, start_time, end_time)
        return next_start, next_end, None

    # ------------------------------------------------------------------ #
    # Request -> job/anomaly adapter
    # ------------------------------------------------------------------ #
    def _job_status_from_request(self, request: WorkloadAttributionRequest) -> SlurmJobExecutionStatus:
        """Adapt a WorkloadAttributionRequest into the SlurmJobExecutionStatus the FACT session expects.

        Args:
            request (WorkloadAttributionRequest): The documented attribution request.

        Returns:
            SlurmJobExecutionStatus: The minimal job-status shape needed by ``create_request`` and the
            query getters, sourced from ``request.scope`` / ``workload_context`` / ``user_id``.
        """
        wc = request.workload_context
        return SlurmJobExecutionStatus(
            job_id=request.workload_id,
            job_attempt_index=wc.attempt_index,
            workflow_id=wc.workflow_id,
            start_time=request.scope.start_time,
            end_time=request.scope.end_time,
            allocated_node_list=request.scope.nodes,
            command=wc.command,
            cluster=wc.cluster,
            job_name=wc.workload_name,
            state=wc.state,
            node_count=wc.node_count,
            total_gpu_count=wc.total_gpu_count,
            user=request.user_id,
        )

    def _anomaly_from_request(self, request: WorkloadAttributionRequest) -> JobAnomaly:
        """Adapt a WorkloadAttributionRequest's failure context into a JobAnomaly.

        Args:
            request (WorkloadAttributionRequest): The documented attribution request.

        Returns:
            JobAnomaly: The minimal anomaly shape needed by ``create_request`` (anomaly_type / timestamp /
            uuid), sourced from ``failure_context`` / ``workload_context.anomaly_uuid``.
        """
        return JobAnomaly(
            anomaly_type=request.failure_context.detection_signal,
            timestamp=request.failure_context.detected_at,
            uuid=request.workload_context.anomaly_uuid,
        )

    # ------------------------------------------------------------------ #
    # Public entry
    # ------------------------------------------------------------------ #
    def attribute(self, request: WorkloadAttributionRequest) -> WorkloadAttributionResponse:
        """Run attribution for a single FAST or SLOW pass.

        Adapts the request into a job-status/anomaly, branches on phase, runs the ported FACT
        fetch/supply loops, retrieves the FACT attribution, and projects it onto a
        ``WorkloadAttributionResponse`` via ``response_builder.build_response``.

        Any top-level FACT/log-query/Prometheus failure is caught and turned into a SERVICE_ERROR
        response (per-stage syslog/applog/metric errors are caught-and-continue inside the
        ported loops, matching legacy behavior).

        Args:
            request (WorkloadAttributionRequest): The documented attribution request.

        Returns:
            WorkloadAttributionResponse: The projected attribution result, or a SERVICE_ERROR
            response on a top-level failure.
        """
        attribution_id = request.continue_session or ""
        try:
            job_status = self._job_status_from_request(request)

            if request.phase == AttributionPhase.FAST:
                anomaly = self._anomaly_from_request(request)
                start = time.time()
                fact_attribution, attribution_id = self._process_fast_signals_with_fact(
                    anomaly, job_status, run_checkers=True
                )
                FAST_SIGNALS_PROCESS_DURATION_FACT.observe(float(time.time() - start))
                logger.info(
                    f"Fast signals processing completed for job {job_status.job_id}/{job_status.job_attempt_index}"
                )
            else:
                # Mirrors legacy ``_process_slow_signals_with_fact``'s ``return None`` abort: when the
                # epilog window is absent (scope.start_time or scope.end_time is None), skip the whole
                # slow pass (no prometheus, no get_attribution, no fabricated attribution) and return an
                # empty NO_ATTRIBUTION_AVAILABLE response.
                if request.scope.start_time is None or request.scope.end_time is None:
                    logger.error(
                        f"Job {job_status.job_id} has no end time in Job manager response, skipping slow signals processing! This is highly UNUSUAL and should be investigated."
                    )
                    return WorkloadAttributionResponse(
                        schema_version=ATTRIBUTION_SCHEMA_VERSION,
                        service_identifier=SERVICE_IDENTIFIER,
                        service_version=_service_version(),
                        request_id=request.workload_id,
                        attribution_session_id=attribution_id,
                        attributions=[],
                        error=AttributionError(
                            type=AttributionErrorType.NO_ATTRIBUTION_AVAILABLE,
                            message="slow signals skipped: workload has no end_time",
                        ),
                    )
                start = time.time()
                fact_attribution = self._process_slow_signals_with_fact(
                    job_status,
                    attribution_id,
                    epilog_start_time=request.scope.start_time,
                    epilog_end_time=request.scope.end_time,
                )
                SLOW_SIGNALS_PROCESS_DURATION_FACT.observe(float(time.time() - start))

            return build_response(fact_attribution, request, attribution_session_id=attribution_id)
        except Exception as e:
            logger.exception(
                f"Error processing attribution for workload {request.workload_id}",
                extra={"workload_id": request.workload_id, "error": str(e)},
            )
            return WorkloadAttributionResponse(
                schema_version=ATTRIBUTION_SCHEMA_VERSION,
                service_identifier=SERVICE_IDENTIFIER,
                service_version=_service_version(),
                request_id=request.workload_id,
                attribution_session_id=attribution_id,
                attributions=[],
                error=AttributionError(type=AttributionErrorType.SERVICE_ERROR, message=str(e)),
            )

    # ------------------------------------------------------------------ #
    # FAST signals (ported verbatim from AnomalyProcessor)
    # ------------------------------------------------------------------ #
    def _process_fast_signals_with_fact(
        self,
        anomaly: JobAnomaly,
        job_status: SlurmJobExecutionStatus,
        run_checkers: bool,
    ) -> tuple[AttributionResponseV2, str]:
        """Process an anomaly using the FACT service (fast signals).

        Args:
            anomaly (JobAnomaly): The anomaly to process
            job_status (SlurmJobExecutionStatus): Status information about the job
            run_checkers (bool): Whether to run additional checkers (dead in the ported body; kept for
                signature parity with the legacy AnomalyProcessor)

        Returns:
            AttributionResponseV2: The FACT attribution result
            str: The attribution ID to continue with slow signals
        """
        attribution_id = self.fact_client.create_request(job_status, anomaly)

        # fetch sys logs and supply to FACT
        try:
            syslog_queries = self._get_log_queries_syslogs(job_status)
            metrics_start_time = time.time()
            for query, type in syslog_queries.items():
                # Subtracting 5 minutes from the start time to account for the time it takes to sync job status to JM
                job_start = job_status.start_time or anomaly.timestamp
                job_end = job_status.end_time or anomaly.timestamp
                start_time = max(job_start - timedelta(minutes=5), anomaly.timestamp - timedelta(minutes=30))
                end_time = job_end + timedelta(minutes=5)
                max_entries = self.max_entries_per_request_syslogs
                page_token = None

                fetch_deadline = self._log_fetch_deadline(self.log_fetch_timeout_syslogs)
                while True:
                    remaining_timeout = self._remaining_log_fetch_timeout(fetch_deadline)
                    if remaining_timeout is not None and remaining_timeout <= 0:
                        logger.warning(
                            f"Syslog fetch timeout ({self.log_fetch_timeout_syslogs}s) for job {job_status.job_id}"
                        )
                        self._record_log_fetch_timeout("syslogs", job_status)
                        break

                    try:
                        log_streams, continuation_timestamp = self.log_fetcher.get_sys_telemetry_logs(
                            job_id="",
                            log_query=query,
                            end_time=end_time,
                            start_time=start_time,
                            limit=max_entries,
                            direction=self.log_fetch_direction,
                            page_token=page_token,
                            deadline_monotonic=fetch_deadline,
                        )
                    except QueryFailure as error:
                        if error.kind == QueryFailureKind.TIMEOUT:
                            self._record_log_fetch_timeout("syslogs", job_status)
                        raise

                    if log_streams is None:
                        raise NoErrorsFoundInLogStream(f"empty syslog stream for job id: {job_status.job_id}")

                    _record_log_lines_metrics(
                        log_streams,
                        self.settings.log_query_backend,
                        "syslogs",
                        job_status.job_id,
                        job_status.job_attempt_index,
                    )

                    logger.debug(
                        f"Supplying logs to FACT for job id: {job_status.job_id}, log type: {type}, size: {len(log_streams)}, continuation_timestamp: {continuation_timestamp}"
                    )

                    try:
                        self.fact_client.supply_logs(attribution_id, log_streams, type, start_time, end_time)
                    except RequestException as e:
                        if e.response is not None and e.response.status_code == HTTPStatus.REQUEST_ENTITY_TOO_LARGE:
                            max_entries = int(max_entries * 0.9)
                            logger.info(f"Request size exceeded, reducing max entries to {max_entries}")
                            continue
                        else:
                            logger.error(
                                f"Error submitting logs to FACT, status {e.response.status_code if e.response is not None else 'unknown'}: {e.response.text if e.response is not None else ''}"
                            )
                            raise

                    if continuation_timestamp is None:
                        break
                    start_time, end_time, page_token = self._next_log_query_page(
                        continuation_timestamp, self.log_fetch_direction, start_time, end_time
                    )
            metrics_end_time = time.time()
            syslogs_duration = float(metrics_end_time - metrics_start_time)
            SYSLOGS_PROCESS_DURATION_FACT.observe(syslogs_duration)
            logger.info(
                f"Syslogs processing completed for job {job_status.job_id}/{job_status.job_attempt_index} in {syslogs_duration:.2f}s"
            )
        except Exception as e:
            logger.exception(
                f"Error processing syslogs for job id: {job_status.job_id}",
                extra={"job_id": job_status.job_id, "error": str(e)},
            )

        # fetch app logs and supply to FACT
        try:
            application_log_queries = self._get_log_queries_app_logs(job_status)
            metrics_start_time = time.time()
            for query, type in application_log_queries.items():
                # Subtracting 5 minutes from the start time to account for the time it takes to sync job status to JM
                job_start_ap = job_status.start_time or anomaly.timestamp
                start_time = max(job_start_ap - timedelta(minutes=5), anomaly.timestamp - timedelta(minutes=30))
                end_time = anomaly.timestamp + timedelta(minutes=5)
                max_entries = self.max_entries_per_request_applogs
                page_token = None

                fetch_deadline = self._log_fetch_deadline(self.log_fetch_timeout_applogs)
                while True:
                    remaining_timeout = self._remaining_log_fetch_timeout(fetch_deadline)
                    if remaining_timeout is not None and remaining_timeout <= 0:
                        logger.warning(
                            f"Applog fetch timeout ({self.log_fetch_timeout_applogs}s) for job {job_status.job_id}"
                        )
                        self._record_log_fetch_timeout("applogs", job_status)
                        break

                    try:
                        log_streams, continuation_timestamp = self.log_fetcher.get_app_telemetry_logs(
                            job_id="",
                            log_query=query,
                            end_time=end_time,
                            start_time=start_time,
                            limit=max_entries,
                            direction=self.log_fetch_direction,
                            page_token=page_token,
                            deadline_monotonic=fetch_deadline,
                        )
                    except QueryFailure as error:
                        if error.kind == QueryFailureKind.TIMEOUT:
                            self._record_log_fetch_timeout("applogs", job_status)
                        raise
                    if log_streams is None:
                        raise NoErrorsFoundInLogStream(f"empty applog stream for job id: {job_status.job_id}")

                    _record_log_lines_metrics(
                        log_streams,
                        self.settings.log_query_backend,
                        "applogs",
                        job_status.job_id,
                        job_status.job_attempt_index,
                    )

                    # TODO: Add performance metrics for FACT supply logs
                    logger.info(
                        f"Supplying logs to FACT for job id: {job_status.job_id}, log type: {type}, size: {len(log_streams)}, continuation_timestamp: {continuation_timestamp}"
                    )
                    try:
                        self.fact_client.supply_logs(attribution_id, log_streams, type, start_time, end_time)
                    except RequestException as e:
                        if e.response is not None and e.response.status_code == HTTPStatus.REQUEST_ENTITY_TOO_LARGE:
                            max_entries = int(max_entries * 0.9)
                            logger.info(f"Request size exceeded, reducing max entries to {max_entries}")
                            continue
                        else:
                            logger.error(
                                f"Error submitting logs to FACT, status {e.response.status_code if e.response is not None else 'unknown'}: {e.response.text if e.response is not None else ''}"
                            )
                            raise

                    if continuation_timestamp is None:
                        break
                    start_time, end_time, page_token = self._next_log_query_page(
                        continuation_timestamp, self.log_fetch_direction, start_time, end_time
                    )
            metrics_end_time = time.time()
            applogs_duration = float(metrics_end_time - metrics_start_time)
            APPLOGS_PROCESS_DURATION_FACT.observe(applogs_duration)
            logger.info(
                f"Applogs processing completed for job {job_status.job_id}/{job_status.job_attempt_index} in {applogs_duration:.2f}s"
            )
        except Exception as e:
            logger.exception(
                f"Error processing applogs for job id: {job_status.job_id}",
                extra={"job_id": job_status.job_id, "error": str(e)},
            )

        # fetch prometheus metrics and supply to FACT
        try:
            prometheus_queries = self._get_prom_queries(job_status)
            for query, type in prometheus_queries.items():
                # Subtracting 5 minutes from the start time to account for the time it takes to sync job status to JM
                job_start_pm = job_status.start_time or anomaly.timestamp
                start_time = max(job_start_pm - timedelta(minutes=5), anomaly.timestamp - timedelta(minutes=30))
                end_time = anomaly.timestamp + timedelta(minutes=5)

                # TODO: Add performance metrics for FACT get metrics
                logger.debug(
                    f"Fetching metrics for job id: {job_status.job_id}, query: {query}, start time: {start_time}, end time: {end_time}"
                )
                metrics = self.metrics_fetcher.get_metrics(query, start_time, end_time)
                logger.debug(
                    f"Supplying metrics to FACT for job id: {job_status.job_id}, query: {query}, start time: {start_time}, end time: {end_time}"
                )
                if metrics is not None:
                    self.fact_client.supply_metrics(attribution_id, metrics, type)
        except Exception as e:
            logger.exception(
                f"Error processing prometheus metrics for job id: {job_status.job_id}",
                extra={"job_id": job_status.job_id, "error": str(e)},
            )

        # get attribution from FACT
        # TODO: Add performance metrics for FACT get attribution
        fact_attribution = self.fact_client.get_attribution(attribution_id)

        logger.debug(
            f"Received attribution for job id: {job_status.job_id}, symptoms: {len(fact_attribution.symptoms)}"
        )

        return fact_attribution, attribution_id

    # ------------------------------------------------------------------ #
    # SLOW signals (ported verbatim from AnomalyProcessor)
    # ------------------------------------------------------------------ #
    def _process_slow_signals_with_fact(
        self,
        job_status: SlurmJobExecutionStatus,
        fact_attribution_id: str,
        epilog_start_time: datetime | None,
        epilog_end_time: datetime | None,
    ) -> AttributionResponseV2:
        """Process slow signals for a job using the FACT service.

        Args:
            job_status (SlurmJobExecutionStatus): Status information about the job
            fact_attribution_id (str): The ID of the FACT attribution
            epilog_start_time (datetime | None): The start of the epilog query window (request.scope.start_time,
                i.e. the real job end time).
            epilog_end_time (datetime | None): The end of the epilog query window (request.scope.end_time,
                i.e. the epilog_end_time, distinct and later than epilog_start_time).

        Returns:
            AttributionResponseV2: The FACT attribution result
        """
        if not fact_attribution_id:
            logger.error(
                f"No FACT attribution ID provided for job id: {job_status.job_id}, skipping slow signals processing!"
            )
            raise ValueError("No FACT attribution ID provided for slow signals processing")

        try:
            slow_signal_log_queries = self._get_slow_signals_log_queries(job_status=job_status)
            for query, type in slow_signal_log_queries.items():
                # The caller (``attribute``) already aborts the SLOW pass when the epilog window is
                # absent, so this is a defensive guard mirroring legacy's in-loop end_time check.
                if epilog_start_time is None or epilog_end_time is None:
                    logger.error(
                        f"Job {job_status.job_id} has no end time in Job manager response, skipping slow signals processing! This is highly UNUSUAL and should be investigated."
                    )
                    break

                # Epilog events are written AFTER job end; query the explicit epilog window
                # (scope.start_time .. scope.end_time) so the window is never degenerate.
                start_time = epilog_start_time
                end_time = epilog_end_time
                page_token = None

                fetch_deadline = self._log_fetch_deadline(self.log_fetch_timeout_slow_signals)
                while True:
                    remaining_timeout = self._remaining_log_fetch_timeout(fetch_deadline)
                    if remaining_timeout is not None and remaining_timeout <= 0:
                        logger.warning(
                            f"Slow signals fetch timeout ({self.log_fetch_timeout_slow_signals}s) for job {job_status.job_id}"
                        )
                        self._record_log_fetch_timeout("slow_signals", job_status)
                        break

                    try:
                        log_streams, continuation_timestamp = self.log_fetcher.get_sys_telemetry_logs(
                            job_id="",
                            log_query=query,
                            end_time=end_time,
                            start_time=start_time,
                            limit=self.max_entries_per_request_syslogs,
                            direction=self.log_fetch_direction,
                            page_token=page_token,
                            deadline_monotonic=fetch_deadline,
                        )
                    except QueryFailure as error:
                        if error.kind == QueryFailureKind.TIMEOUT:
                            self._record_log_fetch_timeout("slow_signals", job_status)
                        raise
                    if log_streams is None:
                        raise NoErrorsFoundInLogStream(f"empty syslog stream for job id: {job_status.job_id}")

                    _record_log_lines_metrics(
                        log_streams,
                        self.settings.log_query_backend,
                        "slow_signals",
                        job_status.job_id,
                        job_status.job_attempt_index,
                    )

                    self.fact_client.supply_logs(fact_attribution_id, log_streams, type, start_time, end_time)

                    if continuation_timestamp is None:
                        break
                    start_time, end_time, page_token = self._next_log_query_page(
                        continuation_timestamp, self.log_fetch_direction, start_time, end_time
                    )
        except Exception as e:
            logger.exception(
                f"Error processing slow signals logs for job id: {job_status.job_id}",
                extra={"job_id": job_status.job_id, "error": str(e)},
            )

        try:
            prom_slow_signals_queries = self._get_slow_signals_prom_queries()
            for query, type in prom_slow_signals_queries.items():
                ss_end_time: datetime = job_status.end_time or datetime.now(tz=UTC)
                ss_start_time: datetime = max(job_status.start_time or ss_end_time, ss_end_time - timedelta(minutes=30))

                metrics = self.metrics_fetcher.get_metrics(query, ss_start_time, ss_end_time)
                if metrics is not None:
                    self.fact_client.supply_metrics(fact_attribution_id, metrics, type)
        except Exception as e:
            logger.exception(
                f"Error processing slow signals metrics for job id: {job_status.job_id}",
                extra={"job_id": job_status.job_id, "error": str(e)},
            )

        fact_attribution = self.fact_client.get_attribution(fact_attribution_id)

        return fact_attribution

    # ------------------------------------------------------------------ #
    # Query getters (ported verbatim from AnomalyProcessor)
    # ------------------------------------------------------------------ #
    def _get_log_queries_syslogs(self, job_status: SlurmJobExecutionStatus) -> dict[str, Source]:
        """Get the selected native system-log query.

        Args:
            job_status (SlurmJobExecutionStatus): Status information about the job

        Returns:
            dict: Dictionary mapping query strings to observation sources
        """
        del job_status
        query = self.settings.log_query_providers[self.settings.log_query_backend].queries.system
        return {query: Source.SYSLOG}

    def _get_log_queries_app_logs(self, job_status: SlurmJobExecutionStatus) -> dict[str, Source]:
        """Get the selected native application-log query.

        Args:
            job_status (SlurmJobExecutionStatus): Status information about the job

        Returns:
            dict: Dictionary mapping query strings to observation sources
        """
        query_template = self.settings.log_query_providers[self.settings.log_query_backend].queries.application
        if self.settings.log_query_backend is BackendName.VICTORIALOGS:
            return {query_template.replace("{job_id}", job_status.job_id): Source.APPLOG}

        return {query_template.format(job_status.job_id): Source.APPLOG}

    def _get_prom_queries(self, job_status: SlurmJobExecutionStatus) -> dict[str, Source]:
        """Get Prometheus queries for metrics.

        Args:
            job_status (SlurmJobExecutionStatus): Status information about the job

        Returns:
            dict: Dictionary mapping query strings to observation sources
        """
        return {
            # Disabling IB and ETH metrics queries for now as they're not consumable by FACT
            # TODO: Re-enable these once FACT is updated to support it
            # self.settings.ib_metrics_query.format(job_status.cluster): Source.IB_METRICS,
            # self.settings.eth_metrics_query.format(job_status.cluster): Source.ETH_METRICS,
        }

    def _get_slow_signals_log_queries(self, job_status: SlurmJobExecutionStatus) -> dict[str, Source]:
        """Get the selected native slow-signal query.

        Args:
            job_status (SlurmJobExecutionStatus): Status information about the job

        Returns:
            dict: Dictionary mapping query strings to observation sources
        """
        query_template = self.settings.log_query_providers[self.settings.log_query_backend].queries.epilogue
        if self.settings.log_query_backend is BackendName.VICTORIALOGS:
            return {query_template.replace("{job_id}", job_status.job_id): Source.HEALTH_CHECK}

        return {query_template.format(job_status.job_id): Source.HEALTH_CHECK}

    def _get_slow_signals_prom_queries(self) -> dict[str, Source]:
        """Get Prometheus queries for slow signals.

        Returns:
            dict: Dictionary mapping query strings to observation sources
        """
        return {
            # Disabling health check query for now as it's not consumable by FACT
            # TODO: Re-enable this once FACT is updated to support it
            # self.settings.health_check_query: Source.NODE_METRICS,
        }
