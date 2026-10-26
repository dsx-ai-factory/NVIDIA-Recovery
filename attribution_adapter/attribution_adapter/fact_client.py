# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import json
import time

# Standard library imports
from datetime import datetime, timedelta
from typing import Any

import requests
from nvidia_attribution_client.models.attribution_parameters import AttributionParameters
from nvidia_attribution_client.models.attribution_response_v2 import AttributionResponseV2
from nvidia_attribution_client.models.attributor_info_v2 import AttributorInfoV2
from nvidia_attribution_client.models.attributor_metadata import AttributorMetadata
from nvidia_attribution_client.models.attributor_workload_info_v2 import AttributorWorkloadInfoV2
from nvidia_attribution_client.models.new_response_v2 import NewResponseV2
from nvidia_attribution_client.models.observation_context_v2 import ObservationContextV2
from nvidia_attribution_client.models.observation_response_v2 import ObservationResponseV2
from nvidia_attribution_client.models.observation_v2 import ObservationV2
from nvidia_attribution_client.models.source import Source
from nvidia_attribution_client.models.time_interval_v2 import TimeIntervalV2
from nvidia_attribution_client.models.workload_type_enum import WorkloadTypeEnum

# Third party imports
from prometheus_client import Counter, Histogram

# Recovery library imports
from recovery_libs.schema.anomaly_type import AnomalyType
from recovery_libs.schema.job_manager.models import JobAnomaly, SlurmJobExecutionStatus

from attribution_adapter.settings.settings import Settings
from attribution_adapter.utils.logger import logger

ADI_FACT_CREATE_ATTRIBUTION_SUCCESS_COUNTER = Counter(
    "adi_downstream_fact_create_attribution_success_count",
    "Number of successful attributions created in FACT",
)

ADI_FACT_CREATE_ATTRIBUTION_ERROR_COUNTER = Counter(
    "adi_downstream_fact_create_attribution_error_count",
    "Number of errors when creating an attribution in FACT",
    ["error_code"],
)

ADI_FACT_CREATE_ATTRIBUTION_DUPLICATE_COUNTER = Counter(
    "adi_downstream_fact_create_attribution_duplicate_count",
    "Number of duplicate attributions created in FACT",
)

ADI_FACT_CREATE_ATTRIBUTION_TIME = Histogram(
    "adi_downstream_fact_create_attribution_time",
    "Time taken to create an attribution in FACT",
)

ADI_FACT_SUBMIT_OBSERVATION_SUCCESS_COUNTER = Counter(
    "adi_downstream_fact_submit_observation_success_count",
    "Number of successful observations submitted in FACT",
)

ADI_FACT_SUBMIT_OBSERVATION_ERROR_COUNTER = Counter(
    "adi_downstream_fact_submit_observation_error_count",
    "Number of errors when submitting an observation in FACT",
    ["error_code"],
)

ADI_FACT_SUBMIT_OBSERVATION_TIME = Histogram(
    "adi_downstream_fact_submit_observation_time",
    "Time taken to submit an observation in FACT",
)

ADI_FACT_GET_ATTRIBUTION_SUCCESS_COUNTER = Counter(
    "adi_downstream_fact_get_attribution_success_count",
    "Number of successful attributions retrieved in FACT",
)

ADI_FACT_GET_ATTRIBUTION_ERROR_COUNTER = Counter(
    "adi_downstream_fact_get_attribution_error_count",
    "Number of errors when getting an attribution in FACT",
    ["error_code"],
)

ADI_FACT_GET_ATTRIBUTION_TIME = Histogram(
    "adi_downstream_fact_get_attribution_time",
    "Time taken to get an attribution in FACT",
)


def _node_resources(node: str | None, replica: str | None = None) -> list[dict[str, str]] | None:
    """Declare the entry's node (and replica), or None so the provider parses the body per source."""
    resources: list[dict[str, str]] = []
    if node:
        resources.append({"kind": "node", "hostname": node})
    if replica:
        resources.append({"kind": "replica", "name": replica})
    return resources or None


def _unwrap_log_payload(payload: str, node: str | None) -> tuple[str, str | None]:
    """Return the matchable body and node of one structured log value.

    Rules match the message text, so JSON envelopes (``body``/``attributes.hostname``,
    ``message``, or CloudEvents ``data.message``/``data.hostname``) are unwrapped as the
    provider's legacy Loki decoder did. A stream's ``node_name`` label takes precedence over a
    hostname inside the payload. Non-JSON or unrecognized payloads are matched verbatim.
    """
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return payload, node
    if not isinstance(parsed, dict):
        return payload, node
    if node is None and isinstance(attributes := parsed.get("attributes"), dict):
        node = attributes.get("hostname")
    body = parsed.get("body") or parsed.get("message") or ""
    if not body and isinstance(data := parsed.get("data"), dict):
        body = data.get("message") or ""
        node = node or data.get("hostname")
    return body or payload, node


def loki_streams_to_entries(log_streams: str, source: Source) -> list[dict[str, Any]]:
    """Convert Loki-shaped streams into V2 ``entry_timeseries`` entries.

    The log fetchers serialize records as Loki streams whose values are
    ``[timestamp_ns, line]`` pairs and whose ``node_name`` label identifies the emitting node.
    Values whose timestamp is not an integer are skipped, as the provider's legacy decoder did,
    and so are malformed non-pair values. Only application logs carry the ``pod`` label as a
    replica.

    Args:
        log_streams: JSON-encoded list of ``{"stream": {...}, "values": [[ts_ns, line], ...]}``.
        source: The observation source the streams were queried for.

    Returns:
        One entry per log value.
    """
    entries: list[dict[str, Any]] = []
    for stream in json.loads(log_streams):
        labels = stream.get("stream") or {}
        stream_node = labels.get("node_name")
        replica = labels.get("pod") if source == Source.APPLOG else None
        for value in stream["values"]:
            if len(value) != 2:
                continue
            try:
                ts = int(value[0])
            except ValueError:
                continue
            body, node = _unwrap_log_payload(value[1], stream_node)
            entries.append({"body": body, "ts": ts, "resources": _node_resources(node, replica)})
    return entries


def _metric_value(raw: Any) -> Any:
    """Render a sample value as the provider's metric rules compare it: int when integral."""
    if not isinstance(raw, str):
        return raw
    try:
        return int(raw)
    except ValueError:
        return float(raw)


def prometheus_to_entries(metrics: dict, hostname_label: str) -> list[dict[str, Any]]:
    """Convert a Prometheus vector or matrix result into V2 ``entry_timeseries`` entries.

    Metric rules match a rendered body rather than structured fields, so each sample is rendered
    in the provider's documented metric-body form. Each entry declares its node because the
    provider never parses one from a metric body; a sample without the hostname label is rejected
    so hardware evidence cannot be attributed to an unrelated job node.

    Args:
        metrics: Prometheus query ``data`` with ``resultType`` and ``result``.
        hostname_label: The label that names the sample's node.

    Returns:
        One entry per sample.

    Raises:
        ValueError: If the result type is unsupported or a series lacks the hostname label.
    """
    if metrics.get("resultType") not in {"vector", "matrix"}:
        raise ValueError("Attribution metrics require a Prometheus vector or matrix result")
    entries: list[dict[str, Any]] = []
    for series in metrics["result"]:
        labels = series["metric"]
        node = labels.get(hostname_label)
        if not node:
            raise ValueError("Attribution metric is missing the configured hostname label")
        values = series["values"] if metrics["resultType"] == "matrix" else [series["value"]]
        for timestamp, value in values:
            body = (
                f"time: {timestamp}, metric_name: {labels['__name__']}, metric_value: {_metric_value(value)}, "
                f"threshold: None, cluster: {labels.get('s_cluster')}, node: {node}, "
                f"component: {labels.get('device', '')}"
            )
            # Prometheus timestamps are float seconds; the provider resolves time to microseconds.
            ts = round(float(timestamp) * 1_000_000) * 1_000
            entries.append({"body": body, "ts": ts, "resources": _node_resources(node)})
    return entries


class FACTClient:
    def __init__(self, settings: Settings):
        """Initialize FACT client with base URL.

        Args:
            fact_url (str): Base URL for FACT service
        """
        self.fact_url = (settings.fact_url or "").rstrip("/")  # Remove trailing slash if present
        self.settings = settings
        self.timeout = settings.fact_http_timeout_seconds
        self.logger = logger

    def _workload_status_for_fact(self, job_status: SlurmJobExecutionStatus) -> str:
        """Normalize inconsistent lifecycle state only for the FACT request."""
        state = job_status.state or ""
        if state == "RUNNING" and job_status.end_time is not None:
            self.logger.warning(
                f"Normalizing FACT workload status from RUNNING to FAILED for job {job_status.job_id} "
                f"because end_time is set ({job_status.end_time.isoformat()})."
            )
            return "FAILED"
        return state

    def create_request(self, job_status: SlurmJobExecutionStatus, job_anomaly: JobAnomaly | None = None) -> str:
        """Create a new attribution request in the FACT v2 service.

        Creates an attribution request by sending job metadata to FACT. This is the first step
        in the attribution process and must be called before supplying logs or metrics.

        Args:
            job_status (SlurmJobExecutionStatus): Status object containing job metadata
            job_anomaly (Optional[JobAnomaly]): Optional anomaly object. If provided and the
                anomaly type is STRAGGLER, the interest time window will be set to 30 minutes
                before the anomaly timestamp. Otherwise, the full job duration is used.

        Returns:
            str: Attribution ID that can be used for subsequent API calls

        Raises:
            requests.exceptions.RequestException: If the API call fails
        """
        endpoint = f"{self.fact_url}/v2/attributor"
        node_count = job_status.node_count or len(job_status.allocated_node_list or [])
        total_ranks = job_status.total_gpu_count
        # Handle division by zero and invalid values
        ranks_per_node = (
            int(total_ranks / node_count)
            if (node_count and total_ranks and node_count > 0)
            else self.settings.fact_default_ranks_per_node
        )

        # Calculate interest time window based on anomaly type
        job_start_time: datetime = job_status.start_time or datetime.min
        job_end_time: datetime = job_status.end_time or datetime.max

        if job_anomaly and job_anomaly.anomaly_type == AnomalyType.STRAGGLER.value:
            # For STRAGGLER anomalies, use a 30-minute window ending at the anomaly timestamp
            # but bounded by the job's start and end times, this also ensures a new attributor is created
            # for each straggler anomaly even for the same job.
            interest_end_time = min(job_anomaly.timestamp, job_end_time)
            interest_start_time = max(job_anomaly.timestamp - timedelta(minutes=30), job_start_time)
            self.logger.info(
                f"Using STRAGGLER-specific time window for job {job_status.job_id}: "
                f"start={interest_start_time}, end={interest_end_time}"
            )
        else:
            # For all other cases, use the full job duration
            interest_start_time = job_start_time
            interest_end_time = job_end_time

        workload_status = self._workload_status_for_fact(job_status)
        attributor_info = AttributorInfoV2(
            workload=AttributorWorkloadInfoV2(
                id=job_status.job_id,
                type=WorkloadTypeEnum.SLURM,
                job_start_time=job_start_time,
                timezone="UTC",
                job_end_time=job_end_time,
                interest_start_time=interest_start_time,
                interest_end_time=interest_end_time,
                srun_cmd=job_status.command or "",
                exit_code_signal=0,
                cancelled_by="",
                last_tflops_time=None,
                status=workload_status,
                # node -> GLOBAL contiguous rank ids (matches FACT's canonical v1 node_to_ranks:
                # node i gets [i*rpn, (i+1)*rpn)); the lib uses this mapping for rank->node lookup,
                # so per-node-local [0..rpn) would collide across nodes on multi-node jobs.
                nodes={
                    node: list(range(i * ranks_per_node, (i + 1) * ranks_per_node))
                    for i, node in enumerate(job_status.allocated_node_list or [])
                },
                name=job_status.job_name or "",
                username=job_status.user or "",
                framework="",
            ),
            metadata=AttributorMetadata(
                cluster=job_status.cluster or "", agent="Heimdall", tenant="tenant", ruleset=""
            ),
            # FACT v2 requires `parameters.rule_params` to be a dict: fact-client 2.0.0rc2
            # serializes the default value of `rule_params` (None) as JSON null, which the fact-service v2.0.0-rc2
            # API rejects with a 422 dict_type validation error (found via live e2e).
            # Pass an explicit empty dict; v2 auto-detects stragglers, so rule_params
            # re-expression is a follow-up (spec §14).
            parameters=AttributionParameters(rule_params={}),
        )

        start_time = time.time()
        response = requests.post(
            endpoint,
            data=attributor_info.model_dump_json(),
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )
        end_time = time.time()

        self.logger.info(
            f"FACT request endpoint: {endpoint}, request: {attributor_info.model_dump_json()}, response: {response.text}"
        )
        ADI_FACT_CREATE_ATTRIBUTION_TIME.observe(float(end_time - start_time))

        if not response.ok:
            if response.status_code == 409:
                ADI_FACT_CREATE_ATTRIBUTION_DUPLICATE_COUNTER.inc()
                att_id = self._parse_duplicate_attribution_error(response.text)
                if att_id:
                    self.logger.info(
                        f"Duplicate attribution found for job {job_status.job_id}, using existing attribution ID {att_id} from {response.text}"
                    )
                else:
                    self.logger.error(
                        f"Duplicate attribution found for job {job_status.job_id}, but could not parse attribution ID from {response.text}"
                    )
                    raise requests.exceptions.RequestException(
                        f"Error submitting attribution to FACT, status {response.status_code}: {response.text}"
                    )

                return att_id

            ADI_FACT_CREATE_ATTRIBUTION_ERROR_COUNTER.labels(error_code=response.status_code).inc()
            self.logger.error(
                f"Error submitting attribution to FACT: {response.text} while sending {attributor_info.model_dump_json()}"
            )
            raise requests.exceptions.RequestException(
                f"Error submitting attribution to FACT, status {response.status_code}: {response.text}"
            )

        ADI_FACT_CREATE_ATTRIBUTION_SUCCESS_COUNTER.inc()
        new_response = NewResponseV2.from_dict(response.json())
        if new_response is None:
            raise ValueError(f"Failed to parse FACT response: {response.json()}")
        return new_response.attributor_id

    def supply_logs(
        self, attribution_id: str, log_lines: str, source: Source, logs_start_time: datetime, logs_end_time: datetime
    ) -> int:
        """Supply log data to an existing attribution request.

        Takes log lines and sends them to FACT as an observation for analysis. The logs are associated
        with a specific attribution request and source type.

        Args:
            attribution_id (str): ID of the attribution request to add logs to
            log_lines (str): JSON-encoded Loki streams of ``[timestamp_ns, line]`` values
            source (Source): Source type of the logs (e.g. SYSLOG, APPLOG)
            logs_start_time (datetime): Start time of the log interval
            logs_end_time (datetime): End time of the log interval

        Returns:
            int: Observation ID for the submitted logs

        Raises:
            requests.exceptions.RequestException: If the API call fails
        """
        endpoint = f"{self.fact_url}/v2/attributor/{attribution_id}/observation"

        # V2 accepts only `entry_timeseries`; the declared interval is the query window, and the
        # provider discards entries outside it.
        observation = ObservationV2(
            context=ObservationContextV2(
                time_interval=TimeIntervalV2(start=logs_start_time, end=logs_end_time),
                source=source,
            ),
            body=json.dumps(loki_streams_to_entries(log_lines, source)),
        )

        self.logger.info(f"Sending logs to FACT: {observation.model_dump_json()}")

        start_time = time.time()
        response = requests.post(
            endpoint,
            data=observation.model_dump_json(),
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )
        end_time = time.time()
        ADI_FACT_SUBMIT_OBSERVATION_TIME.observe(float(end_time - start_time))

        self.logger.info(f"FACT request endpoint: {endpoint}, response: {response.text}")

        if not response.ok:
            ADI_FACT_SUBMIT_OBSERVATION_ERROR_COUNTER.labels(error_code=response.status_code).inc()
            self.logger.error(
                f"""Error submitting logs to FACT: {response.text};
                while sending context: {observation.context.model_dump_json()};
                to endpoint: {endpoint};
                size of data: {len(log_lines)};
                full payload: {observation.model_dump_json()}"""
            )
            raise requests.exceptions.RequestException(
                f"Error submitting logs to FACT, status {response.status_code}: {response.text}",
                response=response,
            )

        observation_response = ObservationResponseV2.from_json(response.text)
        if observation_response is None:
            raise ValueError(f"Failed to parse FACT observation response: {response.text}")
        self.logger.debug(f"Submitted logs to FACT: {observation_response.observation_id}")
        ADI_FACT_SUBMIT_OBSERVATION_SUCCESS_COUNTER.inc()
        return observation_response.observation_id

    def supply_metrics(
        self,
        attribution_id: str,
        metrics: dict,
        source: Source,
        metrics_start_time: datetime,
        metrics_end_time: datetime,
    ) -> int:
        """Supply metrics data to an existing attribution request.

        Takes metrics data and sends it to FACT as an observation for analysis. The metrics are
        associated with a specific attribution request and source type.

        Args:
            attribution_id (str): ID of the attribution request to add metrics to
            metrics (dict): Prometheus vector or matrix data. Each series must
                retain its metric name and the configured hostname label.
            source (Source): Source type of the metrics
            metrics_start_time (datetime): Start of the metrics query window
            metrics_end_time (datetime): End of the metrics query window

        Returns:
            int: Observation ID for the submitted metrics

        Raises:
            ValueError: If the result type or node identity is unsupported
            requests.exceptions.RequestException: If the API call fails
        """
        endpoint = f"{self.fact_url}/v2/attributor/{attribution_id}/observation"

        observation = ObservationV2(
            context=ObservationContextV2(
                time_interval=TimeIntervalV2(start=metrics_start_time, end=metrics_end_time),
                source=source,
            ),
            body=json.dumps(prometheus_to_entries(metrics, self.settings.prometheus_hostname_label)),
        )

        self.logger.info(f"Sending metrics to FACT: {observation.model_dump_json()}")

        start_time = time.time()
        response = requests.post(
            endpoint,
            data=observation.model_dump_json(),
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )
        end_time = time.time()
        ADI_FACT_SUBMIT_OBSERVATION_TIME.observe(float(end_time - start_time))

        if not response.ok:
            ADI_FACT_SUBMIT_OBSERVATION_ERROR_COUNTER.labels(error_code=response.status_code).inc()
            self.logger.error(
                f"Error submitting metrics to FACT: {response.text} while sending {observation.model_dump_json()}"
            )
            raise requests.exceptions.RequestException(
                f"Error submitting metrics to FACT, status {response.status_code}: {response.text}"
            )

        observation_response = ObservationResponseV2.from_json(response.text)
        if observation_response is None:
            raise ValueError(f"Failed to parse FACT metrics observation response: {response.text}")
        self.logger.info(f"Submitted metrics to FACT: {observation_response.observation_id}")
        ADI_FACT_SUBMIT_OBSERVATION_SUCCESS_COUNTER.inc()
        return observation_response.observation_id

    def get_attribution(self, attribution_id: str) -> AttributionResponseV2:
        """Get attribution results from FACT v2.

        Retrieves the final attribution results for a completed analysis. This should be called
        after all observations (logs/metrics) have been supplied.

        Args:
            attribution_id (str): ID of the attribution request to get results for

        Returns:
            AttributionResponseV2: Object containing the v2 attribution analysis results

        Raises:
            requests.exceptions.RequestException: If the API call fails
        """
        endpoint = f"{self.fact_url}/v2/attributor/{attribution_id}/attribution"

        start_time = time.time()
        response = requests.get(endpoint, timeout=self.timeout)
        end_time = time.time()
        ADI_FACT_GET_ATTRIBUTION_TIME.observe(float(end_time - start_time))

        self.logger.debug(f"FACT request endpoint: {endpoint}, response: {response.text}")

        if not response.ok:
            ADI_FACT_GET_ATTRIBUTION_ERROR_COUNTER.labels(error_code=response.status_code).inc()
            self.logger.error(f"Error getting attribution from FACT: {response.text} while hitting {endpoint}")
            raise requests.exceptions.RequestException(
                f"Error getting attribution from FACT, status {response.status_code}: {response.text}"
            )
        self.logger.info(f"Received attribution from FACT: {response.text}")

        ADI_FACT_GET_ATTRIBUTION_SUCCESS_COUNTER.inc()
        attribution = AttributionResponseV2.from_json(response.text)
        if attribution is None:
            raise ValueError(f"Failed to parse FACT attribution response: {response.text}")
        return attribution

    def _parse_duplicate_attribution_error(self, response_text: str) -> str | None:
        """Parse the attribution ID from the duplicate attribution error response.

        Args:
            response_text (str): The response text from the duplicate attribution error

        Returns:
            str: The attribution ID from the duplicate attribution error
        """
        self.logger.info(f"Parsing duplicate attribution error: {response_text}")
        try:
            json_response = json.loads(response_text)
            if "model_type" in json_response and json_response["model_type"] == "DuplicateAttributorError":
                return json_response["att_id"]
        except json.JSONDecodeError:
            self.logger.error(f"Error parsing duplicate attribution error: {response_text}")
        return None
