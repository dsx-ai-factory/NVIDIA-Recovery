# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import TypeAdapter
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.aps_spec.spec import (
    ApsSlurmJobSpec,
    AutoResumeModeEnum,
)
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import Issue
from recovery_libs.storage.dataclass import (
    AdviceRecord,
    SlurmJobExecutionRecord,
    WorkflowRecord,
)
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.node_events import (
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
    NodeEvent,
)

_JSON_MAPPING_ADAPTER = TypeAdapter(dict[str, object])


def expected_job_audit_snapshot(record: SlurmJobExecutionRecord) -> dict[str, object]:
    """Build the independently specified approved Job audit projection."""
    return _JSON_MAPPING_ADAPTER.dump_python(
        {
            "job_id": record.job_id,
            "heimdall_job_id": record.heimdall_job_id,
            "job_attempt_index": record.job_attempt_index,
            "state": record.state,
            "state_reason": record.state_reason,
            "submit_time": record.submit_time,
            "start_time": record.start_time,
            "end_time": record.end_time,
            "cluster": record.cluster,
            "partition": record.partition,
            "reservation": record.reservation,
            "namespace": record.namespace,
            "worker_cluster_name": record.worker_cluster_name,
            "node_count": record.node_count,
            "total_gpu_count": record.total_gpu_count,
            "total_cpu_count": record.total_cpu_count,
            "total_memory_mb": record.total_memory_mb,
            "allocated_node_list": record.allocated_node_list,
            "inclusion_node_list": record.inclusion_node_list,
            "exclusion_node_list": record.exclusion_node_list,
            "failed_node_list": record.failed_node_list,
            "sticky_exclusion_node_list": record.sticky_exclusion_node_list,
            "aps_spec": record.aps_spec,
            "config_validation_failure_types": (
                [failure.name for failure in record.config_validation_failure_types]
                if record.config_validation_failure_types
                else None
            ),
        },
        mode="json",
    )


def create_basic_service_config(enable_leader_election: bool = False) -> dict[str, Any]:
    """
    Create a basic config for testing.

    Args:
        enable_leader_election: Whether to enable Kubernetes leader election.

    Returns:
        dict: A configuration dictionary for the JobManager.
    """
    config = {
        "service": {
            "stale_job_scan_frequency_seconds": 5,
            "stale_job_threshold_seconds": 300,
            "advice_enabled": False,
            "job_end_event_generation_enabled": True,
            "system_max_requeue_times": 100,
            "checkpoint_based_requeue_stop_threshold": 5,
            "requeue_early_stop_scan_frequency_seconds": 60,
            "requeue_early_stop_threshold_seconds": 600,
            "job_state_based_auto_resume_time_window_secs": 900,
            "job_state_based_auto_resume_threshold_secs": 300,
            "job_state_based_auto_resume_scan_interval_secs": 30,
            "environment": "prod",
            "log_level": "debug",
            "notification_service_url": "http://nvidia-recovery-notifications:8080",
            "storage": {
                "mode": "MYSQL",
                "sql": {
                    "host": "localhost",
                    "port": 3306,
                    "username": "test",
                    "password": "test",
                    "database_name": "test_db",
                },
            },
            "dag_support": {
                "enabled": False,
                "action_timeout_seconds": 120,
                "retry_scan_frequency_seconds": 60,
            },
            "attribution_insight_config": {
                "confirmed": {
                    "insight": "No action is needed. Heimdall has already requeued the job for you, excluding the bad node and reallocating a healthy one for the new run.",
                },
                "temporary": {
                    "insight": "No action is needed at this time. Heimdall has already requeued the job for you, excluding the potentially bad node, and is actively verifying the root cause for your job.",
                },
                "none": {
                    "insight": "Heimdall has already requeued the job for you. But manual intervention may be required to resolve the issue.",
                },
            },
            "attribution_summary_config": {
                "infra": {
                    "summary": "Job failed due to infrastructure issues.",
                },
                "potential_infra": {
                    "summary": "Job failed due to a potential infrastructure issue.",
                },
                "not_infra": {
                    "summary": "Job failed but not due to infrastructure issues.",
                },
            },
            "http": {"host": "0.0.0.0", "port": 8080},
            "event_bus": {
                "host": "localhost",
                "username": "rabbit",
                "password": "rabbit",
                "vhost": "/",
                "workflow_controller_exchange": "test_exchange",
                "job_queue": "test_queue",
                "job_routing_key": "job.update",
                "anomaly_exchange": "anomaly_exchange",
                "anomaly_queue": "anomaly_queue",
                "anomaly_routing_key": "job.anomaly",
                # Setting the delay to 0 for immediate retry in unit tests.
                "anomaly_queue_delivery_retry_delay_secs": 0,
                "anomaly_dead_letter_routing_key": "test_dead_letter",
                "anomaly_dead_letter_redelivery_routing_key": "test_dead_letter_redelivery",
                "anomaly_isolator_exchange": "anomaly_isolator_exchange",
                "anomaly_attribution_queue": "anomaly_isolator_queue",
                "anomaly_attribution_routing_key": "anomaly_isolator.attribution",
            },
            "guardrail_rules": [
                "NO FAKE DATA - Never use fake or simulated data",
                "VERIFY EVERYTHING - Cross-check Heimdall anomalies with Loki",
                "MARK UNVERIFIED - Use ⚠️ for anomalies without evidence",
                "CONFIDENCE LEVEL - Provide the confidence level of the findings",
                "TOKEN LIMIT - Use REST API fallback if response > 25000 tokens",
                "LOG EVERYTHING - Keep audit trail of all operations",
                "SEARCH EARLY - Look 30 minutes before visible failures",
                "SEARCH MORE EVIDENCE - Look for more evidence to either corrobote Heimdall anomaly detection and attribution or challenge them",
                "When is today must be verified with date or python code",
                "When you do epoch calculation, use a script to validate the conversion",
            ],
        }
    }

    # Add Kubernetes leader election config if enabled
    if enable_leader_election:
        config["service"]["kubernetes_leader_election"] = {
            "lease_name": "job-manager-leader-test",
            "lease_duration_seconds": 15,
            "renew_deadline_seconds": 10,
            "retry_period_seconds": 5,
            "in_cluster": False,
            "kubeconfig_path": "~/.kube/config",
        }

    return config


def create_example_slurm_job_record(
    job_id: str = "12345",
    attempt_index: int = 0,
    state: str = "PENDING",
    state_reason: str = "Dependency",
    submit_time: datetime | None = None,
    end_time: datetime | None = None,
    last_updated_at: datetime | None = None,
    last_state_transition_time: datetime | None = None,
    environment: str | None = None,
    auto_resume_mode: AutoResumeModeEnum | None = None,
    max_requeue_times: int | None = None,
    log_file_paths: list[str] | None = None,
    nvrx_enabled: str = "disabled",
) -> SlurmJobExecutionRecord:
    now = datetime.now()
    return SlurmJobExecutionRecord(
        job_id=job_id,
        job_attempt_index=attempt_index,
        workflow_id="test-workflow",
        job_name="Test Job",
        state=state,
        state_reason=state_reason,
        submit_time=submit_time or now,
        end_time=end_time,
        cluster="test_cluster",
        partition="recovery",
        reservation="recovery_prod",
        namespace="test-k8s-namespace",
        user="test_user",
        command="sbatch test.sh",
        dependency="10000",
        total_task_count=50 * 8,
        node_count=50,
        total_cpu_count=800,
        total_gpu_count=50 * 8,
        total_memory_mb=50 * 100 * 1000,
        inclusion_node_list=[f"node_{i}" for i in range(5)],
        aps_spec=ApsSlurmJobSpec(
            workflow_name="test_workflow",
            environment=environment,
            auto_resume_mode=auto_resume_mode,
            max_requeue_times=max_requeue_times,
            nvrx=nvrx_enabled,
        ),
        log_file_paths=log_file_paths,
        last_updated_at=last_updated_at or now,
        last_state_transition_time=last_state_transition_time or now,
    )


def create_example_workflow_record(
    workflow_id: str = "test_workflow_id",
    submit_time: datetime | None = None,
    end_time: datetime | None = None,
) -> WorkflowRecord:
    return WorkflowRecord(id=workflow_id, start_time=submit_time or datetime.now(), end_time=end_time)


def create_example_anoamly_attribution_result(node_count: int = 3) -> AnomalyAttributionResult:
    return AnomalyAttributionResult(
        job_id="123456",
        job_attempt_index=0,
        anomaly_type=EventType.CRASH,
        anomaly_uuid="test-anomaly-uuid",
        attribution_type=ErrorAttributionType.FAST,
        node_attributions={
            JobExecutionError.create(
                code=123,
                message="Test error message",
                description="Test description",
                signature="test_signature",
            ): [
                NodeEvent.create(
                    node=f"test_node_{i}",
                    job_id="123456",
                    job_attempt_index=0,
                    timestamp=datetime.now(),
                    error_attribution_type=ErrorAttributionType.FAST,
                    error_attribution_origin=ErrorAttributionOrigin.FACT,
                    error_attribution_sources=[
                        ErrorAttributionSource.APPLICATION_LOGS,
                        ErrorAttributionSource.SYSLOGS,
                    ],
                    exclusion_type=ExclusionType.TEMPORARY,
                    fact_status=FactStatus.OK,
                )
                for i in range(node_count)
            ]
        },
        global_attributions={},
    )


def create_example_advice_record(
    heimdall_id: uuid.UUID | None = None,
    job_id: str = "test-job-123",
    attempt_index: int = 0,
    advice_state: str = "Restarted",
    visible: bool = True,
) -> AdviceRecord:
    """Helper to create an example AdviceRecord for testing."""
    now = datetime.now(UTC)
    return AdviceRecord(
        heimdall_id=heimdall_id or uuid.uuid4(),
        visible=visible,
        job_id=job_id,
        scheduler_metadata={"namespace": "test-namespace", "kind": "MPIJob"},
        attempt_index=attempt_index,
        advice_state=advice_state,
        node_exclusions=["node-1", "node-2"],
        node_inclusions=None,
        issues=[
            Issue(
                issue_type="CRASH",
                timestamp=now,
                relevant_log_lines=["ERROR: OOM killed"],
            )
        ],
        created_at=now,
        updated_at=now,
    )
