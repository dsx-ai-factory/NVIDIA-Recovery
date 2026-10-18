# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from job_manager.advice_mapping import http_to_storage_advice, storage_to_http_advice
from job_manager.job_mapping import storage_to_http_job_status
from job_manager.storage_utils import (
    create_advice_issue_from_event,
    create_advice_issues_from_anomalies,
    create_terminal_advice_for_job,
    maybe_persist_job_released_terminal_advice_from_attribution,
    maybe_persist_requeue_terminal_advice,
    persist_advice_record,
    persist_dag_release_advice,
    populate_sticky_exclusion_node_list,
    sanitize_log_file_paths_for_retry,
    storage_attribution_to_summary,
    storage_to_http_attributions,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from recovery_libs.audit import NoOpAuditBuffer
from recovery_libs.schema.aps_spec.spec import AutoResumeModeEnum
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import (
    Advice,
    AdviceRecommendation,
    AdviceReconciliationDisposition,
    AdviceReconciliationReason,
    AdviceState,
    Issue,
    JobAnomaly,
    JobTerminalAdvice,
    NodeConstraints,
    RecoveryContext,
    RecoveryOperation,
    RemediationAction,
    SystemRequeueConfig,
)
from recovery_libs.schema.job_manager.models import GlobalEvent as GlobalEventHTTP
from recovery_libs.schema.job_manager.models import NodeEvent as NodeEventHTTP
from recovery_libs.schema.watcher.model import Attributes, Event
from recovery_libs.storage.base import RecoveryStorageException
from recovery_libs.storage.dataclass import (
    AdviceReconciliationStatusRecord,
    AdviceRecord,
    SlurmJobFinalStates,
    SlurmJobNonFinalStates,
)
from recovery_libs.storage.global_events import GlobalEvent as GlobalEventInternal
from recovery_libs.storage.job_execution_errors import (
    JobExecutionError as JobExecutionErrorInternal,
)
from recovery_libs.storage.mysql_storage import AdviceIssueMerge, UpsertAdviceResult
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    ExclusionType,
    FactStatus,
)
from recovery_libs.storage.node_events import NodeEvent as NodeEventInternal
from test_utils import (
    create_example_advice_record,
    create_example_anoamly_attribution_result,
    create_example_slurm_job_record,
)

ATTRIBUTION_SUMMARY_CONFIG = {
    "infra": {
        "summary": "Job failed due to infrastructure issues.",
    },
    "potential_infra": {
        "summary": "Job failed due to a potential infrastructure issue.",
    },
    "not_infra": {
        "summary": "Job failed but not due to infrastructure issues.",
    },
}

ATTRIBUTION_INSIGHT_CONFIG = {
    "confirmed": {
        "insight": "No action is needed. Heimdall has already requeued the job for you, excluding the bad node and reallocating a healthy one for the new run.",
    },
    "temporary": {
        "insight": "No action is needed at this time. Heimdall has already requeued the job for you, excluding the potentially bad node, and is actively verifying the root cause for your job.",
    },
    "none": {
        "insight": "Heimdall has already requeued the job for you. But manual intervention may be required to resolve the issue.",
    },
}


def get_dummy_attributions(
    node_count: int = 10, node_id_start: int = 0, job_id: str = "123456", anomaly_uuid: str | None = None
) -> Sequence[NodeEventInternal]:
    """
    Get a list of dummy error attributions. This is temporary to allow the developemnt of the Web UI.
    In the next MR this function will be moved to the unit tests.
    """
    node_events = []
    for i in range(node_count):
        error = JobExecutionErrorInternal.create(
            code=i + 1,
            message=f"error message {i}",
            description=f"error description {i}",
            signature=f"error signature {i}",
        )

        event = NodeEventInternal.create(
            node=f"node_{node_id_start + i}",
            timestamp=datetime.now(),
            job_id=job_id,
            job_attempt_index=0,
            job_start_time=datetime.now(),
            job_end_time=datetime.now(),
            job_anomaly_id=anomaly_uuid,  # type: ignore[arg-type]
            cluster="test cluster",
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type=ErrorAttributionType.FAST,
            error_attribution_origin=ErrorAttributionOrigin.FACT,
            error_attribution_sources=[ErrorAttributionSource.SYSLOGS, ErrorAttributionSource.APPLICATION_LOGS],
            exclusion_type=ExclusionType.TEMPORARY,
            fact_status=FactStatus.OK,
        )
        event.job_execution_error = error
        node_events.append(event)
    return node_events


def test_storage_to_http_attributions():
    """
    Test that storage_to_http_attributions correctly converts storage attributions to HTTP attributions.

    This test:
    1. Gets dummy attribution data using get_dummy_attributions()
    2. Passes the data through storage_to_http_attributions()
    3. Verifies the converted data has the expected format and values
    """
    # Get test data
    dummy_data = get_dummy_attributions()

    # Convert to HTTP format - pass node events and empty global events list
    node_events_http, global_events_http = storage_to_http_attributions(dummy_data, [])

    # Verify results
    assert len(node_events_http) == 10
    assert len(global_events_http) == 0

    # Check first attribution as sample
    first_attr = node_events_http[0]
    assert first_attr.node == "node_0"
    assert isinstance(first_attr.timestamp, datetime)
    assert first_attr.job_id == "123456"
    assert first_attr.job_attempt_index == 0
    assert isinstance(first_attr.job_start_time, datetime)
    assert isinstance(first_attr.job_end_time, datetime)
    assert first_attr.cluster == "test cluster"
    assert first_attr.error_attribution == ErrorAttribution.HARDWARE.name
    assert first_attr.error_attribution_type == ErrorAttributionType.FAST.name
    assert first_attr.error_attribution_origin == ErrorAttributionOrigin.FACT.name
    assert first_attr.error_attribution_sources == [
        ErrorAttributionSource.SYSLOGS.name,
        ErrorAttributionSource.APPLICATION_LOGS.name,
    ]
    assert first_attr.exclusion_type == ExclusionType.TEMPORARY.name
    assert first_attr.original_exclusion_type == ExclusionType.UNDEFINED.name
    assert first_attr.fact_status == FactStatus.OK.name

    # Verify job execution error
    assert first_attr.job_execution_error.code == 1
    assert first_attr.job_execution_error.message == "error message 0"
    assert first_attr.job_execution_error.description == "error description 0"
    assert first_attr.job_execution_error.signature == "error signature 0"


def test_storage_to_http_attributions_with_original_exclusion_type():
    """
    Test that storage_to_http_attributions correctly converts original_exclusion_type.

    This test verifies that when a node event has both exclusion_type and original_exclusion_type set,
    both are properly converted in the HTTP response.
    """
    error = JobExecutionErrorInternal.create(
        code=1,
        message="test error",
        description="test description",
        signature="test signature",
    )

    # Create a node event where exclusion_type was changed during sanitization
    event = NodeEventInternal.create(
        node="node_sanitized",
        timestamp=datetime.now(),
        job_id="test-job",
        job_attempt_index=0,
        error_attribution=ErrorAttribution.HARDWARE,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.ADI,
        error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
        exclusion_type=ExclusionType.NONE,
        original_exclusion_type=ExclusionType.TEMPORARY,
        fact_status=FactStatus.OK,
    )
    event.job_execution_error = error

    # Convert to HTTP format
    node_events_http, _ = storage_to_http_attributions([event], [])

    # Verify original_exclusion_type is preserved
    assert len(node_events_http) == 1
    attr = node_events_http[0]
    assert attr.exclusion_type == ExclusionType.NONE.name
    assert attr.original_exclusion_type == ExclusionType.TEMPORARY.name


def test_storage_to_http_attributions_with_global_events():
    """
    Test that storage_to_http_attributions correctly converts GlobalEvents alongside NodeEvents.

    This test verifies that when a job has both NodeEvent and GlobalEvent attributions,
    both types are properly converted to the HTTP response format, and GlobalEvents
    don't have node-specific fields (node, exclusion_type, original_exclusion_type).
    """
    # Create a shared error
    error = JobExecutionErrorInternal.create(
        code=1,
        message="mixed event test",
        description="test with both node and global events",
        signature="mixed_sig",
    )

    # Create a NodeEvent (node-specific attribution)
    node_event = NodeEventInternal.create(
        node="node_1",
        timestamp=datetime.now(),
        job_id="test-job",
        job_attempt_index=0,
        error_attribution=ErrorAttribution.HARDWARE,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
        exclusion_type=ExclusionType.TEMPORARY,
        fact_status=FactStatus.OK,
    )
    node_event.job_execution_error = error

    # Create a GlobalEvent (job-level attribution, no node)
    global_event = GlobalEventInternal.create(
        timestamp=datetime.now(),
        job_id="test-job",
        job_attempt_index=0,
        error_attribution=ErrorAttribution.APPLICATION,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
        fact_status=FactStatus.OK,
    )
    global_event.job_execution_error = error

    # Convert to HTTP format - pass separate lists for node and global events
    node_events_http, global_events_http = storage_to_http_attributions([node_event], [global_event])

    # Verify we got correct counts
    assert len(node_events_http) == 1
    assert len(global_events_http) == 1

    # Verify NodeEvent fields
    node_attr = node_events_http[0]
    assert isinstance(node_attr, NodeEventHTTP)
    assert node_attr.node == "node_1"
    assert node_attr.error_attribution == ErrorAttribution.HARDWARE.name
    assert node_attr.exclusion_type == ExclusionType.TEMPORARY.name

    # Verify GlobalEvent fields
    global_attr = global_events_http[0]
    assert isinstance(global_attr, GlobalEventHTTP)
    assert global_attr.error_attribution == ErrorAttribution.APPLICATION.name
    # GlobalEvent should not have node, exclusion_type, original_exclusion_type, job_start_time, job_end_time, or cluster fields
    assert not hasattr(global_attr, "node")
    assert not hasattr(global_attr, "exclusion_type")
    assert not hasattr(global_attr, "original_exclusion_type")
    assert not hasattr(global_attr, "job_start_time")
    assert not hasattr(global_attr, "job_end_time")
    assert not hasattr(global_attr, "cluster")


def test_populate_sticky_node_list_singleton_dep_first_job():
    node_storage_mock = MagicMock()
    job_record_storage_mock = MagicMock()

    job_update = create_example_slurm_job_record(job_id="12345", state="RUNNING")
    known_job_record = create_example_slurm_job_record(job_id="12345", state="RUNNING")

    job_update.exclusion_node_list = [f"node_{i}" for i in range(10, 12)]
    job_update.sticky_exclusion_node_list = None

    # No previous job attempts found for the singletop depdenency chain.
    job_record_storage_mock.read_job_records.return_value = ([], 0)

    populate_sticky_exclusion_node_list(job_record_storage_mock, node_storage_mock, job_update, known_job_record)

    # Sticky exclusion node list should be the same as the exclusion node list.
    assert job_update.sticky_exclusion_node_list == [f"node_{i}" for i in range(10, 12)]
    job_record_storage_mock.read_job_records.assert_called_once_with(
        states=[s.value for s in SlurmJobFinalStates],
        job_name=job_update.job_name,
        sort_by=[("end_time", "desc")],
        limit=1,
    )


def test_populate_sticky_node_list_singleton_dep_with_early_jobs():
    node_storage_mock = MagicMock()
    job_record_storage_mock = MagicMock()

    job_update = create_example_slurm_job_record(job_id="12345", state="RUNNING")
    known_job_record = create_example_slurm_job_record(job_id="12345", state="RUNNING")

    now = datetime.now()
    job_update.sticky_exclusion_node_list = None

    history_job_records = [
        create_example_slurm_job_record(job_id="12340", state="FAILED", end_time=now - timedelta(minutes=60)),
    ]
    history_job_records[0].sticky_exclusion_node_list = [f"node_{i}" for i in range(10, 12)]
    job_record_storage_mock.read_job_records.return_value = (history_job_records, 1)

    # One confirmed bad node out of 3 total bad nodes were identified for the previous job.
    node_events = get_dummy_attributions(node_count=3, job_id="12340", node_id_start=12)
    node_events[2].exclusion_type = ExclusionType.CONFIRMED
    node_storage_mock.read_node_attributions_for_job.return_value = node_events

    populate_sticky_exclusion_node_list(job_record_storage_mock, node_storage_mock, job_update, known_job_record)

    # Sticky exclusion node list should be populated from the first history job and the confirmed bad nodes from the
    # node storage.
    assert job_update.sticky_exclusion_node_list == ["node_10", "node_11", "node_14"]
    job_record_storage_mock.read_job_records.assert_called_once_with(
        states=[s.value for s in SlurmJobFinalStates],
        job_name=job_update.job_name,
        sort_by=[("end_time", "desc")],
        limit=1,
    )
    node_storage_mock.read_node_attributions_for_job.assert_called_once_with(job_id="12340", job_attempt_index=0)


def test_populate_sticky_node_list_requeue_first_job():
    node_storage_mock = MagicMock()
    job_record_storage_mock = MagicMock()

    job_update = create_example_slurm_job_record(job_id="12345", state="RUNNING", auto_resume_mode="requeue")
    known_job_record = create_example_slurm_job_record(job_id="12345", state="RUNNING", auto_resume_mode="requeue")

    job_update.exclusion_node_list = [f"node_{i}" for i in range(10, 12)]
    job_update.sticky_exclusion_node_list = None

    # No previous job attempts found.
    job_record_storage_mock.read_job_records.return_value = ([known_job_record], 0)

    populate_sticky_exclusion_node_list(job_record_storage_mock, node_storage_mock, job_update, known_job_record)

    # Sticky exclusion node list should be the same as the exclusion node list.
    assert job_update.sticky_exclusion_node_list == [f"node_{i}" for i in range(10, 12)]
    job_record_storage_mock.read_job_records.assert_called_once_with(
        job_id=job_update.job_id,
        sort_by=[("job_attempt_index", "desc")],
        limit=2,
    )


def test_populate_sticky_node_requeue_with_early_jobs():
    node_storage_mock = MagicMock()
    job_record_storage_mock = MagicMock()

    job_update = create_example_slurm_job_record(
        job_id="12345", attempt_index=3, state="RUNNING", auto_resume_mode="requeue"
    )
    known_job_record = create_example_slurm_job_record(
        job_id="12345", attempt_index=3, state="RUNNING", auto_resume_mode="requeue"
    )

    now = datetime.now()
    job_update.sticky_exclusion_node_list = None

    history_job_records = [
        known_job_record,
        create_example_slurm_job_record(
            job_id="12345", attempt_index=2, state="FAILED", end_time=now - timedelta(minutes=60)
        ),
    ]
    history_job_records[1].sticky_exclusion_node_list = [f"node_{i}" for i in range(10, 12)]
    job_record_storage_mock.read_job_records.return_value = (history_job_records, 2)

    # One confirmed bad node out of 3 total bad nodes were identified for the previous job.
    node_events = get_dummy_attributions(node_count=3, job_id="12345", node_id_start=12)
    node_events[2].exclusion_type = ExclusionType.CONFIRMED
    node_storage_mock.read_node_attributions_for_job.return_value = node_events

    populate_sticky_exclusion_node_list(job_record_storage_mock, node_storage_mock, job_update, known_job_record)

    # Sticky exclusion node list should be populated from the first history job attempt and the confirmed bad nodes from
    # the node storage.
    assert job_update.sticky_exclusion_node_list == ["node_10", "node_11", "node_14"]
    job_record_storage_mock.read_job_records.assert_called_once_with(
        job_id=job_update.job_id,
        sort_by=[("job_attempt_index", "desc")],
        limit=2,
    )
    node_storage_mock.read_node_attributions_for_job.assert_called_once_with(job_id="12345", job_attempt_index=2)


@pytest.mark.parametrize(
    "auto_resume_mode",
    ["requeue", "singleton_dependency"],
)
def test_sticky_node_list_not_populated_with_pending_jobs(auto_resume_mode):
    # Test that sticky exclusion node list is not populated when the job is not in RUNNING state.
    node_storage_mock = MagicMock()
    job_record_storage_mock = MagicMock()

    job_update = create_example_slurm_job_record(
        job_id="12345", attempt_index=3, state="PENDING", auto_resume_mode=auto_resume_mode
    )
    known_job_record = create_example_slurm_job_record(
        job_id="12345", attempt_index=3, state="PENDING", auto_resume_mode=auto_resume_mode
    )

    populate_sticky_exclusion_node_list(job_record_storage_mock, node_storage_mock, job_update, known_job_record)

    assert job_update.sticky_exclusion_node_list is None
    job_record_storage_mock.read_job_records.assert_not_called()
    node_storage_mock.read_node_attributions_for_job.assert_not_called()


def test_storage_attribution_to_summary_empty():
    """Test that empty sequence returns default values."""
    summary, insight = storage_attribution_to_summary(ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [], [])
    assert summary == "Unknown"
    assert insight == "Unknown"


def test_storage_attribution_to_summary_hardware_confirmed():
    """Test that CONFIRMED exclusion type returns correct summary and insight."""
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.HARDWARE,
        error_attribution_type="TEST_TYPE",
        error_attribution_origin="TEST_ORIGIN",
        error_attribution_sources=["TEST_SOURCE"],
        exclusion_type=ExclusionType.CONFIRMED,
        fact_status="TEST_STATUS",
    )
    node_event.job_execution_error = JobExecutionErrorInternal(
        code="TEST_ERROR",
        message="Test error message",
        description="Test error description",
        signature="test-signature",
    )

    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], []
    )
    assert summary == ATTRIBUTION_SUMMARY_CONFIG["infra"]["summary"]
    assert insight == ATTRIBUTION_INSIGHT_CONFIG["confirmed"]["insight"]


def test_storage_attribution_to_summary_nonhardware_confirmed():
    """Test that CONFIRMED exclusion type returns correct summary and insight."""
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.SYSTEM,
        error_attribution_type="TEST_TYPE",
        error_attribution_origin="TEST_ORIGIN",
        error_attribution_sources=["TEST_SOURCE"],
        exclusion_type=ExclusionType.CONFIRMED,
        fact_status="TEST_STATUS",
    )
    node_event.job_execution_error = JobExecutionErrorInternal(
        code="TEST_ERROR",
        message="Test error message",
        description="Test error description",
        signature="test-signature",
    )

    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], []
    )
    assert summary == ATTRIBUTION_SUMMARY_CONFIG["potential_infra"]["summary"]
    assert insight == ATTRIBUTION_INSIGHT_CONFIG["confirmed"]["insight"]


def test_storage_attribution_to_summary_temporary():
    """Test that TEMPORARY exclusion type returns correct summary and insight."""
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution="TEST_ATTRIBUTION",
        error_attribution_type="TEST_TYPE",
        error_attribution_origin="TEST_ORIGIN",
        error_attribution_sources=["TEST_SOURCE"],
        exclusion_type=ExclusionType.TEMPORARY,
        fact_status="TEST_STATUS",
    )
    node_event.job_execution_error = JobExecutionErrorInternal(
        code="TEST_ERROR",
        message="Test error message",
        description="Test error description",
        signature="test-signature",
    )

    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], []
    )
    assert summary == "Job failed due to a potential infrastructure issue."
    assert (
        insight
        == "No action is needed at this time. Heimdall has already requeued the job for you, excluding the potentially bad node, and is actively verifying the root cause for your job."
    )


def test_storage_attribution_to_summary_none():
    """Test that NONE exclusion type returns correct summary and insight."""
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.APPLICATION,
        error_attribution_type="TEST_TYPE",
        error_attribution_origin="TEST_ORIGIN",
        error_attribution_sources=["TEST_SOURCE"],
        exclusion_type=ExclusionType.NONE,
        fact_status="TEST_STATUS",
    )
    node_event.job_execution_error = JobExecutionErrorInternal(
        code="TEST_ERROR",
        message="Test error message",
        description="Test error description",
        signature="test-signature",
    )
    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], []
    )
    assert summary == ATTRIBUTION_SUMMARY_CONFIG["not_infra"]["summary"]
    assert insight == ATTRIBUTION_INSIGHT_CONFIG["none"]["insight"]


def test_storage_attribution_to_summary_multiple_events():
    """Test that multiple events use the maximum exclusion type."""
    events = [
        NodeEventInternal(
            node="test-node-1",
            timestamp=datetime.now(),
            job_id="123",
            job_attempt_index=1,
            job_start_time=datetime.now(),
            job_end_time=datetime.now(),
            cluster="test-cluster",
            error_attribution=ErrorAttribution.SYSTEM,
            error_attribution_type="TEST_TYPE",
            error_attribution_origin="TEST_ORIGIN",
            error_attribution_sources=["TEST_SOURCE"],
            exclusion_type=ExclusionType.NONE,
            fact_status="TEST_STATUS",
        ),
        NodeEventInternal(
            node="test-node-2",
            timestamp=datetime.now(),
            job_id="123",
            job_attempt_index=1,
            job_start_time=datetime.now(),
            job_end_time=datetime.now(),
            cluster="test-cluster",
            error_attribution=ErrorAttribution.HARDWARE,
            error_attribution_type="TEST_TYPE",
            error_attribution_origin="TEST_ORIGIN",
            error_attribution_sources=["TEST_SOURCE"],
            exclusion_type=ExclusionType.CONFIRMED,
            fact_status="TEST_STATUS",
        ),
    ]

    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, events, []
    )
    assert summary == ATTRIBUTION_SUMMARY_CONFIG["infra"]["summary"]
    assert insight == ATTRIBUTION_INSIGHT_CONFIG["confirmed"]["insight"]


def test_storage_attribution_to_summary_only_global_events_application():
    """Test that global events with APPLICATION attribution return not_infra summary."""
    error = JobExecutionErrorInternal.create(
        code=1,
        message="global error",
        description="global error description",
        signature="global-sig",
    )

    global_event = GlobalEventInternal.create(
        timestamp=datetime.now(),
        job_id="test-job",
        job_attempt_index=0,
        error_attribution=ErrorAttribution.APPLICATION,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
        fact_status=FactStatus.OK,
    )
    global_event.job_execution_error = error

    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [], [global_event]
    )
    assert summary == ATTRIBUTION_SUMMARY_CONFIG["not_infra"]["summary"]
    assert insight == ATTRIBUTION_INSIGHT_CONFIG["none"]["insight"]


def test_storage_attribution_to_summary_only_global_events_system():
    """Test that global events with SYSTEM attribution return potential_infra summary."""
    error = JobExecutionErrorInternal.create(
        code=1,
        message="global error",
        description="global error description",
        signature="global-sig",
    )

    global_event = GlobalEventInternal.create(
        timestamp=datetime.now(),
        job_id="test-job",
        job_attempt_index=0,
        error_attribution=ErrorAttribution.SYSTEM,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
        fact_status=FactStatus.OK,
    )
    global_event.job_execution_error = error

    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [], [global_event]
    )
    assert summary == ATTRIBUTION_SUMMARY_CONFIG["potential_infra"]["summary"]
    assert insight == ATTRIBUTION_INSIGHT_CONFIG["none"]["insight"]


def test_storage_attribution_to_summary_mixed_node_and_global_events():
    """Test that both node and global events are considered for summary."""
    error = JobExecutionErrorInternal.create(
        code=1,
        message="mixed error",
        description="mixed error description",
        signature="mixed-sig",
    )

    # Node event with APPLICATION attribution
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.APPLICATION,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.SYSLOGS],
        exclusion_type=ExclusionType.TEMPORARY,
        fact_status=FactStatus.OK,
    )
    node_event.job_execution_error = error

    # Global event with SYSTEM attribution
    global_event = GlobalEventInternal.create(
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        error_attribution=ErrorAttribution.SYSTEM,
        error_attribution_type=ErrorAttributionType.FAST,
        error_attribution_origin=ErrorAttributionOrigin.FACT,
        error_attribution_sources=[ErrorAttributionSource.APPLICATION_LOGS],
        fact_status=FactStatus.OK,
    )
    global_event.job_execution_error = error

    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], [global_event]
    )
    # Should be potential_infra because global event has SYSTEM attribution
    assert summary == ATTRIBUTION_SUMMARY_CONFIG["potential_infra"]["summary"]
    # Insight based on node event's exclusion type
    assert insight == ATTRIBUTION_INSIGHT_CONFIG["temporary"]["insight"]


def test_storage_attribution_to_summary_running_job_state():
    """Test that RUNNING job state returns Unknown for both summary and insight."""
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.HARDWARE,
        error_attribution_type="TEST_TYPE",
        error_attribution_origin="TEST_ORIGIN",
        error_attribution_sources=["TEST_SOURCE"],
        exclusion_type=ExclusionType.CONFIRMED,
        fact_status="TEST_STATUS",
    )
    node_event.job_execution_error = JobExecutionErrorInternal(
        code="TEST_ERROR",
        message="Test error message",
        description="Test error description",
        signature="test-signature",
    )

    # Pass RUNNING job state - should return Unknown regardless of events
    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], [], job_state="RUNNING"
    )
    assert summary == "Unknown"
    assert insight == "Unknown"


def test_storage_attribution_to_summary_all_non_final_states():
    """Test that all non-final job states return Unknown for both summary and insight."""
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.HARDWARE,
        error_attribution_type="TEST_TYPE",
        error_attribution_origin="TEST_ORIGIN",
        error_attribution_sources=["TEST_SOURCE"],
        exclusion_type=ExclusionType.CONFIRMED,
        fact_status="TEST_STATUS",
    )
    node_event.job_execution_error = JobExecutionErrorInternal(
        code="TEST_ERROR",
        message="Test error message",
        description="Test error description",
        signature="test-signature",
    )

    # Test all non-final states
    for state in SlurmJobNonFinalStates:
        summary, insight = storage_attribution_to_summary(
            ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], [], job_state=state.value
        )
        assert summary == "Unknown", f"Failed for state {state.value}"
        assert insight == "Unknown", f"Failed for state {state.value}"


def test_storage_attribution_to_summary_all_final_job_states():
    """Test that all final job states allow normal summary/insight computation."""
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.HARDWARE,
        error_attribution_type="TEST_TYPE",
        error_attribution_origin="TEST_ORIGIN",
        error_attribution_sources=["TEST_SOURCE"],
        exclusion_type=ExclusionType.CONFIRMED,
        fact_status="TEST_STATUS",
    )
    node_event.job_execution_error = JobExecutionErrorInternal(
        code="TEST_ERROR",
        message="Test error message",
        description="Test error description",
        signature="test-signature",
    )

    # Test all final states - should compute normal summary/insight for each
    for state in SlurmJobFinalStates:
        summary, insight = storage_attribution_to_summary(
            ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], [], job_state=state.value
        )
        assert summary == ATTRIBUTION_SUMMARY_CONFIG["infra"]["summary"], f"Failed for state {state.value}"
        assert insight == ATTRIBUTION_INSIGHT_CONFIG["confirmed"]["insight"], f"Failed for state {state.value}"


def test_storage_attribution_to_summary_none_job_state():
    """Test that None job state allows normal summary/insight computation."""
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.HARDWARE,
        error_attribution_type="TEST_TYPE",
        error_attribution_origin="TEST_ORIGIN",
        error_attribution_sources=["TEST_SOURCE"],
        exclusion_type=ExclusionType.CONFIRMED,
        fact_status="TEST_STATUS",
    )
    node_event.job_execution_error = JobExecutionErrorInternal(
        code="TEST_ERROR",
        message="Test error message",
        description="Test error description",
        signature="test-signature",
    )

    # Pass None job state - should compute normal summary/insight
    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], [], job_state=None
    )
    assert summary == ATTRIBUTION_SUMMARY_CONFIG["infra"]["summary"]
    assert insight == ATTRIBUTION_INSIGHT_CONFIG["confirmed"]["insight"]


def test_storage_attribution_to_summary_backward_compatibility():
    """Test that calling without job_state parameter works (backward compatibility)."""
    node_event = NodeEventInternal(
        node="test-node",
        timestamp=datetime.now(),
        job_id="123",
        job_attempt_index=1,
        job_start_time=datetime.now(),
        job_end_time=datetime.now(),
        cluster="test-cluster",
        error_attribution=ErrorAttribution.HARDWARE,
        error_attribution_type="TEST_TYPE",
        error_attribution_origin="TEST_ORIGIN",
        error_attribution_sources=["TEST_SOURCE"],
        exclusion_type=ExclusionType.CONFIRMED,
        fact_status="TEST_STATUS",
    )
    node_event.job_execution_error = JobExecutionErrorInternal(
        code="TEST_ERROR",
        message="Test error message",
        description="Test error description",
        signature="test-signature",
    )

    # Call without job_state parameter - should compute normal summary/insight
    summary, insight = storage_attribution_to_summary(
        ATTRIBUTION_SUMMARY_CONFIG, ATTRIBUTION_INSIGHT_CONFIG, [node_event], []
    )
    assert summary == ATTRIBUTION_SUMMARY_CONFIG["infra"]["summary"]
    assert insight == ATTRIBUTION_INSIGHT_CONFIG["confirmed"]["insight"]


def test_sanitize_log_file_paths_first_attempt_no_validation():
    storage_mock = MagicMock()
    job_record = create_example_slurm_job_record(job_id="12345", attempt_index=0, state="RUNNING")
    job_record.log_file_paths = ["path/a.log", "path/b.log"]

    sanitize_log_file_paths_for_retry(job_record, storage_mock)

    assert job_record.log_file_paths == ["path/a.log", "path/b.log"]
    storage_mock.read_job_record.assert_not_called()


def test_sanitize_log_file_paths_preserves_declared_shared_retry_path():
    storage_mock = MagicMock()
    job_record = create_example_slurm_job_record(job_id="12345", attempt_index=1, state="RUNNING")
    job_record.aps_spec.log_file_paths_shared_across_attempts = True
    job_record.log_file_paths = ["path/shared.log"]

    sanitize_log_file_paths_for_retry(job_record, storage_mock)

    assert job_record.log_file_paths == ["path/shared.log"]
    storage_mock.read_job_record.assert_not_called()


def test_sanitize_log_file_paths_no_previous_attempt():
    storage_mock = MagicMock()
    storage_mock.read_job_record.return_value = None

    job_record = create_example_slurm_job_record(job_id="12345", attempt_index=1, state="RUNNING")
    job_record.log_file_paths = ["path/a.log"]

    sanitize_log_file_paths_for_retry(job_record, storage_mock)

    assert job_record.log_file_paths == ["path/a.log"]
    storage_mock.read_job_record.assert_called_once_with(job_id="12345", job_attempt_index=0)


def test_sanitize_log_file_paths_drops_only_duplicate_keeps_unique():
    # The shared "path/a.log" (like the stable batch StdOut slurm-<jobid>.out that
    # is the same file every attempt) is dropped, but the unique per-attempt
    # "path/new.log" (the timestamped application log) is kept -- so HANGNOOUTPUT
    # and other log_file_paths-gated detection still works on the requeued attempt.
    storage_mock = MagicMock()
    previous = create_example_slurm_job_record(job_id="12345", attempt_index=0, state="FAILED")
    previous.log_file_paths = ["path/a.log", "path/old.log"]
    storage_mock.read_job_record.return_value = previous

    job_record = create_example_slurm_job_record(job_id="12345", attempt_index=1, state="RUNNING")
    job_record.log_file_paths = ["path/a.log", "path/new.log"]

    sanitize_log_file_paths_for_retry(job_record, storage_mock)

    assert job_record.log_file_paths == ["path/new.log"]
    storage_mock.read_job_record.assert_called_once_with(job_id="12345", job_attempt_index=0)


def test_sanitize_log_file_paths_all_duplicate_clears_paths():
    # True requeue race: the entire previous list is carried over, so every current
    # path is a duplicate -> cleared to None (unchanged from prior behavior).
    storage_mock = MagicMock()
    previous = create_example_slurm_job_record(job_id="12345", attempt_index=0, state="FAILED")
    previous.log_file_paths = ["path/a.log", "path/b.log"]
    storage_mock.read_job_record.return_value = previous

    job_record = create_example_slurm_job_record(job_id="12345", attempt_index=1, state="RUNNING")
    job_record.log_file_paths = ["path/a.log", "path/b.log"]

    sanitize_log_file_paths_for_retry(job_record, storage_mock)

    assert job_record.log_file_paths is None
    storage_mock.read_job_record.assert_called_once_with(job_id="12345", job_attempt_index=0)


def test_sanitize_log_file_paths_no_duplicate_keeps_paths():
    storage_mock = MagicMock()
    previous = create_example_slurm_job_record(job_id="12345", attempt_index=0, state="FAILED")
    previous.log_file_paths = ["path/old.log"]
    storage_mock.read_job_record.return_value = previous

    job_record = create_example_slurm_job_record(job_id="12345", attempt_index=1, state="RUNNING")
    job_record.log_file_paths = ["path/new.log"]

    sanitize_log_file_paths_for_retry(job_record, storage_mock)

    assert job_record.log_file_paths == ["path/new.log"]
    storage_mock.read_job_record.assert_called_once_with(job_id="12345", job_attempt_index=0)


def test_sanitize_log_file_paths_none_current_with_previous_attempt():
    """When current job has no log_file_paths (None) but a previous attempt exists, no error should occur."""
    storage_mock = MagicMock()
    previous = create_example_slurm_job_record(job_id="12345", attempt_index=0, state="FAILED")
    previous.log_file_paths = ["path/old.log"]
    storage_mock.read_job_record.return_value = previous

    job_record = create_example_slurm_job_record(job_id="12345", attempt_index=1, state="RUNNING")
    job_record.log_file_paths = None

    sanitize_log_file_paths_for_retry(job_record, storage_mock)

    assert job_record.log_file_paths is None
    storage_mock.read_job_record.assert_called_once_with(job_id="12345", job_attempt_index=0)


# ============================================================================
# Advice Conversion Tests
# ============================================================================


def test_storage_to_http_advice_basic():
    """Test basic conversion of AdviceRecord to Advice."""
    now = datetime.now(UTC)
    desired_state_changed_at = now - timedelta(minutes=1)
    heimdall_id = uuid.uuid4()

    record = AdviceRecord(
        heimdall_id=heimdall_id,
        visible=True,
        job_id="test-job-123",
        scheduler_metadata={"namespace": "default", "kind": "MPIJob"},
        attempt_index=2,
        advice_state="Restarted",
        node_exclusions=["bad-node-1", "bad-node-2"],
        node_inclusions=None,
        issues=None,
        desired_state_changed_at=desired_state_changed_at,
        created_at=now,
        updated_at=now,
    )

    advice = storage_to_http_advice(record)

    assert advice.id == str(heimdall_id)
    assert advice.visible is True
    assert advice.job_id == "test-job-123"
    assert advice.restart_count == 2
    assert advice.scheduler_metadata == {"namespace": "default", "kind": "MPIJob"}
    assert advice.advice.state == AdviceState.RESTARTED
    assert advice.advice.node_constraints.exclusions == ["bad-node-1", "bad-node-2"]
    assert advice.advice.node_constraints.inclusions is None
    assert advice.issues is None
    assert advice.desired_state_changed_at == desired_state_changed_at
    assert advice.created_at == now
    assert advice.updated_at == now


def test_storage_to_http_advice_exposes_only_current_generation_status():
    """Only the status matching the current Advice generation is public."""
    now = datetime.now(UTC)
    advice_id = uuid.uuid4()
    record = AdviceRecord(
        heimdall_id=advice_id,
        visible=True,
        job_id="status-job",
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=0,
        advice_state="Restarted",
        generation=2,
        created_at=now,
        updated_at=now,
    )
    record.reconciliation_statuses.extend(
        [
            AdviceReconciliationStatusRecord(
                advice_id=advice_id,
                observed_generation=1,
                disposition=AdviceReconciliationDisposition.ABANDONED.value,
                reason=AdviceReconciliationReason.ADVICE_EXPIRED.value,
                observed_at=now,
                transitioned_at=now,
            ),
            AdviceReconciliationStatusRecord(
                advice_id=advice_id,
                observed_generation=2,
                disposition=AdviceReconciliationDisposition.CONVERGED.value,
                reason=AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
                observed_at=now,
                transitioned_at=now,
            ),
        ]
    )

    advice = storage_to_http_advice(record)

    assert advice.reconciliation_status is not None
    assert advice.reconciliation_status.observed_generation == 2
    assert advice.reconciliation_status.disposition == AdviceReconciliationDisposition.CONVERGED


def test_storage_to_http_advice_omits_status_when_current_generation_is_unobserved():
    """An older generation's history does not become the current status."""
    now = datetime.now(UTC)
    advice_id = uuid.uuid4()
    record = AdviceRecord(
        heimdall_id=advice_id,
        visible=True,
        job_id="reopened-job",
        scheduler_metadata={"scheduler": "slurm"},
        attempt_index=0,
        advice_state="Restarted",
        generation=2,
        created_at=now,
        updated_at=now,
    )
    record.reconciliation_statuses.append(
        AdviceReconciliationStatusRecord(
            advice_id=advice_id,
            observed_generation=1,
            disposition=AdviceReconciliationDisposition.CONVERGED.value,
            reason=AdviceReconciliationReason.DESIRED_STATE_OBSERVED.value,
            observed_at=now,
            transitioned_at=now,
        )
    )

    advice = storage_to_http_advice(record)

    assert advice.reconciliation_status is None


def test_storage_to_http_advice_with_issues():
    """Test conversion of AdviceRecord with issues."""
    now = datetime.now(UTC)
    heimdall_id = uuid.uuid4()

    record = AdviceRecord(
        heimdall_id=heimdall_id,
        visible=True,
        job_id="test-job-456",
        scheduler_metadata={},
        attempt_index=0,
        advice_state="Terminated",
        node_exclusions=None,
        node_inclusions=None,
        issues=[
            Issue(
                issue_type="CRASH",
                timestamp=now,
                relevant_log_lines=["ERROR: OOM killed", "Stack trace follows"],
            ),
            Issue(
                issue_type="HANG",
                timestamp=now,
                relevant_log_lines=None,
            ),
        ],
        created_at=now,
        updated_at=now,
    )

    advice = storage_to_http_advice(record)

    assert advice.advice.state == AdviceState.TERMINATED
    assert len(advice.issues) == 2
    assert advice.issues[0].issue_type == "CRASH"
    assert advice.issues[0].relevant_log_lines == ["ERROR: OOM killed", "Stack trace follows"]
    assert advice.issues[1].issue_type == "HANG"


def test_storage_to_http_advice_no_state():
    """Test conversion when advice_state is None."""
    now = datetime.now(UTC)

    record = AdviceRecord(
        heimdall_id=uuid.uuid4(),
        visible=False,
        job_id="test-job",
        scheduler_metadata={},
        attempt_index=0,
        advice_state=None,
        node_exclusions=None,
        node_inclusions=None,
        issues=None,
        created_at=now,
        updated_at=now,
    )

    advice = storage_to_http_advice(record)

    assert advice.advice.state is None
    assert advice.advice.node_constraints is None
    assert advice.visible is False


def test_http_to_storage_advice_basic():
    """Test basic conversion of Advice to AdviceRecord."""
    now = datetime.now(UTC)
    desired_state_changed_at = now - timedelta(minutes=1)
    advice_id = str(uuid.uuid4())

    advice = Advice(
        id=advice_id,
        visible=True,
        job_id="test-job-789",
        restart_count=1,
        scheduler_metadata={"namespace": "production"},
        advice=AdviceRecommendation(
            state=AdviceState.RESTARTED,
            node_constraints=NodeConstraints(
                exclusions=["node-x"],
                inclusions=["node-y"],
            ),
        ),
        issues=None,
        desired_state_changed_at=desired_state_changed_at,
        created_at=now,
        updated_at=now,
    )

    record = http_to_storage_advice(advice)

    assert record.heimdall_id == uuid.UUID(advice_id)
    assert record.visible is True
    assert record.job_id == "test-job-789"
    assert record.attempt_index == 1
    assert record.scheduler_metadata == {"namespace": "production"}
    assert record.advice_state == "Restarted"
    assert record.node_exclusions == ["node-x"]
    assert record.node_inclusions == ["node-y"]
    assert record.issues is None
    assert record.desired_state_changed_at == desired_state_changed_at


def test_http_to_storage_advice_with_issues():
    """Test conversion of Advice with issues."""
    now = datetime.now(UTC)
    advice_id = str(uuid.uuid4())

    advice = Advice(
        id=advice_id,
        visible=False,
        job_id="test-job",
        restart_count=0,
        scheduler_metadata={},
        advice=AdviceRecommendation(state=AdviceState.TERMINATED),
        issues=[
            Issue(
                issue_type="CRASH",
                timestamp=now,
                relevant_log_lines=["error line 1"],
                remediation_actions=[
                    RemediationAction(
                        action_type="restart",
                        timestamp=now,
                        reason="Pod crashed due to OOM",
                    ),
                ],
            ),
        ],
        created_at=now,
        updated_at=now,
    )

    record = http_to_storage_advice(advice)

    assert record.advice_state == "Terminated"
    assert record.visible is False
    assert len(record.issues) == 1
    assert record.issues[0].issue_type == "CRASH"
    assert record.issues[0].relevant_log_lines == ["error line 1"]
    assert len(record.issues[0].remediation_actions) == 1


def test_advice_roundtrip_conversion():
    """Test that converting Advice -> AdviceRecord -> Advice preserves data."""
    now = datetime.now(UTC)
    advice_id = str(uuid.uuid4())

    original_advice = Advice(
        id=advice_id,
        visible=True,
        job_id="roundtrip-test-job",
        restart_count=3,
        scheduler_metadata={"key": "value"},
        advice=AdviceRecommendation(
            state=AdviceState.RESTARTED,
            node_constraints=NodeConstraints(
                exclusions=["node-a", "node-b"],
            ),
        ),
        issues=[
            Issue(
                issue_type="STRAGGLER",
                timestamp=now,
                relevant_log_lines=["slow iteration"],
            ),
        ],
        created_at=now,
        updated_at=now,
    )

    # Convert to storage and back
    record = http_to_storage_advice(original_advice)
    converted_advice = storage_to_http_advice(record)

    # Verify key fields are preserved
    assert converted_advice.id == original_advice.id
    assert converted_advice.visible == original_advice.visible
    assert converted_advice.job_id == original_advice.job_id
    assert converted_advice.restart_count == original_advice.restart_count
    assert converted_advice.scheduler_metadata == original_advice.scheduler_metadata
    assert converted_advice.advice.state == original_advice.advice.state
    assert converted_advice.advice.node_constraints.exclusions == original_advice.advice.node_constraints.exclusions
    assert len(converted_advice.issues) == len(original_advice.issues)
    assert converted_advice.issues[0].issue_type == original_advice.issues[0].issue_type


@pytest.mark.parametrize("event_type", [EventType.CHECKPOINTLOAD, EventType.CHECKPOINTSAVE])
@pytest.mark.parametrize("has_existing_advice", [False, True])
def test_create_advice_issue_persists_checkpoints_without_update_audit(event_type, has_existing_advice):
    """Checkpoint issues persist and propagate, but checkpoint-only updates are not audited."""
    storage = MagicMock()
    job_record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    existing = create_example_advice_record(job_id="job-1", attempt_index=2)
    job_record.heimdall_job_id = existing.heimdall_id
    storage.read_advice.return_value = existing if has_existing_advice else None
    storage.upsert_advice.side_effect = lambda record, **_: (
        record,
        not has_existing_advice,
        existing if has_existing_advice else None,
    )
    audit_buffer = MagicMock()
    on_advice_persisted = MagicMock()

    create_advice_issue_from_event(
        storage,
        Event(body="checkpoint activity", type=event_type),
        job_record,
        timestamp_parser=lambda _: datetime(2025, 1, 1, tzinfo=UTC),
        on_advice_persisted=on_advice_persisted,
        audit_buffer=audit_buffer,
    )

    storage.upsert_advice.assert_called_once()
    persisted = storage.upsert_advice.call_args.args[0]
    assert persisted.issues[-1].issue_type == event_type.value
    assert persisted.issues[:-1] == (existing.issues if has_existing_advice else [])
    on_advice_persisted.assert_called_once_with(persisted, None)
    if has_existing_advice:
        audit_buffer.add.assert_not_called()
    else:
        audit_buffer.add.assert_called_once()
        assert audit_buffer.add.call_args.args[0].event_type == "advice_created"


def test_repair_malformed_anomaly_preserves_source_and_continues_without_duplicate_audit():
    """Malformed and unsupported legacy events cannot lose later causes or duplicate repaired Issues."""
    storage = MagicMock()
    audit_buffer = MagicMock()
    record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    record.heimdall_job_id = uuid.uuid4()
    original_time = datetime(2025, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)
    later_time = original_time + timedelta(seconds=1)
    later_event = Event(
        type=EventType.HANG,
        timestamp=later_time.isoformat(),
        body="Original generated cause",
        attributes=Attributes(job_id=record.job_id, uuid="later-anomaly", job_manager_generated=True),
    )
    record.anomalies = [
        JobAnomaly(
            anomaly_type=EventType.CRASH,
            timestamp=original_time,
            original_watcher_anomaly_event="{malformed-json",
        ),
        JobAnomaly(anomaly_type="UNSUPPORTED_LEGACY_TYPE", timestamp=original_time),
        JobAnomaly(
            anomaly_type=EventType.HANG,
            timestamp=later_time,
            original_watcher_anomaly_event=later_event.model_dump_json(),
        ),
    ]
    storage.read_advice.return_value = None

    def persist_advice(advice, **_):
        previous = storage.read_advice.return_value
        storage.read_advice.return_value = advice
        return advice, previous is None, previous

    storage.upsert_advice.side_effect = persist_advice

    create_advice_issues_from_anomalies(storage, record, audit_buffer=audit_buffer)

    advice = storage.read_advice.return_value
    assert advice.heimdall_id == record.heimdall_job_id
    assert advice.attempt_index == 2
    assert [(issue.issue_type, issue.timestamp, issue.detected_by) for issue in advice.issues] == [
        ("CRASH", original_time, "Heimdall Watcher"),
        ("HANG", later_time, "Heimdall Job Manager"),
    ]
    assert advice.issues[0].relevant_log_lines is None
    assert advice.issues[1].relevant_log_lines == [later_event.body]
    events = [call.args[0] for call in audit_buffer.add.call_args_list]
    assert [event.event_type for event in events] == ["advice_created", "advice_updated"]
    assert events[0].payload.creation_reasons == ["CRASH"]
    assert events[1].payload.change_reasons == ["HANG"]

    create_advice_issues_from_anomalies(storage, record, audit_buffer=audit_buffer)

    assert storage.upsert_advice.call_count == 2
    assert audit_buffer.add.call_count == 2
    assert len(storage.read_advice.return_value.issues) == 2


@pytest.mark.parametrize("failed_operation", ["read_advice", "upsert_advice"])
def test_anomaly_advice_repair_propagates_storage_failure(failed_operation):
    """Storage failures remain retryable errors, with no audit claiming a committed Issue."""
    storage = MagicMock()
    audit_buffer = MagicMock()
    record = create_example_slurm_job_record()
    record.heimdall_job_id = uuid.uuid4()
    record.anomalies = [JobAnomaly(anomaly_type=EventType.CRASH, timestamp=datetime(2025, 1, 1, tzinfo=UTC))]
    storage.read_advice.return_value = None
    error = RecoveryStorageException("Advice storage unavailable")
    getattr(storage, failed_operation).side_effect = error

    with pytest.raises(RecoveryStorageException) as raised:
        create_advice_issues_from_anomalies(storage, record, audit_buffer=audit_buffer)

    assert raised.value is error
    audit_buffer.add.assert_not_called()


def test_create_advice_issue_skips_without_timestamp():
    """Skips creating issues when no timestamp is available."""
    storage = MagicMock()
    job_record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    event = Event(body="boom", type=EventType.CRASH)

    create_advice_issue_from_event(
        storage,
        event,
        job_record,
        timestamp_parser=lambda _: None,
        audit_buffer=NoOpAuditBuffer(),
    )

    storage.read_advice.assert_not_called()
    storage.upsert_advice.assert_not_called()


def test_create_advice_issue_creates_new_advice():
    """Creates a new advice record with a single issue using heimdall_job_id."""
    storage = MagicMock()
    job_record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    known_heimdall_id = uuid.uuid4()
    job_record.heimdall_job_id = known_heimdall_id
    event = Event(body="boom", type=EventType.CRASH)
    now = datetime(2025, 1, 1, tzinfo=UTC)

    storage.read_advice.return_value = None

    create_advice_issue_from_event(
        storage,
        event,
        job_record,
        timestamp_parser=lambda _: now,
        audit_buffer=NoOpAuditBuffer(),
    )

    storage.read_advice.assert_called_once()
    storage.upsert_advice.assert_called_once()

    advice_record = storage.upsert_advice.call_args[0][0]
    assert advice_record.issues
    assert advice_record.heimdall_id == known_heimdall_id
    assert advice_record.issues[0].detected_by == "Heimdall Watcher"


def test_create_advice_issue_persists_explicit_detection_source():
    storage = MagicMock()
    job_record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    job_record.heimdall_job_id = uuid.uuid4()
    storage.read_advice.return_value = None

    create_advice_issue_from_event(
        storage,
        Event(body="State-based CRASH event for failed job", type=EventType.CRASH),
        job_record,
        timestamp_parser=lambda _: datetime(2025, 1, 1, tzinfo=UTC),
        detected_by="Heimdall Job Manager",
        audit_buffer=NoOpAuditBuffer(),
    )

    advice_record = storage.upsert_advice.call_args.args[0]
    assert advice_record.issues[0].detected_by == "Heimdall Job Manager"


def test_create_advice_issue_appends_to_existing():
    """Appends a new issue to an existing advice record."""
    storage = MagicMock()
    job_record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    known_heimdall_id = uuid.uuid4()
    job_record.heimdall_job_id = known_heimdall_id
    event = Event(body="boom", type=EventType.CRASH)
    now = datetime(2025, 1, 1, tzinfo=UTC)
    previous_timestamp = datetime(2024, 12, 31, tzinfo=UTC)

    storage.read_advice.return_value = AdviceRecord(
        heimdall_id=known_heimdall_id,
        visible=True,
        job_id=job_record.job_id,
        scheduler_metadata={},
        attempt_index=job_record.job_attempt_index,
        advice_state=None,
        node_exclusions=None,
        node_inclusions=None,
        issues=[
            Issue(
                issue_type="HANG",
                timestamp=previous_timestamp,
                detected_by="watcher",
                relevant_log_lines=["previous issue"],
            )
        ],
        created_at=previous_timestamp,
        updated_at=previous_timestamp,
    )

    create_advice_issue_from_event(
        storage,
        event,
        job_record,
        timestamp_parser=lambda _: now,
        audit_buffer=NoOpAuditBuffer(),
    )

    storage.upsert_advice.assert_called_once()
    advice_record = storage.upsert_advice.call_args[0][0]
    assert len(advice_record.issues) == 2
    assert advice_record.heimdall_id == known_heimdall_id


def test_create_advice_issue_reads_db_when_inmemory_missing():
    """Falls back to DB record when in-memory job_record has no heimdall_job_id (e.g., from_json() RabbitMQ path)."""
    storage = MagicMock()
    job_record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    job_record.heimdall_job_id = None  # simulates in-memory object from RabbitMQ

    db_heimdall_id = uuid.uuid4()
    db_record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    db_record.heimdall_job_id = db_heimdall_id
    storage.read_job_record.return_value = db_record

    event = Event(body="boom", type=EventType.CRASH)
    now = datetime(2025, 1, 1, tzinfo=UTC)

    storage.read_advice.return_value = None

    create_advice_issue_from_event(
        storage,
        event,
        job_record,
        timestamp_parser=lambda _: now,
        audit_buffer=NoOpAuditBuffer(),
    )

    storage.read_job_record.assert_called_once_with(
        job_id="job-1",
        job_attempt_index=2,
    )
    storage.upsert_advice.assert_called_once()
    advice_record = storage.upsert_advice.call_args[0][0]
    assert advice_record.heimdall_id == db_heimdall_id


def test_create_advice_issue_raises_when_no_heimdall_job_id_anywhere():
    """Logs a warning when heimdall_job_id is missing from both in-memory and DB records."""
    storage = MagicMock()
    job_record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    job_record.heimdall_job_id = None

    db_record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    db_record.heimdall_job_id = None
    storage.read_job_record.return_value = db_record

    event = Event(body="boom", type=EventType.CRASH)
    now = datetime(2025, 1, 1, tzinfo=UTC)

    # The ValueError should be caught by the outer except Exception block,
    # so the function should not raise — it should log and return.
    create_advice_issue_from_event(
        storage,
        event,
        job_record,
        timestamp_parser=lambda _: now,
        audit_buffer=NoOpAuditBuffer(),
    )

    storage.read_advice.assert_not_called()
    storage.upsert_advice.assert_not_called()


def test_maybe_persist_requeue_terminal_advice_skips_singleton_mode():
    storage = MagicMock()
    job_record = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=1,
        auto_resume_mode=AutoResumeModeEnum.SINGLETON_DEPENDENCY,
    )

    maybe_persist_requeue_terminal_advice(
        storage,
        job_record,
        audit_buffer=NoOpAuditBuffer(),
        system_requeue_config=SystemRequeueConfig(max_requeue_times=5),
        get_jobs=lambda **_kwargs: [],
    )

    storage.upsert_advice.assert_not_called()


def test_cancelled_singleton_job_requires_dag_abort_policy():
    storage = MagicMock()
    storage.read_advice_by_job.return_value = None
    job_record = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=0,
        state=SlurmJobFinalStates.CANCELLED.value,
        auto_resume_mode=AutoResumeModeEnum.SINGLETON_DEPENDENCY,
    )

    maybe_persist_requeue_terminal_advice(
        storage,
        job_record,
        audit_buffer=NoOpAuditBuffer(),
        system_requeue_config=SystemRequeueConfig(max_requeue_times=5),
        get_jobs=lambda **_kwargs: [],
    )

    storage.upsert_advice.assert_not_called()


@pytest.mark.parametrize(
    ("state", "auto_resume_mode", "allow_explicit_dag_abort"),
    [
        (SlurmJobFinalStates.CANCELLED.value, AutoResumeModeEnum.REQUEUE, False),
        (SlurmJobFinalStates.CANCELLED.value, AutoResumeModeEnum.SINGLETON_DEPENDENCY, True),
        (SlurmJobFinalStates.STOPPED.value, AutoResumeModeEnum.SINGLETON_DEPENDENCY, True),
    ],
)
def test_maybe_persist_explicit_abort_terminal_advice(state, auto_resume_mode, allow_explicit_dag_abort):
    storage = MagicMock()
    job_record = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=2,
        state=state,
        auto_resume_mode=auto_resume_mode,
    )
    storage.read_advice_by_job.return_value = None

    maybe_persist_requeue_terminal_advice(
        storage,
        job_record,
        audit_buffer=NoOpAuditBuffer(),
        system_requeue_config=SystemRequeueConfig(max_requeue_times=5),
        get_jobs=lambda **_kwargs: [],
        allow_explicit_dag_abort=allow_explicit_dag_abort,
    )

    storage.upsert_advice.assert_called_once()
    advice_record = storage.upsert_advice.call_args[0][0]
    assert advice_record.advice_state == AdviceState.TERMINATED.value
    assert advice_record.visible is True


@pytest.mark.parametrize(("attempt_index", "terminated"), [(4, False), (5, True)])
def test_failed_parent_only_terminates_after_requeue_limit(attempt_index, terminated):
    """A configured retry limit is retained as the cause of created termination Advice."""
    storage = MagicMock()
    audit_buffer = MagicMock()
    storage.read_advice_by_job.return_value = None
    storage.upsert_advice.side_effect = lambda advice, **_: (advice, True, None)
    job_record = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=attempt_index,
        state=SlurmJobFinalStates.FAILED.value,
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
        max_requeue_times=5,
    )

    maybe_persist_requeue_terminal_advice(
        storage,
        job_record,
        audit_buffer=audit_buffer,
        system_requeue_config=SystemRequeueConfig(max_requeue_times=75),
        get_jobs=lambda **_kwargs: [],
    )

    assert storage.upsert_advice.called is terminated
    if terminated:
        assert storage.upsert_advice.call_args.args[0].advice_state == AdviceState.TERMINATED.value
        audit_buffer.add.assert_called_once()
        event = audit_buffer.add.call_args.args[0]
        assert event.event_type == "advice_created"
        assert any(
            "attempt 5" in reason and "requeue limit of 5" in reason for reason in event.payload.creation_reasons
        )
    else:
        audit_buffer.add.assert_not_called()


@pytest.mark.parametrize("from_attribution", [False, True], ids=["observed-state", "attribution"])
@pytest.mark.parametrize(("attempt_index", "terminated"), [(1, False), (2, True)])
def test_checkpoint_stop_reason_survives_advice_update_audit(from_attribution, attempt_index, terminated):
    """Three failed attempts count the initial run plus two retries, and retain the decision cause."""
    storage = MagicMock()
    audit_buffer = MagicMock()
    attempts = [
        create_example_slurm_job_record(
            job_id="job-1",
            attempt_index=index,
            state=SlurmJobFinalStates.FAILED.value,
            auto_resume_mode=AutoResumeModeEnum.REQUEUE,
        )
        for index in range(attempt_index, -1, -1)
    ]
    job_record = attempts[0]
    existing = create_example_advice_record(
        job_id=job_record.job_id,
        attempt_index=attempt_index,
        advice_state=None,
        visible=False,
    )
    storage.read_job_record.return_value = job_record
    storage.read_advice_by_job.return_value = existing
    storage.upsert_advice.side_effect = lambda advice, **_: (advice, False, existing)
    config = SystemRequeueConfig(max_requeue_times=75, checkpoint_based_requeue_stop_threshold=3)
    get_jobs = MagicMock(return_value=[storage_to_http_job_status(attempt) for attempt in attempts])

    if from_attribution:
        attribution = create_example_anoamly_attribution_result(node_count=0)
        attribution.job_id = job_record.job_id
        attribution.job_attempt_index = attempt_index
        maybe_persist_job_released_terminal_advice_from_attribution(
            storage,
            attribution,
            audit_buffer=audit_buffer,
            exclusion_stickiness_enabled=False,
            get_previous_attempt=lambda _job_id, _attempt: None,
            system_requeue_config=config,
            get_jobs=get_jobs,
        )
    else:
        maybe_persist_requeue_terminal_advice(
            storage,
            job_record,
            audit_buffer=audit_buffer,
            system_requeue_config=config,
            get_jobs=get_jobs,
        )

    if not from_attribution and not terminated:
        storage.upsert_advice.assert_not_called()
        audit_buffer.add.assert_not_called()
        return

    storage.upsert_advice.assert_called_once()
    expected_state = AdviceState.TERMINATED if terminated else AdviceState.RESTARTED
    assert storage.upsert_advice.call_args.args[0].advice_state == expected_state.value
    audit_buffer.add.assert_called_once()
    event = audit_buffer.add.call_args.args[0]
    assert event.event_type == "advice_updated"
    if terminated:
        assert any(
            "checkpoint in the last 3 consecutive" in reason and "checkpoint-based early stop" in reason
            for reason in event.payload.change_reasons
        )
    else:
        assert any("requeue will continue" in reason for reason in event.payload.change_reasons)


def test_maybe_persist_requeue_terminal_advice_does_not_rewrite_terminal_cancellation():
    storage = MagicMock()
    job_record = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=2,
        state="CANCELLED",
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
    )
    storage.read_advice_by_job.return_value = http_to_storage_advice(
        Advice(
            id=str(uuid.uuid4()),
            visible=True,
            job_id="job-1",
            restart_count=2,
            scheduler_metadata={"scheduler": "slurm"},
            advice=AdviceRecommendation(state=AdviceState.TERMINATED),
        )
    )

    maybe_persist_requeue_terminal_advice(
        storage,
        job_record,
        audit_buffer=NoOpAuditBuffer(),
        system_requeue_config=SystemRequeueConfig(max_requeue_times=5),
        get_jobs=lambda **_kwargs: [],
    )

    storage.upsert_advice.assert_not_called()


def test_create_terminal_advice_merges_existing_issues():
    storage = MagicMock()
    heimdall_id = uuid.uuid4()
    existing_issue = Issue(
        issue_type="CRASH",
        timestamp=datetime(2025, 1, 1, tzinfo=UTC),
        relevant_log_lines=["line"],
    )
    storage.read_advice_by_job.return_value = http_to_storage_advice(
        Advice(
            id=str(heimdall_id),
            visible=False,
            job_id="job-1",
            restart_count=1,
            scheduler_metadata={},
            advice=AdviceRecommendation(state=None),
            issues=[existing_issue],
        )
    )
    job_record = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=1,
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
    )
    status = storage_to_http_job_status(job_record)

    create_terminal_advice_for_job(
        storage,
        audit_buffer=NoOpAuditBuffer(),
        job_id="job-1",
        job_attempt_index=1,
        terminal_advice=JobTerminalAdvice.USER_CANCELED,
        status=status,
    )

    advice_record = storage.upsert_advice.call_args[0][0]
    assert advice_record.issues[0] == existing_issue
    assert advice_record.issues[1].issue_type == JobTerminalAdvice.USER_CANCELED.value
    assert advice_record.issues[1].detected_by == "Heimdall Job Manager"
    assert advice_record.visible is True


def test_persist_dag_release_advice_targets_exact_attempt():
    storage = MagicMock()
    storage.read_advice_by_job.return_value = None
    storage.upsert_advice.side_effect = lambda advice, **_: (advice, True, None)
    job_record = create_example_slurm_job_record(job_id="job-1", attempt_index=0)

    advice = persist_dag_release_advice(storage, job_record, audit_buffer=MagicMock())

    storage.read_advice_by_job.assert_called_once_with(job_id="job-1", attempt_index=0)
    record = storage.upsert_advice.call_args.args[0]
    assert record.job_id == "job-1"
    assert record.attempt_index == 0
    assert record.advice_state == AdviceState.RELEASED.value
    assert record.scheduler_metadata["scheduler"] == "slurm"
    assert advice.advice.state == AdviceState.RELEASED


def test_persist_advice_span_restores_the_per_item_recovery_parent():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    storage = MagicMock()
    advice = Advice(
        id=str(uuid.uuid4()),
        visible=True,
        job_id="job-1",
        restart_count=2,
        scheduler_metadata={"scheduler": "slurm", "environment": "cluster-a"},
        advice=AdviceRecommendation(state=AdviceState.RESTARTED),
        recoveryContext=RecoveryContext(
            sequenceId=str(uuid.uuid4()),
            traceparent="00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
            triggeredAt=datetime.now(UTC),
            operation=RecoveryOperation.RESTART,
        ),
    )
    advice_record = http_to_storage_advice(advice)
    storage.upsert_advice.side_effect = lambda record, **_: UpsertAdviceResult(record, True, None)

    audit_buffer = MagicMock()
    with patch("job_manager.storage_utils._TRACER", tracer):
        persist_advice_record(storage, audit_buffer, advice_record)

    (span,) = exporter.get_finished_spans()
    assert span.name == "job.recovery.advice"
    assert span.context.trace_id == 0x1234567890ABCDEF1234567890ABCDEF
    assert span.parent is not None
    assert span.parent.span_id == 0x1234567890ABCDEF
    assert span.attributes["recovery.sequence.id"] == advice.recovery_context.sequence_id
    assert span.attributes["recovery.outcome"] == "applied"

    event = audit_buffer.add.call_args.args[0]
    assert event.payload.recovery_sequence_id == advice.recovery_context.sequence_id
    assert event.payload.trace_id == format(span.context.trace_id, "032x")
    assert event.payload.span_id == format(span.context.span_id, "016x")
    assert event.payload.advice["generation"] == span.attributes["advice.generation"]


def test_persist_dag_release_advice_preserves_existing_evidence():
    storage = MagicMock()
    issue = Issue(issue_type="CRASH", timestamp=datetime(2026, 8, 8, tzinfo=UTC))
    storage.read_advice_by_job.return_value = http_to_storage_advice(
        Advice(
            id=str(uuid.uuid4()),
            visible=False,
            job_id="job-1",
            restart_count=0,
            scheduler_metadata={"environment": "prod"},
            advice=AdviceRecommendation(state=AdviceState.RELEASED),
            issues=[issue],
            fact_attribution={"source": "test"},
        )
    )
    storage.upsert_advice.side_effect = lambda advice, **_: (advice, False, storage.read_advice_by_job.return_value)

    persist_dag_release_advice(
        storage,
        create_example_slurm_job_record(job_id="job-1", attempt_index=0),
        audit_buffer=MagicMock(),
    )

    record = storage.upsert_advice.call_args.args[0]
    assert record.visible is True
    assert record.issues == [issue]
    assert record.fact_attribution == {"source": "test"}
    assert record.scheduler_metadata == {"environment": "prod", "scheduler": "slurm"}


def test_persist_dag_release_advice_rejects_lifecycle_advice_replacement():
    storage = MagicMock()
    storage.read_advice_by_job.return_value = http_to_storage_advice(
        Advice(
            id=str(uuid.uuid4()),
            visible=True,
            job_id="job-1",
            restart_count=0,
            scheduler_metadata={"scheduler": "slurm"},
            advice=AdviceRecommendation(state=AdviceState.TERMINATED),
        )
    )

    with pytest.raises(ValueError, match="Cannot replace Terminated advice with RELEASED"):
        persist_dag_release_advice(
            storage,
            create_example_slurm_job_record(job_id="job-1", attempt_index=0),
            audit_buffer=MagicMock(),
        )

    storage.upsert_advice.assert_not_called()


@pytest.mark.parametrize(
    "job_scheduler_metadata, expected",
    [
        # Job not yet registered by the controller -> no metadata; must still be stamped.
        ({}, {"scheduler": "slurm"}),
        # Existing keys must be preserved alongside the stamp.
        ({"partition": "gpu"}, {"partition": "gpu", "scheduler": "slurm"}),
    ],
)
def test_create_terminal_advice_stamps_slurm_scheduler_metadata(job_scheduler_metadata, expected):
    """JM-authored advice must carry scheduler=slurm for controller ownership."""
    storage = MagicMock()
    storage.read_advice_by_job.return_value = None
    job_record = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=1,
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
    )
    status = storage_to_http_job_status(job_record)
    status.scheduler_metadata = job_scheduler_metadata

    create_terminal_advice_for_job(
        storage,
        audit_buffer=NoOpAuditBuffer(),
        job_id="job-1",
        job_attempt_index=1,
        terminal_advice=JobTerminalAdvice.USER_CANCELED,
        status=status,
    )

    advice_record = storage.upsert_advice.call_args[0][0]
    assert advice_record.scheduler_metadata == expected


def test_job_released_advice_targets_failed_attempt():
    storage = MagicMock()
    storage.read_advice_by_job.return_value = None
    job_record = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=1,
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
    )
    status = storage_to_http_job_status(job_record)

    create_terminal_advice_for_job(
        storage,
        audit_buffer=NoOpAuditBuffer(),
        job_id="job-1",
        job_attempt_index=1,
        terminal_advice=JobTerminalAdvice.JOB_RELEASED,
        status=status,
    )

    storage.read_advice_by_job.assert_called_once_with(job_id="job-1", attempt_index=1)
    advice_record = storage.upsert_advice.call_args[0][0]
    assert advice_record.attempt_index == 1


def test_maybe_persist_job_released_skips_slow_attribution():
    storage = MagicMock()
    attribution = MagicMock()
    attribution.attribution_type = ErrorAttributionType.SLOW

    maybe_persist_job_released_terminal_advice_from_attribution(
        storage,
        attribution,
        audit_buffer=NoOpAuditBuffer(),
        exclusion_stickiness_enabled=False,
        get_previous_attempt=lambda _job_id, _attempt: None,
        system_requeue_config=SystemRequeueConfig(max_requeue_times=5),
        get_jobs=lambda **_kwargs: [],
    )

    storage.read_job_record.assert_not_called()
    storage.upsert_advice.assert_not_called()


def test_maybe_persist_job_released_does_not_replace_terminated_advice():
    """Late attribution for an already-terminated attempt must not try to restart it.

    Terminated Advice is sticky: a user cancel or requeue-limit decision that
    landed first must win over attribution that arrives afterwards, without the
    attribution consumer issuing a write that storage rejects.
    """
    storage = MagicMock()
    storage.read_job_record.return_value = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=0,
        state=SlurmJobFinalStates.CANCELLED.value,
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
    )
    storage.read_advice_by_job.return_value = create_example_advice_record(
        job_id="job-1",
        attempt_index=0,
        advice_state=AdviceState.TERMINATED.value,
    )
    attribution = create_example_anoamly_attribution_result(node_count=1)
    attribution.job_id = "job-1"
    attribution.job_attempt_index = 0
    on_persisted = MagicMock()

    maybe_persist_job_released_terminal_advice_from_attribution(
        storage,
        attribution,
        audit_buffer=NoOpAuditBuffer(),
        exclusion_stickiness_enabled=False,
        get_previous_attempt=lambda _job_id, _attempt: None,
        system_requeue_config=SystemRequeueConfig(max_requeue_times=5),
        get_jobs=lambda **_kwargs: [],
        on_persisted=on_persisted,
    )

    storage.upsert_advice.assert_not_called()
    on_persisted.assert_not_called()


@pytest.mark.parametrize(
    ("attempt_index", "expected_terminal_advice"),
    [
        (4, JobTerminalAdvice.JOB_RELEASED),
        (5, JobTerminalAdvice.MAX_REQUEUE_REACHED),
    ],
)
def test_maybe_persist_job_released_enforces_requeue_limit(attempt_index, expected_terminal_advice):
    """Attribution Advice records the specific stop or continuation reason in its creation audit."""
    storage = MagicMock()
    audit_buffer = MagicMock()
    storage.read_job_record.return_value = create_example_slurm_job_record(
        job_id="job-1",
        attempt_index=attempt_index,
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
        max_requeue_times=5,
    )
    storage.read_advice_by_job.return_value = None
    storage.upsert_advice.side_effect = lambda advice, **_: (advice, True, None)
    attribution = create_example_anoamly_attribution_result(node_count=0)
    attribution.job_id = "job-1"
    attribution.job_attempt_index = attempt_index

    maybe_persist_job_released_terminal_advice_from_attribution(
        storage,
        attribution,
        audit_buffer=audit_buffer,
        exclusion_stickiness_enabled=False,
        get_previous_attempt=lambda _job_id, _attempt: None,
        system_requeue_config=SystemRequeueConfig(max_requeue_times=75),
        get_jobs=lambda **_kwargs: [],
    )

    storage.upsert_advice.assert_called_once()
    audit_buffer.add.assert_called_once()
    event = audit_buffer.add.call_args.args[0]
    assert event.event_type == "advice_created"
    if expected_terminal_advice == JobTerminalAdvice.MAX_REQUEUE_REACHED:
        assert storage.upsert_advice.call_args.args[0].advice_state == AdviceState.TERMINATED.value
        assert any(
            "attempt 5" in reason and "requeue limit of 5" in reason for reason in event.payload.creation_reasons
        )
    else:
        assert storage.upsert_advice.call_args.args[0].advice_state == AdviceState.RESTARTED.value
        assert any("requeue will continue" in reason for reason in event.payload.creation_reasons)


@pytest.mark.parametrize("state", [AdviceState.RESTARTED, AdviceState.TERMINATED, AdviceState.RELEASED])
def test_advice_persists_when_correlation_job_read_fails(state):
    """Optional tracing must not suppress any actionable Advice transition."""
    storage = MagicMock()
    storage.read_job_record.side_effect = RuntimeError("correlation unavailable")
    advice = create_example_advice_record()
    advice.advice_state = state.value
    storage.upsert_advice.return_value = UpsertAdviceResult(advice, True, None)
    result = persist_advice_record(storage, NoOpAuditBuffer(), advice)
    assert result[0] is advice
    storage.upsert_advice.assert_called_once_with(advice, issue_merge=AdviceIssueMerge.REPLACE)


def test_dag_advice_starts_at_dependency_decision_with_its_scheduler_scope():
    """An old held job's submission/end time must not inflate DAG decision latency."""
    storage = MagicMock()
    job = create_example_slurm_job_record(job_id="child", attempt_index=0)
    job.scheduler_metadata = {"scheduler": "slurm", "environment": "stale-scope"}
    job.end_time = datetime(2020, 1, 1, tzinfo=UTC)
    storage.read_job_record.return_value = job
    advice = create_example_advice_record(job_id="child", attempt_index=0)
    advice.advice_state = AdviceState.RELEASED.value
    advice.scheduler_metadata = {"scheduler": "slurm", "environment": "advice-scope"}
    storage.upsert_advice.return_value = UpsertAdviceResult(advice, True, None)
    with patch("job_manager.storage_utils.RecoveryService") as service:
        service.return_value.begin_for_job.return_value = RecoveryContext(
            sequence_id=str(uuid.uuid4()),
            traceparent="00-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
            triggered_at=datetime.now(UTC),
            operation=RecoveryOperation.DAG_RELEASE,
        )
        persist_advice_record(storage, NoOpAuditBuffer(), advice)
    kwargs = service.return_value.begin_for_job.call_args.kwargs
    assert kwargs["triggered_at"] is None
    assert kwargs["scheduler_metadata"]["environment"] == "advice-scope"
    assert advice.recovery_sequence_id is not None


def test_advice_operation_change_replaces_inherited_recovery_context():
    """Restart context cannot parent a later termination generation's observation."""
    storage = MagicMock()
    job = create_example_slurm_job_record(job_id="job", attempt_index=0)
    storage.read_job_record.return_value = job
    advice = create_example_advice_record(job_id="job", attempt_index=0)
    advice.advice_state = AdviceState.TERMINATED.value
    old_sequence = uuid.uuid4()
    advice.recovery_sequence_id = old_sequence
    advice.recovery_operation = "restart"
    advice.recovery_traceparent = "00-11111111111111111111111111111111-1111111111111111-01"
    storage.upsert_advice.return_value = UpsertAdviceResult(advice, False, None)
    replacement = RecoveryContext(
        sequence_id=str(uuid.uuid4()),
        traceparent="00-22222222222222222222222222222222-2222222222222222-01",
        triggered_at=datetime.now(UTC),
        operation=RecoveryOperation.TERMINATE,
    )
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    with (
        patch("job_manager.storage_utils.RecoveryService") as service,
        patch("job_manager.storage_utils._TRACER", provider.get_tracer("test")),
    ):
        service.return_value.begin_for_job.return_value = replacement
        persist_advice_record(storage, NoOpAuditBuffer(), advice)
    (span,) = exporter.get_finished_spans()
    assert str(advice.recovery_sequence_id) == replacement.sequence_id
    assert advice.recovery_sequence_id != old_sequence
    assert span.context.trace_id == int("22222222222222222222222222222222", 16)
    assert span.attributes["recovery.operation"] == "terminate"
