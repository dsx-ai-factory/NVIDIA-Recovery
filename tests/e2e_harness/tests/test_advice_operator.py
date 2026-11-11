# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Ensure deployed evidence cannot pass for another owner or an unpolled backend."""

import json
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from e2e_harness.advice_operator import AdviceOperatorServices
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.testing.launch.jobs import Job, JobState


@pytest.fixture
def job():
    """Return one exact Kubernetes incarnation."""
    return Job(
        job_id="recovery-dev/owner/uid-original",
        name="owner",
        state=JobState.RUNNING,
        nodes=(),
        exit_code=None,
        restart_count=None,
        submitted_at=datetime.now(UTC),
    )


@pytest.fixture
def services(tmp_path):
    """Inject transports so evidence checks need no cluster or fixture server."""
    client = Mock(namespace="recovery-dev")
    service = AdviceOperatorServices(client, "http://fixture.invalid", tmp_path)
    service._request = Mock(return_value={"reads": 2})
    service.advice_id = str(generate_heimdall_job_id("owner", 0))
    return service


def test_missing_advice_needs_successful_source_reads(services):
    services.client.get.return_value = None
    services._request.return_value = {"reads": 0}
    assert not services.absent().observed
    services._request.return_value = {"reads": 2}
    assert services.absent().observed


def test_existing_advice_is_not_absence(services):
    services.client.get.return_value = {"metadata": {"name": services.scope}}
    assert not services.absent().observed


def test_evidence_requires_exact_workload_incarnation(services, job):
    services.client.get.return_value = {
        "metadata": {
            "namespace": "recovery-dev",
            "ownerReferences": [
                {"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet", "name": "owner", "uid": "uid-original"}
            ],
        },
        "spec": {
            "job": {
                "heimdallId": services.advice_id,
                "apiVersion": "jobset.x-k8s.io/v1alpha2",
                "kind": "JobSet",
                "name": "owner",
                "jobId": "owner",
                "attemptIndex": 0,
            },
            "advice": {"state": "ADVICE_STATE_RESTARTED"},
        },
    }
    assert services.matches(job, "ADVICE_STATE_RESTARTED").observed
    services.client.get.return_value["metadata"]["ownerReferences"][0]["blockOwnerDeletion"] = False
    assert services.matches(job, "ADVICE_STATE_RESTARTED").observed
    services.client.get.return_value["metadata"]["ownerReferences"][0]["uid"] = "uid-replacement"
    assert not services.matches(job, "ADVICE_STATE_RESTARTED").observed


def test_publish_preserves_workload_identity_and_scope(services, job):
    services.publish(job, "ADVICE_STATE_RESTARTED")
    method, records = services._request.call_args.args
    assert method == "PUT"
    assert records[0]["job_id"] == "owner"
    assert records[0]["scheduler_metadata"]["owner_uid"] == "uid-original"
    assert records[0]["scheduler_metadata"]["namespace"] == "recovery-dev"
    assert services.endpoint.endswith("/advice/" + services.scope)
    assert records[0]["advice_id"] == "7bd71cdb-0aac-59b4-851d-ee3a94aaba2c"


def test_publish_rejects_cross_namespace_handle(services, job):
    services.client.namespace = "different"
    with pytest.raises(ValueError, match="namespace"):
        services.publish(job, "ADVICE_STATE_RESTARTED")
    services._request.assert_not_called()


def watch_event(services, kind, *, owner_uid="uid-original", version="11"):
    """Represent a Kubernetes watch event, including writes later removed by GC."""
    return {
        "type": kind,
        "object": {
            "metadata": {
                "name": services.advice_id,
                "resourceVersion": version,
                "ownerReferences": [
                    {"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet", "name": "owner", "uid": owner_uid}
                ],
            }
        },
    }


@pytest.mark.parametrize("kind", ["ADDED", "MODIFIED"])
def test_watch_catches_invalid_write_even_if_garbage_collected(services, kind):
    services.watch_version = "10"
    services.allowed_owner = ("jobset.x-k8s.io/v1alpha2", "JobSet", "owner", "uid-original")
    events = [
        watch_event(services, kind, owner_uid="wrong-uid"),
        watch_event(services, "DELETED", owner_uid="wrong-uid", version="12"),
    ]
    services.client.command.return_value = "\n".join(json.dumps(event) for event in events)
    services.client.get.return_value = None  # A periodic absence check would incorrectly pass.
    with pytest.raises(AssertionError, match="invalid owner"):
        services.absent()
    assert services.watch_error
    assert services.watch_events[0] == events[0]


def test_watch_allows_deletion_of_preexisting_valid_advice(services):
    services.watch_version = "10"
    services.allowed_owner = ("jobset.x-k8s.io/v1alpha2", "JobSet", "owner", "uid-original")
    services.client.command.return_value = json.dumps(watch_event(services, "DELETED"))
    services.client.get.return_value = None
    assert services.absent().observed
    assert services.watch_version == "11"


@pytest.mark.parametrize(
    "output",
    [
        '{"type":"ERROR","object":{"code":410,"reason":"Expired"}}',
        '{"type":"unexpected","object":{}}',
        "truncated json",
    ],
)
def test_watch_gaps_fail_closed_and_remain_failed(services, output):
    services.watch_version = "10"
    services.client.command.return_value = output
    services.client.get.return_value = None
    with pytest.raises(AssertionError, match="watch failed"):
        services.absent()
    services.client.command.return_value = ""
    with pytest.raises(AssertionError, match="watch failed"):
        services.absent()


def test_watch_uses_pre_mutation_list_cursor_for_empty_resource(services, job):
    services.client.command.side_effect = [
        json.dumps(
            {
                "apiVersion": "heimdall.nvidia.com/v1alpha1",
                "kind": "AdviceList",
                "metadata": {"resourceVersion": "42"},
                "items": [],
            }
        ),
        "",
    ]
    services.guard_owner(job)
    list_request = services.client.command.call_args_list[0].args
    assert list_request[:2] == ("get", "--raw")
    assert "/namespaces/recovery-dev/advices?fieldSelector=" in list_request[2]
    path = services.client.command.call_args.args[2]
    assert "resourceVersion=42" in path
    assert "watch=true" in path
    assert services.advice_id in path


def test_watch_transport_failure_is_retained(services):
    services.watch_version = "10"
    services.client.command.side_effect = RuntimeError("connection lost")
    with pytest.raises(AssertionError, match="connection lost"):
        services.absent()
    assert "connection lost" in services.watch_error


@pytest.mark.parametrize("invalid_write", [False, True])
def test_finishing_guard_checks_pending_events_before_positive_control(services, invalid_write):
    services.watch_version = "10"
    services.client.command.return_value = (
        json.dumps(watch_event(services, "ADDED", owner_uid="wrong-uid")) if invalid_write else ""
    )
    if invalid_write:
        with pytest.raises(AssertionError, match="invalid owner"):
            services.finish_owner_guard()
        assert services.watch_error
    else:
        services.finish_owner_guard()
        assert services.watch_version is None
