# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Ensure deployed evidence cannot pass for another owner or an unpolled backend."""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from e2e_harness.advice_operator import AdviceOperatorServices
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
                "heimdallId": services.scope,
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
    assert services.endpoint.endswith("/advice/" + records[0]["advice_id"])


def test_publish_rejects_cross_namespace_handle(services, job):
    services.client.namespace = "different"
    with pytest.raises(ValueError, match="namespace"):
        services.publish(job, "ADVICE_STATE_RESTARTED")
    services._request.assert_not_called()


def test_deleted_owner_is_republished_after_deletion(services, job):
    from e2e_kubernetes.test_advice_operator import delete_owner_and_republish
    from recovery_libs.testing.scenario import Run

    launcher = Mock()
    operations = Mock()
    operations.attach_mock(launcher.cancel, "cancel")
    operations.attach_mock(services._request, "request")
    delete_owner_and_republish(Run({"job": job}, launcher, services))
    assert operations.mock_calls[0][0] == "cancel"
    assert operations.mock_calls[0].args == ()
    assert operations.mock_calls[0].kwargs == {"job_id": job.job_id}
    assert operations.mock_calls[1][0] == "request"
    assert operations.mock_calls[1].args[0] == "PUT"
    fixture = operations.mock_calls[1].args[1][0]
    assert fixture["scheduler_metadata"]["owner_uid"] == "uid-original"
