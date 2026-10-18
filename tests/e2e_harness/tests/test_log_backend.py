# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Backend-independent collection evidence with provider-specific wire fixtures."""

from __future__ import annotations

import json

import pytest
from e2e_harness.config import e2e_config_from_environment
from e2e_harness.log_backend import decode_log_response
from e2e_harness.recovery import evaluate_log_response, evaluate_system_log_response

START = "2026-07-14T12:00:00Z"
OBSERVED = "2026-07-14T12:00:01Z"
MARKER = 'marker."[123]\\value'
APPLICATION_LABELS = {
    "cluster": "slurm",
    "appName": "slurm-app-log",
    "job_id": "123",
    "filename": "/home/log-links/slurm-123.out",
}
SYSTEM_LABELS = {"cluster": "slurm", "component": "journal", "node_name": "e2e-a-0"}


def response(backend: str, labels: dict[str, str], message: str = MARKER, timestamp: str = OBSERVED) -> str:
    """Build wire evidence matching each store's documented response format."""

    if backend == "loki":
        nanoseconds = "1784030401000000000" if timestamp == OBSERVED else "1784030399000000000"
        return json.dumps(
            {
                "status": "success",
                "data": {"resultType": "streams", "result": [{"stream": labels, "values": [[nanoseconds, message]]}]},
            }
        )
    return json.dumps({"_time": timestamp, "_msg": message, "_stream": "{}", **labels}) + "\n"


def test_backend_defaults_to_loki() -> None:
    assert e2e_config_from_environment({}).log_backend == "loki"


@pytest.mark.parametrize("invalid", ("", "VictoriaLogs", "victoria", "unsupported"))
def test_config_rejects_unknown_backend(invalid: str) -> None:
    with pytest.raises(ValueError, match="invalid E2E_LOG_BACKEND"):
        e2e_config_from_environment({"E2E_LOG_BACKEND": invalid})


def test_config_backend_is_independent_of_profile_and_attribution() -> None:
    url = "http://victorialogs.monitoring.svc.cluster.local:9428"
    config = e2e_config_from_environment(
        {
            "E2E_LOG_BACKEND": "victorialogs",
            "E2E_PROFILE": "custom",
            "E2E_ATTRIBUTION_MODE": "available",
            "LOKI_URL": "http://unused",
            "VICTORIALOGS_URL": url,
        },
    )
    assert config.log_backend_url == url
    assert config.profile == "custom"
    assert config.attribution_mode == "available"
    assert config.victorialogs_url == url


@pytest.mark.parametrize(
    "backend,url",
    (
        ("victorialogs", "http://loki-gateway.monitoring.svc.cluster.local"),
        ("loki", "http://victorialogs.monitoring.svc.cluster.local:9428"),
        ("victorialogs", "http://elsewhere:9428"),
        ("victorialogs", "http://victorialogs.monitoring.svc.cluster.local:9428/other"),
    ),
)
def test_config_rejects_endpoint_mismatch(backend: str, url: str) -> None:
    key = "LOKI_URL" if backend == "loki" else "VICTORIALOGS_URL"
    with pytest.raises(ValueError, match="requires the local endpoint"):
        e2e_config_from_environment({"E2E_LOG_BACKEND": backend, key: url})


@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
@pytest.mark.parametrize("message", (MARKER, json.dumps({"cluster": "spoofed", "job_id": "other", "message": MARKER})))
def test_decoder_preserves_exact_message_and_collection_fields(backend: str, message: str) -> None:
    records = decode_log_response(response(backend, APPLICATION_LABELS, message), backend)
    assert len(records) == 1
    assert records[0].labels == APPLICATION_LABELS
    assert records[0].message == message
    assert records[0].observed_at == OBSERVED


@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
@pytest.mark.parametrize(
    "labels",
    (
        {**APPLICATION_LABELS, "job_id": "124"},
        {**APPLICATION_LABELS, "filename": ""},
        {**APPLICATION_LABELS, "appName": "other"},
    ),
)
def test_application_evidence_rejects_wrong_identity(backend: str, labels: dict[str, str]) -> None:
    evidence = evaluate_log_response(
        response(backend, labels), job_id="123", marker=MARKER, submitted_at=START, backend=backend
    )
    assert not evidence.observed


@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
def test_application_evidence_rejects_stale_marker(backend: str) -> None:
    evidence = evaluate_log_response(
        response(backend, APPLICATION_LABELS, timestamp="2026-07-14T11:59:59Z"),
        job_id="123",
        marker=MARKER,
        submitted_at=START,
        backend=backend,
    )
    assert not evidence.observed


@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
@pytest.mark.parametrize(
    "violation", ({"node_name": "wrong"}, {"job_id": "123"}, {"appName": ""}, {"job_attempt": "0"})
)
def test_system_evidence_rejects_contamination_even_after_valid_marker(backend: str, violation: dict[str, str]) -> None:
    valid = response(backend, SYSTEM_LABELS)
    invalid = response(backend, {**SYSTEM_LABELS, **violation})
    if backend == "loki":
        payload = json.loads(valid)
        payload["data"]["result"].extend(json.loads(invalid)["data"]["result"])
        combined = json.dumps(payload)
    else:
        combined = valid + invalid
    evidence = evaluate_system_log_response(
        combined, node_name="e2e-a-0", marker=MARKER, submitted_at=START, backend=backend
    )
    assert not evidence.observed
    assert "contract violations" in evidence.summary


@pytest.mark.parametrize(
    "backend,bad",
    (
        ("loki", "[]"),
        ("loki", '{"data": null}'),
        (
            "loki",
            '{"status":"success","data":{"resultType":"streams","result":[{"stream":{},"values":[["bad","marker"]]}]}}',
        ),
        ("victorialogs", "[]"),
        ("victorialogs", '{"_msg":"marker"}'),
        ("victorialogs", '{"_msg":"marker","_time":"2026-07-14T12:00:01"}'),
        ("victorialogs", '{"_msg":"marker","_time":"2026-07-14T12:00:01Z","job_id":123}'),
    ),
)
def test_decoder_rejects_malformed_responses(backend: str, bad: str) -> None:
    with pytest.raises(ValueError, match="malformed"):
        decode_log_response(bad, backend)


@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
def test_decoder_rejects_other_backend_wire_format(backend: str) -> None:
    other = "victorialogs" if backend == "loki" else "loki"
    with pytest.raises(ValueError, match="malformed"):
        decode_log_response(response(other, APPLICATION_LABELS), backend)


def test_victorialogs_truncated_tail_cannot_hide_behind_valid_marker() -> None:
    wire = response("victorialogs", APPLICATION_LABELS) + '{"_time":'
    evidence = evaluate_log_response(wire, job_id="123", marker=MARKER, submitted_at=START, backend="victorialogs")
    assert not evidence.observed
    assert "malformed" in evidence.summary


@pytest.mark.parametrize("backend", ("loki", "victorialogs"))
def test_saturated_response_cannot_hide_contaminated_records(backend: str) -> None:
    wire = response(backend, SYSTEM_LABELS)
    if backend == "loki":
        payload = json.loads(wire)
        payload["data"]["result"] *= 1000
        wire = json.dumps(payload)
    else:
        wire *= 1000
    evidence = evaluate_system_log_response(
        wire, node_name="e2e-a-0", marker=MARKER, submitted_at=START, backend=backend
    )
    assert not evidence.observed
    assert "may be truncated" in evidence.summary


def test_future_timestamp_cannot_satisfy_collection_evidence() -> None:
    evidence = evaluate_log_response(
        response("victorialogs", APPLICATION_LABELS, timestamp="2099-01-01T00:00:00Z"),
        job_id="123",
        marker=MARKER,
        submitted_at=START,
        backend="victorialogs",
    )
    assert not evidence.observed


@pytest.mark.parametrize("missing_field", ("status", "resultType"))
@pytest.mark.parametrize("system", (False, True))
def test_loki_missing_envelope_field_cannot_satisfy_collection_evidence(missing_field: str, system: bool) -> None:
    payload = json.loads(response("loki", SYSTEM_LABELS if system else APPLICATION_LABELS))
    if missing_field == "status":
        del payload[missing_field]
    else:
        del payload["data"][missing_field]
    wire = json.dumps(payload)

    with pytest.raises(ValueError, match="malformed loki response"):
        decode_log_response(wire, "loki")
    evidence = (
        evaluate_system_log_response(wire, node_name="e2e-a-0", marker=MARKER, submitted_at=START)
        if system
        else evaluate_log_response(wire, job_id="123", marker=MARKER, submitted_at=START)
    )
    assert not evidence.observed
    assert "malformed loki response" in evidence.summary
