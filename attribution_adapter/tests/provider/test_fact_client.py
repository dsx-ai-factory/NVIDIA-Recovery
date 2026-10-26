# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import json
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import requests
from attribution_adapter.fact_client import FACTClient
from nvidia_attribution_client.models.attributor_info_v2 import AttributorInfoV2
from nvidia_attribution_client.models.source import Source
from recovery_libs.schema.anomaly_type import AnomalyType
from recovery_libs.schema.job_manager.models import JobAnomaly, SlurmJobExecutionStatus


def _settings(default_ranks_per_node=8):
    s = MagicMock()
    s.fact_url = "http://fact.test"
    s.fact_http_timeout_seconds = 30
    s.fact_default_ranks_per_node = default_ranks_per_node
    s.prometheus_hostname_label = "net_host_name"
    return s


def _declared_window(payload):
    interval = payload["context"]["time_interval"]
    return datetime.fromisoformat(interval["start"]), datetime.fromisoformat(interval["end"])


def _job_status():
    return SlurmJobExecutionStatus(
        job_id="123",
        job_attempt_index=0,
        workflow_id="workflow-test",
        cluster="c1",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 1, 1, 1, tzinfo=UTC),
        allocated_node_list=["nodeA", "nodeB"],
        node_count=2,
        total_gpu_count=16,
        state="FAILED",
        job_name="job",
        user="alice",
        command="srun x",
    )


def _wide_job_status():
    """Job status with a wide time window (2 hours) for STRAGGLER interest-window tests."""
    return SlurmJobExecutionStatus(
        job_id="456",
        job_attempt_index=0,
        workflow_id="workflow-test",
        cluster="c1",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 1, 1, 2, tzinfo=UTC),
        allocated_node_list=["nodeA"],
        node_count=1,
        total_gpu_count=8,
        state="RUNNING",
        job_name="job-wide",
        user="bob",
        command="srun y",
    )


# ---------------------------------------------------------------------------
# Existing tests (kept from original 3)
# ---------------------------------------------------------------------------


@patch("attribution_adapter.fact_client.requests.post")
def test_create_request_builds_v2_workload_info(mock_post):
    mock_post.return_value = MagicMock(
        ok=True, status_code=200, json=lambda: {"attributor_id": "att-1", "model_type": "NewResponseV2"}
    )
    client = FACTClient(_settings())
    att_id = client.create_request(
        _job_status(), JobAnomaly(anomaly_type="CRASH", timestamp=datetime(2026, 1, 1, tzinfo=UTC), uuid="u1")
    )
    assert att_id == "att-1"
    sent = AttributorInfoV2.from_json(mock_post.call_args.kwargs["data"])
    assert sent.workload.id == "123"
    # GLOBAL contiguous rank ids per node (16 gpu / 2 nodes -> rpn=8): nodeA [0-7], nodeB [8-15].
    assert sent.workload.nodes == {
        "nodeA": [0, 1, 2, 3, 4, 5, 6, 7],
        "nodeB": [8, 9, 10, 11, 12, 13, 14, 15],
    }
    assert sent.workload.status == "FAILED"
    assert sent.workload.timezone == "UTC"
    assert sent.metadata.agent == "Heimdall"
    # FACT v2 requires `parameters` present (a None value 500s server-side); regression guard.
    assert sent.parameters is not None


@pytest.mark.parametrize(
    ("state", "end_time", "expected_fact_status"),
    [
        ("RUNNING", datetime(2026, 1, 1, 1, tzinfo=UTC), "FAILED"),
        ("RUNNING", None, "RUNNING"),
        ("CANCELLED", datetime(2026, 1, 1, 1, tzinfo=UTC), "CANCELLED"),
    ],
)
def test_create_request_normalizes_running_with_end_time_only_for_fact(state, end_time, expected_fact_status):
    job = _job_status()
    job.state = state
    job.end_time = end_time

    with patch("attribution_adapter.fact_client.requests.post") as mock_post:
        mock_post.return_value = MagicMock(
            ok=True, status_code=200, json=lambda: {"attributor_id": "att-1", "model_type": "NewResponseV2"}
        )
        FACTClient(_settings()).create_request(job, None)

    sent = AttributorInfoV2.from_json(mock_post.call_args.kwargs["data"])
    assert sent.workload.status == expected_fact_status
    assert job.state == state


@patch("attribution_adapter.fact_client.requests.post")
def test_create_request_nodes_use_global_rank_ids(mock_post):
    """nodes maps each node to GLOBAL contiguous rank ids (node i -> [i*rpn, (i+1)*rpn)).

    Per-node-local [0..rpn) would collide across nodes and break FACT's rank->node lookup
    (attribution_lib JobInfo uses node_to_ranks for rank->node resolution).
    """
    mock_post.return_value = MagicMock(
        ok=True, status_code=200, json=lambda: {"attributor_id": "att-1", "model_type": "NewResponseV2"}
    )
    job = SlurmJobExecutionStatus(
        job_id="999",
        job_attempt_index=0,
        workflow_id="workflow-test",
        cluster="c1",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 1, 1, 1, tzinfo=UTC),
        allocated_node_list=["n1", "n2", "n3", "n4"],
        node_count=4,
        total_gpu_count=16,  # -> ranks_per_node = 4
        state="FAILED",
        job_name="job",
        user="alice",
        command="x",
    )
    FACTClient(_settings()).create_request(job, None)
    sent = AttributorInfoV2.from_json(mock_post.call_args.kwargs["data"])
    assert sent.workload.nodes == {
        "n1": [0, 1, 2, 3],
        "n2": [4, 5, 6, 7],
        "n3": [8, 9, 10, 11],
        "n4": [12, 13, 14, 15],
    }


@patch("attribution_adapter.fact_client.requests.post")
def test_create_request_default_ranks_per_node_is_configurable(mock_post):
    """When node_count/total_gpu_count are unavailable, ranks-per-node comes from settings."""
    mock_post.return_value = MagicMock(
        ok=True, status_code=200, json=lambda: {"attributor_id": "att-1", "model_type": "NewResponseV2"}
    )
    job = SlurmJobExecutionStatus(
        job_id="888",
        job_attempt_index=0,
        workflow_id="workflow-test",
        cluster="c1",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 1, 1, 1, tzinfo=UTC),
        allocated_node_list=["only"],
        node_count=None,
        total_gpu_count=None,
        state="FAILED",
        job_name="job",
        user="alice",
        command="x",
    )
    FACTClient(_settings(default_ranks_per_node=4)).create_request(job, None)
    sent = AttributorInfoV2.from_json(mock_post.call_args.kwargs["data"])
    assert sent.workload.nodes == {"only": [0, 1, 2, 3]}


@patch("attribution_adapter.fact_client.requests.post")
def test_supply_logs_posts_v2_entry_timeseries(mock_post):
    """Loki streams become V2 entries that keep each line's time, node and replica."""
    mock_post.return_value = MagicMock(
        ok=True, status_code=201, text='{"observation_id": 42, "model_type": "ObservationResponseV2"}'
    )
    streams = [
        {
            "stream": {"node_name": "nodeA", "pod": "worker-0"},
            "values": [
                ["1767225600000000001", "[rank0] CUDA error"],
                ["1767225601000000000", '{"body": "[rank1] NCCL timeout"}'],
            ],
        },
        {"stream": {}, "values": [["1767225602000000000", "[rank2] unscoped"]]},
    ]
    start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 1, tzinfo=UTC)
    obs_id = FACTClient(_settings()).supply_logs("att-1", json.dumps(streams), Source.APPLOG, start, end)

    assert obs_id == 42
    assert mock_post.call_args.args[0] == "http://fact.test/v2/attributor/att-1/observation"
    payload = json.loads(mock_post.call_args.kwargs["data"])
    assert _declared_window(payload) == (start, end)
    assert payload["context"]["source"] == "applog"
    node_a = [{"kind": "node", "hostname": "nodeA"}, {"kind": "replica", "name": "worker-0"}]
    assert json.loads(payload["body"]) == [
        {"body": "[rank0] CUDA error", "ts": 1767225600000000001, "resources": node_a},
        {"body": "[rank1] NCCL timeout", "ts": 1767225601000000000, "resources": node_a},
        {"body": "[rank2] unscoped", "ts": 1767225602000000000, "resources": None},
    ]


@patch("attribution_adapter.fact_client.requests.get")
def test_get_attribution_returns_v2(mock_get):
    mock_get.return_value = MagicMock(
        ok=True,
        status_code=200,
        text=(
            '{"model_type":"AttributionResponseV2","verdict":{"attribution_outcome":"Pass","terminal_states":["Pass"],'
            '"detail":"Not Applicable","root_symptom":{"symptom":"the process completes",'
            '"source":{"observation_id":1,"source":"slurm_info","pos":null,"line":"COMPLETED","rule":"r","time":null}}},'
            '"symptoms":{"the process completes":[{"type":"ResourcelessSource","status":"bad",'
            '"source":{"observation_id":1,"source":"slurm_info","pos":null,"line":"COMPLETED","rule":"r","time":null}}]},'
            '"observation_context":[1]}'
        ),
    )
    client = FACTClient(_settings())
    resp = client.get_attribution("att-1")
    assert resp.verdict.attribution_outcome == "Pass"
    assert "the process completes" in resp.symptoms


# ---------------------------------------------------------------------------
# Fix 1a: 409 duplicate path
# ---------------------------------------------------------------------------


@patch("attribution_adapter.fact_client.requests.post")
def test_create_request_409_returns_existing_att_id(mock_post):
    """A 409 DuplicateAttributorError response causes create_request to return the existing att_id."""
    mock_post.return_value = MagicMock(
        ok=False,
        status_code=409,
        text='{"model_type":"DuplicateAttributorError","att_id":"existing-att"}',
    )
    client = FACTClient(_settings())
    result = client.create_request(_job_status())
    assert result == "existing-att"


def test_parse_duplicate_attribution_error_valid():
    """_parse_duplicate_attribution_error returns att_id from a valid JSON payload."""
    client = FACTClient(_settings())
    att_id = client._parse_duplicate_attribution_error(
        '{"model_type": "DuplicateAttributorError", "att_id": "existing-att"}'
    )
    assert att_id == "existing-att"


def test_parse_duplicate_attribution_error_non_json_returns_none():
    """_parse_duplicate_attribution_error returns None for non-JSON input."""
    client = FACTClient(_settings())
    result = client._parse_duplicate_attribution_error("not json at all")
    assert result is None


# ---------------------------------------------------------------------------
# Fix 1b: Error/raise paths (parametrized)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method,args,patch_target",
    [
        (
            "create_request",
            (lambda js: (js,), lambda client: client.create_request),
            "attribution_adapter.fact_client.requests.post",
        ),
        (
            "supply_logs",
            (
                lambda js: (
                    "att-1",
                    '[{"stream":{},"values":[]}]',
                    Source.APPLOG,
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 1, 1, tzinfo=UTC),
                ),
                lambda client: client.supply_logs,
            ),
            "attribution_adapter.fact_client.requests.post",
        ),
        (
            "get_attribution",
            (lambda js: ("att-1",), lambda client: client.get_attribution),
            "attribution_adapter.fact_client.requests.get",
        ),
    ],
)
def test_non_ok_response_raises_request_exception(method, args, patch_target):
    """A non-ok HTTP response (status 500) raises requests.exceptions.RequestException."""
    job_status = _job_status()
    client = FACTClient(_settings())
    positional_args, method_getter = args
    with patch(patch_target) as mock_http:
        mock_http.return_value = MagicMock(ok=False, status_code=500, text="Server error")
        with pytest.raises(requests.exceptions.RequestException):
            method_getter(client)(*positional_args(job_status))


# ---------------------------------------------------------------------------
# Fix 1c: STRAGGLER interest-window
# ---------------------------------------------------------------------------


@patch("attribution_adapter.fact_client.requests.post")
def test_straggler_interest_window_bounded_by_job_times(mock_post):
    """create_request with STRAGGLER anomaly sets interest window to [max(T-30m, job_start), min(T, job_end)]."""
    mock_post.return_value = MagicMock(
        ok=True,
        status_code=200,
        json=lambda: {"attributor_id": "att-straggler", "model_type": "NewResponseV2"},
    )
    job_status = _wide_job_status()
    job_start = job_status.start_time  # 2026-01-01 00:00 UTC
    job_end = job_status.end_time  # 2026-01-01 02:00 UTC

    # Anomaly at T=01:00 UTC — 30m before = 00:30, well within job window
    anomaly_timestamp = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    anomaly = JobAnomaly(
        anomaly_type=AnomalyType.STRAGGLER.value,
        timestamp=anomaly_timestamp,
        uuid=str(uuid.uuid4()),
    )
    client = FACTClient(_settings())
    att_id = client.create_request(job_status, anomaly)
    assert att_id == "att-straggler"

    sent = AttributorInfoV2.from_json(mock_post.call_args.kwargs["data"])
    expected_interest_start = max(anomaly_timestamp - timedelta(minutes=30), job_start)
    expected_interest_end = min(anomaly_timestamp, job_end)
    assert sent.workload.interest_start_time == expected_interest_start
    assert sent.workload.interest_end_time == expected_interest_end
    # Job times are unchanged
    assert sent.workload.job_start_time == job_start
    assert sent.workload.job_end_time == job_end


# ---------------------------------------------------------------------------
# Fix 1d: supply_metrics happy-path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("result_type", ["vector", "matrix"])
@patch("attribution_adapter.fact_client.requests.post")
def test_supply_metrics_happy_path(mock_post, result_type):
    """Prometheus samples retain their timestamps, values, labels and node identity."""
    mock_post.return_value = MagicMock(
        ok=True,
        status_code=200,
        text='{"observation_id": 7, "model_type":"ObservationResponseV2"}',
    )
    client = FACTClient(_settings())
    labels = {"__name__": "node_infiniband_link_downed_total", "net_host_name": "nodeA", "device": "mlx5_0"}
    series = {"metric": labels}
    if result_type == "vector":
        series["value"] = [1767225600, "1"]
    else:
        series["values"] = [[1767225600, "1"], [1767225660, "2"]]
    start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 1, tzinfo=UTC)
    result = client.supply_metrics(
        "att-1", {"resultType": result_type, "result": [series]}, Source.NODE_METRICS, start, end
    )
    assert result == 7
    assert isinstance(result, int)
    # Verify correct endpoint, data encoding (not double-encoded JSON), and Content-Type header
    call_kwargs = mock_post.call_args.kwargs
    assert mock_post.call_args.args[0] == "http://fact.test/v2/attributor/att-1/observation"
    assert call_kwargs.get("headers") == {"Content-Type": "application/json"}
    assert "data" in call_kwargs
    assert "json" not in call_kwargs
    payload = json.loads(call_kwargs["data"])
    assert _declared_window(payload) == (start, end)
    assert payload["context"]["source"] == "node_metrics"

    def entry(ts, value):
        return {
            "body": (
                f"time: {ts}, metric_name: node_infiniband_link_downed_total, metric_value: {value}, "
                "threshold: None, cluster: None, node: nodeA, component: mlx5_0"
            ),
            "ts": ts * 1_000_000_000,
            "resources": [{"kind": "node", "hostname": "nodeA"}],
        }

    expected = [entry(1767225600, 1)]
    if result_type == "matrix":
        expected.append(entry(1767225660, 2))
    assert json.loads(payload["body"]) == expected


def test_supply_metrics_rejects_missing_node_identity():
    """A sample without a hostname must not become job-wide hardware evidence."""
    with (
        patch("attribution_adapter.fact_client.requests.post") as post,
        pytest.raises(ValueError, match="hostname"),
    ):
        FACTClient(_settings()).supply_metrics(
            "att-1",
            {"resultType": "vector", "result": [{"metric": {"__name__": "errors"}, "value": [1, "2"]}]},
            Source.NODE_METRICS,
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 1, 1, tzinfo=UTC),
        )
    post.assert_not_called()


@patch("attribution_adapter.fact_client.requests.post")
def test_create_request_serializes_rule_params_as_empty_dict(mock_post):
    """Wire-level regression guard for the FACT v2 422 (found via live e2e).

    fact-client 2.0.0rc2 serializes a default ``AttributionParameters()`` as
    ``{"rule_params": null}``, and fact-service v2.0.0-rc2 rejects null with a
    422 dict_type validation error. The client must send an explicit empty dict.
    """
    mock_post.return_value = MagicMock(
        ok=True, status_code=200, json=lambda: {"attributor_id": "att-1", "model_type": "NewResponseV2"}
    )
    client = FACTClient(_settings())
    client.create_request(
        _job_status(), JobAnomaly(anomaly_type="CRASH", timestamp=datetime(2026, 1, 1, tzinfo=UTC), uuid="u1")
    )
    payload = json.loads(mock_post.call_args.kwargs["data"])
    assert payload["parameters"]["rule_params"] == {}
