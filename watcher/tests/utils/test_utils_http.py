# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import httpx
import pytest
from watcher.lib.utils import http as http_module
from watcher.lib.utils.http import _make_response_hook, _normalize_path, get_async


class TestNormalizePath:
    @pytest.mark.parametrize(
        "url, expected",
        [
            ("http://host/v1/jobs/123", "/v1/jobs"),
            ("http://host/job_status/608070", "/job_status/608070"),
            ("http://host/a/b/c/d", "/a/b"),
            ("http://host/single", "/single"),
            ("http://host/", "/"),
            ("http://host", "/"),
        ],
    )
    def test_truncates_to_max_segments(self, url, expected):
        assert _normalize_path(urlparse(url)) == expected


class TestMakeResponseHook:
    def _fake_response(self, url="http://host:8080/foo/bar", status=200, method="GET"):
        resp = MagicMock()
        resp.request.url = url
        resp.request.method = method
        resp.status_code = status
        return resp

    async def test_increments_counter(self):
        hook = _make_response_hook("test_svc")
        with patch("watcher.lib.utils.http.HTTP_REQUESTS_TOTAL") as mock_counter:
            await hook(self._fake_response())
            mock_counter.labels.assert_called_once_with(
                method="GET",
                host="host",
                path="/foo/bar",
                service="test_svc",
                status_code=200,
            )
            mock_counter.labels.return_value.inc.assert_called_once()

    async def test_empty_service_default(self):
        hook = _make_response_hook("")
        with patch("watcher.lib.utils.http.HTTP_REQUESTS_TOTAL") as mock_counter:
            await hook(self._fake_response())
            assert mock_counter.labels.call_args.kwargs["service"] == ""

    async def test_does_not_raise_on_metric_error(self):
        hook = _make_response_hook("svc")
        with patch("watcher.lib.utils.http.HTTP_REQUESTS_TOTAL") as mock_counter:
            mock_counter.labels.side_effect = RuntimeError("boom")
            await hook(self._fake_response())


async def test_get_async_awaits_response_hook(mocker):
    """Regression: AsyncClient awaits hooks; a sync hook crashes with `await None` -> TypeError."""
    mocker.patch.object(http_module, "transport", httpx.MockTransport(lambda r: httpx.Response(200, content=b"{}")))
    assert await get_async("http://h/x") == b"{}"
