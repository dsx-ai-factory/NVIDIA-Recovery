# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import collections
import json
import ssl
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import httpx
import requests
from requests.adapters import HTTPAdapter

from recovery_libs.log_query import QueryFailure, QueryFailureKind
from recovery_libs.loki_client.loki_log_fetcher import (
    LokiLogFetcher,
    LokiLogFetcherException,
    MtlsAdapter,
)


class LokiLogFetcherTests(unittest.TestCase):
    @patch.object(requests.Session, "get")
    def test_fetch_logs_success(self, mock_get):
        test_log_data = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp", "level": "INFO"},
                        "values": ["1669884500000000000", '{"body": "Sample log message 1"'],
                    },
                    {
                        "stream": {"app": "myotherapp", "level": "WARNING"},
                        "values": ["1669885500000000000", '{"body": "Sample log message 2"'],
                    },
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 2,
                    }
                },
            },
        }
        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = test_log_data
        mock_get.return_value = mock_response

        fetcher = LokiLogFetcher("http://test_loki_server")
        result = fetcher.fetch_logs(query="{app='myapp'}").streams

        self.assertEqual(result, test_log_data["data"]["result"])

    @patch.object(requests.Session, "get")
    def test_fetch_logs_returns_full_result(self, mock_get):
        test_log_data = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {
                            "app": "syslog",
                            "cluster": "devworker01",
                            "detected_level": "unknown",
                            "node_name": "fargate-ip-10-0-82-80.ec2.internal",
                            "service_name": "varlogs",
                        },
                        "values": [
                            ["1747417011272019084", "May 16 17:36:51 lepton-poc-001 systemd[1]: Failed to start gpud."],
                            [
                                "1747417011268900686",
                                "May 16 17:36:51 lepton-poc-001 systemd[1]: gpud.service: Failed with result 'exit-code'.",
                            ],
                        ],
                    }
                ],
                "stats": {"summary": {"totalEntriesReturned": 2}},
            },
        }
        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = test_log_data
        mock_get.return_value = mock_response

        fetcher = LokiLogFetcher("http://test_loki_server")
        result = fetcher.fetch_logs(query="{app='syslog'}").streams

        expected_result = [
            {
                "stream": {
                    "app": "syslog",
                    "cluster": "devworker01",
                    "detected_level": "unknown",
                    "node_name": "fargate-ip-10-0-82-80.ec2.internal",
                    "service_name": "varlogs",
                },
                "values": [
                    ["1747417011272019084", "May 16 17:36:51 lepton-poc-001 systemd[1]: Failed to start gpud."],
                    [
                        "1747417011268900686",
                        "May 16 17:36:51 lepton-poc-001 systemd[1]: gpud.service: Failed with result 'exit-code'.",
                    ],
                ],
            }
        ]

        self.assertEqual(result, expected_result)

    @patch.object(requests.Session, "get")
    def test_fetch_logs_success_with_stats(self, mock_get):
        test_log_data = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp", "level": "INFO"},
                        "values": ["1669884500000000000", '{"body": "Sample log message 1"'],
                    },
                    {
                        "stream": {"app": "myotherapp", "level": "WARNING"},
                        "values": ["1669885500000000000", '{"body": "Sample log message 2"'],
                    },
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 2,
                    }
                },
            },
        }
        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = test_log_data
        mock_get.return_value = mock_response

        fetcher = LokiLogFetcher("http://test_loki_server")
        result = fetcher.fetch_logs(query="{app='myapp'}")

        self.assertEqual(result.streams, test_log_data["data"]["result"])
        self.assertEqual(result.stats, [test_log_data["data"]["stats"]])

    @patch.object(requests.Session, "get")
    def test_fetch_logs_api_error(self, mock_get):
        mock_response = MagicMock(status_code=400)
        mock_response.text = "Invalid query parameter"
        mock_get.return_value = mock_response

        fetcher = LokiLogFetcher("http://test_loki_server")

        with self.assertRaises(LokiLogFetcherException) as cm:
            fetcher.fetch_logs(query="bad_query")

        self.assertEqual(str(cm.exception), "Error getting the loki logs Invalid query parameter")

    @patch.object(requests.Session, "get")
    def test_fetch_logs_empty_result(self, mock_get):
        test_log_data = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 0,
                    }
                },
            },
        }
        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = test_log_data
        mock_get.return_value = mock_response

        fetcher = LokiLogFetcher("http://test_loki_server")
        result = fetcher.fetch_logs().streams

        self.assertEqual(result, test_log_data["data"]["result"])

    @patch.object(requests.Session, "get")
    def test_fetch_logs_network_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("Simulated network failure")

        fetcher = LokiLogFetcher("http://test_loki_server")

        with self.assertRaises(LokiLogFetcherException):
            fetcher.fetch_logs()

    @patch.object(requests.Session, "get")
    def test_fetch_logs_pagination_small_chunks(self, mock_get):
        first_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp", "level": "INFO"},
                        "values": [["1669884500000000000", "Log message 1"], ["1669884550000000000", "Log message 2"]],
                    },
                    {
                        "stream": {"app": "myapp", "level": "DEBUG"},
                        "values": [["1669884600000000000", "Log message 3"]],
                    },
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 3,
                    }
                },
            },
        }
        second_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [{"stream": {"app": "otherapp"}, "values": [["1669885500000000000", "Log message 4"]]}],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 1,
                    }
                },
            },
        }

        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: first_response),
            MagicMock(status_code=200, json=lambda: second_response),
        ]

        fetcher = LokiLogFetcher("http://loki_server", auth_token="token", max_entries_per_request=3)

        result = fetcher.fetch_logs(
            query="{app='myapp'}",
            start_time=datetime.fromtimestamp(1669884500),
            end_time=datetime.fromtimestamp(1669900000),
        )

        mock_get.assert_has_calls(
            [
                unittest.mock.call(
                    "http://loki_server/loki/api/v1/query_range",
                    params={
                        "query": "{app='myapp'}",
                        "start": 1669884500.0,
                        "end": 1669900000.0,
                        "limit": 3,
                        "direction": "forward",
                    },
                    timeout=None,
                ),
                unittest.mock.call(
                    "http://loki_server/loki/api/v1/query_range",
                    params={
                        "query": "{app='myapp'}",
                        "start": (1669884600000000000 + 1) / 1e9,
                        "end": 1669900000.0,
                        "limit": 3,
                        "direction": "forward",
                    },
                    timeout=None,
                ),
            ]
        )

        expected_result = first_response["data"]["result"] + second_response["data"]["result"]
        self.assertEqual(result.streams, expected_result)

    @patch.object(requests.Session, "get")
    def test_fetch_logs_pagination_small_chunks_contiunation_timestamp(self, mock_get):
        first_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp", "level": "INFO"},
                        "values": [["1669884500000000000", "Log message 1"], ["1669884550000000000", "Log message 2"]],
                    },
                    {
                        "stream": {"app": "myapp", "level": "DEBUG"},
                        "values": [["1669884600000000000", "Log message 3"]],
                    },
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 3,
                    }
                },
            },
        }
        second_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [{"stream": {"app": "otherapp"}, "values": [["1669885500000000000", "Log message 4"]]}],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 1,
                    }
                },
            },
        }

        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: first_response),
            MagicMock(status_code=200, json=lambda: second_response),
        ]

        fetcher = LokiLogFetcher("http://loki_server", auth_token="token", max_entries_per_request=3)

        first_page = fetcher.fetch_logs(
            query="{app='myapp'}",
            start_time=datetime.fromtimestamp(1669884500),
            end_time=datetime.fromtimestamp(1669900000),
            max_num_pages=1,
        )

        mock_get.assert_has_calls(
            [
                unittest.mock.call(
                    "http://loki_server/loki/api/v1/query_range",
                    params={
                        "query": "{app='myapp'}",
                        "start": 1669884500.0,
                        "end": 1669900000.0,
                        "limit": 3,
                        "direction": "forward",
                    },
                    timeout=None,
                ),
            ]
        )

        self.assertEqual(first_page.continuation_timestamp, (1669884600000000000 + 1) / 1e9)
        self.assertEqual(first_page.streams, first_response["data"]["result"])

        second_page = fetcher.fetch_logs(
            query="{app='myapp'}",
            continuation_timestamp=first_page.continuation_timestamp,
            end_time=datetime.fromtimestamp(1669900000),
            max_num_pages=1,
        )
        mock_get.assert_has_calls(
            [
                unittest.mock.call(
                    "http://loki_server/loki/api/v1/query_range",
                    params={
                        "query": "{app='myapp'}",
                        "start": 1669884500.0,
                        "end": 1669900000.0,
                        "limit": 3,
                        "direction": "forward",
                    },
                    timeout=None,
                ),
            ]
        )

        self.assertEqual(second_page.continuation_timestamp, None)
        self.assertEqual(second_page.streams, second_response["data"]["result"])

    @patch.object(requests.Session, "get")
    def test_fetch_logs_with_headers(self, mock_get):
        test_log_data = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 0,
                    }
                },
            },
        }
        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = test_log_data
        mock_get.return_value = mock_response

        fetcher = LokiLogFetcher("http://test_loki_server")
        test_headers = {"X-Scope-OrgId": "test-org"}
        fetcher.fetch_logs(headers=test_headers)

        mock_get.assert_called_once_with(
            "http://test_loki_server/loki/api/v1/query_range",
            params={
                "query": "",
                "start": unittest.mock.ANY,
                "end": unittest.mock.ANY,
                "limit": 5000,
                "direction": "forward",
            },
            timeout=None,
            headers=test_headers,
        )


class TestLokiAsyncLogFetcher(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.loki_url = "http://test_loki_server"
        self.auth_token = "fake_token"
        self.max_entries_per_request = 5000
        self.fetcher = LokiLogFetcher(
            loki_url=self.loki_url,
            auth_token=self.auth_token,
            max_entries_per_request=self.max_entries_per_request,
        )
        self.query = "{app='myapp'}"

    @patch.object(LokiLogFetcher, "async_make_get_request", new_callable=AsyncMock)
    async def test_async_fetch_logs_success(self, mock_async_make_get_request):
        test_log_data = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp", "level": "INFO"},
                        "values": ["1669884500000000000", '{"body": "Sample log message 1"'],
                    },
                    {
                        "stream": {"app": "myotherapp", "level": "WARNING"},
                        "values": ["1669885500000000000", '{"body": "Sample log message 2"'],
                    },
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 2,
                    }
                },
            },
        }
        mock_async_make_get_request.return_value = test_log_data

        result = await self.fetcher.async_fetch_logs(query=self.query)
        logs, continuation = result.streams, result.continuation_timestamp

        self.assertEqual(len(logs), 2)
        self.assertEqual(logs, test_log_data["data"]["result"])
        self.assertEqual(logs[0]["values"], ["1669884500000000000", '{"body": "Sample log message 1"'])
        self.assertIsNone(continuation)

    @patch.object(LokiLogFetcher, "async_make_get_request", new_callable=AsyncMock)
    async def test_async_fetch_logs_multiple_pages(self, mock_async_make_get_request):
        mock_response_page_1 = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp", "level": "INFO"},
                        "values": ["1669884500000000000", '{"body": "Sample log message 1"'],
                    },
                    {
                        "stream": {"app": "myotherapp", "level": "WARNING"},
                        "values": ["1669885500000000000", '{"body": "Sample log message 2"'],
                    },
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 2,
                    }
                },
            },
        }
        mock_response_page_2 = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp", "level": "DEBUG"},
                        "values": ["1669884500000000000", '{"body": "Sample log message 3"'],
                    },
                    {
                        "stream": {"app": "myotherapp", "level": "ERROR"},
                        "values": ["1669885500000000000", '{"body": "Sample log message 4"'],
                    },
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 2,
                    }
                },
            },
        }
        mock_async_make_get_request.side_effect = [mock_response_page_1, mock_response_page_2]
        result = await self.fetcher.async_fetch_logs(query=self.query, max_num_pages=2)
        logs, continuation = result.streams, result.continuation_timestamp
        self.assertEqual(len(logs), 2)
        self.assertEqual(logs[0]["values"], ["1669884500000000000", '{"body": "Sample log message 1"'])
        self.assertEqual(logs[1]["values"], ["1669885500000000000", '{"body": "Sample log message 2"'])
        self.assertIsNone(continuation)

    @patch.object(LokiLogFetcher, "async_make_get_request", new_callable=AsyncMock)
    async def test_async_fetch_logs_with_error(self, mock_async_make_get_request):
        mock_async_make_get_request.side_effect = httpx.HTTPStatusError(
            "Error getting the loki logs", request=None, response=MagicMock(status_code=500)
        )

        with self.assertRaises(QueryFailure):
            await self.fetcher.async_fetch_logs(query=self.query)

    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_async_make_get_request_timeout_configurations(self, mock_get):
        """Test timeout configurations are correctly passed to httpx for float, tuple, and None."""
        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = {"status": "success"}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Float: applies to all phases
        await self.fetcher.async_make_get_request("http://test_loki/query", timeout=15.0)
        t = mock_get.call_args[1]["timeout"]
        self.assertIsInstance(t, httpx.Timeout)
        self.assertEqual((t.connect, t.read), (15.0, 15.0))

        # Tuple: (connect, read/write/pool)
        await self.fetcher.async_make_get_request("http://test_loki/query", timeout=(5.0, 30.0))
        t = mock_get.call_args[1]["timeout"]
        self.assertEqual((t.connect, t.read, t.write, t.pool), (5.0, 30.0, 30.0, 30.0))

        # None: no timeout
        await self.fetcher.async_make_get_request("http://test_loki/query", timeout=None)
        self.assertIsNone(mock_get.call_args[1]["timeout"])

    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_async_fetch_logs_failure_follows_query_failure_contract(self, mock_get):
        """Failures reach callers as the QueryFailure kind the shared backends raise for the same error."""
        request = httpx.Request("GET", "http://test_loki/loki/api/v1/query_range")
        cases = [
            (httpx.Response(401, request=request), QueryFailureKind.UNAUTHENTICATED),
            (httpx.Response(403, request=request), QueryFailureKind.FORBIDDEN_OR_TENANT),
            (httpx.Response(404, request=request), QueryFailureKind.INVALID_REQUEST),
            (httpx.Response(429, request=request), QueryFailureKind.RATE_LIMITED),
            (httpx.Response(503, request=request), QueryFailureKind.UNAVAILABLE),
            (
                httpx.Response(200, text="<html>Bad Gateway</html>", request=request),
                QueryFailureKind.MALFORMED_RESPONSE,
            ),
            (httpx.ReadTimeout("", request=request), QueryFailureKind.TIMEOUT),
        ]
        for outcome, expected_kind in cases:
            with self.subTest(expected_kind=expected_kind):
                if isinstance(outcome, BaseException):
                    mock_get.side_effect = outcome
                else:
                    mock_get.side_effect = None
                    mock_get.return_value = outcome

                with self.assertRaises(QueryFailure) as raised:
                    await self.fetcher.async_fetch_logs(query=self.query, query_id="hang_app_0")

                self.assertEqual(raised.exception.kind, expected_kind)
                self.assertEqual(raised.exception.query_id, "hang_app_0")


class TestLokiLogFetcherAuxFunctions(unittest.TestCase):
    def setUp(self):
        self.loki_url = "http://test_loki_server"
        self.auth_token = "fake_token"
        self.max_entries_per_request = 5000
        self.fetcher = LokiLogFetcher(
            loki_url=self.loki_url,
            auth_token=self.auth_token,
            max_entries_per_request=self.max_entries_per_request,
        )
        self.query = "{app='myapp'}"
        self.start_time = datetime.now() - timedelta(hours=24)
        self.end_time = datetime.now()

    def test_prepare_params(self):
        current_start = self.start_time.timestamp()
        current_end = self.end_time.timestamp()
        expected_params = {
            "query": self.query,
            "start": current_start,
            "end": current_end,
            "limit": self.fetcher.max_entries_per_request,
            "direction": "forward",
        }
        params = self.fetcher._prepare_params(self.query, current_start, current_end)
        self.assertEqual(params, expected_params)

    def test_prepare_params_forward_default(self):
        """Verify direction=forward is the default when not specified."""
        current_start = self.start_time.timestamp()
        current_end = self.end_time.timestamp()
        params = self.fetcher._prepare_params(self.query, current_start, current_end)
        self.assertEqual(params["direction"], "forward")

    def test_prepare_params_backward(self):
        """Verify direction=backward is passed through when specified."""
        current_start = self.start_time.timestamp()
        current_end = self.end_time.timestamp()
        params = self.fetcher._prepare_params(self.query, current_start, current_end, direction="backward")
        self.assertEqual(params["direction"], "backward")

    def test_prepare_params_with_limit(self):
        current_start = self.start_time.timestamp()
        current_end = self.end_time.timestamp()
        limit = 1
        expected_params = {
            "query": self.query,
            "start": current_start,
            "end": current_end,
            "limit": limit,
            "direction": "forward",
        }
        params = self.fetcher._prepare_params(self.query, current_start, current_end, limit)
        self.assertEqual(params, expected_params)

    def test_update_end_timestamp(self):
        """Verify _update_end_timestamp decrements by 1 nanosecond."""
        first_value_timestamp = "1625097603000000000"
        expected_end = (int(first_value_timestamp) - 1) / 1e9
        end = self.fetcher._update_end_timestamp(first_value_timestamp)
        self.assertEqual(end, expected_end)

    def test_update_start_timestamp(self):
        last_value_timestamp = "1625097603000000000"
        expected_start = (int(last_value_timestamp) + 1) / 1e9
        start = self.fetcher._update_start_timestamp(last_value_timestamp)
        self.assertEqual(start, expected_start)

    def test_process_response(self):
        response = {
            "data": {
                "result": [
                    {"values": [["1625097600000000000", "log line 1"], ["1625097601000000000", "log line 2"]]},
                ],
                "stats": {"summary": {"totalEntriesReturned": 2}},
            }
        }
        response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp", "level": "DEBUG"},
                        "values": ["1669884500000000000", '{"body": "Sample log message 3"'],
                    },
                    {
                        "stream": {"app": "myotherapp", "level": "ERROR"},
                        "values": ["1669885500000000000", '{"body": "Sample log message 4"'],
                    },
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 2,
                    }
                },
            },
        }
        expected_result = [
            {
                "stream": {"app": "myapp", "level": "DEBUG"},
                "values": ["1669884500000000000", '{"body": "Sample log message 3"'],
            },
            {
                "stream": {"app": "myotherapp", "level": "ERROR"},
                "values": ["1669885500000000000", '{"body": "Sample log message 4"'],
            },
        ]
        expected_number_of_events = 2
        result, number_of_events, stats = self.fetcher._process_response(response)
        self.assertEqual(result, expected_result)
        self.assertEqual(number_of_events, expected_number_of_events)
        self.assertEqual(stats, response["data"]["stats"])


class TestGetMaxTimestampFromStreams(unittest.TestCase):
    """Test cases for _get_max_timestamp_from_streams method."""

    def setUp(self):
        self.fetcher = LokiLogFetcher("http://test_loki_server")

    def test_single_stream_single_value(self):
        """Test with a single stream containing one log entry."""
        streams = [{"stream": {"host_name": "host1"}, "values": [["1669884500000000000", "log line 1"]]}]
        result = self.fetcher._get_max_timestamp_from_streams(streams)
        self.assertEqual(result, "1669884500000000000")

    def test_single_stream_multiple_values(self):
        """Test with a single stream containing multiple log entries."""
        streams = [
            {
                "stream": {"host_name": "host1"},
                "values": [
                    ["1669884500000000000", "log line 1"],
                    ["1669884600000000000", "log line 2"],
                    ["1669884700000000000", "log line 3"],
                ],
            }
        ]
        result = self.fetcher._get_max_timestamp_from_streams(streams)
        # Should return the last (most recent) timestamp
        self.assertEqual(result, "1669884700000000000")

    def test_multiple_streams_sorted_by_max_timestamp(self):
        """Test with multiple streams already sorted by their max timestamp."""
        streams = [
            {
                "stream": {"host_name": "host1"},
                "values": [["1669884500000000000", "log line 1"], ["1669884600000000000", "log line 2"]],
            },
            {
                "stream": {"host_name": "host2"},
                "values": [["1669884700000000000", "log line 3"], ["1669884800000000000", "log line 4"]],
            },
        ]
        result = self.fetcher._get_max_timestamp_from_streams(streams)
        # Should return the maximum across all streams
        self.assertEqual(result, "1669884800000000000")

    def test_multiple_streams_unsorted_last_stream_not_max(self):
        """
        Test with multiple streams where the last stream does NOT have the max timestamp.
        This is the critical case we're fixing.
        """
        streams = [
            {
                "stream": {"host_name": "host1"},
                "values": [
                    ["1669884500000000000", "log line 1"],
                    ["1669884900000000000", "log line 2"],  # This is the maximum!
                ],
            },
            {
                "stream": {"host_name": "host2"},
                "values": [
                    ["1669884600000000000", "log line 3"],
                    ["1669884700000000000", "log line 4"],  # Last stream, but NOT max
                ],
            },
        ]
        result = self.fetcher._get_max_timestamp_from_streams(streams)
        # Should return the maximum, which is from host1, not the last stream (host2)
        self.assertEqual(result, "1669884900000000000")

    def test_many_streams_with_varying_timestamps(self):
        """Test with many streams where max timestamp is in the middle."""
        streams = [
            {"stream": {"host_name": "host1"}, "values": [["1669884500000000000", "log"]]},
            {"stream": {"host_name": "host2"}, "values": [["1669884600000000000", "log"]]},
            {"stream": {"host_name": "host3"}, "values": [["1669885000000000000", "log"]]},  # Maximum in the middle
            {"stream": {"host_name": "host4"}, "values": [["1669884700000000000", "log"]]},
            {"stream": {"host_name": "host5"}, "values": [["1669884800000000000", "log"]]},
        ]
        result = self.fetcher._get_max_timestamp_from_streams(streams)
        self.assertEqual(result, "1669885000000000000")

    def test_realistic_scenario_from_production(self):
        """
        Test with realistic scenario from 24-hour pagination test.
        Last stream has timestamp 176 seconds earlier than the maximum.
        """
        # Simulating: host1 max_ts=1761533629.036446, host2 last_ts=1761533452.982905
        # Difference: 176.053541 seconds
        streams = [
            {
                "stream": {"host_name": "nvl72074-T06"},
                "values": [
                    ["1761533200000000000", "log 1"],
                    ["1761533629036446000", "log 2"],  # Max timestamp: 1761533629.036446
                ],
            },
            {
                "stream": {"host_name": "nvl72109-T09"},
                "values": [
                    ["1761533400000000000", "log 3"],
                    ["1761533452982905000", "log 4"],  # Last stream: 1761533452.982905 (176s earlier!)
                ],
            },
        ]
        result = self.fetcher._get_max_timestamp_from_streams(streams)
        # Should return the maximum from nvl72074-T06, not the last stream
        self.assertEqual(result, "1761533629036446000")

    def test_empty_streams_list(self):
        """Test with empty streams list."""
        streams = []
        with self.assertRaises(ValueError) as context:
            self.fetcher._get_max_timestamp_from_streams(streams)
        self.assertIn("No streams provided", str(context.exception))

    def test_streams_with_no_values(self):
        """Test with streams that have no values."""
        streams = [{"stream": {"host_name": "host1"}, "values": []}, {"stream": {"host_name": "host2"}, "values": []}]
        with self.assertRaises(ValueError) as context:
            self.fetcher._get_max_timestamp_from_streams(streams)
        self.assertIn("No values found", str(context.exception))

    def test_mixed_empty_and_nonempty_streams(self):
        """Test with some streams having values and others empty."""
        streams = [
            {"stream": {"host_name": "host1"}, "values": []},  # Empty
            {
                "stream": {"host_name": "host2"},
                "values": [["1669884500000000000", "log 1"], ["1669884600000000000", "log 2"]],
            },
            {"stream": {"host_name": "host3"}, "values": []},  # Empty
            {"stream": {"host_name": "host4"}, "values": [["1669884700000000000", "log 3"]]},
        ]
        result = self.fetcher._get_max_timestamp_from_streams(streams)
        # Should return max from non-empty streams
        self.assertEqual(result, "1669884700000000000")

    def test_string_vs_int_timestamp_comparison(self):
        """Test that string timestamps are correctly compared as integers."""
        streams = [
            {
                "stream": {"host_name": "host1"},
                "values": [["2000000000000000000", "log 1"]],  # String "2" < "9" lexicographically
            },
            {
                "stream": {"host_name": "host2"},
                "values": [["9000000000000000000", "log 2"]],  # But 9e18 > 2e18 numerically
            },
        ]
        result = self.fetcher._get_max_timestamp_from_streams(streams)
        # Should correctly identify 9e18 as larger
        self.assertEqual(result, "9000000000000000000")

    def test_very_close_timestamps(self):
        """Test with timestamps that differ by only microseconds."""
        streams = [
            {"stream": {"host_name": "host1"}, "values": [["1669884500000001000", "log 1"]]},  # ...001000 ns
            {
                "stream": {"host_name": "host2"},
                "values": [["1669884500000002000", "log 2"]],  # ...002000 ns (1 microsecond later)
            },
        ]
        result = self.fetcher._get_max_timestamp_from_streams(streams)
        self.assertEqual(result, "1669884500000002000")


class TestPaginationWithMultipleStreams(unittest.TestCase):
    """Integration tests for pagination with multiple streams (e.g., keep host_name)."""

    @patch.object(requests.Session, "get")
    def test_pagination_with_unsorted_streams(self, mock_get):
        """
        Test pagination where streams are not sorted by max timestamp.
        Ensures continuation_timestamp is correctly set to the maximum across all streams.
        """
        # Page 1: Last stream (host2) has older timestamp than host1
        first_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"host_name": "host1"},
                        "values": [
                            ["1669884500000000000", "log 1"],
                            ["1669884600000000000", "log 2"],
                            ["1669884900000000000", "log 3"],  # Maximum timestamp
                        ],
                    },
                    {
                        "stream": {"host_name": "host2"},
                        "values": [
                            ["1669884550000000000", "log 4"],
                            ["1669884700000000000", "log 5"],  # Last stream, but not max
                        ],
                    },
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 5,
                    }
                },
            },
        }

        # Page 2: Should start after the actual maximum (1669884900000000000)
        # Return 5 entries to trigger pagination check
        second_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"host_name": "host1"},
                        "values": [
                            ["1669884900100000000", "log 6"],
                            ["1669884900200000000", "log 7"],
                            ["1669884900300000000", "log 8"],
                            ["1669884900400000000", "log 9"],
                            ["1669884900500000000", "log 10"],  # 5 entries total
                        ],
                    }
                ],
                "stats": {
                    "summary": {
                        "totalEntriesReturned": 5,
                    }
                },
            },
        }

        mock_response_1 = MagicMock(status_code=200)
        mock_response_1.json.return_value = first_response
        mock_response_2 = MagicMock(status_code=200)
        mock_response_2.json.return_value = second_response

        mock_get.side_effect = [mock_response_1, mock_response_2]

        fetcher = LokiLogFetcher("http://loki_server", auth_token="token", max_entries_per_request=5)

        result = fetcher.fetch_logs(
            query="{cluster='test'}",
            start_time=datetime.fromtimestamp(1669884500),
            end_time=datetime.fromtimestamp(1669900000),
            max_num_pages=2,
        )

        # Verify we got logs from both pages
        self.assertEqual(len(result.streams), 3)  # 2 streams from page 1 + 1 stream from page 2 = 3 total

        # Verify the continuation timestamp was set correctly
        # Page 2 last timestamp: 1669884900500000000, so continuation should be:
        # (1669884900500000000 + 1) / 1e9
        self.assertEqual(result.continuation_timestamp, (1669884900500000000 + 1) / 1e9)

        # Verify second request used correct start timestamp
        second_call_params = mock_get.call_args_list[1][1]["params"]
        self.assertEqual(second_call_params["start"], (1669884900000000000 + 1) / 1e9)


class TestGetMinTimestampFromStreams(unittest.TestCase):
    """Test cases for _get_min_timestamp_from_streams method."""

    def setUp(self):
        self.fetcher = LokiLogFetcher("http://test_loki_server")

    def test_single_stream(self):
        """Test with a single stream — last value is the oldest in backward direction."""
        streams = [
            {
                "stream": {"host_name": "host1"},
                "values": [
                    ["1669884700000000000", "log line 3"],
                    ["1669884600000000000", "log line 2"],
                    ["1669884500000000000", "log line 1"],
                ],
            }
        ]
        result = self.fetcher._get_min_timestamp_from_streams(streams)
        self.assertEqual(result, "1669884500000000000")

    def test_multiple_streams(self):
        """Test with multiple streams — finds minimum across all streams."""
        streams = [
            {
                "stream": {"host_name": "host1"},
                "values": [
                    ["1669884700000000000", "log 3"],
                    ["1669884600000000000", "log 2"],
                ],
            },
            {
                "stream": {"host_name": "host2"},
                "values": [
                    ["1669884550000000000", "log 5"],
                    ["1669884400000000000", "log 4"],  # This is the minimum
                ],
            },
        ]
        result = self.fetcher._get_min_timestamp_from_streams(streams)
        self.assertEqual(result, "1669884400000000000")

    def test_empty_streams_list(self):
        """Test with empty streams list raises ValueError."""
        with self.assertRaises(ValueError) as context:
            self.fetcher._get_min_timestamp_from_streams([])
        self.assertIn("No streams provided", str(context.exception))

    def test_streams_with_no_values(self):
        """Test with streams that have no values raises ValueError."""
        streams = [{"stream": {"host_name": "host1"}, "values": []}]
        with self.assertRaises(ValueError) as context:
            self.fetcher._get_min_timestamp_from_streams(streams)
        self.assertIn("No values found", str(context.exception))

    def test_mixed_empty_and_nonempty_streams(self):
        """Test with some streams having values and others empty."""
        streams = [
            {"stream": {"host_name": "host1"}, "values": []},
            {
                "stream": {"host_name": "host2"},
                "values": [["1669884600000000000", "log 2"], ["1669884500000000000", "log 1"]],
            },
        ]
        result = self.fetcher._get_min_timestamp_from_streams(streams)
        self.assertEqual(result, "1669884500000000000")


class TestFetchLogsBackward(unittest.TestCase):
    """Test backward direction support in fetch_logs."""

    @patch.object(requests.Session, "get")
    def test_fetch_logs_backward_pagination(self, mock_get):
        """Verify current_end retreats between backward pages."""
        # Page 1: 3 entries (matches max_entries_per_request=3), triggers pagination
        first_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp"},
                        "values": [
                            ["1669884900000000000", "Log 3"],
                            ["1669884800000000000", "Log 2"],
                            ["1669884700000000000", "Log 1"],  # oldest in this page
                        ],
                    },
                ],
                "stats": {"summary": {"totalEntriesReturned": 3}},
            },
        }
        # Page 2: fewer entries, loop should end
        second_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp"},
                        "values": [
                            ["1669884600000000000", "Log B"],
                            ["1669884500000000000", "Log A"],
                        ],
                    },
                ],
                "stats": {"summary": {"totalEntriesReturned": 2}},
            },
        }

        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: first_response),
            MagicMock(status_code=200, json=lambda: second_response),
        ]

        fetcher = LokiLogFetcher("http://loki_server", max_entries_per_request=3)
        result = fetcher.fetch_logs(
            query="{app='myapp'}",
            start_time=datetime.fromtimestamp(1669884000),
            end_time=datetime.fromtimestamp(1669885000),
            direction="backward",
        )

        # Verify second request has retreated end timestamp
        second_call_params = mock_get.call_args_list[1][1]["params"]
        expected_end = (1669884700000000000 - 1) / 1e9
        self.assertAlmostEqual(second_call_params["end"], expected_end, places=10)
        self.assertEqual(second_call_params["direction"], "backward")

        # Verify result contains data from both pages
        self.assertEqual(len(result.streams), 2)  # 2 stream results

    @patch.object(requests.Session, "get")
    def test_fetch_logs_backward_continuation_return(self, mock_get):
        """Verify returns current_end when num_page == max_num_pages."""
        response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp"},
                        "values": [
                            ["1669884900000000000", "Log 3"],
                            ["1669884800000000000", "Log 2"],
                            ["1669884700000000000", "Log 1"],
                        ],
                    },
                ],
                "stats": {"summary": {"totalEntriesReturned": 3}},
            },
        }

        mock_get.return_value = MagicMock(status_code=200, json=lambda: response)

        fetcher = LokiLogFetcher("http://loki_server", max_entries_per_request=3)
        result = fetcher.fetch_logs(
            query="{app='myapp'}",
            start_time=datetime.fromtimestamp(1669884000),
            end_time=datetime.fromtimestamp(1669885000),
            max_num_pages=1,
            direction="backward",
        )

        # Should return current_end (retreated) as continuation
        expected_end = (1669884700000000000 - 1) / 1e9
        self.assertAlmostEqual(result.continuation_timestamp, expected_end, places=10)

    @patch.object(requests.Session, "get")
    def test_fetch_logs_backward_end_past_start(self, mock_get):
        """Verify loop breaks when current_end <= current_start."""
        response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp"},
                        "values": [
                            # Oldest timestamp is before start_time, should trigger guard
                            ["1669884100000000000", "Log 2"],
                            ["1669884000000000000", "Log 1"],
                        ],
                    },
                ],
                "stats": {"summary": {"totalEntriesReturned": 2}},
            },
        }

        mock_get.return_value = MagicMock(status_code=200, json=lambda: response)

        fetcher = LokiLogFetcher("http://loki_server", max_entries_per_request=2)
        result = fetcher.fetch_logs(
            query="{app='myapp'}",
            start_time=datetime.fromtimestamp(1669884000),
            end_time=datetime.fromtimestamp(1669885000),
            max_num_pages=5,
            direction="backward",
        )

        # Only one request should be made (guard breaks the loop)
        self.assertEqual(mock_get.call_count, 1)
        # continuation should be None since loop broke before num_page == max_num_pages
        self.assertIsNone(result.continuation_timestamp)

    @patch.object(requests.Session, "get")
    def test_fetch_logs_forward_unchanged(self, mock_get):
        """Regression test: forward direction still works as before."""
        first_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp"},
                        "values": [["1669884500000000000", "Log 1"], ["1669884600000000000", "Log 2"]],
                    },
                ],
                "stats": {"summary": {"totalEntriesReturned": 2}},
            },
        }
        second_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp"},
                        "values": [["1669884700000000000", "Log 3"]],
                    },
                ],
                "stats": {"summary": {"totalEntriesReturned": 1}},
            },
        }

        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: first_response),
            MagicMock(status_code=200, json=lambda: second_response),
        ]

        fetcher = LokiLogFetcher("http://loki_server", max_entries_per_request=2)
        fetcher.fetch_logs(
            query="{app='myapp'}",
            start_time=datetime.fromtimestamp(1669884000),
            end_time=datetime.fromtimestamp(1669885000),
            direction="forward",
        )

        # Second request should have advanced start timestamp
        second_call_params = mock_get.call_args_list[1][1]["params"]
        expected_start = (1669884600000000000 + 1) / 1e9
        self.assertAlmostEqual(second_call_params["start"], expected_start, places=10)
        self.assertEqual(second_call_params["direction"], "forward")

    @patch.object(requests.Session, "get")
    def test_fetch_logs_backward_returns_stats_with_continuation(self, mock_get):
        """Verify a backward fetch returns current_end as the continuation alongside the page stats."""
        response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp"},
                        "values": [
                            ["1669884900000000000", "Log 3"],
                            ["1669884800000000000", "Log 2"],
                            ["1669884700000000000", "Log 1"],
                        ],
                    },
                ],
                "stats": {"summary": {"totalEntriesReturned": 3}},
            },
        }

        mock_get.return_value = MagicMock(status_code=200, json=lambda: response)

        fetcher = LokiLogFetcher("http://loki_server", max_entries_per_request=3)
        result = fetcher.fetch_logs(
            query="{app='myapp'}",
            start_time=datetime.fromtimestamp(1669884000),
            end_time=datetime.fromtimestamp(1669885000),
            max_num_pages=1,
            direction="backward",
        )

        expected_end = (1669884700000000000 - 1) / 1e9
        self.assertAlmostEqual(result.continuation_timestamp, expected_end, places=10)
        self.assertEqual(result.stats, [response["data"]["stats"]])


class TestAsyncFetchLogsBackward(unittest.IsolatedAsyncioTestCase):
    """Test backward direction support in async_fetch_logs."""

    def setUp(self):
        self.fetcher = LokiLogFetcher(loki_url="http://test_loki_server", max_entries_per_request=3)

    @patch.object(LokiLogFetcher, "async_make_get_request", new_callable=AsyncMock)
    async def test_async_fetch_logs_backward(self, mock_request):
        """Verify async path handles backward direction correctly."""
        first_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp"},
                        "values": [
                            ["1669884900000000000", "Log 3"],
                            ["1669884800000000000", "Log 2"],
                            ["1669884700000000000", "Log 1"],
                        ],
                    },
                ],
                "stats": {"summary": {"totalEntriesReturned": 3}},
            },
        }
        second_response = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "myapp"},
                        "values": [
                            ["1669884600000000000", "Log B"],
                        ],
                    },
                ],
                "stats": {"summary": {"totalEntriesReturned": 1}},
            },
        }

        mock_request.side_effect = [first_response, second_response]

        result = await self.fetcher.async_fetch_logs(
            query="{app='myapp'}",
            start_time=datetime.fromtimestamp(1669884000),
            end_time=datetime.fromtimestamp(1669885000),
            direction="backward",
        )

        # Verify two requests were made
        self.assertEqual(mock_request.call_count, 2)

        # Verify second request has retreated end timestamp
        second_call_params = mock_request.call_args_list[1][1]["params"]
        expected_end = (1669884700000000000 - 1) / 1e9
        self.assertAlmostEqual(second_call_params["end"], expected_end, places=10)
        self.assertEqual(second_call_params["direction"], "backward")

        # continuation should be None since result_size < max_entries on page 2
        self.assertIsNone(result.continuation_timestamp)


class TestMtlsConfiguration(unittest.TestCase):
    @patch("recovery_libs.loki_client.loki_log_fetcher.httpx.AsyncClient")
    @patch("recovery_libs.loki_client.loki_log_fetcher.ssl.create_default_context")
    def test_mtls_adapter_with_ca_cert(self, mock_create_ctx, mock_async_client):
        mock_ctx = MagicMock()
        mock_create_ctx.return_value = mock_ctx
        with patch.dict("sys.modules", {"certifi": MagicMock(where=MagicMock(return_value="/etc/certifi.pem"))}):
            fetcher = LokiLogFetcher("https://test", cert_file="/cert.pem", key_file="/key.pem", ca_cert="/ca.pem")

        mock_create_ctx.assert_called_once_with(ssl.Purpose.SERVER_AUTH)
        # Baseline bundle (certifi) then custom CA; both are added (load_verify_locations is additive)
        self.assertEqual(mock_ctx.load_verify_locations.call_count, 2)
        mock_ctx.load_verify_locations.assert_any_call(cafile="/etc/certifi.pem")
        mock_ctx.load_verify_locations.assert_any_call(cafile="/ca.pem")
        mock_ctx.load_cert_chain.assert_called_once_with(certfile="/cert.pem", keyfile="/key.pem")
        adapter = fetcher.session.get_adapter("https://x")
        self.assertIsInstance(adapter, MtlsAdapter)
        self.assertIs(adapter._ssl_context, mock_ctx)
        mock_async_client.assert_called_once_with(
            auth=None, verify=mock_ctx, headers=unittest.mock.ANY, limits=unittest.mock.ANY
        )

    @patch("recovery_libs.loki_client.loki_log_fetcher.httpx.AsyncClient")
    @patch("recovery_libs.loki_client.loki_log_fetcher.ssl.create_default_context")
    def test_mtls_without_ca_cert_trusts_system_cas(self, mock_create_ctx, mock_async_client):
        mock_ctx = MagicMock()
        mock_create_ctx.return_value = mock_ctx
        with patch.dict("sys.modules", {"certifi": MagicMock(where=MagicMock(return_value="/etc/ssl/certifi.pem"))}):
            LokiLogFetcher("https://test", cert_file="/cert.pem", key_file="/key.pem")

        mock_create_ctx.assert_called_once_with(ssl.Purpose.SERVER_AUTH)
        # When no ca_cert we load certifi bundle as baseline CA store
        mock_ctx.load_verify_locations.assert_called_once_with(cafile="/etc/ssl/certifi.pem")
        mock_ctx.load_cert_chain.assert_called_once_with(certfile="/cert.pem", keyfile="/key.pem")

    def test_no_mtls_uses_standard_adapter(self):
        fetcher = LokiLogFetcher("https://test")
        adapter = fetcher.session.get_adapter("https://x")
        self.assertNotIsInstance(adapter, MtlsAdapter)
        self.assertIsInstance(adapter, HTTPAdapter)

    @patch("recovery_libs.loki_client.loki_log_fetcher.httpx.AsyncClient")
    @patch("recovery_libs.loki_client.loki_log_fetcher.ssl.create_default_context")
    def test_mtls_invalid_ca_cert_fallback_to_system_cas(self, mock_create_ctx, mock_async_client):
        """When custom ca_cert fails to load, we continue with system/certifi only."""
        mock_ctx = MagicMock()
        mock_create_ctx.return_value = mock_ctx
        # First call (certifi) succeeds; second call (ca_cert) raises
        mock_ctx.load_verify_locations.side_effect = [None, OSError("No such file")]
        with patch.dict("sys.modules", {"certifi": MagicMock(where=MagicMock(return_value="/etc/certifi.pem"))}):
            LokiLogFetcher(
                "https://test",
                cert_file="/cert.pem",
                key_file="/key.pem",
                ca_cert="/nonexistent/ca.pem",
            )
        # certifi loaded; custom CA raised, so only certifi is in the store
        self.assertEqual(mock_ctx.load_verify_locations.call_count, 2)
        mock_ctx.load_verify_locations.assert_any_call(cafile="/etc/certifi.pem")
        mock_ctx.load_verify_locations.assert_any_call(cafile="/nonexistent/ca.pem")
        mock_ctx.load_cert_chain.assert_called_once_with(certfile="/cert.pem", keyfile="/key.pem")


class TestCertificateExpiration(unittest.TestCase):
    """Test certificate expiration methods."""

    def test_cert_expiration_no_certs(self):
        """Test both methods return None when no certificates configured."""
        fetcher = LokiLogFetcher(loki_url="http://test")
        self.assertIsNone(fetcher.days_until_cert_expires())
        self.assertIsNone(fetcher.days_until_ca_cert_expires())

    @patch("recovery_libs.loki_client.loki_log_fetcher.x509.load_pem_x509_certificate")
    @patch("builtins.open", new_callable=mock_open, read_data=b"fake_cert_data")
    def test_days_until_cert_expires_valid(self, mock_file, mock_load_cert):
        """Test days_until_cert_expires returns correct days for valid certificate."""
        mock_cert = MagicMock()
        mock_cert.not_valid_after_utc = datetime.now(UTC) + timedelta(days=30)
        mock_load_cert.return_value = mock_cert

        # Create fetcher without cert_file, then set it manually to avoid SSL context validation
        fetcher = LokiLogFetcher(loki_url="http://test")
        fetcher.cert_file = "/path/to/cert.pem"
        self.assertIn(fetcher.days_until_cert_expires(), [29, 30])

    @patch("recovery_libs.loki_client.loki_log_fetcher.x509.load_pem_x509_certificate")
    @patch("builtins.open", new_callable=mock_open, read_data=b"fake_cert_data")
    def test_days_until_ca_cert_expires_valid(self, mock_file, mock_load_cert):
        """Test days_until_ca_cert_expires returns correct days for valid CA certificate."""
        mock_cert = MagicMock()
        mock_cert.not_valid_after_utc = datetime.now(UTC) + timedelta(days=60)
        mock_load_cert.return_value = mock_cert

        # Create fetcher without ca_cert, then set it manually to avoid httpx validation
        fetcher = LokiLogFetcher(loki_url="http://test")
        fetcher.ca_cert = "/path/to/ca.pem"
        self.assertIn(fetcher.days_until_ca_cert_expires(), [59, 60])

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_cert_expiration_file_errors(self, mock_file):
        """Test both methods return None when certificate files cannot be read."""
        fetcher = LokiLogFetcher(loki_url="http://test")
        fetcher.cert_file = "/invalid/cert.pem"
        fetcher.ca_cert = "/invalid/ca.pem"
        self.assertIsNone(fetcher.days_until_cert_expires())
        self.assertIsNone(fetcher.days_until_ca_cert_expires())


class TestFetchLogsDirectionEquivalence(unittest.TestCase):
    """
    Prove that changing fetch direction does NOT change which logs are returned —
    only the order changes. This is critical for production confidence.
    """

    def setUp(self):
        self.fetcher = LokiLogFetcher("http://test_loki_server", max_entries_per_request=3)
        self.start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        self.end_time = datetime(2024, 1, 1, 1, 0, 0, tzinfo=UTC)

    @staticmethod
    def _flatten_streams_to_tuples(all_streams):
        """Extract all (timestamp, log_line) tuples from Loki stream results."""
        tuples = []
        for stream in all_streams:
            for ts, line in stream.get("values", []):
                tuples.append((int(ts), line))
        return tuples

    @staticmethod
    def _make_loki_response(streams, total_entries):
        """Build a realistic Loki API response dict."""
        return {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": streams,
                "stats": {"summary": {"totalEntriesReturned": total_entries}},
            },
        }

    @patch.object(requests.Session, "get")
    def test_fetch_logs_forward_backward_equivalence(self, mock_get):
        """
        Two streams, 2 pages worth of data (limit=3, 6 entries total).
        Forward fetches oldest-first; backward fetches newest-first.
        Both must return identical log content.
        """
        # Define the complete dataset: 2 streams, 3 entries each
        # Stream A: host1 logs at T1, T3, T5
        # Stream B: host2 logs at T2, T4, T6
        ts1 = "1704067200000000000"  # 2024-01-01T00:00:00Z
        ts2 = "1704067260000000000"  # +1min
        ts3 = "1704067320000000000"  # +2min
        ts4 = "1704067380000000000"  # +3min
        ts5 = "1704067440000000000"  # +4min
        ts6 = "1704067500000000000"  # +5min

        # Forward page 1: oldest entries (3 total = limit, triggers pagination)
        fwd_page1_streams = [
            {"stream": {"host_name": "host1"}, "values": [[ts1, "log-A1"], [ts3, "log-A3"]]},
            {"stream": {"host_name": "host2"}, "values": [[ts2, "log-B2"]]},
        ]
        # Forward page 2: remaining entries (2 < limit, stops pagination)
        fwd_page2_streams = [
            {"stream": {"host_name": "host1"}, "values": [[ts5, "log-A5"]]},
            {"stream": {"host_name": "host2"}, "values": [[ts4, "log-B4"], [ts6, "log-B6"]]},
        ]

        # Backward page 1: newest entries (3 total = limit, triggers pagination)
        # Within each stream, values are sorted DESCENDING
        bwd_page1_streams = [
            {"stream": {"host_name": "host1"}, "values": [[ts5, "log-A5"]]},
            {"stream": {"host_name": "host2"}, "values": [[ts6, "log-B6"], [ts4, "log-B4"]]},
        ]
        # Backward page 2: remaining older entries (3 total but < limit after end retreat)
        bwd_page2_streams = [
            {"stream": {"host_name": "host1"}, "values": [[ts3, "log-A3"], [ts1, "log-A1"]]},
            {"stream": {"host_name": "host2"}, "values": [[ts2, "log-B2"]]},
        ]

        def mock_response_side_effect(*args, **kwargs):
            params = kwargs.get("params", {})
            direction = params.get("direction", "forward")
            mock_resp = MagicMock(status_code=200)

            if direction == "forward":
                # Determine page based on start param
                if params["start"] < 1704067320:  # Before ts3 epoch
                    mock_resp.json.return_value = self._make_loki_response(fwd_page1_streams, 3)
                else:
                    mock_resp.json.return_value = self._make_loki_response(fwd_page2_streams, 2)
            else:  # backward
                # Determine page based on end param
                if params["end"] > 1704067380:  # After ts4 epoch
                    mock_resp.json.return_value = self._make_loki_response(bwd_page1_streams, 3)
                else:
                    mock_resp.json.return_value = self._make_loki_response(bwd_page2_streams, 2)
            return mock_resp

        mock_get.side_effect = mock_response_side_effect

        # Fetch forward
        fwd_result = self.fetcher.fetch_logs(
            query="{app='test'}",
            start_time=self.start_time,
            end_time=self.end_time,
            max_num_pages=10,
            direction="forward",
        )

        # Fetch backward
        bwd_result = self.fetcher.fetch_logs(
            query="{app='test'}",
            start_time=self.start_time,
            end_time=self.end_time,
            max_num_pages=10,
            direction="backward",
        )

        # Flatten both results to (timestamp, log_line) tuples
        fwd_tuples = self._flatten_streams_to_tuples(fwd_result.streams)
        bwd_tuples = self._flatten_streams_to_tuples(bwd_result.streams)

        # Assert identical content using Counter (handles duplicates)
        self.assertEqual(
            collections.Counter(fwd_tuples),
            collections.Counter(bwd_tuples),
            "Forward and backward fetches must return identical log content",
        )

        # Assert ordering: forward's first entry should be oldest
        self.assertEqual(fwd_tuples[0], (int(ts1), "log-A1"), "Forward first entry should be the oldest timestamp")

        # Assert ordering: backward's first entry should be newest
        self.assertEqual(bwd_tuples[0], (int(ts5), "log-A5"), "Backward first entry should be from the newest page")

        # Verify we got all 6 entries in both directions
        self.assertEqual(len(fwd_tuples), 6, "Forward should return all 6 log entries")
        self.assertEqual(len(bwd_tuples), 6, "Backward should return all 6 log entries")

    @patch.object(requests.Session, "get")
    def test_fetch_logs_forward_backward_equivalence_multi_stream(self, mock_get):
        """
        3 streams with interleaved timestamps across 2 pages.
        Verifies pagination works correctly with multiple streams in both directions.
        """
        # 3 streams with interleaved timestamps
        # Stream A (node-001): T1, T4, T7
        # Stream B (node-002): T2, T5, T8
        # Stream C (node-003): T3, T6, T9
        ts1 = "1704067200000000000"
        ts2 = "1704067210000000000"
        ts3 = "1704067220000000000"
        ts4 = "1704067230000000000"
        ts5 = "1704067240000000000"
        ts6 = "1704067250000000000"
        ts7 = "1704067260000000000"
        ts8 = "1704067270000000000"
        ts9 = "1704067280000000000"

        # Forward page 1: 5 entries (= limit=5), triggers pagination
        fwd_page1_streams = [
            {"stream": {"host_name": "node-001"}, "values": [[ts1, "A1"], [ts4, "A4"]]},
            {"stream": {"host_name": "node-002"}, "values": [[ts2, "B2"], [ts5, "B5"]]},
            {"stream": {"host_name": "node-003"}, "values": [[ts3, "C3"]]},
        ]
        # Forward page 2: 4 entries (< limit=5), stops pagination
        fwd_page2_streams = [
            {"stream": {"host_name": "node-001"}, "values": [[ts7, "A7"]]},
            {"stream": {"host_name": "node-002"}, "values": [[ts8, "B8"]]},
            {"stream": {"host_name": "node-003"}, "values": [[ts6, "C6"], [ts9, "C9"]]},
        ]

        # Backward page 1: 5 newest entries (= limit=5), descending within each stream
        bwd_page1_streams = [
            {"stream": {"host_name": "node-001"}, "values": [[ts7, "A7"]]},
            {"stream": {"host_name": "node-002"}, "values": [[ts8, "B8"], [ts5, "B5"]]},
            {"stream": {"host_name": "node-003"}, "values": [[ts9, "C9"], [ts6, "C6"]]},
        ]
        # Backward page 2: 4 remaining entries (< limit=5), descending within each stream
        bwd_page2_streams = [
            {"stream": {"host_name": "node-001"}, "values": [[ts4, "A4"], [ts1, "A1"]]},
            {"stream": {"host_name": "node-002"}, "values": [[ts2, "B2"]]},
            {"stream": {"host_name": "node-003"}, "values": [[ts3, "C3"]]},
        ]

        fetcher = LokiLogFetcher("http://test_loki_server", max_entries_per_request=5)

        def mock_response_side_effect(*args, **kwargs):
            params = kwargs.get("params", {})
            direction = params.get("direction", "forward")
            mock_resp = MagicMock(status_code=200)

            if direction == "forward":
                # After page 1, start advances past ts5 ((1704067240000000000 + 1) / 1e9)
                if params["start"] < (1704067240000000000 + 1) / 1e9:
                    mock_resp.json.return_value = self._make_loki_response(fwd_page1_streams, 5)
                else:
                    mock_resp.json.return_value = self._make_loki_response(fwd_page2_streams, 4)
            else:  # backward
                # After page 1, end retreats before ts5 ((1704067240000000000 - 1) / 1e9)
                # bwd page 1 min ts across streams is ts5 (1704067240s) from B5
                if params["end"] > (1704067240000000000 - 1) / 1e9:
                    mock_resp.json.return_value = self._make_loki_response(bwd_page1_streams, 5)
                else:
                    mock_resp.json.return_value = self._make_loki_response(bwd_page2_streams, 4)
            return mock_resp

        mock_get.side_effect = mock_response_side_effect

        # Fetch forward
        fwd_result = fetcher.fetch_logs(
            query="{app='test'}",
            start_time=self.start_time,
            end_time=self.end_time,
            max_num_pages=10,
            direction="forward",
        )

        # Fetch backward
        bwd_result = fetcher.fetch_logs(
            query="{app='test'}",
            start_time=self.start_time,
            end_time=self.end_time,
            max_num_pages=10,
            direction="backward",
        )

        # Flatten results
        fwd_tuples = self._flatten_streams_to_tuples(fwd_result.streams)
        bwd_tuples = self._flatten_streams_to_tuples(bwd_result.streams)

        # Assert identical content
        self.assertEqual(
            collections.Counter(fwd_tuples),
            collections.Counter(bwd_tuples),
            "Forward and backward must return identical log content across 3 streams",
        )

        # Verify all 9 entries are present
        self.assertEqual(len(fwd_tuples), 9, "Forward should return all 9 log entries")
        self.assertEqual(len(bwd_tuples), 9, "Backward should return all 9 log entries")

        # Verify forward starts with oldest timestamp
        fwd_timestamps = [t for t, _ in fwd_tuples]
        self.assertEqual(fwd_timestamps[0], int(ts1), "Forward first timestamp should be the oldest (T1)")

        # Verify backward starts with newest timestamp
        bwd_timestamps = [t for t, _ in bwd_tuples]
        self.assertIn(
            bwd_timestamps[0],
            [int(ts7), int(ts8), int(ts9)],
            "Backward first timestamp should be from the newest page",
        )

        # Verify all 9 unique log lines present
        all_expected_lines = {"A1", "A4", "A7", "B2", "B5", "B8", "C3", "C6", "C9"}
        fwd_lines = {line for _, line in fwd_tuples}
        bwd_lines = {line for _, line in bwd_tuples}
        self.assertEqual(fwd_lines, all_expected_lines, "Forward should contain all expected log lines")
        self.assertEqual(bwd_lines, all_expected_lines, "Backward should contain all expected log lines")


class TestLokiLogFetcherRetryConfig(unittest.TestCase):
    def test_default_retry_config_preserves_backward_compatibility(self):
        """Verify default retry config matches pre-existing behavior."""
        fetcher = LokiLogFetcher(loki_url="http://test:3100")
        adapter = fetcher.session.get_adapter("https://test")
        retry = adapter.max_retries
        self.assertEqual(retry.total, 5)
        self.assertAlmostEqual(retry.backoff_factor, 0.5)
        self.assertIn(403, retry.status_forcelist)
        self.assertIn(500, retry.status_forcelist)

    def test_custom_retry_config_applied(self):
        """Verify custom retry parameters are applied to the session adapter."""
        fetcher = LokiLogFetcher(
            loki_url="http://test:3100",
            retry_total=3,
            retry_backoff_factor=1.0,
            retry_status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = fetcher.session.get_adapter("https://test")
        retry = adapter.max_retries
        self.assertEqual(retry.total, 3)
        self.assertAlmostEqual(retry.backoff_factor, 1.0)
        self.assertIn(429, retry.status_forcelist)
        self.assertIn(502, retry.status_forcelist)
        self.assertIn(503, retry.status_forcelist)
        self.assertIn(504, retry.status_forcelist)
        self.assertNotIn(403, retry.status_forcelist)

    def test_retry_status_forcelist_none_uses_default(self):
        """Verify None retry_status_forcelist falls back to [403, 500]."""
        fetcher = LokiLogFetcher(
            loki_url="http://test:3100",
            retry_total=2,
            retry_status_forcelist=None,
        )
        adapter = fetcher.session.get_adapter("https://test")
        retry = adapter.max_retries
        self.assertEqual(retry.total, 2)
        self.assertIn(403, retry.status_forcelist)
        self.assertIn(500, retry.status_forcelist)

    def test_retry_allowed_methods_is_get(self):
        """Verify retry only applies to GET requests."""
        fetcher = LokiLogFetcher(loki_url="http://test:3100")
        adapter = fetcher.session.get_adapter("https://test")
        retry = adapter.max_retries
        self.assertIn("GET", retry.allowed_methods)

    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    @patch("urllib3.connectionpool.HTTPConnectionPool._get_conn")
    def test_retry_actually_retries_on_500_then_succeeds(self, mock_get_conn, mock_make_request):
        """Simulate a 500 followed by a 200 at the connection level, verifying urllib3 retries."""
        import io

        import urllib3

        mock_get_conn.return_value = MagicMock()

        error_resp = urllib3.HTTPResponse(
            body=io.BytesIO(b"Internal Server Error"),
            status=500,
            headers={"Content-Type": "text/plain"},
            preload_content=False,
        )
        success_body = json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {"stream": {"app": "test"}, "values": [["1000000000000000000", "log line"]]},
                    ],
                    "stats": {"summary": {"totalEntriesReturned": 1}},
                },
            }
        ).encode()
        success_resp = urllib3.HTTPResponse(
            body=io.BytesIO(success_body),
            status=200,
            headers={"Content-Type": "application/json"},
            preload_content=False,
        )

        mock_make_request.side_effect = [error_resp, success_resp]

        fetcher = LokiLogFetcher(
            loki_url="https://localhost:3100",
            retry_total=2,
            retry_backoff_factor=0,
            retry_status_forcelist=[500],
        )
        result = fetcher.fetch_logs(
            query='{app="test"}',
            start_time=datetime.now() - timedelta(hours=1),
            end_time=datetime.now(),
            max_num_pages=1,
        )

        self.assertEqual(len(result.streams), 1)
        self.assertEqual(mock_make_request.call_count, 2)

    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    @patch("urllib3.connectionpool.HTTPConnectionPool._get_conn")
    def test_retry_exhausted_raises_exception(self, mock_get_conn, mock_make_request):
        """Verify that exhausting all retries raises LokiLogFetcherException."""
        import io

        import urllib3

        mock_get_conn.return_value = MagicMock()

        def _make_500():
            return urllib3.HTTPResponse(
                body=io.BytesIO(b"Internal Server Error"),
                status=500,
                headers={"Content-Type": "text/plain"},
                preload_content=False,
            )

        mock_make_request.side_effect = [_make_500() for _ in range(5)]

        fetcher = LokiLogFetcher(
            loki_url="https://localhost:3100",
            retry_total=1,
            retry_backoff_factor=0,
            retry_status_forcelist=[500],
        )

        with self.assertRaises(LokiLogFetcherException):
            fetcher.fetch_logs(
                query='{app="test"}',
                start_time=datetime.now() - timedelta(hours=1),
                end_time=datetime.now(),
                max_num_pages=1,
            )

        # Initial attempt + 1 retry = 2 calls
        self.assertEqual(mock_make_request.call_count, 2)

    @patch("urllib3.connectionpool.HTTPConnectionPool._make_request")
    @patch("urllib3.connectionpool.HTTPConnectionPool._get_conn")
    def test_no_retry_on_status_not_in_forcelist(self, mock_get_conn, mock_make_request):
        """Verify that a 400 (not in forcelist) is NOT retried."""
        import io

        import urllib3

        mock_get_conn.return_value = MagicMock()

        resp_400 = urllib3.HTTPResponse(
            body=io.BytesIO(b"Bad Request"),
            status=400,
            headers={"Content-Type": "text/plain"},
            preload_content=False,
        )
        mock_make_request.return_value = resp_400

        fetcher = LokiLogFetcher(
            loki_url="https://localhost:3100",
            retry_total=3,
            retry_backoff_factor=0,
            retry_status_forcelist=[500, 502, 503],
        )

        with self.assertRaises(LokiLogFetcherException):
            fetcher.fetch_logs(
                query='{app="test"}',
                start_time=datetime.now() - timedelta(hours=1),
                end_time=datetime.now(),
                max_num_pages=1,
            )

        # 400 is not in forcelist, so no retry — only 1 call
        self.assertEqual(mock_make_request.call_count, 1)


if __name__ == "__main__":
    unittest.main()
