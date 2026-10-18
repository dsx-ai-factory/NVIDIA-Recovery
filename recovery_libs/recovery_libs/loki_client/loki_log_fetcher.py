# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta
from json import JSONDecodeError
from typing import Any

import httpx
import requests
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from recovery_libs.log_query._http import query_failure_from_http_error

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LokiQueryResult:
    """Streams a Loki range query returned across the pages fetched, where to resume, and per-page statistics."""

    streams: list[dict[str, Any]]
    """Loki streams, each a mapping of stream labels to their log values."""
    continuation_timestamp: float | None
    """Where the next fetch resumes when the page limit cut the query short; None when it read every page."""
    stats: list[dict[str, Any]]
    """Loki's query statistics, one entry per page fetched."""


class LokiLogFetcherException(Exception):
    pass


class MtlsAdapter(HTTPAdapter):
    """HTTPAdapter that injects a custom ssl.SSLContext for mutual TLS."""

    def __init__(self, ssl_context, **kwargs):
        self._ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._ssl_context
        return super().init_poolmanager(*args, **kwargs)


class LokiLogFetcher:
    """
    Fetches logs from a loki server accessible via provided loki url.
    """

    def __init__(
        self,
        loki_url="",
        auth_token=None,
        tenants: str | None = None,
        cert_file: str | None = None,
        key_file: str | None = None,
        ca_cert: str | None = None,
        max_entries_per_request=5000,
        extra_headers: dict[str, str] | None = None,
        retry_total: int = 5,
        retry_backoff_factor: float = 0.5,
        retry_status_forcelist: list[int] | None = None,
    ):
        self.loki_url = loki_url
        self.auth_token = auth_token
        self.tenants = tenants
        self.cert_file = cert_file
        self.key_file = key_file
        self.ca_cert = ca_cert
        self.max_entries_per_request = max_entries_per_request
        self.retry_total = retry_total
        self.retry_backoff_factor = retry_backoff_factor
        self.retry_status_forcelist = retry_status_forcelist

        # sync client
        headers = {"Content-Type": "application/json"}
        self.session = session = requests.Session()

        if self.tenants and self.auth_token:
            session.auth = (self.tenants, self.auth_token)
        elif self.auth_token:
            headers["Authorization"] = self.auth_token

        session.headers.update(headers)
        if extra_headers is not None:
            session.headers.update(extra_headers)
            headers.update(extra_headers)

        ssl_context = None
        if self.cert_file and self.key_file:
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            # Always load a baseline CA bundle first (system defaults may be empty in minimal containers).
            # load_verify_locations() adds certs; it does not replace, so custom ca_cert is additive.
            try:
                import certifi

                ssl_context.load_verify_locations(cafile=certifi.where())
            except Exception as e:
                logger.debug("Could not load certifi CA bundle for mTLS", exc_info=True, extra={"error": str(e)})
            if self.ca_cert:
                try:
                    ssl_context.load_verify_locations(cafile=self.ca_cert)
                except (OSError, ssl.SSLError) as e:
                    logger.warning(
                        "Could not load custom CA cert from %s (%s); continuing with system/certifi CAs only.",
                        self.ca_cert,
                        e,
                    )
            ssl_context.load_cert_chain(certfile=self.cert_file, keyfile=self.key_file)

        retry = Retry(
            total=self.retry_total,
            backoff_factor=self.retry_backoff_factor,
            status_forcelist=self.retry_status_forcelist if self.retry_status_forcelist is not None else [403, 500],
            allowed_methods=["GET"],
        )
        if ssl_context:
            adapter: HTTPAdapter = MtlsAdapter(
                ssl_context=ssl_context, max_retries=retry, pool_connections=100, pool_maxsize=100
            )
        else:
            adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        self.session.mount("https://", adapter)

        # async client
        async_auth = None
        if self.tenants and self.auth_token:
            async_auth = (self.tenants, self.auth_token)

        self.async_session = httpx.AsyncClient(
            auth=async_auth,
            verify=ssl_context if ssl_context else (self.ca_cert or True),
            headers=headers,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=100,
            ),
        )

    def _prepare_params(
        self, query: str, current_start: float, current_end: float, limit: int | None = None, direction: str = "forward"
    ) -> dict[str, Any]:
        """
        Prepares Loki HTTP GET params.
        """
        return {
            "query": query,
            "start": current_start,
            "end": current_end,
            "limit": limit if limit is not None else self.max_entries_per_request,
            "direction": direction,
        }

    def _update_start_timestamp(self, last_value_timestamp: float | str) -> float:
        """
        Updates start timestamp for forward pagination by advancing past the last seen entry.
        Uses +1 nanosecond (Loki's minimum granularity) to minimize pagination gaps.
        Note: entries sharing the exact boundary nanosecond across truncated streams may still
        be lost, but this is an acceptable tradeoff vs. deduplication complexity.
        """
        return (int(last_value_timestamp) + 1) / 1e9

    def _update_end_timestamp(self, first_value_timestamp: float | str) -> float:
        """
        Updates end timestamp for backward pagination by retreating before the oldest seen entry.
        Uses -1 nanosecond (Loki's minimum granularity) to minimize pagination gaps.
        """
        return (int(first_value_timestamp) - 1) / 1e9

    def _get_max_timestamp_from_streams(self, streams: list[dict[str, Any]]) -> str:
        """
        Get the maximum timestamp across all streams.

        When using queries with labels that create multiple streams (e.g., `keep host_name`),
        Loki may return streams in arbitrary order, not sorted by their maximum timestamp.
        This method finds the actual maximum timestamp across all streams to ensure
        correct pagination continuation.

        Args:
            streams: List of stream objects from Loki response

        Returns:
            Maximum timestamp as string (nanoseconds)

        Raises:
            ValueError: If no streams or no values found
        """
        if not streams:
            raise ValueError("No streams provided")

        max_timestamp = None

        for stream in streams:
            values = stream.get("values", [])
            if values:
                # Get the last (most recent) timestamp from this stream
                # values is a list of [timestamp, log_line] pairs
                stream_max_ts = values[-1][0]

                if max_timestamp is None or int(stream_max_ts) > int(max_timestamp):
                    max_timestamp = stream_max_ts

        if max_timestamp is None:
            raise ValueError("No values found in any stream")

        return max_timestamp

    def _get_min_timestamp_from_streams(self, streams: list[dict[str, Any]]) -> str:
        """
        Get the minimum timestamp across all streams.

        When using backward direction, Loki returns values sorted descending within each stream.
        The last entry in each stream's values list is the oldest. This method finds the actual
        minimum timestamp across all streams to ensure correct backward pagination continuation.

        Args:
            streams: List of stream objects from Loki response

        Returns:
            Minimum timestamp as string (nanoseconds)

        Raises:
            ValueError: If no streams or no values found
        """
        if not streams:
            raise ValueError("No streams provided")

        min_timestamp = None

        for stream in streams:
            values = stream.get("values", [])
            if values:
                # Get the last (oldest in backward direction) timestamp from this stream
                # values is a list of [timestamp, log_line] pairs
                stream_min_ts = values[-1][0]

                if min_timestamp is None or int(stream_min_ts) < int(min_timestamp):
                    min_timestamp = stream_min_ts

        if min_timestamp is None:
            raise ValueError("No values found in any stream")

        return min_timestamp

    def _process_response(self, response: dict[str, Any]) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
        """
        Processes Loki API response object.
        """
        current_data = response["data"]
        current_result = current_data["result"]
        result_size = current_data["stats"]["summary"]["totalEntriesReturned"]
        stats = current_data["stats"]
        return current_result, result_size, stats

    def fetch_logs(
        self,
        query: str = "",
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        max_num_pages: int = 5,
        continuation_timestamp: float | None = None,
        timeout: float | tuple[float, float] | None = None,
        limit: int | None = None,
        headers: dict[str, str] | None = None,
        direction: str = "forward",
    ) -> LokiQueryResult:
        """Fetches queried logs from loki for a specific duration.

        Args:
            query (str): The query to run.
            start_time (datetime | None): Start time of the window for which the logs are being fetched.
                Defaults to 24 hours before the call.
            end_time (datetime | None): End time of the window for which the logs are being fetched.
                Defaults to the time of the call.
            max_num_pages (int): Number of pages to fetch, 1 page = 5000 entries max.
            continuation_timestamp (float): if provided, this will be used in lieu of start_time.
                Note: this unconditionally sets current_start regardless of direction.
            timeout (float | tuple[float, float] | None): Timeout in seconds. If float, applies to all phases.
                If tuple, first value is connect timeout, second is read timeout.
            limit (Optional(int)): number of max entries in response.
            headers (Optional[Dict[str, str]]): Additional headers to include in the request.
            direction (str): Loki query direction, "forward" (oldest first) or "backward" (newest first).

        Returns:
            LokiQueryResult: fetched streams, the continuation timestamp, and per-page statistics.

        Raises:
            RuntimeError: In case of errors while fetching logs.
        """
        all_log_data = []
        all_stats_data = []
        start_time = start_time or datetime.now() - timedelta(hours=24)
        end_time = end_time or datetime.now()
        current_start = continuation_timestamp if continuation_timestamp else start_time.timestamp()
        current_end = end_time.timestamp()
        max_entries = limit if limit is not None else self.max_entries_per_request
        prev_boundary_ts = None  # Track previous boundary to detect stalls

        num_page = 0
        while num_page < max_num_pages:
            logger.debug(
                f"Fetching logs from Loki, current_start: {current_start}, current_end: {current_end}, limit: {limit}, num_page: {num_page}, max_num_pages: {max_num_pages}, max_entries: {max_entries}, direction: {direction}"
            )
            params = self._prepare_params(
                query=query,
                current_start=current_start,
                current_end=current_end,
                limit=max_entries,
                direction=direction,
            )
            logger.debug(f"Fetching logs with params: {params}")

            try:
                url = f"{self.loki_url}/loki/api/v1/query_range"
                if headers:
                    response = self.session.get(url, params=params, timeout=timeout, headers=headers)
                else:
                    response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
            except requests.exceptions.RequestException as re:
                raise LokiLogFetcherException(f"Error getting the loki logs: {re}")

            if response is None or response.status_code != 200:
                raise LokiLogFetcherException(
                    f"Error getting the loki logs {response.text if response is not None else ''}"
                )

            current_result, result_size, stats = self._process_response(response.json())
            all_log_data.extend(current_result)
            all_stats_data.append(stats)

            logger.debug(
                f"Fetched {result_size} logs from Loki, max entries per request: {max_entries}, stats: {stats}"
            )

            if result_size < max_entries:
                break

            if direction == "backward":
                # Get the minimum timestamp across all streams for backward pagination
                first_value_timestamp = self._get_min_timestamp_from_streams(current_result)
                # Stall detection: if boundary timestamp hasn't changed, all entries share the same
                # nanosecond and we can't paginate further without risking an infinite loop
                if first_value_timestamp == prev_boundary_ts:
                    logger.warning(
                        f"Pagination stall detected: boundary timestamp {first_value_timestamp} unchanged. "
                        f"Likely >limit entries at the same nanosecond. Breaking to avoid infinite loop."
                    )
                    break
                prev_boundary_ts = first_value_timestamp
                current_end = self._update_end_timestamp(first_value_timestamp)
                if current_end <= current_start:
                    break
            else:
                # Get the maximum timestamp across all streams to ensure correct pagination
                # This handles cases where streams are not sorted by their max timestamp
                last_value_timestamp = self._get_max_timestamp_from_streams(current_result)
                # Stall detection: same guard for forward direction
                if last_value_timestamp == prev_boundary_ts:
                    logger.warning(
                        f"Pagination stall detected: boundary timestamp {last_value_timestamp} unchanged. "
                        f"Likely >limit entries at the same nanosecond. Breaking to avoid infinite loop."
                    )
                    break
                prev_boundary_ts = last_value_timestamp
                current_start = self._update_start_timestamp(last_value_timestamp)
                if current_start >= current_end:
                    break
            num_page += 1

        continuation_ts = current_end if direction == "backward" else current_start
        return LokiQueryResult(
            streams=all_log_data,
            continuation_timestamp=continuation_ts if num_page == max_num_pages else None,
            stats=all_stats_data,
        )

    async def async_fetch_logs(
        self,
        query: str = "",
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        max_num_pages: int = 5,
        continuation_timestamp: float | None = None,
        timeout: float | tuple[float, float] | None = None,
        limit: int | None = None,
        headers: dict[str, str] | None = None,
        direction: str = "forward",
        query_id: str = "unknown",
    ) -> LokiQueryResult:
        """Async method to fetch queried logs from loki for a specific duration.

        Args:
            query (str): The query to run.
            start_time (datetime | None): Start time of the window for which the logs are being fetched.
                Defaults to 24 hours before the call.
            end_time (datetime | None): End time of the window for which the logs are being fetched.
                Defaults to the time of the call.
            max_num_pages (int): Number of pages to fetch, 1 page = 5000 entries max.
            continuation_timestamp (float): if provided, this will be used in lieu of start_time.
                Note: this unconditionally sets current_start regardless of direction.
            timeout (float | tuple[float, float] | None): Timeout in seconds. If float, applies to all phases.
                If tuple, first value is connect timeout, second is read/write/pool timeout.
            limit (Optional(int)): number of max entries in response.
            headers (Optional[Dict[str, str]]): Additional headers to include in the request.
            direction (str): Loki query direction, "forward" (oldest first) or "backward" (newest first).
            query_id (str): Stable logical query identifier, reported on failures.

        Returns:
            LokiQueryResult: fetched streams, the continuation timestamp, and per-page statistics.

        Raises:
            QueryFailure: If a request fails; the kind matches the HTTP status or transport error.
        """
        all_log_data = []
        all_stats_data = []
        start_time = start_time or datetime.now() - timedelta(hours=24)
        end_time = end_time or datetime.now()
        current_start = continuation_timestamp if continuation_timestamp else start_time.timestamp()
        current_end = end_time.timestamp()
        max_entries = limit if limit is not None else self.max_entries_per_request
        num_page = 0

        while num_page < max_num_pages:
            params = self._prepare_params(
                query=query,
                current_start=current_start,
                current_end=current_end,
                limit=max_entries,
                direction=direction,
            )
            logger.debug(f"Fetching logs with params: {params}")

            try:
                response = await self.async_make_get_request(
                    url=f"{self.loki_url}/loki/api/v1/query_range",
                    params=params,
                    timeout=timeout,
                    headers=headers,
                )
            except (httpx.ReadTimeout, httpx.RequestError, httpx.HTTPStatusError) as err:
                logger.exception("Error getting the loki logs", extra={"error": str(err)})
                raise query_failure_from_http_error(err, backend="loki", query_id=query_id) from err

            current_result, result_size, stats = self._process_response(response)
            all_log_data.extend(current_result)
            all_stats_data.append(stats)

            if result_size < max_entries:
                break

            if direction == "backward":
                # Get the minimum timestamp across all streams for backward pagination
                first_value_timestamp = self._get_min_timestamp_from_streams(current_result)
                current_end = self._update_end_timestamp(first_value_timestamp)
                if current_end <= current_start:
                    break
            else:
                # Get the maximum timestamp across all streams to ensure correct pagination
                # This handles cases where streams are not sorted by their max timestamp
                last_value_timestamp = self._get_max_timestamp_from_streams(current_result)
                current_start = self._update_start_timestamp(last_value_timestamp)
            num_page += 1

        continuation_ts = current_end if direction == "backward" else current_start
        return LokiQueryResult(
            streams=all_log_data,
            continuation_timestamp=continuation_ts if num_page == max_num_pages else None,
            stats=all_stats_data,
        )

    async def async_make_get_request(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        timeout: float | tuple[float, float] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Makes an async GET request.

        Args:
            url (str): The URL to send the GET request to.
            params (Optional[Dict[str, Any]]): The query parameters to include in the GET request. Defaults to None.
            timeout (Optional[Union[float, int, tuple[float, float]]]): The timeout for the request in seconds.
                If float/int, applies to all phases. If tuple, first value is connect timeout,
                second is read/write/pool timeout. Defaults to None.
            headers (Optional[Dict[str, str]]): Additional headers to include in the request. Defaults to None.

        Returns:
            dict[str, Any]: JSON object containing response object.
                     Raises httpx.HTTPStatusError for any error status, including 404.

        Example:
            response = await async_make_get_request(
                url="https://api.example.com/loki/api/v1/query_range",
                params={"query": "{{exporter="OTLP", appName="slurm-app-log"}"},
                timeout=15,
            )
        """
        # Start with the session's default headers
        request_headers = dict(self.async_session.headers)
        # Add the Connection: close header required by httpx
        # https://github.com/encode/httpx/discussions/2959
        request_headers["Connection"] = "close"
        # Update with any additional headers passed in
        if headers:
            request_headers.update(headers)

        httpx_timeout = None
        if isinstance(timeout, (int, float)):
            # Single value applies to all phases
            httpx_timeout = httpx.Timeout(timeout)
        elif isinstance(timeout, tuple):
            # Tuple: (connect_timeout, read/write/pool_timeout)
            connect_timeout, other_timeout = timeout
            httpx_timeout = httpx.Timeout(
                connect=connect_timeout,
                read=other_timeout,
                write=other_timeout,
                pool=other_timeout,
            )
        else:
            httpx_timeout = timeout

        response = await self.async_session.get(
            url=url,
            params=params,
            timeout=httpx_timeout,
            headers=request_headers,
        )
        response.raise_for_status()
        try:
            return response.json()
        except (JSONDecodeError, ValueError) as err:
            raise httpx.RequestError(
                f"Unable to decode JSON payload (status_code={response.status_code}, response={response.text}): {err}"
            ) from err

    def _get_certificate_days_until_expiration(self, cert_path: str) -> int | None:
        """Helper method to calculate days until certificate expiration.

        Args:
            cert_path: Path to the certificate file.

        Returns:
            int: Number of days until certificate expires (negative if expired).
            None: If certificate file cannot be read.
        """
        try:
            with open(cert_path, "rb") as f:
                cert_data = f.read()
            cert = x509.load_pem_x509_certificate(cert_data, default_backend())
            days_remaining = (cert.not_valid_after_utc - datetime.now(cert.not_valid_after_utc.tzinfo)).days
            return days_remaining
        except Exception as e:
            logger.warning(
                f"Failed to read certificate expiration from {cert_path}",
                exc_info=True,
                extra={"error": str(e)},
            )
            return None

    def days_until_cert_expires(self) -> int | None:
        """Returns the number of days until cert_file expires.

        Returns:
            int: Number of days until certificate expires (negative if expired).
            None: If cert_file is not configured or file cannot be read.
        """
        return self._get_certificate_days_until_expiration(self.cert_file) if self.cert_file else None

    def days_until_ca_cert_expires(self) -> int | None:
        """Returns the number of days until ca_cert expires.

        Returns:
            int: Number of days until CA certificate expires (negative if expired).
            None: If ca_cert is not configured or file cannot be read.
        """
        return self._get_certificate_days_until_expiration(self.ca_cert) if self.ca_cert else None
