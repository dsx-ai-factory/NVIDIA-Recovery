# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from datetime import datetime

import requests
from recovery_libs.utils.retry import exponential_backoff_retry

from attribution_adapter.settings.settings import Settings
from attribution_adapter.utils.logger import logger


class PrometheusException(Exception):
    pass


class ADIMetricsFetcher:
    def __init__(
        self,
        settings: Settings,
        auth_token: str | None = None,
        cert_file: str | None = None,
        key_file: str | None = None,
        ca_cert: str | None = None,
        timeout: int = 300,
    ):
        self.settings = settings
        self.logger = logger
        self.timeout = timeout
        self.rs = requests.Session()

        self.prometheus_url = self.settings.prometheus_url
        self.prometheus_cluster = self.settings.prometheus_cluster
        self.prometheus_tenants = self.settings.prometheus_tenants
        self.prometheus_checks_enabled = self.settings.prometheus_checks_enabled
        self.prometheus_hostname_label = self.settings.prometheus_hostname_label

        self.qepr = f"{self.prometheus_url}/api/v1/query_range"  # Query range endpoint

        # Authentication
        headers = {}

        # Token based authentication
        if auth_token is not None:
            headers["Authorization"] = auth_token

        self.rs.headers.update(headers)

        # mTLS authentication
        if cert_file is not None and key_file is not None:
            self.rs.cert = (cert_file, key_file)
            self.rs.verify = ca_cert or True

    def get_metrics(self, query: str, start_time: datetime, end_time: datetime) -> dict | None:
        """Fetches metrics from Prometheus using the provided query and time range.

        Args:
            query (str): The PromQL query to execute
            start_time (datetime): Start time for the query range
            end_time (datetime): End time for the query range

        Returns:
            dict | None: The metrics data if successful, None if the query fails or there's an error
                        connecting to Prometheus. The data is in the format returned by Prometheus'
                        query_range API endpoint.
        """
        filter = {"query": query}
        resp = None

        try:
            resp = self._get_or_post(ep=self.prometheus_url, filter=filter)
            self.logger.error(f"Prometheus response: {resp}")
        except Exception as ex:
            self.logger.critical("Cannot contact Prometheus endpoint", exc_info=True, extra={"error": str(ex)})
        if resp is not None:
            if resp["status"] == "success":
                return resp["data"]
            self.logger.critical(f"Prometheus Query Failed [{resp['error']}]")

        return None

    @exponential_backoff_retry(max_retries=2, base_delay=2, exceptions=(PrometheusException,))
    def _get_or_post(self, ep: str, filter: dict[str, str] | None = None):
        """Runs the prometheus call in isolation from other methods. Retries for 5 times with exponential back off.

        Args:
            ep (str): End point to query prometheus end point.
            filter (Union[Dict[str, str], None], optional): Filters to be applies to the query. Defaults to None.

        Raises:
            PrometheusException: In case there are 'requests' error, or if it receives an invalid response.

        Returns:
            str: Response from prometheus in json format.
        """
        self.logger.info(f"PromBuilder calling {ep} with timeout: {self.timeout}, filter: {filter} ")
        try:
            if filter is None:
                resp = self.rs.get(url=ep, timeout=self.timeout)
            else:
                resp = self.rs.post(url=ep, timeout=self.timeout, data=filter)
        except requests.RequestException as e:
            self.logger.error(f"Error calling Prometheus: {e}")
            raise PrometheusException(e)

        if resp is None or resp.status_code != 200:
            raise PrometheusException(f"Prometheus call failed: {resp.json() if resp is not None else ''}")

        if "status" not in resp.json() or resp.json()["status"] != "success":
            raise PrometheusException(f"Invalid response from prometheus: {resp}")

        return resp.json()
