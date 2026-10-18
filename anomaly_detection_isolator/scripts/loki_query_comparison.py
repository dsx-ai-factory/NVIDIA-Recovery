#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Loki Query Comparison Tool

Compares performance of two LogQL queries by running them multiple times
and computing statistical metrics (mean, median, outlier detection).

Supports direction comparison (forward vs backward) to validate log equivalence.
"""

import argparse
import base64
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime
from statistics import mean, median, stdev
from urllib.parse import urlsplit

import requests

logger = logging.getLogger(__name__)

# Try to import recovery_libs Loki client (optional)
try:
    from recovery_libs.loki_client.loki_log_fetcher import LokiLogFetcher

    RECOVERY_LIBS_AVAILABLE = True
except ImportError:
    RECOVERY_LIBS_AVAILABLE = False

# Only workload-independent comparison parameters have defaults.
DEFAULT_LIMIT = 10000
DEFAULT_ITERATIONS = 3
DEFAULT_SLEEP_BETWEEN_QUERIES = 10


class SimpleKubectl:
    """Simple kubectl wrapper."""

    def __init__(self, kubeconfig_path: str, context: str):
        self.kubeconfig_path = os.path.expanduser(kubeconfig_path)
        self.context = context

    def get_secret_value(self, secret_name: str, key: str) -> str:
        """Fetch and decode secret value."""
        env = os.environ.copy()
        env["KUBECONFIG"] = self.kubeconfig_path

        # Escape the key for jsonpath - dots need to be escaped with backslash
        escaped_key = key.replace(".", "\\.")

        cmd = [
            "kubectl",
            "--context",
            self.context,
            "get",
            "secret",
            secret_name,
            "-o",
            f"jsonpath={{.data.{escaped_key}}}",
        ]

        result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
        b64_value = result.stdout.strip()
        if not b64_value:
            raise RuntimeError(f"No data returned for secret {secret_name} key {key}")
        return base64.b64decode(b64_value).decode("utf-8")

    def write_secret_to_file(self, secret_name: str, key: str, output_path: str) -> None:
        """Fetch secret value and write to file."""
        content = self.get_secret_value(secret_name, key)
        if not content:
            raise RuntimeError(f"Empty content from secret {secret_name} key {key}")
        with open(output_path, "w") as f:
            bytes_written = f.write(content)
        if bytes_written == 0:
            raise RuntimeError(f"No bytes written to {output_path}")


class SimpleLokiClient:
    """Simple Loki HTTP client with support for bearer token or certificate-based auth."""

    def __init__(
        self,
        loki_url: str,
        auth_token: str | None = None,
        x_scope_org_id: str | None = None,
        cert_file: str | None = None,
        key_file: str | None = None,
        ca_cert: str | None = None,
    ):
        self.loki_url = loki_url.rstrip("/")
        self.session = requests.Session()

        if auth_token:
            self.session.headers["Authorization"] = f"Bearer {auth_token}"

        if x_scope_org_id:
            self.session.headers["X-Scope-OrgId"] = x_scope_org_id

        # Configure certificate-based authentication
        if cert_file and key_file:
            self.session.cert = (cert_file, key_file)
            self.session.verify = ca_cert or True

    def query_range(
        self,
        query: str,
        start_ns: int,
        end_ns: int,
        limit: int = 700,
        verbose: bool = False,
        direction: str = "forward",
    ) -> tuple[dict, float, int]:
        """Execute Loki query_range and return results, duration, line count."""
        endpoint = f"{self.loki_url}/loki/api/v1/query_range"

        params: dict[str, str | int] = {
            "query": query,
            "start": start_ns,
            "end": end_ns,
            "limit": limit,
            "direction": direction,
        }

        if verbose:
            print(" [querying...]", end="", flush=True)

        start_time = time.time()
        response = self.session.get(endpoint, params=params, timeout=120)
        duration = time.time() - start_time

        if verbose:
            print(f" [received in {duration:.3f}s]", end="", flush=True)

        response.raise_for_status()
        data = response.json()

        # Count log lines
        total_lines = 0
        if "data" in data and "result" in data["data"]:
            for stream in data["data"]["result"]:
                total_lines += len(stream.get("values", []))

        return data, duration, total_lines


class RecoveryLokiClient:
    """Wrapper around recovery_libs LokiLogFetcher to match SimpleLokiClient interface."""

    def __init__(
        self,
        loki_url: str,
        auth_token: str | None = None,
        x_scope_org_id: str | None = None,
        cert_file: str | None = None,
        key_file: str | None = None,
        ca_cert: str | None = None,
    ):
        if not RECOVERY_LIBS_AVAILABLE:
            raise ImportError("recovery_libs is not available. Install it or omit --use-adi-client")

        extra_headers = {}
        if x_scope_org_id:
            extra_headers["X-Scope-OrgId"] = x_scope_org_id

        self.fetcher = LokiLogFetcher(
            loki_url=loki_url,
            auth_token=auth_token,
            cert_file=cert_file,
            key_file=key_file,
            ca_cert=ca_cert,
            extra_headers=extra_headers if extra_headers else None,
        )

    def query_range(
        self,
        query: str,
        start_ns: int,
        end_ns: int,
        limit: int = 700,
        verbose: bool = False,
        direction: str = "forward",
    ) -> tuple[dict, float, int]:
        """
        Execute Loki query using fetch_logs with chunked pagination (similar to ADI's get_telemetry_logs).

        This fetches logs in chunks of 700 lines per request and continues pagination
        until all logs within the time period are retrieved. The 'limit' parameter is
        ignored for RecoveryLokiClient - we fetch all data in the time range.
        """
        if verbose:
            print(" [querying...]", end="", flush=True)

        start_time_obj = datetime.fromtimestamp(start_ns / 1e9)
        end_time_obj = datetime.fromtimestamp(end_ns / 1e9)

        # Fetch in chunks of 700 lines, continue until time period is exhausted
        # max_num_pages set to a high value to ensure we fetch all logs in the time range
        chunk_size = 700
        max_pages = 1000  # High enough to fetch all data in the time range

        clock_start = time.time()

        # Use fetch_logs which handles pagination automatically
        # This mimics ADI's get_telemetry_logs behavior
        # It will continue fetching until it exhausts the time range
        all_logs = self.fetcher.fetch_logs(
            query=query,
            start_time=start_time_obj,
            end_time=end_time_obj,
            limit=chunk_size,
            max_num_pages=max_pages,
            timeout=120,
            direction=direction,
        ).streams

        clock_end = time.time()
        duration = clock_end - clock_start

        if verbose:
            print(f" [received in {duration:.3f}s]", end="", flush=True)

        # Count total lines from all fetched streams
        total_lines = 0
        for stream in all_logs:
            if "values" in stream:
                total_lines += len(stream["values"])

        # Reconstruct response in the format expected by the comparison tool
        # This matches the Loki API response structure
        data = {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": all_logs,
            },
        }

        return data, duration, total_lines


def parse_timestamp(ts_str: str) -> datetime:
    """Parse timestamp string to datetime."""
    return datetime.fromisoformat(ts_str)


def remove_outliers(values: list[float], threshold: float = 2.0) -> tuple[list[float], list[int]]:
    """
    Remove outliers using z-score method.

    Returns:
        Tuple of (filtered_values, outlier_indices)
    """
    if len(values) < 3:
        return values, []

    values_mean = mean(values)
    values_stdev = stdev(values)

    if values_stdev == 0:
        return values, []

    outlier_indices = []
    filtered_values = []

    for i, value in enumerate(values):
        z_score = abs((value - values_mean) / values_stdev)
        if z_score > threshold:
            outlier_indices.append(i)
        else:
            filtered_values.append(value)

    return filtered_values, outlier_indices


def compute_statistics(durations: list[float]) -> dict:
    """Compute statistical metrics for query durations."""
    filtered_durations, outlier_indices = remove_outliers(durations)

    if not filtered_durations:
        filtered_durations = durations
        outlier_indices = []

    return {
        "raw_durations": durations,
        "filtered_durations": filtered_durations,
        "outlier_indices": outlier_indices,
        "mean": mean(filtered_durations),
        "median": median(filtered_durations),
        "min": min(filtered_durations),
        "max": max(filtered_durations),
        "stdev": stdev(filtered_durations) if len(filtered_durations) > 1 else 0,
        "count": len(filtered_durations),
        "outliers_removed": len(outlier_indices),
    }


def extract_log_tuples(data: dict) -> list[tuple[int, str]]:
    """Extract (timestamp_ns, log_line) tuples from Loki response data."""
    tuples = []
    for stream in data.get("data", {}).get("result", []):
        for ts_ns, line in stream.get("values", []):
            tuples.append((int(ts_ns), line))
    return tuples


def run_query_multiple_times(
    client,
    query: str,
    start_ns: int,
    end_ns: int,
    limit: int,
    iterations: int,
    query_name: str,
    sleep_between: int = 0,
    direction: str = "forward",
) -> dict:
    """Run a query multiple times and collect statistics."""
    print(f"\n{'=' * 80}")
    print(f"Running Query: {query_name}")
    print(f"{'=' * 80}")
    print(f"Query: {query}")
    print(f"Direction: {direction}")
    print(f"Iterations: {iterations}")
    if sleep_between > 0:
        print(f"Sleep between queries: {sleep_between}s")
    print()

    durations = []
    line_counts = []
    stream_counts = []

    for i in range(iterations):
        print(f"  Run {i + 1}/{iterations}...", end="", flush=True)
        try:
            data, duration, line_count = client.query_range(
                query, start_ns, end_ns, limit, verbose=True, direction=direction
            )
            durations.append(duration)
            line_counts.append(line_count)
            stream_counts.append(len(data.get("data", {}).get("result", [])))
            print(f" ✓ {duration:.3f}s ({line_count} lines)")

            # Sleep between queries (except after the last one)
            if sleep_between > 0 and i < iterations - 1:
                print(f"    Sleeping {sleep_between}s...", flush=True)
                time.sleep(sleep_between)
        except Exception as e:
            print(f" ✗ Error: {e}")
            raise

    stats = compute_statistics(durations)

    print(f"\n{'─' * 80}")
    print(f"Statistics for {query_name}:")
    print(f"  Mean duration:   {stats['mean']:.3f}s")
    print(f"  Median duration: {stats['median']:.3f}s")
    print(f"  Min duration:    {stats['min']:.3f}s")
    print(f"  Max duration:    {stats['max']:.3f}s")
    print(f"  Std deviation:   {stats['stdev']:.3f}s")
    print(f"  Outliers found:  {stats['outliers_removed']}")
    if stats["outliers_removed"] > 0:
        print(f"  Outlier runs:    {[i + 1 for i in stats['outlier_indices']]}")
    print(f"  Avg line count:  {mean(line_counts):.0f}")
    print(f"  Avg streams:     {mean(stream_counts):.0f}")

    return {
        "query": query,
        "direction": direction,
        "statistics": stats,
        "line_counts": line_counts,
        "stream_counts": stream_counts,
    }


def compare_results(query1_results: dict, query2_results: dict) -> None:
    """Print comparison between two queries."""
    stats1 = query1_results["statistics"]
    stats2 = query2_results["statistics"]

    mean1 = stats1["mean"]
    mean2 = stats2["mean"]
    improvement = ((mean1 - mean2) / mean1) * 100

    print(f"\n{'=' * 80}")
    print("COMPARISON SUMMARY")
    print(f"{'=' * 80}")
    print(f"\n{'Query 1':<40} {'Query 2':<40}")
    print(f"{'-' * 40} {'-' * 40}")
    print(f"{'Mean: ' + f'{mean1:.3f}s':<40} {'Mean: ' + f'{mean2:.3f}s':<40}")
    print(f"{'Median: ' + f'{stats1["median"]:.3f}s':<40} {'Median: ' + f'{stats2["median"]:.3f}s':<40}")
    print(f"{'Std Dev: ' + f'{stats1["stdev"]:.3f}s':<40} {'Std Dev: ' + f'{stats2["stdev"]:.3f}s':<40}")
    print()

    if improvement > 0:
        print(f"✓ Query 2 is FASTER by {improvement:.1f}% (improvement of {mean1 - mean2:.3f}s)")
    elif improvement < 0:
        print(f"✗ Query 2 is SLOWER by {abs(improvement):.1f}% (regression of {abs(mean1 - mean2):.3f}s)")
    else:
        print("⚬ Both queries have similar performance")

    print()


def run_direction_comparison(
    client,
    query: str,
    start_ns: int,
    end_ns: int,
    limit: int,
    iterations: int,
    sleep_between: int = 0,
) -> dict:
    """
    Run the same query with both forward and backward directions, alternating iterations
    to avoid cache-warming bias. Compare log equivalence and performance.

    Returns a dict with comparison results.
    """
    print(f"\n{'=' * 80}")
    print("DIRECTION COMPARISON MODE")
    print(f"{'=' * 80}")
    print(f"Query: {query}")
    print(f"Iterations per direction: {iterations}")
    print("Alternating pattern: F, B, F, B, ... (to avoid cache-warming bias)")
    if sleep_between > 0:
        print(f"Sleep between queries: {sleep_between}s")
    print()

    forward_durations = []
    backward_durations = []
    forward_line_counts = []
    backward_line_counts = []
    forward_log_tuples_all = []
    backward_log_tuples_all = []

    total_runs = iterations * 2  # iterations for each direction
    run_number = 0

    for i in range(iterations):
        # Forward iteration
        run_number += 1
        print(f"  Run {run_number}/{total_runs} [FORWARD]...", end="", flush=True)
        try:
            data, duration, line_count = client.query_range(
                query, start_ns, end_ns, limit, verbose=True, direction="forward"
            )
            forward_durations.append(duration)
            forward_line_counts.append(line_count)
            forward_log_tuples_all.append(extract_log_tuples(data))
            print(f" ✓ {duration:.3f}s ({line_count} lines)")
        except Exception as e:
            print(f" ✗ Error: {e}")
            raise

        if sleep_between > 0:
            print(f"    Sleeping {sleep_between}s...", flush=True)
            time.sleep(sleep_between)

        # Backward iteration
        run_number += 1
        print(f"  Run {run_number}/{total_runs} [BACKWARD]...", end="", flush=True)
        try:
            data, duration, line_count = client.query_range(
                query, start_ns, end_ns, limit, verbose=True, direction="backward"
            )
            backward_durations.append(duration)
            backward_line_counts.append(line_count)
            backward_log_tuples_all.append(extract_log_tuples(data))
            print(f" ✓ {duration:.3f}s ({line_count} lines)")
        except Exception as e:
            print(f" ✗ Error: {e}")
            raise

        # Sleep between iterations (except after the last one)
        if sleep_between > 0 and i < iterations - 1:
            print(f"    Sleeping {sleep_between}s...", flush=True)
            time.sleep(sleep_between)

    # === Log Equivalence Check ===
    print(f"\n{'=' * 80}")
    print("LOG EQUIVALENCE CHECK")
    print(f"{'=' * 80}")

    equivalence_results = []
    all_equivalent = True

    for i in range(iterations):
        fwd_tuples = forward_log_tuples_all[i]
        bwd_tuples = backward_log_tuples_all[i]

        fwd_counter = Counter(fwd_tuples)
        bwd_counter = Counter(bwd_tuples)

        is_equivalent = fwd_counter == bwd_counter
        if not is_equivalent:
            all_equivalent = False

        result = {
            "iteration": i + 1,
            "equivalent": is_equivalent,
            "forward_count": len(fwd_tuples),
            "backward_count": len(bwd_tuples),
        }

        print(f"\n  Iteration {i + 1}:")
        print(f"    Forward entries:  {len(fwd_tuples)}")
        print(f"    Backward entries: {len(bwd_tuples)}")

        if is_equivalent:
            print("    Result: EQUIVALENT (identical log content)")
        else:
            # Find differences
            only_in_forward = fwd_counter - bwd_counter
            only_in_backward = bwd_counter - fwd_counter

            print("    Result: DIFFERENT")
            if only_in_forward:
                print(f"    Entries only in forward:  {sum(only_in_forward.values())}")
                # Show first few differences
                for (ts, line), count in list(only_in_forward.most_common(3)):
                    print(f"      ts={ts} count={count}: {line[:80]}...")
            if only_in_backward:
                print(f"    Entries only in backward: {sum(only_in_backward.values())}")
                for (ts, line), count in list(only_in_backward.most_common(3)):
                    print(f"      ts={ts} count={count}: {line[:80]}...")

            result["only_in_forward_count"] = sum(only_in_forward.values())
            result["only_in_backward_count"] = sum(only_in_backward.values())

        equivalence_results.append(result)

    print(f"\n  {'─' * 76}")
    if all_equivalent:
        print(f"  OVERALL: ALL {iterations} iterations returned EQUIVALENT log content")
    else:
        equiv_count = sum(1 for r in equivalence_results if r["equivalent"])
        print(f"  OVERALL: {equiv_count}/{iterations} iterations returned equivalent content")
        print("  WARNING: Some iterations returned different logs between directions")

    # === Order Check ===
    print(f"\n{'=' * 80}")
    print("ORDER CHECK")
    print(f"{'=' * 80}")

    order_ok = True
    for i in range(iterations):
        fwd_tuples = forward_log_tuples_all[i]
        bwd_tuples = backward_log_tuples_all[i]

        if not fwd_tuples or not bwd_tuples:
            print(f"\n  Iteration {i + 1}: SKIPPED (empty results)")
            continue

        fwd_first_ts = fwd_tuples[0][0]
        fwd_last_ts = fwd_tuples[-1][0]
        bwd_first_ts = bwd_tuples[0][0]
        bwd_last_ts = bwd_tuples[-1][0]

        fwd_order_ok = fwd_first_ts <= fwd_last_ts
        bwd_order_ok = bwd_first_ts >= bwd_last_ts

        print(f"\n  Iteration {i + 1}:")
        print(
            f"    Forward:  first_ts={fwd_first_ts} last_ts={fwd_last_ts} (oldest first: {'OK' if fwd_order_ok else 'FAIL'})"
        )
        print(
            f"    Backward: first_ts={bwd_first_ts} last_ts={bwd_last_ts} (newest first: {'OK' if bwd_order_ok else 'FAIL'})"
        )

        if not fwd_order_ok or not bwd_order_ok:
            order_ok = False

    print(f"\n  {'─' * 76}")
    if order_ok:
        print("  ORDER: Forward returns oldest first, backward returns newest first - CORRECT")
    else:
        print("  ORDER: Some iterations had unexpected ordering")

    # === Performance Comparison ===
    print(f"\n{'=' * 80}")
    print("PERFORMANCE COMPARISON")
    print(f"{'=' * 80}")
    print("  NOTE: Iterations are alternated (F, B, F, B...) to reduce cache-warming bias,")
    print("  but some residual cache effects may remain.")

    fwd_stats = compute_statistics(forward_durations)
    bwd_stats = compute_statistics(backward_durations)

    print(f"\n  {'Forward':<40} {'Backward':<40}")
    print(f"  {'-' * 38} {'-' * 38}")
    print(f"  {'Mean: ' + f'{fwd_stats["mean"]:.3f}s':<40} {'Mean: ' + f'{bwd_stats["mean"]:.3f}s':<40}")
    print(f"  {'Median: ' + f'{fwd_stats["median"]:.3f}s':<40} {'Median: ' + f'{bwd_stats["median"]:.3f}s':<40}")
    print(f"  {'Std Dev: ' + f'{fwd_stats["stdev"]:.3f}s':<40} {'Std Dev: ' + f'{bwd_stats["stdev"]:.3f}s':<40}")
    print(
        f"  {'Avg lines: ' + f'{mean(forward_line_counts):.0f}':<40} {'Avg lines: ' + f'{mean(backward_line_counts):.0f}':<40}"
    )

    if fwd_stats["mean"] > 0:
        diff_pct = ((fwd_stats["mean"] - bwd_stats["mean"]) / fwd_stats["mean"]) * 100
        if abs(diff_pct) < 5:
            print(f"\n  Performance: Similar ({abs(diff_pct):.1f}% difference)")
        elif diff_pct > 0:
            print(f"\n  Performance: Backward is FASTER by {diff_pct:.1f}%")
        else:
            print(f"\n  Performance: Forward is FASTER by {abs(diff_pct):.1f}%")

    print()

    return {
        "mode": "direction_comparison",
        "query": query,
        "equivalence": {
            "all_equivalent": all_equivalent,
            "iterations": equivalence_results,
        },
        "order_check": {
            "correct": order_ok,
        },
        "forward": {
            "statistics": fwd_stats,
            "line_counts": forward_line_counts,
        },
        "backward": {
            "statistics": bwd_stats,
            "line_counts": backward_line_counts,
        },
    }


def parse_args(argv=None):
    """Validate explicit target and credential settings before any external access."""
    parser = argparse.ArgumentParser(description="Compare performance of two Loki queries")

    # Kubernetes config
    parser.add_argument("--kubeconfig", required=True, help="Path to kubeconfig")
    parser.add_argument("--context", required=True, help="Kubectl context")

    # Authentication - choose one method
    parser.add_argument("--secret-name", help="Kubernetes secret name for Loki bearer token")
    parser.add_argument("--secret-token-key", help="Key in secret containing auth token")
    parser.add_argument("--cert-secret", help="Kubernetes secret name for client certificate")
    parser.add_argument("--cert-secret-key", help="Key in secret for certificate")
    parser.add_argument("--key-secret", help="Kubernetes secret name for client key")
    parser.add_argument("--key-secret-key", help="Key in secret for private key")
    parser.add_argument("--ca-cert-secret", help="Kubernetes secret name for CA certificate")
    parser.add_argument("--ca-cert-secret-key", help="Key in secret for CA cert")

    # Loki config
    parser.add_argument("--loki-url", required=True, help="Loki URL (e.g., https://loki.example.com)")
    parser.add_argument("--x-scope-org-id", help="X-Scope-OrgId header value")

    # Time range parameters
    parser.add_argument(
        "--start-time",
        required=True,
        help="Start time (ISO format with timezone)",
    )
    parser.add_argument(
        "--end-time",
        required=True,
        help="End time (ISO format with timezone)",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Query limit")

    # Query configuration
    parser.add_argument("--query1", required=True, help="First LogQL query")
    parser.add_argument("--query1-name", default="Query 1", help="Name for Query 1")
    parser.add_argument("--query2", help="Second LogQL query (required unless comparing directions)")
    parser.add_argument("--query2-name", default="Query 2", help="Name for Query 2")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS, help="Number of iterations per query")
    parser.add_argument(
        "--sleep-between",
        type=int,
        default=DEFAULT_SLEEP_BETWEEN_QUERIES,
        help="Seconds to sleep between queries (default: 10)",
    )

    # Direction configuration
    parser.add_argument(
        "--direction",
        choices=["forward", "backward"],
        default="forward",
        help="Loki query direction",
    )
    parser.add_argument(
        "--compare-directions",
        action="store_true",
        help="Run same query with both directions, compare results (requires --use-adi-client)",
    )

    # Client configuration
    parser.add_argument(
        "--use-adi-client",
        action="store_true",
        default=False,
        help="Use recovery_libs LokiLogFetcher instead of SimpleLokiClient",
    )

    # Output
    parser.add_argument("--output", help="Save results to JSON file")

    args = parser.parse_args(argv)
    for name in ("kubeconfig", "context", "loki_url", "query1"):
        if not getattr(args, name).strip():
            parser.error(f"--{name.replace('_', '-')} must not be empty")
    if not args.compare_directions and not (args.query2 and args.query2.strip()):
        parser.error("--query2 is required unless --compare-directions is set")
    cert_options = (
        args.cert_secret,
        args.cert_secret_key,
        args.key_secret,
        args.key_secret_key,
        args.ca_cert_secret,
        args.ca_cert_secret_key,
    )
    if args.secret_name:
        if any(cert_options):
            parser.error("choose bearer-token or certificate authentication, not both")
        if not args.secret_token_key:
            parser.error("--secret-token-key is required with --secret-name")
    elif not all(cert_options):
        parser.error("provide --secret-name and --secret-token-key, or all certificate secret names and keys")
    if args.secret_token_key and not args.secret_name:
        parser.error("--secret-token-key requires --secret-name")
    endpoint = urlsplit(args.loki_url)
    if endpoint.scheme != "https" or not endpoint.hostname:
        parser.error("--loki-url must be an HTTPS URL for authenticated requests")
    if endpoint.username or endpoint.password:
        parser.error("--loki-url must not contain embedded credentials")
    if args.iterations < 1 or args.limit < 1 or args.sleep_between < 0:
        parser.error("iterations and limit must be positive; sleep-between must be nonnegative")
    try:
        start, end = parse_timestamp(args.start_time), parse_timestamp(args.end_time)
        if start.tzinfo is None or end.tzinfo is None or start >= end:
            raise ValueError("provide timezone-aware timestamps with start before end")
    except ValueError as error:
        parser.error(str(error))
    return args


def main():
    """Run a comparison against the operator-supplied Loki target."""
    args = parse_args()

    # Validate --compare-directions requires --use-adi-client
    if args.compare_directions and not args.use_adi_client:
        print(
            "Error: --compare-directions requires --use-adi-client.\n"
            "SimpleLokiClient has no pagination, so for queries exceeding Loki's max_entries_limit,\n"
            "forward returns the oldest N entries and backward returns the newest N entries,\n"
            "making comparison invalid. Use --use-adi-client for full paginated fetching."
        )
        return 1

    print("=" * 80)
    print("Loki Query Performance Comparison")
    print("=" * 80)

    # Step 1: Fetch credentials from kubectl
    print("\n[1/4] Fetching Loki credentials from Kubernetes...")
    auth_token = None
    cert_file = None
    key_file = None
    ca_cert = None
    temp_files = []

    try:
        kubectl = SimpleKubectl(args.kubeconfig, args.context)

        # Determine authentication method
        if args.cert_secret and args.key_secret and args.ca_cert_secret:
            print("Using certificate-based authentication...")

            cert_file = tempfile.mktemp(suffix=".crt")
            key_file = tempfile.mktemp(suffix=".key")
            ca_cert = tempfile.mktemp(suffix=".crt")

            temp_files = [cert_file, key_file, ca_cert]

            kubectl.write_secret_to_file(args.cert_secret, args.cert_secret_key, cert_file)
            kubectl.write_secret_to_file(args.key_secret, args.key_secret_key, key_file)
            kubectl.write_secret_to_file(args.ca_cert_secret, args.ca_cert_secret_key, ca_cert)

            # Verify files exist and have content
            for f, name in [(cert_file, "cert"), (key_file, "key"), (ca_cert, "CA")]:
                if not os.path.exists(f):
                    raise FileNotFoundError(f"{name} file doesn't exist: {f}")
                if os.path.getsize(f) == 0:
                    raise RuntimeError(f"{name} file is empty: {f}")

            print("✓ Certificates fetched from secrets")

        elif args.secret_name:
            print("Using bearer token authentication...")
            auth_token = kubectl.get_secret_value(args.secret_name, args.secret_token_key)
            print(f"✓ Token fetched from secret '{args.secret_name}'")

        else:
            print("✗ Error: Must provide either --secret-name or --cert-secret/--key-secret/--ca-cert-secret")
            return 1

    except Exception:
        logger.exception("✗ Error")
        for f in temp_files:
            if f and os.path.exists(f):
                os.unlink(f)
        return 1

    # Step 2: Parse timestamps
    print("\n[2/4] Parsing time range...")
    try:
        start_dt = parse_timestamp(args.start_time)
        end_dt = parse_timestamp(args.end_time)
        start_ns = int(start_dt.timestamp() * 1e9)
        end_ns = int(end_dt.timestamp() * 1e9)

        duration = (end_dt - start_dt).total_seconds()
        print(f"✓ Time range: {start_dt} to {end_dt}")
        print(f"  Duration: {duration:.2f} seconds ({duration / 60:.2f} minutes)")
    except (ValueError, TypeError, OverflowError, OSError) as e:
        print(f"✗ Failed to parse timestamps: {e}")
        return 1

    # Step 3: Load queries from config
    print("\n[3/4] Loading queries...")

    if args.compare_directions:
        print("  Mode: Direction comparison (forward vs backward)")
        print(f"  Query: {args.query1}")
        print(f"  Iterations per direction: {args.iterations}")
    else:
        if not args.query1 or not args.query2:
            print("✗ Error: Both --query1 and --query2 must be specified")
            return 1

        query1 = args.query1
        query2 = args.query2
        query1_name = args.query1_name
        query2_name = args.query2_name

        print("✓ Queries loaded from configuration")
        print(f"  Query 1: {query1_name}")
        print(f"  Query 2: {query2_name}")
        print(f"  Direction: {args.direction}")
        print(f"  Iterations: {args.iterations}")

    # Step 4: Execute queries
    print("\n[4/4] Executing queries...")

    # Choose client
    if args.use_adi_client:
        print("Using recovery_libs LokiLogFetcher...")
        if not RECOVERY_LIBS_AVAILABLE:
            print("✗ Error: recovery_libs not available. Install dependencies or set --use-adi-client=false")
            return 1
        client_class = RecoveryLokiClient
    else:
        print("Using SimpleLokiClient...")
        client_class = SimpleLokiClient

    try:
        client = client_class(
            loki_url=args.loki_url,
            auth_token=auth_token,
            x_scope_org_id=args.x_scope_org_id,
            cert_file=cert_file,
            key_file=key_file,
            ca_cert=ca_cert,
        )

        if args.compare_directions:
            # Direction comparison mode
            comparison_results = run_direction_comparison(
                client,
                args.query1,
                start_ns,
                end_ns,
                args.limit,
                args.iterations,
                sleep_between=args.sleep_between,
            )

            # Save to file if requested
            if args.output:
                result = {
                    "timestamp": datetime.now().isoformat(),
                    "parameters": {
                        "start_time": args.start_time,
                        "end_time": args.end_time,
                        "limit": args.limit,
                        "iterations": args.iterations,
                        "mode": "direction_comparison",
                    },
                    "results": comparison_results,
                }

                with open(args.output, "w") as f:
                    json.dump(result, f, indent=2)

                print(f"Results saved to: {args.output}")

        else:
            # Standard two-query comparison mode
            query1_results = run_query_multiple_times(
                client,
                query1,
                start_ns,
                end_ns,
                args.limit,
                args.iterations,
                query1_name,
                sleep_between=args.sleep_between,
                direction=args.direction,
            )

            query2_results = run_query_multiple_times(
                client,
                query2,
                start_ns,
                end_ns,
                args.limit,
                args.iterations,
                query2_name,
                sleep_between=args.sleep_between,
                direction=args.direction,
            )

            # Compare results
            compare_results(query1_results, query2_results)

            # Save to file if requested
            if args.output:
                result = {
                    "timestamp": datetime.now().isoformat(),
                    "parameters": {
                        "start_time": args.start_time,
                        "end_time": args.end_time,
                        "limit": args.limit,
                        "iterations": args.iterations,
                        "direction": args.direction,
                    },
                    "query1": query1_results,
                    "query2": query2_results,
                }

                with open(args.output, "w") as f:
                    json.dump(result, f, indent=2)

                print(f"Results saved to: {args.output}")

        print("\n" + "=" * 80)

        # Clean up temp files
        for f in temp_files:
            if f and os.path.exists(f):
                os.unlink(f)

        return 0

    except Exception:
        logger.exception("✗ Error")

        # Clean up temp files
        for f in temp_files:
            if f and os.path.exists(f):
                os.unlink(f)

        return 1


if __name__ == "__main__":
    sys.exit(main())
