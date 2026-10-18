# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Wait for the E2E stack components to be Ready before any scenario runs.

Reads E2E_STACK_NAMESPACE, E2E_STACK_DEPLOYMENTS, E2E_STACK_STATEFULSETS, and
E2E_STACK_TIMEOUT. An empty namespace disables the gate. Exits 1 if any
component is not Ready within the timeout. Failure reasons show the last line of
kubectl's output.

Requires the recovery-libs ``e2e`` extra for tabulate.
"""

import logging
import os
import subprocess
import sys
import time

from tabulate import tabulate

logger = logging.getLogger(__name__)

GREEN = "\033[32m"
RED = "\033[31m"
RESET = "\033[0m"


def rollout_ready(namespace: str, workload: str, timeout_seconds: int) -> tuple[bool, str]:
    """Return whether the workload finished rolling out, with the reason when it did not."""

    try:
        result = subprocess.run(
            ["kubectl", "rollout", "status", "-n", namespace, workload, f"--timeout={timeout_seconds}s"],
            capture_output=True,
            text=True,
            timeout=timeout_seconds + 10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return False, str(error)
    return result.returncode == 0, (result.stderr or result.stdout).strip()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    namespace = os.environ.get("E2E_STACK_NAMESPACE", "")
    if not namespace:
        return 0
    timeout_seconds = int(os.environ.get("E2E_STACK_TIMEOUT", "180"))
    workloads = [f"deploy/{name}" for name in os.environ.get("E2E_STACK_DEPLOYMENTS", "").split()] + [
        f"statefulset/{name}" for name in os.environ.get("E2E_STACK_STATEFULSETS", "").split()
    ]

    use_color = sys.stdout.isatty() and "NO_COLOR" not in os.environ
    rows: list[tuple[str, str, str]] = []
    failures: list[str] = []
    for workload in workloads:
        started = time.monotonic()
        ready, reason = rollout_ready(namespace, workload, timeout_seconds)
        elapsed = f"{time.monotonic() - started:.1f}s"
        status, color = ("READY", GREEN) if ready else ("FAILED", RED)
        rows.append((f"{color}{status}{RESET}" if use_color else status, elapsed, workload))
        if not ready:
            detail = reason.splitlines()[-1] if reason else "no kubectl output"
            failures.append(f"{workload}: {detail}")

    header = (
        f"\nE2E Stack Gate Check:\n\nLog backend: {os.environ.get('E2E_LOG_BACKEND', 'unknown')}\nScheduler: slurm\n"
    )
    table = tabulate(
        rows, headers=("STATUS", "TIME", "COMPONENT"), tablefmt="simple", colalign=("left", "right", "left")
    )
    total = sum(float(elapsed.removesuffix("s")) for _, elapsed, _ in rows)
    if not failures:
        logger.info("%s\n%s\n\nStack ready: %d components (%.1fs)\n", header, table, len(rows), total)
        return 0

    logger.error(
        "%s\n%s\n\n%s\n\nStack not ready: %d of %d components failed (%.1fs)\n",
        header,
        table,
        "\n".join(failures),
        len(failures),
        len(rows),
        total,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
