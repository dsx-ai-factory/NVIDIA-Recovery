# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reach Kubernetes services from a test run that has no other route to them, and change the deployed stack."""

from __future__ import annotations

import json
import queue
import re
import subprocess
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

_FORWARDING = re.compile(r"Forwarding from 127\.0\.0\.1:(\d+) ->")


@contextmanager
def service_url(
    *,
    namespace: str,
    service: str,
    port: int,
    url: str | None = None,
    kubeconfig: str | None = None,
) -> Iterator[str]:
    """Yield `url` when the caller has one, and otherwise forward a local port to the service and yield its URL."""

    if url:
        yield url
        return
    with port_forward(namespace=namespace, service=service, port=port, kubeconfig=kubeconfig) as forwarded:
        yield forwarded


@contextmanager
def port_forward(
    *,
    namespace: str,
    service: str,
    port: int,
    kubeconfig: str | None = None,
    timeout: float = 30.0,
) -> Iterator[str]:
    """Forward a free local port to a service port and yield its base URL, stopping the forward on exit.

    kubectl attaches a forward to one pod and exits when that pod goes away, as it does when a test restarts the
    service. A supervising thread restarts kubectl on the same local port until the context exits; clients see
    connection errors only while it reconnects.
    """

    def start(local: str) -> subprocess.Popen[str]:
        argv = (
            "kubectl",
            *(("--kubeconfig", kubeconfig) if kubeconfig else ()),
            "port-forward",
            "-n",
            namespace,
            f"svc/{service}",
            f"{local}:{port}",
            "--address",
            "127.0.0.1",
        )
        return subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    stopping = threading.Event()
    current = [start("")]
    try:
        local_port = _local_port(current[0], timeout)
    except BaseException:
        current[0].terminate()
        raise

    # Held while deciding to restart and swapping the process, so shutdown never misses a kubectl started after it.
    swapping = threading.Lock()

    def supervise() -> None:
        while True:
            _discard_output(current[0])
            current[0].wait()
            if stopping.wait(1.0):
                return
            with swapping:
                if stopping.is_set():
                    return
                current[0] = start(local_port)

    supervisor = threading.Thread(target=supervise, daemon=True)
    supervisor.start()
    try:
        yield f"http://127.0.0.1:{local_port}"
    finally:
        with swapping:
            stopping.set()
            current[0].terminate()
        supervisor.join(timeout=10)


@dataclass(frozen=True)
class StackControl:
    """Changes a test makes to the deployed stack in one namespace, through kubectl."""

    namespace: str
    kubeconfig: str | None = None
    timeout: float = 300.0

    def replicas(self, deployment: str) -> int:
        """The replica count the deployment asks for."""

        return int(self._kubectl("get", "deployment", deployment, "-o", "jsonpath={.spec.replicas}") or 0)

    def scale(self, deployment: str, replicas: int) -> None:
        """Scale the deployment and wait until exactly `replicas` pods remain, ready."""

        if replicas:
            self._kubectl("scale", f"deployment/{deployment}", f"--replicas={replicas}")
            self._kubectl("rollout", "status", f"deployment/{deployment}", f"--timeout={self.timeout:g}s")
            return
        pods = self._pods(deployment)
        self._kubectl("scale", f"deployment/{deployment}", "--replicas=0")
        self._wait_deleted(pods)

    def get(self, kind: str, name: str) -> dict[str, Any]:
        """The named resource, as kubectl returns it in JSON."""

        return json.loads(self._kubectl("get", kind, name, "-o", "json"))

    def patch(self, kind: str, name: str, patch: Mapping[str, Any]) -> None:
        """Apply a JSON merge patch to the named resource."""

        self._kubectl("patch", kind, name, "--type=merge", "-p", json.dumps(patch))

    def exec(self, selector: str, *command: str) -> str:
        """Run `command` in the first pod matching the label `selector` and return its output."""

        return self._kubectl("exec", self._pod(selector), "--", *command)

    def copy(self, selector: str, source: Path, destination: str) -> None:
        """Copy the local file `source` to `destination` in the first pod matching the label `selector`."""

        self._kubectl("cp", "--", str(source), f"{self._pod(selector)}:{destination}")

    def logs(self, deployment: str, *, since: str, tail: int = 1000) -> str:
        """Recent log output of every container of `deployment` since the RFC 3339 time `since`."""

        return self._kubectl(
            "logs", f"deploy/{deployment}", f"--since-time={since}", f"--tail={tail}", "--all-containers=true"
        )

    def restart(self, deployment: str, *, wait_for_old_pods: bool = False) -> None:
        """Replace the deployment's pods and wait until the new ones are ready.

        With `wait_for_old_pods`, also wait until every replaced pod is gone. A rollout reports ready while old pods
        are still terminating, and those can keep serving connections opened before the restart.
        """

        old_pods = self._pods(deployment) if wait_for_old_pods else ()
        self._kubectl("rollout", "restart", f"deployment/{deployment}")
        self._kubectl("rollout", "status", f"deployment/{deployment}", f"--timeout={self.timeout:g}s")
        self._wait_deleted(old_pods)

    def _pods(self, deployment: str) -> tuple[str, ...]:
        # Name the pods up front: some kubectl versions fail a delete wait whose selector matches nothing.
        selector = self.get("deployment", deployment)["spec"]["selector"]
        labels = selector.get("matchLabels", {})
        if not labels or selector.get("matchExpressions"):
            raise RuntimeError(f"deployment {deployment} must select its pods by matchLabels alone")
        names = self._kubectl("get", "pods", "-l", ",".join(f"{k}={v}" for k, v in labels.items()), "-o", "name")
        return tuple(names.split())

    def _wait_deleted(self, pods: tuple[str, ...]) -> None:
        if pods:
            self._kubectl("wait", "--for=delete", *pods, f"--timeout={self.timeout:g}s")

    def _pod(self, selector: str) -> str:
        pod = self._kubectl("get", "pods", "-l", selector, "-o", "jsonpath={.items[0].metadata.name}")
        if not pod:
            raise RuntimeError(f"no pod matches {selector} in {self.namespace}")
        return pod

    def _kubectl(self, *arguments: str) -> str:
        argv = (
            "kubectl",
            *(("--kubeconfig", self.kubeconfig) if self.kubeconfig else ()),
            "--namespace",
            self.namespace,
            *arguments,
        )
        result = subprocess.run(argv, capture_output=True, text=True, timeout=self.timeout + 30, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"{' '.join(argv)} failed: {result.stderr.strip() or result.stdout.strip()}")
        return result.stdout.strip()


def _local_port(process: subprocess.Popen[str], timeout: float) -> str:
    """The local port kubectl reports forwarding from, waiting at most `timeout` seconds.

    Output is read on a helper thread because a read blocks until kubectl prints, and a stalled kubectl may never
    print; the helper stops at the forwarding line, leaving the rest of the output for the caller to drain.
    """

    found: queue.Queue[str | None] = queue.Queue(maxsize=1)

    def read() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            if match := _FORWARDING.search(line):
                found.put(match.group(1))
                return
        found.put(None)

    threading.Thread(target=read, daemon=True).start()
    try:
        port = found.get(timeout=timeout)
    except queue.Empty:
        port = None
    if port is None:
        raise RuntimeError(f"kubectl port-forward did not start: {' '.join(cast(Sequence[str], process.args))}")
    return port


def _discard_output(process: subprocess.Popen[str]) -> None:
    assert process.stdout is not None
    for _ in process.stdout:
        pass
