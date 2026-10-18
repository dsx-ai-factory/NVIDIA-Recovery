# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Slurm clients that reach the cluster over SSH, kubectl exec, or the slurmrestd HTTP API."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import shlex
import subprocess
import time
from collections.abc import Callable, Generator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx
import paramiko
from opentelemetry import trace
from pydantic import AliasChoices, Field, FilePath, HttpUrl, PositiveFloat
from pydantic_settings import BaseSettings, SettingsConfigDict

from recovery_libs.testing.http.retry import RetryingTransport, RetryPolicy
from recovery_libs.testing.http.tracing import TRACER_NAME, TracedTransport

TIMED_OUT = 124
REST_API_VERSION = "v0.0.43"
DEFAULT_BATCH_PATH = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# `--export=NAME=value,...` makes Slurm recreate the user's login environment
# before every start, including after a requeue, and a containerized login
# node cannot always run that helper. `--export-file` passes the same values
# without it. The first argument counts the NAME=value arguments that follow;
# the rest are sbatch options, and the job script arrives on stdin.
SBATCH_PROGRAM = """set -euo pipefail
job_file="$(mktemp "${TMPDIR:-/tmp}/e2e_job.XXXXXXXX")"
environment_file="$(mktemp "${TMPDIR:-/tmp}/e2e_env.XXXXXXXX")"
trap 'rm -f "$job_file" "$environment_file"' EXIT
environment_count="$1"
shift
: > "$environment_file"
for ((index = 0; index < environment_count; index++)); do
    printf '%s\\0' "$1" >> "$environment_file"
    shift
done
cat > "$job_file"
umask 0027
sbatch --parsable --export-file="$environment_file" "$@" "$job_file"
"""


class SlurmClient(Protocol):
    """Carries Slurm operations to the cluster and returns Slurm's JSON job records.

    Jobs are submitted as Slurm job descriptions, using slurmrestd's field names; `SlurmLauncher` builds them.
    `supports_requeue_hold` is False when the client cannot reach Slurm's requeue-hold operation.
    """

    supports_requeue_hold: bool

    def submit(self, description: Mapping[str, Any]) -> str: ...

    def get(self, *, job_id: str) -> Mapping[str, Any] | None: ...

    def accounting(self, *, job_id: str) -> Mapping[str, Any] | None:
        """The job's allocation record from Slurm accounting, which outlives the controller's record."""
        ...

    def list(self, *, name: str | None = None) -> Sequence[Mapping[str, Any]]: ...

    def cancel(self, *, job_id: str) -> None: ...

    def release(self, *, job_id: str) -> None: ...

    def requeue_hold(self, *, job_id: str) -> None: ...


@dataclass(frozen=True)
class CommandResult:
    """The outcome of one command on the login node."""

    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    def checked(self) -> CommandResult:
        if self.returncode != 0:
            raise CommandFailed(self)
        return self


class CommandFailed(RuntimeError):
    """A command that exited unsuccessfully."""

    def __init__(self, result: CommandResult) -> None:
        output = result.stderr.strip() or result.stdout.strip() or "no output"
        super().__init__(f"{result.command[0]} exited {result.returncode}: {output[-2000:]}")
        self.result = result


class _SlurmCommandClient:
    """A `SlurmClient` that runs Slurm commands on a login node. Subclasses define how a command runs."""

    supports_requeue_hold = True
    timeout: float

    def run(self, command: Sequence[str], stdin: bytes | None, timeout: float) -> CommandResult:
        raise NotImplementedError

    def submit(self, description: Mapping[str, Any]) -> str:
        assignments = tuple(description.get("environment", ()))
        time_limit = description.get("time_limit")
        options = (
            f"--job-name={description['name']}",
            f"--chdir={description['current_working_directory']}",
            *((f"--time={time_limit['number']}",) if time_limit else ()),
            *(("--hold",) if description.get("hold") else ()),
            *((f"--comment={description['comment']}",) if description.get("comment") else ()),
        )
        command = ("bash", "-c", SBATCH_PROGRAM, "_", str(len(assignments)), *assignments, *options)
        output = self._command(command, stdin=description["script"].encode()).checked().stdout.strip()
        job_id = output.split(";", 1)[0]
        if not job_id.isdigit():
            raise RuntimeError(f"sbatch returned an invalid job ID: {output!r}")
        return job_id

    def get(self, *, job_id: str) -> Mapping[str, Any] | None:
        result = self._command(("scontrol", "show", "job", job_id, "--json"))
        if result.returncode != 0 and "Invalid job id" in result.stderr:
            return None
        jobs = json.loads(result.checked().stdout).get("jobs", [])
        return jobs[0] if jobs else None

    def accounting(self, *, job_id: str) -> Mapping[str, Any] | None:
        jobs = json.loads(self._command(("sacct", "-X", "-j", job_id, "--json")).checked().stdout).get("jobs", [])
        return _latest_attempt(jobs)

    def list(self, *, name: str | None = None) -> Sequence[Mapping[str, Any]]:
        command = ("squeue", "--json", *((f"--name={name}",) if name else ()))
        return json.loads(self._command(command).checked().stdout).get("jobs", [])

    def cancel(self, *, job_id: str) -> None:
        self._command(("scancel", "--quiet", job_id)).checked()

    def release(self, *, job_id: str) -> None:
        self._command(("scontrol", "release", job_id)).checked()

    def requeue_hold(self, *, job_id: str) -> None:
        self._command(("scontrol", "requeuehold", job_id)).checked()

    def _command(self, command: Sequence[str], *, stdin: bytes | None = None) -> CommandResult:
        tracer = trace.get_tracer(TRACER_NAME)
        with tracer.start_as_current_span(f"command {command[0]}", attributes={"process.command": command[0]}) as span:
            result = self.run(command, stdin, self.timeout)
            span.set_attribute("process.exit.code", result.returncode)
            if result.returncode != 0:
                span.set_status(trace.Status(trace.StatusCode.ERROR))
            return result


@dataclass(frozen=True)
class SlurmSshClient(_SlurmCommandClient):
    """A `SlurmClient` that runs Slurm commands on a login node over SSH with paramiko.

    Each command opens its own connection. Unknown host keys are rejected unless
    `trust_unknown_hosts` is set, which suits disposable test clusters whose keys
    change on every deployment.
    """

    host: str
    username: str
    port: int = 22
    key_filename: Path | None = None
    known_hosts: Path | None = None
    trust_unknown_hosts: bool = False
    timeout: float = 60.0
    client_factory: Callable[[], paramiko.SSHClient] = paramiko.SSHClient

    class Settings(BaseSettings):
        """`LAUNCHER_SLURM_SSH_*` environment variables."""

        model_config = SettingsConfigDict(env_prefix="LAUNCHER_SLURM_SSH_", frozen=True)

        host: str
        port: int = Field(default=22, ge=1, le=65535)
        username: str = "root"
        key_file: FilePath | None = None
        known_hosts: FilePath | None = None
        trust_unknown_hosts: bool = False
        timeout: PositiveFloat = 60.0

    @classmethod
    def from_environment(cls) -> SlurmSshClient:
        settings = cls.Settings()
        return cls(
            host=settings.host,
            username=settings.username,
            port=settings.port,
            key_filename=settings.key_file,
            known_hosts=settings.known_hosts,
            trust_unknown_hosts=settings.trust_unknown_hosts,
            timeout=settings.timeout,
        )

    def run(self, command: Sequence[str], stdin: bytes | None, timeout: float) -> CommandResult:
        with self.client_factory() as client:
            if self.known_hosts is not None:
                client.load_host_keys(str(self.known_hosts))
            else:
                client.load_system_host_keys()
            client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy() if self.trust_unknown_hosts else paramiko.RejectPolicy()
            )
            client.connect(
                self.host,
                port=self.port,
                username=self.username,
                key_filename=str(self.key_filename) if self.key_filename else None,
                timeout=timeout,
            )
            channel_stdin, channel_stdout, channel_stderr = client.exec_command(shlex.join(command), timeout=timeout)
            if stdin is not None:
                channel_stdin.write(stdin)
            channel_stdin.channel.shutdown_write()
            try:
                stdout = channel_stdout.read().decode(errors="replace")
                stderr = channel_stderr.read().decode(errors="replace")
            except TimeoutError:
                return CommandResult(tuple(command), TIMED_OUT, "", f"timed out after {timeout:g}s")
            return CommandResult(tuple(command), channel_stdout.channel.recv_exit_status(), stdout, stderr)


@dataclass(frozen=True)
class SlurmKubectlClient(_SlurmCommandClient):
    """A `SlurmClient` that runs Slurm commands in a login pod with `kubectl exec`."""

    namespace: str
    pod: str
    container: str | None = None
    kubeconfig: str | None = None
    timeout: float = 60.0

    class Settings(BaseSettings):
        """`LAUNCHER_SLURM_KUBECTL_*` environment variables. The kubeconfig also falls back to `KUBECONFIG`."""

        model_config = SettingsConfigDict(env_prefix="LAUNCHER_SLURM_KUBECTL_", frozen=True)

        namespace: str = "slurm"
        pod_selector: str = "app.kubernetes.io/component=login"
        container: str = "login"
        kubeconfig: FilePath | None = Field(
            default=None, validation_alias=AliasChoices("LAUNCHER_SLURM_KUBECTL_KUBECONFIG", "KUBECONFIG")
        )
        timeout: PositiveFloat = 60.0

    @classmethod
    def from_environment(cls) -> SlurmKubectlClient:
        settings = cls.Settings()
        kubeconfig = str(settings.kubeconfig) if settings.kubeconfig else None
        return cls(
            namespace=settings.namespace,
            pod=_login_pod(settings.namespace, settings.pod_selector, kubeconfig),
            container=settings.container,
            kubeconfig=kubeconfig,
            timeout=settings.timeout,
        )

    def run(self, command: Sequence[str], stdin: bytes | None, timeout: float) -> CommandResult:
        argv = (
            "kubectl",
            *(("--kubeconfig", self.kubeconfig) if self.kubeconfig else ()),
            "exec",
            *(("-i",) if stdin is not None else ()),
            "-n",
            self.namespace,
            self.pod,
            *(("-c", self.container) if self.container else ()),
            "--",
            *command,
        )
        try:
            completed = subprocess.run(argv, input=stdin, capture_output=True, timeout=timeout, check=False)
        except subprocess.TimeoutExpired:
            return CommandResult(tuple(command), TIMED_OUT, "", f"timed out after {timeout:g}s")
        return CommandResult(
            tuple(command),
            completed.returncode,
            completed.stdout.decode(errors="replace"),
            completed.stderr.decode(errors="replace"),
        )


def _login_pod(namespace: str, selector: str, kubeconfig: str | None) -> str:
    pod = subprocess.run(
        (
            "kubectl",
            *(("--kubeconfig", kubeconfig) if kubeconfig else ()),
            "get",
            "pod",
            "-n",
            namespace,
            "-l",
            selector,
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    ).stdout.strip()
    if not pod:
        raise RuntimeError(f"no login pod matches {selector!r} in namespace {namespace}")
    return pod


def _base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


class SlurmJwtAuth(httpx.Auth):
    """Sign a short-lived HS256 token for each request with the cluster's JWT key."""

    def __init__(
        self,
        *,
        username: str,
        key: bytes,
        lifetime_seconds: int = 300,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._username = username
        self._key = key
        self._lifetime = lifetime_seconds
        self._clock = clock

    def token(self) -> str:
        issued = int(self._clock())
        header = _base64url(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
        claims = {"iat": issued, "exp": issued + self._lifetime, "sun": self._username}
        payload = _base64url(json.dumps(claims, separators=(",", ":")).encode())
        signature = hmac.new(self._key, f"{header}.{payload}".encode(), hashlib.sha256).digest()
        return f"{header}.{payload}.{_base64url(signature)}"

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["X-SLURM-USER-NAME"] = self._username
        request.headers["X-SLURM-USER-TOKEN"] = self.token()
        yield request


def _latest_attempt(records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """Accounting keeps one record per attempt of a requeued job; the latest holds its current state."""

    return max(
        records,
        key=lambda record: (record.get("restart_cnt", 0), record.get("time", {}).get("submission", 0)),
        default=None,
    )


class SlurmRestError(RuntimeError):
    """slurmrestd rejected a request or reported errors."""


class SlurmHttpClient:
    """A `SlurmClient` that reaches Slurm through the slurmrestd HTTP API. Build `http` with `http_client`."""

    supports_requeue_hold = False

    def __init__(self, http: httpx.Client, *, api_version: str = REST_API_VERSION) -> None:
        self._http = http
        self._prefix = f"/slurm/{api_version}"
        self._accounting_prefix = f"/slurmdb/{api_version}"

    class Settings(BaseSettings):
        """`LAUNCHER_SLURM_HTTP_*` environment variables."""

        model_config = SettingsConfigDict(env_prefix="LAUNCHER_SLURM_HTTP_", frozen=True)

        url: HttpUrl
        username: str = "root"
        jwt_key_file: FilePath
        api_version: str = REST_API_VERSION
        timeout: PositiveFloat = 30.0

    @classmethod
    def from_environment(cls) -> SlurmHttpClient:
        settings = cls.Settings()
        auth = SlurmJwtAuth(username=settings.username, key=settings.jwt_key_file.read_bytes())
        return cls(
            http_client(str(settings.url), auth=auth, timeout=settings.timeout), api_version=settings.api_version
        )

    def submit(self, description: Mapping[str, Any]) -> str:
        job = dict(description)
        # Slurm cannot write a batch job with an empty environment, and slurmrestd, unlike sbatch, inherits none.
        job["environment"] = list(job.get("environment") or ()) or [DEFAULT_BATCH_PATH]
        return str(self._request("POST", "/job/submit", json={"job": job})["job_id"])

    def get(self, *, job_id: str) -> Mapping[str, Any] | None:
        jobs = self._request("GET", f"/job/{job_id}", missing_ok=True).get("jobs", [])
        return jobs[0] if jobs else None

    def accounting(self, *, job_id: str) -> Mapping[str, Any] | None:
        path = f"{self._accounting_prefix}/job/{job_id}"
        return _latest_attempt(self._request("GET", path, missing_ok=True, absolute=True).get("jobs", []))

    def list(self, *, name: str | None = None) -> Sequence[Mapping[str, Any]]:
        records = self._request("GET", "/jobs").get("jobs", [])
        return [record for record in records if name is None or record.get("name") == name]

    def cancel(self, *, job_id: str) -> None:
        self._request("DELETE", f"/job/{job_id}")

    def release(self, *, job_id: str) -> None:
        self._request("POST", f"/job/{job_id}", json={"hold": False})

    def requeue_hold(self, *, job_id: str) -> None:
        raise NotImplementedError("slurmrestd has no requeue-hold operation; use the ssh or kubectl client")

    def _request(
        self, method: str, path: str, *, json: Any = None, missing_ok: bool = False, absolute: bool = False
    ) -> dict[str, Any]:
        response = self._http.request(method, path if absolute else f"{self._prefix}{path}", json=json)
        body = response.json() if response.content else {}
        errors = [error.get("description") or error.get("error") for error in body.get("errors", [])]
        if missing_ok and (response.status_code == 404 or any("Invalid job id" in str(e) for e in errors)):
            return {"jobs": []}
        if response.is_error or errors:
            raise SlurmRestError(f"{method} {path} returned HTTP {response.status_code}: {errors or response.text}")
        return body


def http_client(
    base_url: str,
    *,
    auth: httpx.Auth,
    timeout: float = 30.0,
    retry: RetryPolicy = RetryPolicy(),
    transport: httpx.BaseTransport | None = None,
) -> httpx.Client:
    """Build an httpx client for slurmrestd that retries transient failures and traces each attempt."""

    return httpx.Client(
        base_url=base_url,
        auth=auth,
        timeout=timeout,
        transport=RetryingTransport(TracedTransport(transport or httpx.HTTPTransport()), retry),
    )
