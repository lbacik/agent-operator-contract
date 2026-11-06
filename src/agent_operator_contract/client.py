"""Client part of the shared library: discovery and online invocation over Docker.

The consumer boundary is the ``docker`` executable: this module talks to
Docker only through the configurable ``docker`` CLI via subprocess (spec
sections 2 and 3.1). It never imports Docker SDKs and never shells out to
anything else.

Discovery lists containers carrying the contract label (running and stopped),
drops Compose one-off containers, reads the four discovery labels and detects
duplicate instance names. Addressing a duplicated name fails without touching
any container. Online invocation is exactly spec section 3.1
(``docker exec --user <exec-user> …``, no TTY, ``agentctl --contract 1 …``
with ``--contract`` first). Classification decides on ``error.code`` and falls
back to the exit code only when stdout is not a valid envelope (spec
section 4).
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import secrets
import subprocess
import tarfile
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from agent_operator_contract import CONTRACT_MAJOR, documents

CONTRACT_LABEL = "io.github.lbacik.agent-operator.contract"
INSTANCE_LABEL = "io.github.lbacik.agent-operator.instance"
EXEC_USER_LABEL = "io.github.lbacik.agent-operator.exec-user"
BACKEND_LABEL = "io.github.lbacik.agent-operator.backend"
ONEOFF_LABEL = "com.docker.compose.oneoff"

AGENTCTL_PATH = "/usr/local/bin/agentctl"

DOCKER_ENV_VAR = "AGENTFLEET_DOCKER"
DEFAULT_TIMEOUT = 10.0

CLIENT_CONTRACT = f"{CONTRACT_MAJOR}.0"

MUTATION_TIMEOUT = 15.0
"""Default consumer timeout for mutations (spec section 3.3)."""

READ_TIMEOUT = 10.0
"""Default consumer timeout for ``identity``, ``status`` and ``command`` (spec section 3.3)."""

ATTEMPTS_LIST_TIMEOUT = 15.0
"""Default consumer timeout for ``attempts list`` (spec section 3.3)."""

ATTEMPTS_SHOW_TIMEOUT = 15.0
"""Default consumer timeout for ``attempts show`` (spec section 3.3)."""

ATTEMPTS_EXPORT_TIMEOUT = 30.0
"""Default consumer progress timeout for ``attempts export`` (spec section 3.3)."""

MAX_MUTATION_ATTEMPTS = 3
"""Bounded retries for one ambiguous mutation call (spec section 8.4)."""

EXPORT_MAX_ATTEMPTS = 3
"""Bounded refetches of a torn export stream (spec section 11.3)."""

EXPORT_CHUNK_SIZE = 65536
"""Read size for one streaming export chunk."""

MUTATION_RETRY_BACKOFF_BASE = 0.05
"""Base backoff in seconds between ambiguous mutation retries; doubled each time."""


def new_request_id() -> str:
    """Generate a mutation request ID before submission (spec section 6.2).

    The ID comes from a cryptographically secure source and matches the
    ``^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`` pattern. A caller-supplied ID
    passes through untouched instead: only generated IDs come from here.
    """
    return f"req-{secrets.token_hex(8)}"


def timeout_for(operation: str, override: float | None = None) -> float:
    """Return the consumer default timeout for one operation class (spec section 3.3).

    ``override`` wins when positive; otherwise reads carry 10 s, mutations and
    ``attempts list``/``attempts show`` carry 15 s, and ``attempts export``
    carries a 30 s progress bound. An unrecognized operation is treated as a
    mutation (the only caller is the mutation retry path), so it carries 15 s:
    a too-short timeout would manufacture ambiguous outcomes (spec §8.4).
    """
    if override is not None and override > 0:
        return override
    if operation in ("identity", "status", "command"):
        return READ_TIMEOUT
    if operation in ("attempts-list", "attempts-show", "attempts", "attempts_list"):
        return ATTEMPTS_LIST_TIMEOUT
    if operation in ("attempts-export", "attempts_export"):
        return ATTEMPTS_EXPORT_TIMEOUT
    return MUTATION_TIMEOUT


def is_ambiguous_envelope(envelope: dict[str, object]) -> bool:
    """Decide whether a mutation outcome is ambiguous (spec sections 4 and 8.4).

    ``agent_unreachable`` is always ambiguous; ``internal`` is ambiguous unless
    ``details.stored`` is ``false``. Every other envelope (success or a
    definitive refusal) is not ambiguous.
    """
    if envelope.get("ok") is True:
        return False
    error = envelope.get("error")
    if not isinstance(error, dict):
        return False
    code = error.get("code")
    if code == "agent_unreachable":
        return True
    if code == "internal":
        details = error.get("details")
        return not (isinstance(details, dict) and details.get("stored") is False)
    return False


class AmbiguousMutationError(Exception):
    """A mutation stayed ambiguous after the bounded retries (spec section 8.4).

    Carries the request ID so the operator can follow up with
    ``agentfleet command <instance> <request-id>`` instead of resubmitting.
    """

    def __init__(
        self,
        request_id: str,
        attempts: int,
        message: str,
        exit_code: int = 6,
        envelope: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.request_id = request_id
        self.attempts = attempts
        self.exit_code = exit_code
        self.envelope = envelope


def invoke_mutation_with_retry(
    docker: str,
    instance: Instance,
    agentctl_argv: list[str],
    request_id: str,
    timeout: float | None = None,
    max_attempts: int | None = None,
    backoff_base: float = MUTATION_RETRY_BACKOFF_BASE,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[ExecResult, dict[str, object]]:
    """Run one mutation, retrying ambiguous outcomes with the same ID and payload.

    The same ``agentctl_argv`` (same request ID and payload) is resent a
    bounded number of times with backoff inside this call. An outcome is
    ambiguous when the exec fails without a producer envelope (timeout or
    Docker exit 125-127, surfaced as :class:`ExecFailedError`) or when the
    envelope is ``agent_unreachable`` or ``internal`` without
    ``details.stored: false``. ``internal`` with ``details.stored: false``
    and every other envelope are definitive and return at once. When the
    retries are exhausted, raises :class:`AmbiguousMutationError` carrying the
    request ID.
    """
    operation = agentctl_argv[0] if agentctl_argv else "mutation"
    effective_timeout = timeout_for(operation, timeout)
    attempts = max_attempts if max_attempts is not None else MAX_MUTATION_ATTEMPTS
    if attempts < 1:
        attempts = 1
    for attempt in range(1, attempts + 1):
        try:
            exec_result = invoke(docker, instance, agentctl_argv, timeout=effective_timeout)
            envelope = interpret(exec_result)
        except ExecFailedError as exc:
            if attempt < attempts:
                sleep(backoff_base * (2.0 ** (attempt - 1)))
                continue
            raise AmbiguousMutationError(
                request_id, attempts, str(exc), exit_code=exc.exit_code
            ) from exc
        if envelope.get("ok") is True or not is_ambiguous_envelope(envelope):
            return exec_result, envelope
        if attempt < attempts:
            sleep(backoff_base * (2.0 ** (attempt - 1)))
            continue
        message_obj = envelope.get("error")
        message = (
            str(message_obj.get("message"))
            if isinstance(message_obj, dict) and isinstance(message_obj.get("message"), str)
            else "mutation outcome is ambiguous"
        )
        raise AmbiguousMutationError(
            request_id,
            attempts,
            message,
            exit_code=exec_result.exit_code,
            envelope=envelope,
        )
    raise AssertionError("unreachable mutation retry loop")  # pragma: no cover


def ensure_mutation_allowed(identity: dict[str, Any], capability: str | None) -> None:
    """Refuse a mutation locally when the identity forbids it (spec section 5).

    ``identity`` is the ``result`` of an ``ok:true`` identity reply. Raises
    :class:`ClientError` without touching the instance further when the
    reported contract major is not ours (``client.contract_mismatch``) or when
    the required capability is not advertised
    (``client.unsupported_capability``).
    """
    contract = identity.get("contract")
    major = contract.get("major") if isinstance(contract, dict) else None
    if major != CONTRACT_MAJOR:
        raise ClientError(
            "client.contract_mismatch",
            f"instance speaks contract major {major!r}, this client speaks {CONTRACT_MAJOR}",
        )
    if capability is not None:
        capabilities = identity.get("capabilities")
        advertised = set(capabilities) if isinstance(capabilities, list) else set()
        if capability not in advertised:
            raise ClientError(
                "client.unsupported_capability",
                f"instance does not advertise {capability!r}; refusing the mutation",
            )


@dataclass(frozen=True)
class Instance:
    """One Fleet member from discovery: labels plus container state."""

    name: str
    container_id: str
    backend: str
    contract: str
    exec_user: str
    state: str


@dataclass(frozen=True)
class Fleet:
    """The discoverable Fleet: members plus the duplicated instance names."""

    instances: tuple[Instance, ...]
    duplicates: frozenset[str]


class ClientError(Exception):
    """An agentfleet client failure with a ``client.`` code and exit class."""

    def __init__(self, code: str, message: str) -> None:
        table = documents.client_table()
        entry = table.get(code, {})
        self.code = code
        self.message = message
        self.exit_code = int(entry.get("exit_code", 7))
        retryable = entry.get("retryable", False)
        self.retryable = retryable is True
        super().__init__(message)

    def document(self, details: dict[str, object] | None = None) -> dict[str, object]:
        """Render this failure as an envelope-shaped document."""
        error: dict[str, object] = {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }
        if details is not None:
            error["details"] = details
        return {"contract": CLIENT_CONTRACT, "ok": False, "error": error}


class UnknownInstanceError(ClientError):
    """No Fleet member carries the requested instance name."""

    def __init__(self, name: str) -> None:
        super().__init__("client.unknown_instance", f"no instance named {name!r} in the Fleet")
        self.name = name


class DuplicateInstanceError(ClientError):
    """More than one container carries the requested instance name."""

    def __init__(self, name: str, count: int) -> None:
        super().__init__(
            "client.duplicate_instance",
            f"instance name {name!r} is duplicated across {count} containers; refusing",
        )
        self.name = name


class DockerUnavailableError(ClientError):
    """The docker CLI could not be run or the daemon did not answer."""

    def __init__(self, detail: str) -> None:
        super().__init__("client.docker_unavailable", f"docker unavailable: {detail}")


class ExecFailedError(ClientError):
    """``docker exec`` produced no producer envelope."""

    def __init__(self, detail: str) -> None:
        super().__init__("client.exec_failed", f"docker exec failed: {detail}")


class MalformedStdoutError(ClientError):
    """The producer's stdout was not a coherent envelope."""

    def __init__(self, detail: str) -> None:
        super().__init__("client.malformed_stdout", f"malformed producer stdout: {detail}")


def docker_bin(configured: str | None = None) -> str:
    """Return the docker executable: explicit flag, env, then ``docker``."""
    if configured:
        return configured
    env = os.environ.get(DOCKER_ENV_VAR)
    if env:
        return env
    return "docker"


def _run_docker(docker: str, args: list[str], timeout: float) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            [docker, *args],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise DockerUnavailableError(f"cannot run {docker!r}: {exc.strerror or exc}") from exc
    except OSError as exc:
        raise DockerUnavailableError(f"cannot run {docker!r}: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ExecFailedError(f"timed out after {timeout}s") from exc


def _labels_of(entry: dict[str, object]) -> dict[str, str]:
    config = entry.get("Config")
    labels: object = None
    if isinstance(config, dict):
        labels = config.get("Labels")
    if not isinstance(labels, dict):
        raw = entry.get("Labels")
        labels = raw if isinstance(raw, dict) else {}
    result: dict[str, str] = {}
    for key, value in labels.items():
        if isinstance(key, str) and isinstance(value, str):
            result[key] = value
    return result


def _state_of(entry: dict[str, object], container_id: str) -> str:
    state = entry.get("State")
    if isinstance(state, dict):
        status = state.get("Status")
        if isinstance(status, str) and status:
            return status
        running = state.get("Running")
        if running is True:
            return "running"
        if running is False:
            return "exited"
    return "unknown:" + container_id[:12]


def discover(docker: str, timeout: float = DEFAULT_TIMEOUT) -> Fleet:
    """List the Fleet through ``docker ps`` and ``docker inspect``.

    Only containers carrying the contract label are members; Compose one-off
    containers are dropped even when labelled. Containers missing any of the
    four discovery labels, or carrying an invalid instance name or a
    non-numeric contract major, are skipped.
    """
    ps = _run_docker(
        docker,
        ["ps", "-a", "--filter", f"label={CONTRACT_LABEL}", "--format", "{{.ID}}"],
        timeout,
    )
    if ps.returncode != 0:
        raise DockerUnavailableError(
            f"docker ps exited {ps.returncode}: {ps.stderr.decode('utf-8', 'replace').strip()}"
        )
    ids = [
        line.strip() for line in ps.stdout.decode("utf-8", "replace").splitlines() if line.strip()
    ]
    if not ids:
        return Fleet(instances=(), duplicates=frozenset())
    inspect = _run_docker(docker, ["inspect", *ids], timeout)
    if inspect.returncode != 0:
        raise DockerUnavailableError(
            f"docker inspect exited {inspect.returncode}: "
            f"{inspect.stderr.decode('utf-8', 'replace').strip()}"
        )
    try:
        entries = json.loads(inspect.stdout.decode("utf-8", "replace"))
    except ValueError as exc:
        raise DockerUnavailableError(f"docker inspect output is not JSON: {exc}") from exc
    if not isinstance(entries, list):
        raise DockerUnavailableError("docker inspect output is not an array")
    instances: list[Instance] = []
    for entry, fallback_id in zip(entries, ids, strict=False):
        if not isinstance(entry, dict):
            continue
        container_id = entry.get("Id")
        if not isinstance(container_id, str) or not container_id:
            container_id = fallback_id
        labels = _labels_of(entry)
        oneoff = labels.get(ONEOFF_LABEL, "")
        if isinstance(oneoff, str) and oneoff.lower() == "true":
            continue
        contract = labels.get(CONTRACT_LABEL, "")
        name = labels.get(INSTANCE_LABEL, "")
        backend = labels.get(BACKEND_LABEL, "")
        exec_user = labels.get(EXEC_USER_LABEL, "")
        if not contract or not name or not backend or not exec_user:
            continue
        if documents.validate_instance_name(name) != []:
            continue
        if not contract.isascii() or not contract.isdigit():
            continue
        instances.append(
            Instance(
                name=name,
                container_id=container_id,
                backend=backend,
                contract=contract,
                exec_user=exec_user,
                state=_state_of(entry, container_id),
            )
        )
    counts: dict[str, int] = {}
    for instance in instances:
        counts[instance.name] = counts.get(instance.name, 0) + 1
    duplicates = frozenset(name for name, count in counts.items() if count > 1)
    ordered = tuple(sorted(instances, key=lambda item: (item.name, item.container_id)))
    return Fleet(instances=ordered, duplicates=duplicates)


def resolve(fleet: Fleet, name: str) -> Instance:
    """Address one instance by name; duplicates fail without touching a container."""
    matches = [item for item in fleet.instances if item.name == name]
    if not matches:
        raise UnknownInstanceError(name)
    if len(matches) > 1 or name in fleet.duplicates:
        raise DuplicateInstanceError(name, len(matches))
    return matches[0]


@dataclass(frozen=True)
class ExecResult:
    """The raw outcome of one ``docker exec`` online invocation."""

    exit_code: int
    stdout: bytes
    stderr: bytes


def invoke(
    docker: str,
    instance: Instance,
    agentctl_argv: list[str],
    timeout: float = DEFAULT_TIMEOUT,
) -> ExecResult:
    """Run one online operation exactly as spec section 3.1.

    No TTY is allocated (no ``-t``/``--tty``/``-i``), stdin is not used, and
    ``--contract <major>`` is the first ``agentctl`` argument.
    """
    argv = [
        "exec",
        "--user",
        instance.exec_user,
        instance.container_id,
        AGENTCTL_PATH,
        "--contract",
        str(CONTRACT_MAJOR),
        *agentctl_argv,
    ]
    completed = _run_docker(docker, argv, timeout)
    return ExecResult(
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def interpret(exec_result: ExecResult) -> dict[str, object]:
    """Decide on ``error.code``; use the exit code only for malformed stdout.

    Returns the parsed envelope when stdout is a coherent producer answer
    (valid envelope whose exit code matches its ``error.code`` class).
    Raises :class:`MalformedStdoutError` or :class:`ExecFailedError` when
    stdout cannot be trusted: transport failures (``docker exec`` exits
    125-127) become ``client.exec_failed``; any other incoherent stdout
    becomes ``client.malformed_stdout``.
    """
    try:
        doc = json.loads(exec_result.stdout.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        if 125 <= exec_result.exit_code <= 127:
            raise ExecFailedError(
                f"docker exec exited {exec_result.exit_code} without an envelope"
            ) from exc
        raise MalformedStdoutError(f"stdout is not JSON: {exc}") from exc
    if not isinstance(doc, dict) or documents.validate_envelope(doc) != []:
        if 125 <= exec_result.exit_code <= 127:
            raise ExecFailedError(f"docker exec exited {exec_result.exit_code} without an envelope")
        raise MalformedStdoutError("stdout is not a valid envelope")
    contract = doc.get("contract")
    major = documents.contract_major_of(contract)
    if major != str(CONTRACT_MAJOR):
        if 125 <= exec_result.exit_code <= 127:
            raise ExecFailedError(
                f"docker exec exited {exec_result.exit_code} without a coherent envelope"
            )
        raise MalformedStdoutError(f"envelope contract {contract!r} is not major {CONTRACT_MAJOR}")
    table = documents.error_table()
    if doc["ok"] is True:
        if exec_result.exit_code != 0:
            raise MalformedStdoutError(f"ok:true with exit code {exec_result.exit_code}")
        return doc
    code = doc["error"]["code"]
    entry = table.get(code)
    if not isinstance(entry, dict) or exec_result.exit_code != entry.get("exit_code"):
        raise MalformedStdoutError(
            f"error.code {code!r} contradicts exit code {exec_result.exit_code}"
        )
    return doc


class ExportProducerError(Exception):
    """``attempts export`` failed before streaming started: relay the producer.

    The producer answered with an ``ok:false`` envelope (for example
    ``not_found`` for an unknown attempt or path) before any tar bytes were
    sent. The consumer relays the envelope and its exit code instead of
    reporting a client failure.
    """

    def __init__(self, envelope: dict[str, object], exit_code: int) -> None:
        super().__init__(f"export failed: {envelope}")
        self.envelope = envelope
        self.exit_code = exit_code


class TornExportError(ClientError):
    """An export stayed torn after the bounded refetch (spec section 11.3).

    Streamed file bytes did not match the manifest ``sha256`` on every
    attempt, so the output was discarded. The archive is unsealed and moving;
    the export may be retried.
    """

    def __init__(self, attempt_id: str, attempts: int, detail: str) -> None:
        super().__init__(
            "client.export_torn",
            f"export of attempt {attempt_id!r} is torn after {attempts} attempts: {detail}",
        )
        self.attempt_id = attempt_id
        self.attempts = attempts


def _popen(docker: str, instance: Instance, agentctl_argv: list[str]) -> subprocess.Popen[bytes]:
    """Start ``agentctl`` with piped stdout exactly as spec section 3.1."""
    argv = [
        "exec",
        "--user",
        instance.exec_user,
        instance.container_id,
        AGENTCTL_PATH,
        "--contract",
        str(CONTRACT_MAJOR),
        *agentctl_argv,
    ]
    try:
        return subprocess.Popen(
            [docker, *argv],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise DockerUnavailableError(f"cannot run {docker!r}: {exc.strerror or exc}") from exc
    except OSError as exc:
        raise DockerUnavailableError(f"cannot run {docker!r}: {exc}") from exc


def _drain_with_progress(proc: subprocess.Popen[bytes], timeout: float) -> tuple[bytes, bytes]:
    """Read a streaming export to EOF; stall past ``timeout`` without bytes.

    Every chunk received restarts the progress deadline, so a slow producer
    keeps streaming while a stalled one trips ``client.exec_failed``. Stderr
    is drained alongside stdout so diagnostics can never block the stream.
    """
    stdout_chunks: list[bytes] = []
    stderr_chunks: list[bytes] = []
    stdout_done = threading.Event()
    stderr_done = threading.Event()

    def _read_stdout() -> None:
        try:
            stream = proc.stdout
            if stream is None:
                return
            while True:
                data = stream.read(EXPORT_CHUNK_SIZE)
                if not data:
                    return
                stdout_chunks.append(data)
        except (OSError, ValueError):
            return
        finally:
            stdout_done.set()

    def _read_stderr() -> None:
        try:
            stream = proc.stderr
            if stream is None:
                return
            while True:
                data = stream.read(EXPORT_CHUNK_SIZE)
                if not data:
                    return
                stderr_chunks.append(data)
        except (OSError, ValueError):
            return
        finally:
            stderr_done.set()

    stdout_reader = threading.Thread(target=_read_stdout, daemon=True)
    stderr_reader = threading.Thread(target=_read_stderr, daemon=True)
    stdout_reader.start()
    stderr_reader.start()
    seen = 0
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        stdout_done.wait(max(0.0, remaining))
        if len(stdout_chunks) != seen:
            seen = len(stdout_chunks)
            deadline = time.monotonic() + timeout
            continue
        if stdout_done.is_set():
            break
        proc.kill()
        stdout_reader.join(timeout=10.0)
        stderr_reader.join(timeout=10.0)
        with contextlib.suppress(OSError, ValueError, subprocess.TimeoutExpired):
            proc.wait(timeout=10.0)
        raise ExecFailedError(f"export stalled: no bytes for {timeout:g}s")
    stdout_reader.join(timeout=10.0)
    stderr_reader.join(timeout=10.0)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        proc.kill()
        raise ExecFailedError(f"export did not finish within {timeout:g}s") from exc
    return b"".join(stdout_chunks), b"".join(stderr_chunks)


def _parse_export_envelope(data: bytes) -> dict[str, object] | None:
    """Return the producer envelope when ``data`` is one, else ``None``."""
    try:
        doc = json.loads(data.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(doc, dict) or documents.validate_envelope(doc) != []:
        return None
    return doc


def _verify_export_stream(data: bytes, attempt_id: str) -> tuple[dict[str, object], str | None]:
    """Check a drained export tar; return its manifest and any torn detail.

    Structural failures (an unreadable tar, a missing or invalid
    ``manifest.json``) raise :class:`MalformedStdoutError`: the producer is
    at fault deterministically. A file whose size or ``sha256`` does not
    match its manifest entry is a torn read of an unsealed archive, returned
    as a detail string so the caller can refetch within its bound.
    """
    members: dict[str, bytes] = {}
    order: list[str] = []
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            for member in archive:
                extracted = archive.extractfile(member)
                order.append(member.name)
                members[member.name] = extracted.read() if extracted is not None else b""
    except (tarfile.TarError, OSError, EOFError) as exc:
        raise MalformedStdoutError(f"export is not a readable tar stream: {exc}") from exc
    if not order or order[0] != "manifest.json":
        raise MalformedStdoutError(f"export must start with manifest.json, got {order[:1]!r}")
    try:
        manifest = json.loads(members["manifest.json"])
    except ValueError as exc:
        raise MalformedStdoutError("export manifest.json is not valid JSON") from exc
    if not isinstance(manifest, dict):
        raise MalformedStdoutError("export manifest.json must be an object")
    problems = documents.validate_export_manifest(manifest)
    if problems:
        raise MalformedStdoutError(f"export manifest.json is invalid: {problems[0]}")
    attempt = manifest.get("attempt")
    if not isinstance(attempt, dict) or attempt.get("attempt_id") != attempt_id:
        raise MalformedStdoutError(f"export manifest is not for attempt {attempt_id!r}")
    entries = manifest.get("files")
    assert isinstance(entries, list)
    for entry in entries:
        assert isinstance(entry, dict)
        content = members.get(str(entry["path"]))
        if content is None:
            raise MalformedStdoutError(
                f"export manifest lists {entry['path']!r} but the tar lacks it"
            )
        if len(content) != entry["size"] or hashlib.sha256(content).hexdigest() != entry["sha256"]:
            return manifest, (f"export file {entry['path']!r} does not match its manifest sha256")
    return manifest, None


def export_attempt(
    docker: str,
    instance: Instance,
    attempt_id: str,
    paths: Sequence[str] | None = None,
    sink: Callable[[bytes], object] | None = None,
    timeout: float | None = None,
    max_attempts: int | None = None,
) -> dict[str, object]:
    """Stream one ``attempts export`` tar to ``sink`` with torn-read refetch.

    The tar is drained with a progress timeout (``timeout``, default 30 s
    without bytes), the streamed ``manifest.json`` is read first and every
    file's size and ``sha256`` is verified as it streams. A torn read of an
    unsealed archive is fetched again up to ``max_attempts`` times (default
    3); when every attempt is torn, raises :class:`TornExportError` and the
    caller discards the output. The verified tar bytes reach ``sink`` exactly
    once, only after verification. Nothing is cached, so an unsealed moving
    snapshot is never mistaken for a sealed archive.

    An envelope answered before streaming starts (for example ``not_found``)
    raises :class:`ExportProducerError` so the caller relays the producer
    instead of reporting a client failure.
    """
    wanted = list(paths) if paths else []
    argv = ["attempts", "export", attempt_id]
    for path in wanted:
        argv += ["--path", path]
    effective_timeout = timeout_for("attempts-export", timeout)
    attempts = max_attempts if max_attempts is not None else EXPORT_MAX_ATTEMPTS
    if attempts < 1:
        attempts = 1
    torn_detail: str | None = None
    for _attempt in range(1, attempts + 1):
        proc = _popen(docker, instance, argv)
        data, _stderr = _drain_with_progress(proc, effective_timeout)
        exit_code = proc.returncode if proc.returncode is not None else 1
        if exit_code != 0:
            envelope = _parse_export_envelope(data)
            if envelope is not None and envelope.get("ok") is False:
                raise ExportProducerError(envelope, exit_code)
            raise MalformedStdoutError(
                f"export exited {exit_code} after streaming started; discarding the output"
            )
        manifest, torn_detail = _verify_export_stream(data, attempt_id)
        if torn_detail is None:
            if sink is not None:
                sink(data)
            return manifest
    raise TornExportError(attempt_id, attempts, torn_detail or "sha256 mismatch")
