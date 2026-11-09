"""Producer suite: black-box scenarios against a producer container.

Runnable as a module from the installed package::

    python -m agent_operator_contract.producer_suite --harness ./harness

The suite talks to the producer only through ``agentctl`` over real
``docker exec`` and ``docker run`` (spec section 3), and to the engine only
through the Conformance Harness (spec section 15.1). Every producer reply is
validated in producer mode against the schemas. Scenarios have stable
identifiers; output is one line per scenario (passed, failed or not run with a
reason) plus a summary, and the exit code is non-zero when any scenario
fails. An engine can pass ``--expect-fail`` scenario IDs for known failures:
a listed scenario that fails does not fail the run, while a listed scenario
that passes (or is missing) is reported and fails the run. ``--require-all``
also fails the run when a scenario does not run (an unadvertised capability
or an unsupported harness verb).

Standard library only.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import secrets
import subprocess
import sys
import tarfile
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from agent_operator_contract import CONTRACT_MAJOR, documents

HarnessStatus = Literal["ok", "unsupported", "error"]
"""How one harness call classified: answered, not supported, or broken."""

ScenarioStatus = Literal["passed", "failed", "not_run"]
"""What one scenario did."""

AGENTCTL = "/usr/local/bin/agentctl"

DEFAULT_REPOSITORY = "example-owner/example-repo"
DEFAULT_CAPABILITIES = ("next-issue", "stop-after")

READ_BUDGET_S = 3.0
"""Spec section 3.3 budget for ``identity``, ``status`` and ``command``."""

MUTATION_BUDGET_S = 10.0
"""Spec section 3.3 budget to commit or reject a mutation."""

ATTEMPTS_BUDGET_S = 5.0
"""Spec section 3.3 budget for ``attempts list`` and ``attempts show``."""

EXPORT_FIRST_BYTE_BUDGET_S = 5.0
"""Spec section 3.3 budget for ``attempts export`` to start streaming."""

SNAPSHOT_MAX_BYTES = 256 * 1024
"""Spec section 9.1 bound for a Status Snapshot in compact JSON bytes."""

LOCK_HOLD_MS = 5000
"""How long ``status-during-lock`` holds the engine lock."""

LOCK_SETTLE_S = 1.0
"""How long ``status-during-lock`` waits for the hold to take effect."""

RESTART_RELEASES = 3
"""Harness ``release`` calls a restart may need: the startup hold, then recovery."""


@dataclass(frozen=True)
class HarnessResult:
    """One harness call, classified for the suite."""

    status: HarnessStatus
    """``ok``, ``unsupported`` or ``error`` (failed, usage or a broken reply)."""

    result: dict[str, Any] | None
    detail: str


@dataclass(frozen=True)
class DockerResult:
    """One ``docker exec`` of ``agentctl``: exit code, stdout and stderr."""

    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class SuiteContext:
    """The instance under test, as reported by harness ``setup``."""

    container: str
    exec_user: str
    repository: str
    capabilities: list[str]
    docker: str = "docker"
    timeout: float = 30.0
    image: str = ""
    volumes: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class ScenarioOutcome:
    """What one scenario did: passed, failed or not run with a reason."""

    scenario_id: str
    status: ScenarioStatus
    detail: str = ""


@dataclass(frozen=True)
class Scenario:
    """One named scenario with a stable identifier."""

    id: str
    title: str


def _call_harness(
    harness_cmd: Sequence[str], verb: str, params: dict[str, Any], timeout: float
) -> HarnessResult:
    """Invoke ``<harness> <verb> '<request-json>'`` and classify the reply."""
    request = json.dumps({"verb": verb, "params": params})
    try:
        completed = subprocess.run(
            [*harness_cmd, verb, request],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return HarnessResult("error", None, f"harness did not run: {exc}")
    try:
        doc = json.loads(completed.stdout)
    except ValueError:
        return HarnessResult("error", None, f"harness stdout is not JSON: {completed.stdout!r}")
    errors = documents.validate_harness_reply(doc)
    if errors:
        return HarnessResult("error", None, f"harness reply is invalid: {'; '.join(errors)}")
    if not isinstance(doc, dict):
        return HarnessResult("error", None, "harness reply is not a document")
    if doc.get("ok") is True:
        if completed.returncode != 0:
            return HarnessResult("error", None, f"harness ok:true with exit {completed.returncode}")
        result = doc.get("result")
        if not isinstance(result, dict):
            return HarnessResult("error", None, "harness ok:true without a result object")
        return HarnessResult("ok", result, "")
    code = doc["error"]["code"]
    expected_exit = documents.HARNESS_EXIT_CODES.get(code)
    if completed.returncode != expected_exit:
        return HarnessResult(
            "error",
            None,
            f"harness error {code!r} with exit {completed.returncode}, expected {expected_exit}",
        )
    if code == "unsupported":
        details = doc["error"].get("details") or {}
        return HarnessResult(
            "unsupported",
            None,
            f"{details.get('what', '?')}:{details.get('name', '?')}",
        )
    message = doc["error"].get("message", "")
    return HarnessResult("error", None, f"harness answered {code}: {message}")


def _docker_exec(
    docker: str, container: str, exec_user: str, agentctl_argv: list[str], timeout: float
) -> DockerResult:
    """Run ``agentctl`` inside the container exactly as spec section 3.1 says."""
    completed = subprocess.run(
        [docker, "exec", "--user", exec_user, container, AGENTCTL, *agentctl_argv],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return DockerResult(completed.returncode, completed.stdout, completed.stderr)


def _popen_export(
    docker: str, container: str, exec_user: str, agentctl_argv: list[str]
) -> subprocess.Popen[bytes]:
    """Start ``agentctl attempts export`` streaming; the caller times first bytes.

    Unbuffered (``bufsize=0``): ``_read_first_byte`` then takes exactly one
    byte from the pipe, and ``communicate`` reads the rest from the same file
    descriptor without losing bytes to a read-ahead buffer.
    """
    return subprocess.Popen(
        [docker, "exec", "--user", exec_user, container, AGENTCTL, *agentctl_argv],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )


def _docker_run_argv(ctx: SuiteContext, agentctl_argv: list[str]) -> list[str]:
    """Build the ``docker run`` argv for one Offline Read (spec section 3.2).

    The stopped container's image runs removed (``--rm``) with no network,
    every volume re-mounted read-only at the same destination, and
    ``agentctl`` as the entrypoint. The caller supplies the full agentctl
    argv including ``--contract 1 --offline``.
    """
    argv: list[str] = [
        ctx.docker,
        "run",
        "--rm",
        "--network",
        "none",
        "--user",
        ctx.exec_user,
    ]
    for volume in ctx.volumes:
        name = volume.get("name")
        path = volume.get("path")
        if isinstance(name, str) and name and isinstance(path, str) and path:
            argv += [
                "--mount",
                f"type=volume,src={name},dst={path},readonly",
            ]
    argv += ["--entrypoint", AGENTCTL, ctx.image, *agentctl_argv]
    return argv


def _docker_run(
    ctx: SuiteContext, agentctl_argv: list[str], timeout: float | None = None
) -> DockerResult:
    """Run one Offline Read through ``docker run`` (spec section 3.2)."""
    argv = _docker_run_argv(ctx, agentctl_argv)
    completed = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=ctx.timeout if timeout is None else timeout,
    )
    return DockerResult(completed.returncode, completed.stdout, completed.stderr)


def _popen_run(ctx: SuiteContext, agentctl_argv: list[str]) -> subprocess.Popen[bytes]:
    """Start one offline ``attempts export`` streaming through ``docker run``.

    Unbuffered for the same reason as ``_popen_export``.
    """
    return subprocess.Popen(
        _docker_run_argv(ctx, agentctl_argv),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )


def _read_first_byte(proc: subprocess.Popen[bytes], budget_s: float) -> tuple[bytes, float]:
    """Read one byte of a streaming export; empty when nothing arrives in budget."""
    chunk: list[bytes] = []

    def _read() -> None:
        try:
            data = proc.stdout.read(1) if proc.stdout is not None else b""
        except (OSError, ValueError):
            data = b""
        chunk.append(data)

    started = time.monotonic()
    reader = threading.Thread(target=_read, daemon=True)
    reader.start()
    reader.join(budget_s)
    return (chunk[0] if chunk else b""), time.monotonic() - started


def _stop_export(proc: subprocess.Popen[bytes]) -> None:
    """Terminate a streaming export after its first bytes were measured."""
    with contextlib.suppress(OSError, ValueError):
        proc.kill()
    with contextlib.suppress(OSError, ValueError, subprocess.TimeoutExpired):
        proc.wait(timeout=10.0)


def _check_export_stream(data: bytes, attempt_id: str) -> list[str]:
    """Check a drained export tar: manifest first, entries matching their hashes."""
    members: dict[str, bytes] = {}
    order: list[str] = []
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            for member in archive:
                extracted = archive.extractfile(member)
                order.append(member.name)
                members[member.name] = extracted.read() if extracted is not None else b""
    except (tarfile.TarError, OSError) as exc:
        return [f"export is not a readable tar stream: {exc}"]
    if not order or order[0] != "manifest.json":
        return [f"export must start with manifest.json, got {order[:1]!r}"]
    try:
        manifest = json.loads(members["manifest.json"])
    except ValueError:
        return ["export manifest.json is not valid JSON"]
    if not isinstance(manifest, dict):
        return ["export manifest.json must be an object"]
    manifest_problems = documents.validate_export_manifest(manifest)
    if manifest_problems:
        return [f"export manifest.json is invalid: {manifest_problems[0]}"]
    attempt = manifest.get("attempt")
    if not isinstance(attempt, dict) or attempt.get("attempt_id") != attempt_id:
        return [f"export manifest is not for attempt {attempt_id!r}"]
    entries = manifest.get("files")
    if not isinstance(entries, list):
        return ["export manifest files must be an array"]
    for entry in entries:
        if not isinstance(entry, dict):
            return ["export manifest entries must be objects"]
        for key in ("path", "size", "sha256"):
            if entry.get(key) is None:
                return [f"export manifest entry is missing {key!r}"]
        content = members.get(str(entry["path"]))
        if content is None:
            return [f"export manifest lists {entry['path']!r} but the tar lacks it"]
        if len(content) != entry["size"]:
            return [f"export file {entry['path']!r} is {len(content)} bytes, not {entry['size']}"]
        if hashlib.sha256(content).hexdigest() != entry["sha256"]:
            return [f"export file {entry['path']!r} does not match its manifest sha256"]
    return []


def _check_ok_result(stdout: str, exit_code: int) -> list[str]:
    """Check a read reply in producer mode: envelope valid, ok:true, object result."""
    problems = _producer_problems(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    if doc.get("ok") is not True:
        return [f"read answered ok:false: {stdout.strip()}"]
    if not isinstance(doc.get("result"), dict):
        return ["read result must be an object"]
    return []


def _exec(
    scenario_id: str, agentctl_argv: list[str], ctx: SuiteContext
) -> DockerResult | ScenarioOutcome:
    """Run ``agentctl`` in the container; a transport failure is already an outcome."""
    try:
        return _docker_exec(ctx.docker, ctx.container, ctx.exec_user, agentctl_argv, ctx.timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return ScenarioOutcome(scenario_id, "failed", f"docker exec failed: {exc}")


def _producer_problems(stdout: str, exit_code: int) -> list[str]:
    """Check a producer reply in producer mode: shape, envelope and exit code."""
    if not stdout.endswith("\n") or stdout.count("\n") != 1:
        return ["stdout must be exactly one JSON document followed by a newline"]
    try:
        doc = json.loads(stdout)
    except ValueError:
        return ["stdout is not valid JSON"]
    errors = documents.validate_envelope(doc)
    if errors:
        return errors
    if not isinstance(doc, dict):
        return ["envelope must be an object"]
    if doc.get("ok") is True:
        if exit_code != 0:
            return [f"ok:true with exit {exit_code}, expected 0"]
        return []
    code = doc["error"]["code"]
    expected = documents.error_table().get(code, {}).get("exit_code")
    if exit_code != expected:
        return [f"error {code!r} with exit {exit_code}, expected {expected}"]
    return []


def _check_identity(stdout: str, exit_code: int, ctx: SuiteContext) -> list[str]:
    problems = _producer_problems(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    if doc.get("ok") is not True:
        return [f"identity answered ok:false: {stdout.strip()}"]
    identity = doc.get("result")
    if not isinstance(identity, dict):
        return ["identity result must be an object"]
    problems = documents.validate_identity(identity)
    if problems:
        return problems
    mismatches = []
    if identity.get("repository") != ctx.repository:
        mismatches.append(f"repository {identity.get('repository')!r} != {ctx.repository!r}")
    if identity.get("mode") != "online":
        mismatches.append(f"mode {identity.get('mode')!r} != 'online'")
    contract = identity.get("contract")
    if not isinstance(contract, dict) or contract.get("major") != CONTRACT_MAJOR:
        mismatches.append(f"contract {contract!r} is not major {CONTRACT_MAJOR}")
    capabilities = identity.get("capabilities")
    if not isinstance(capabilities, list) or capabilities != sorted(capabilities):
        mismatches.append(f"capabilities {capabilities!r} are not sorted")
    elif capabilities != sorted(ctx.capabilities):
        mismatches.append(f"capabilities {capabilities!r} != {sorted(ctx.capabilities)!r}")
    return mismatches


def _check_status(stdout: str, exit_code: int, ctx: SuiteContext) -> list[str]:
    """Check an online ``status`` reply: envelope, snapshot and harness setup."""
    problems = _producer_problems(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    if doc.get("ok") is not True:
        return [f"status answered ok:false: {stdout.strip()}"]
    snapshot = doc.get("result")
    if not isinstance(snapshot, dict):
        return ["status result must be an object"]
    problems = documents.validate_status(snapshot)
    if problems:
        return problems
    mismatches = []
    identity = snapshot.get("identity")
    if not isinstance(identity, dict):
        return ["status identity must be an object"]
    if identity.get("repository") != ctx.repository:
        mismatches.append(f"repository {identity.get('repository')!r} != {ctx.repository!r}")
    if identity.get("mode") != "online":
        mismatches.append(f"mode {identity.get('mode')!r} != 'online'")
    contract = identity.get("contract")
    if not isinstance(contract, dict) or contract.get("major") != CONTRACT_MAJOR:
        mismatches.append(f"contract {contract!r} is not major {CONTRACT_MAJOR}")
    capabilities = identity.get("capabilities")
    if not isinstance(capabilities, list) or capabilities != sorted(capabilities):
        mismatches.append(f"capabilities {capabilities!r} are not sorted")
    elif capabilities != sorted(ctx.capabilities):
        mismatches.append(f"capabilities {capabilities!r} != {sorted(ctx.capabilities)!r}")
    return mismatches


def _check_offline_identity(stdout: str, exit_code: int, ctx: SuiteContext) -> list[str]:
    """Check an offline ``identity`` reply: envelope, offline mode, harness setup."""
    problems = _producer_problems(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    if doc.get("ok") is not True:
        return [f"identity answered ok:false: {stdout.strip()}"]
    identity = doc.get("result")
    if not isinstance(identity, dict):
        return ["identity result must be an object"]
    problems = documents.validate_identity(identity)
    if problems:
        return problems
    mismatches = []
    if identity.get("repository") != ctx.repository:
        mismatches.append(f"repository {identity.get('repository')!r} != {ctx.repository!r}")
    if identity.get("mode") != "offline":
        mismatches.append(f"mode {identity.get('mode')!r} != 'offline'")
    if identity.get("process_started_at") is not None:
        mismatches.append("process_started_at must be null offline")
    contract = identity.get("contract")
    if not isinstance(contract, dict) or contract.get("major") != CONTRACT_MAJOR:
        mismatches.append(f"contract {contract!r} is not major {CONTRACT_MAJOR}")
    capabilities = identity.get("capabilities")
    if not isinstance(capabilities, list) or capabilities != sorted(capabilities):
        mismatches.append(f"capabilities {capabilities!r} are not sorted")
    elif capabilities != sorted(ctx.capabilities):
        mismatches.append(f"capabilities {capabilities!r} != {sorted(ctx.capabilities)!r}")
    return mismatches


def _check_offline_status(stdout: str, exit_code: int, ctx: SuiteContext) -> list[str]:
    """Check an offline ``status`` reply: stopped instance, offline identity."""
    problems = _producer_problems(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    if doc.get("ok") is not True:
        return [f"status answered ok:false: {stdout.strip()}"]
    snapshot = doc.get("result")
    if not isinstance(snapshot, dict):
        return ["status result must be an object"]
    problems = documents.validate_status(snapshot)
    if problems:
        return problems
    mismatches = []
    identity = snapshot.get("identity")
    if not isinstance(identity, dict):
        return ["status identity must be an object"]
    if identity.get("repository") != ctx.repository:
        mismatches.append(f"repository {identity.get('repository')!r} != {ctx.repository!r}")
    if identity.get("mode") != "offline":
        mismatches.append(f"mode {identity.get('mode')!r} != 'offline'")
    if identity.get("process_started_at") is not None:
        mismatches.append("process_started_at must be null offline")
    contract = identity.get("contract")
    if not isinstance(contract, dict) or contract.get("major") != CONTRACT_MAJOR:
        mismatches.append(f"contract {contract!r} is not major {CONTRACT_MAJOR}")
    capabilities = identity.get("capabilities")
    if not isinstance(capabilities, list) or capabilities != sorted(capabilities):
        mismatches.append(f"capabilities {capabilities!r} are not sorted")
    elif capabilities != sorted(ctx.capabilities):
        mismatches.append(f"capabilities {capabilities!r} != {sorted(ctx.capabilities)!r}")
    instance = snapshot.get("instance")
    if not isinstance(instance, dict):
        return [*mismatches, "status instance must be an object"]
    if instance.get("state") != "stopped":
        mismatches.append(f"instance state {instance.get('state')!r} != 'stopped'")
    if snapshot.get("current_attempt") is not None:
        mismatches.append("offline status must carry no current_attempt")
    return mismatches


def _check_error(stdout: str, exit_code: int, code: str) -> list[str]:
    problems = _producer_problems(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    if doc.get("ok") is not False or not isinstance(doc.get("error"), dict):
        return [f"expected error {code!r}, got ok:true: {stdout.strip()}"]
    actual = doc["error"]["code"]
    if actual != code:
        return [f"expected error {code!r}, got {actual!r}: {stdout.strip()}"]
    if documents.contract_major_of(doc.get("contract")) != str(CONTRACT_MAJOR):
        return [f"envelope contract {doc.get('contract')!r} is not major {CONTRACT_MAJOR}"]
    expected_retryable = documents.error_table()[code]["retryable"]
    if doc["error"].get("retryable") is not expected_retryable:
        return [
            f"error {code!r} retryable {doc['error'].get('retryable')!r}, "
            f"expected {expected_retryable!r}"
        ]
    details = doc["error"].get("details")
    if details is not None and not isinstance(details, dict):
        return [f"error {code!r} details must be an object"]
    return []


def _new_request_id() -> str:
    return f"req-{secrets.token_hex(8)}"


def _check_mutation(stdout: str, exit_code: int, *, replayed: bool) -> list[str]:
    """Check a committed mutation reply: envelope, result shape and replay flag."""
    problems = _producer_problems(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    if doc.get("ok") is not True:
        return [f"mutation answered ok:false: {stdout.strip()}"]
    result = doc.get("result")
    if not isinstance(result, dict):
        return ["mutation result must be an object"]
    problems = documents.validate_mutation_result(result)
    if problems:
        return problems
    if result.get("replayed") is not replayed:
        return [f"replayed is {result.get('replayed')!r}, expected {replayed!r}"]
    return []


def _check_command(
    stdout: str,
    exit_code: int,
    *,
    request_id: str | None = None,
    kind: str | None = None,
) -> list[str]:
    """Check a ``command`` read reply: envelope, record shape and identity."""
    problems = _producer_problems(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    if doc.get("ok") is not True:
        return [f"command answered ok:false: {stdout.strip()}"]
    result = doc.get("result")
    if not isinstance(result, dict):
        return ["command result must be an object"]
    problems = documents.validate_command_result(result)
    if problems:
        return problems
    record = result.get("command")
    assert isinstance(record, dict)
    if request_id is not None and record.get("request_id") != request_id:
        return [f"command request_id is {record.get('request_id')!r}, expected {request_id!r}"]
    if kind is not None and record.get("kind") != kind:
        return [f"command kind is {record.get('kind')!r}, expected {kind!r}"]
    return []


def _check_error_request(stdout: str, exit_code: int, code: str, request_id: str) -> list[str]:
    """Check an ``ok:false`` mutation reply and its echoed request ID."""
    problems = _check_error(stdout, exit_code, code)
    if problems:
        return problems
    doc = json.loads(stdout)
    if doc["error"].get("request_id") != request_id:
        return [f"error request_id is {doc['error'].get('request_id')!r}, expected {request_id!r}"]
    return []


def _intake_state(ctx: SuiteContext, scenario_id: str) -> tuple[str | None, ScenarioOutcome | None]:
    """Read the current Intake state, or a failed outcome when unreadable."""
    answered = _exec(scenario_id, ["--contract", "1", "status"], ctx)
    if isinstance(answered, ScenarioOutcome):
        return None, answered
    problems = _check_status(answered.stdout, answered.exit_code, ctx)
    if problems:
        return None, ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    doc = json.loads(answered.stdout)
    return str(doc["result"]["intake"]["state"]), None


def _require_capability(
    ctx: SuiteContext, scenario_id: str, capability: str
) -> ScenarioOutcome | None:
    """Skip a scenario whose Operator Capability the producer does not advertise.

    The ``unsupported-capability`` scenario covers the unadvertised side; the
    capability's own scenario runs only where the capability is advertised.
    """
    if capability in ctx.capabilities:
        return None
    return ScenarioOutcome(scenario_id, "not_run", f"capability {capability!r} is not advertised")


def _ensure_intake(ctx: SuiteContext, scenario_id: str, want: str) -> ScenarioOutcome | None:
    """Steer Intake to ``want`` (``open`` or ``closed``) for one scenario.

    Each mutation scenario sets its own precondition through the contract
    itself, so scenarios stay independent of the order they run in.
    """
    state, outcome = _intake_state(ctx, scenario_id)
    if outcome is not None:
        return outcome
    if state == want:
        return None
    operation = "resume" if want == "open" else "stop"
    answered = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            operation,
            "--request-id",
            _new_request_id(),
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_mutation(answered.stdout, answered.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"could not steer intake {state} -> {want}: {'; '.join(problems)}",
        )
    return None


def scenario_identity(ctx: SuiteContext) -> ScenarioOutcome:
    """Online ``identity`` matches the harness setup."""
    answered = _exec("identity", ["--contract", "1", "identity"], ctx)
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_identity(answered.stdout, answered.exit_code, ctx)
    if problems:
        return ScenarioOutcome("identity", "failed", "; ".join(problems))
    return ScenarioOutcome("identity", "passed")


def scenario_status(ctx: SuiteContext) -> ScenarioOutcome:
    """Online ``status`` validates and matches the harness setup."""
    answered = _exec("status", ["--contract", "1", "status"], ctx)
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_status(answered.stdout, answered.exit_code, ctx)
    if problems:
        return ScenarioOutcome("status", "failed", "; ".join(problems))
    return ScenarioOutcome("status", "passed")


def scenario_argv_usage(ctx: SuiteContext) -> ScenarioOutcome:
    """Malformed argv answers ``usage`` with exit 2."""
    answered = _exec("argv-usage", ["--contract", "1", "stop", "--after"], ctx)
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_error(answered.stdout, answered.exit_code, "usage")
    if problems:
        return ScenarioOutcome("argv-usage", "failed", "; ".join(problems))
    return ScenarioOutcome("argv-usage", "passed")


def scenario_contract_mismatch(ctx: SuiteContext) -> ScenarioOutcome:
    """``--contract 2`` answers ``contract_mismatch`` and executes nothing."""
    answered = _exec("contract-mismatch", ["--contract", "2", "status"], ctx)
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_error(answered.stdout, answered.exit_code, "contract_mismatch")
    if problems:
        return ScenarioOutcome("contract-mismatch", "failed", "; ".join(problems))
    try:
        doc = json.loads(answered.stdout)
    except ValueError:
        return ScenarioOutcome("contract-mismatch", "failed", "stdout is not valid JSON")
    if doc["error"].get("details", {}).get("supported") != [CONTRACT_MAJOR]:
        return ScenarioOutcome(
            "contract-mismatch",
            "failed",
            f"details.supported is not [{CONTRACT_MAJOR}]: {answered.stdout.strip()}",
        )
    identity = _exec("contract-mismatch", ["--contract", "1", "identity"], ctx)
    if isinstance(identity, ScenarioOutcome):
        return ScenarioOutcome(
            "contract-mismatch", "failed", f"identity after the mismatch: {identity.detail}"
        )
    follow_up = _check_identity(identity.stdout, identity.exit_code, ctx)
    if follow_up:
        return ScenarioOutcome(
            "contract-mismatch", "failed", f"identity after the mismatch: {'; '.join(follow_up)}"
        )
    return ScenarioOutcome("contract-mismatch", "passed")


_CAPABILITY_PROBES: dict[str, list[str]] = {
    "stop-after": ["stop", "--after", "2"],
    "next-issue": ["next", "issue", "7"],
    "operator-handoff": ["handoff", "now"],
    "recovery-retry": ["recovery", "retry", "attempt-1"],
    "recovery-release": ["recovery", "release", "attempt-1", "--saved-at", "somewhere"],
    "errors-reset": ["errors", "reset"],
}
"""One probe argv per optional capability with an operation.

``pool`` is a declaration without an operation, so there is nothing to
invoke for it. Every probe carries a valid value: the failure must be the
missing capability (``unsupported``), never the value (``invalid_argument``).
"""


def scenario_unsupported_capability(ctx: SuiteContext) -> ScenarioOutcome:
    """Every unadvertised capability answers ``unsupported`` with exit 3.

    Each probe commits nothing (``command`` is ``not_found``) and the
    request ID stays reusable for a supported operation.
    """
    scenario_id = "unsupported-capability"
    missing = [
        capability for capability in _CAPABILITY_PROBES if capability not in ctx.capabilities
    ]
    if not missing:
        return ScenarioOutcome(scenario_id, "passed")
    for capability in missing:
        request_id = _new_request_id()
        answered = _exec(
            scenario_id,
            [
                "--contract",
                "1",
                *_CAPABILITY_PROBES[capability],
                "--request-id",
                request_id,
                "--expect-repo",
                ctx.repository,
            ],
            ctx,
        )
        if isinstance(answered, ScenarioOutcome):
            return answered
        problems = _check_error_request(
            answered.stdout, answered.exit_code, "unsupported", request_id
        )
        if problems:
            return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
        try:
            doc = json.loads(answered.stdout)
        except ValueError:
            return ScenarioOutcome(scenario_id, "failed", "unsupported is not JSON")
        if doc["error"].get("details", {}).get("capability") != capability:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"details.capability is not {capability!r}: {answered.stdout.strip()}",
            )
        stored = _exec(scenario_id, ["--contract", "1", "command", request_id], ctx)
        if isinstance(stored, ScenarioOutcome):
            return stored
        nothing = _check_error(stored.stdout, stored.exit_code, "not_found")
        if nothing:
            return ScenarioOutcome(
                scenario_id, "failed", f"unsupported {capability} stored {request_id}"
            )
        steer = _ensure_intake(ctx, scenario_id, "open")
        if steer is not None:
            return steer
        reuse = _exec(
            scenario_id,
            [
                "--contract",
                "1",
                "stop",
                "--request-id",
                request_id,
                "--expect-repo",
                ctx.repository,
            ],
            ctx,
        )
        if isinstance(reuse, ScenarioOutcome):
            return reuse
        problems = _check_mutation(reuse.stdout, reuse.exit_code, replayed=False)
        if problems:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"request ID of unsupported {capability} is not reusable: " + "; ".join(problems),
            )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_hold_lock(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """The ``hold-lock`` harness verb holds the engine lock for the duration."""
    duration_ms = 100
    result = _call_harness(harness_cmd, "hold-lock", {"duration_ms": duration_ms}, ctx.timeout)
    if result.status == "unsupported":
        return ScenarioOutcome(
            "hold-lock", "not_run", f"harness verb is not supported ({result.detail})"
        )
    if result.status != "ok" or result.result is None:
        return ScenarioOutcome("hold-lock", "failed", result.detail)
    held_ms = result.result.get("held_ms")
    if not isinstance(held_ms, int) or held_ms < duration_ms:
        return ScenarioOutcome("hold-lock", "failed", f"invalid held_ms: {held_ms!r}")
    return ScenarioOutcome("hold-lock", "passed")


def _measure(
    ctx: SuiteContext, scenario_id: str, agentctl_argv: list[str]
) -> tuple[float, DockerResult | ScenarioOutcome]:
    """Run ``agentctl`` in the container and measure its wall-clock seconds."""
    started = time.monotonic()
    answered = _exec(scenario_id, agentctl_argv, ctx)
    return time.monotonic() - started, answered


def scenario_status_during_lock(
    ctx: SuiteContext,
    harness_cmd: Sequence[str],
    *,
    duration_ms: int = LOCK_HOLD_MS,
    budget_s: float = READ_BUDGET_S,
    settle_s: float = LOCK_SETTLE_S,
) -> ScenarioOutcome:
    """``status`` answers within 3 s with prepared state while the lock is held.

    The harness holds the engine lock for ``duration_ms``; a concurrent
    ``status`` must answer within ``budget_s`` with the last prepared state
    (the baseline sections) and section ``observed_at`` values older than a
    read taken after the hold ends.
    """
    scenario_id = "status-during-lock"
    before, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or before is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no baseline status")
    held: list[HarnessResult] = []

    def _hold() -> None:
        held.append(
            _call_harness(harness_cmd, "hold-lock", {"duration_ms": duration_ms}, ctx.timeout)
        )

    holder = threading.Thread(target=_hold, daemon=True)
    holder.start()
    time.sleep(min(settle_s, duration_ms / 1000.0 / 4))
    elapsed, answered = _measure(ctx, scenario_id, ["--contract", "1", "status"])
    still_held = holder.is_alive()
    holder.join(timeout=max(ctx.timeout, duration_ms / 1000.0 + 5.0))
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_status(answered.stdout, answered.exit_code, ctx)
    if not held:
        return ScenarioOutcome(scenario_id, "failed", "hold-lock never answered")
    if held[0].status == "unsupported":
        return ScenarioOutcome(
            scenario_id, "not_run", f"harness verb is not supported ({held[0].detail})"
        )
    if held[0].status != "ok":
        return ScenarioOutcome(scenario_id, "failed", f"harness hold-lock failed: {held[0].detail}")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    if elapsed > budget_s:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"status took {elapsed:.2f}s while the engine lock was held (budget {budget_s:.2f}s)",
        )
    if not still_held:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            "hold-lock finished before status answered; the lock was not held during the read",
        )
    during = json.loads(answered.stdout)["result"]
    after, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or after is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after the hold")
    for section in ("instance", "intake"):
        if during[section]["state"] != before[section]["state"]:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"status during the hold is not the last prepared state: "
                f"{section} is {during[section]['state']!r}, "
                f"was {before[section]['state']!r}",
            )
        if not during[section]["observed_at"] <= before[section]["observed_at"]:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"status during the hold re-read {section} instead of serving "
                f"prepared state: {during[section]['observed_at']!r} is newer than "
                f"the baseline {before[section]['observed_at']!r}",
            )
        if not during[section]["observed_at"] < after[section]["observed_at"]:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"status during the hold has no older {section} observed_at: "
                f"{during[section]['observed_at']!r} vs {after[section]['observed_at']!r}",
            )
    current_before = before.get("current_attempt")
    current_during = during.get("current_attempt")
    current_after = after.get("current_attempt")
    if (
        isinstance(current_before, dict)
        and isinstance(current_during, dict)
        and current_during.get("attempt_id") == current_before.get("attempt_id")
        and not current_during["observed_at"] <= current_before["observed_at"]
    ):
        return ScenarioOutcome(
            scenario_id,
            "failed",
            "status during the hold re-read current_attempt instead of serving prepared state",
        )
    if (
        isinstance(current_during, dict)
        and isinstance(current_after, dict)
        and current_during.get("attempt_id") == current_after.get("attempt_id")
        and not current_during["observed_at"] < current_after["observed_at"]
    ):
        return ScenarioOutcome(
            scenario_id,
            "failed",
            "status during the hold has no older current_attempt observed_at",
        )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_budget_reads(ctx: SuiteContext, *, budget_s: float = READ_BUDGET_S) -> ScenarioOutcome:
    """``identity``, ``status`` and ``command`` each answer within 3 s."""
    scenario_id = "budget-reads"
    steer = _ensure_intake(ctx, scenario_id, "open")
    if steer is not None:
        return steer
    setup_id = _new_request_id()
    setup = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "stop",
            "--request-id",
            setup_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(setup, ScenarioOutcome):
        return setup
    problems = _check_mutation(setup.stdout, setup.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    request_id = json.loads(setup.stdout)["result"]["command"]["request_id"]
    reads = [
        ("identity", ["--contract", "1", "identity"], _check_identity),
        ("status", ["--contract", "1", "status"], _check_status),
    ]
    for operation, argv, check in reads:
        elapsed, answered = _measure(ctx, scenario_id, argv)
        if isinstance(answered, ScenarioOutcome):
            return answered
        problems = check(answered.stdout, answered.exit_code, ctx)
        if problems:
            return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
        if elapsed > budget_s:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"{operation} took {elapsed:.2f}s (budget {budget_s:.2f}s)",
            )
    elapsed, stored = _measure(ctx, scenario_id, ["--contract", "1", "command", request_id])
    if isinstance(stored, ScenarioOutcome):
        return stored
    problems = _check_command(stored.stdout, stored.exit_code, request_id=request_id, kind="stop")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    if elapsed > budget_s:
        return ScenarioOutcome(
            scenario_id, "failed", f"command took {elapsed:.2f}s (budget {budget_s:.2f}s)"
        )
    reopened = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            _new_request_id(),
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(reopened, ScenarioOutcome):
        return reopened
    problems = _check_mutation(reopened.stdout, reopened.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(
            scenario_id, "failed", f"could not reopen intake: {'; '.join(problems)}"
        )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_budget_mutations(
    ctx: SuiteContext, *, budget_s: float = MUTATION_BUDGET_S
) -> ScenarioOutcome:
    """``stop`` and ``resume`` each commit within 10 s."""
    scenario_id = "budget-mutations"
    steer = _ensure_intake(ctx, scenario_id, "open")
    if steer is not None:
        return steer
    for operation in ("stop", "resume"):
        elapsed, answered = _measure(
            ctx,
            scenario_id,
            [
                "--contract",
                "1",
                operation,
                "--request-id",
                _new_request_id(),
                "--expect-repo",
                ctx.repository,
            ],
        )
        if isinstance(answered, ScenarioOutcome):
            return answered
        problems = _check_mutation(answered.stdout, answered.exit_code, replayed=False)
        if problems:
            return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
        if elapsed > budget_s:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"{operation} took {elapsed:.2f}s (budget {budget_s:.2f}s)",
            )
    return ScenarioOutcome(scenario_id, "passed")


def _attempt_id_for_reads(
    ctx: SuiteContext, harness_cmd: Sequence[str], scenario_id: str, issue: int
) -> tuple[str | None, ScenarioOutcome | None, bool]:
    """Find an attempt to read: current, last finished, or a fresh harness claim.

    Returns the attempt id, a not-run/failed outcome when there is nothing to
    read, and whether the caller must mark ``issue`` ineligible afterwards.
    """
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return None, outcome, False
    current = snapshot.get("current_attempt")
    if isinstance(current, dict) and isinstance(current.get("attempt_id"), str):
        return str(current["attempt_id"]), None, False
    finished = snapshot.get("last_finished")
    if isinstance(finished, dict) and isinstance(finished.get("attempt_id"), str):
        return str(finished["attempt_id"]), None, False
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return None, settled, False
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": issue, "eligible": True}
    )
    if outcome is not None:
        return None, outcome, False
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return None, outcome, False
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return None, outcome, False
    current = snapshot.get("current_attempt")
    if not isinstance(current, dict) or not isinstance(current.get("attempt_id"), str):
        return (
            None,
            ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim"),
            False,
        )
    return str(current["attempt_id"]), None, True


def _drop_issue(
    ctx: SuiteContext, harness_cmd: Sequence[str], scenario_id: str, issue: int
) -> ScenarioOutcome | None:
    """Mark a scenario-created issue ineligible; best-effort finalization first."""
    _harness(ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"})
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": issue, "eligible": False}
    )
    return outcome


def scenario_budget_attempts(
    ctx: SuiteContext,
    harness_cmd: Sequence[str],
    *,
    budget_s: float = ATTEMPTS_BUDGET_S,
) -> ScenarioOutcome:
    """``attempts list`` and ``attempts show`` each answer within 5 s."""
    scenario_id = "budget-attempts"
    issue = 107
    attempt_id, outcome, created = _attempt_id_for_reads(ctx, harness_cmd, scenario_id, issue)
    if outcome is not None or attempt_id is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no attempt to read")
    elapsed, listed = _measure(ctx, scenario_id, ["--contract", "1", "attempts", "list"])
    if isinstance(listed, ScenarioOutcome):
        result: ScenarioOutcome = listed
    else:
        problems = _check_ok_result(listed.stdout, listed.exit_code)
        if problems:
            result = ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
        elif elapsed > budget_s:
            result = ScenarioOutcome(
                scenario_id,
                "failed",
                f"attempts list took {elapsed:.2f}s (budget {budget_s:.2f}s)",
            )
        else:
            elapsed, shown = _measure(
                ctx, scenario_id, ["--contract", "1", "attempts", "show", attempt_id]
            )
            if isinstance(shown, ScenarioOutcome):
                result = shown
            else:
                problems = _check_ok_result(shown.stdout, shown.exit_code)
                if problems:
                    result = ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
                elif elapsed > budget_s:
                    result = ScenarioOutcome(
                        scenario_id,
                        "failed",
                        f"attempts show took {elapsed:.2f}s (budget {budget_s:.2f}s)",
                    )
                else:
                    result = ScenarioOutcome(scenario_id, "passed")
    if created:
        cleanup = _drop_issue(ctx, harness_cmd, scenario_id, issue)
        if result.status == "passed" and cleanup is not None:
            return cleanup
    return result


def _check_attempts_list_result(stdout: str, exit_code: int) -> list[str]:
    """Validate an ``attempts list`` result: envelope, shape and newest-first order."""
    problems = _check_ok_result(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    result = doc.get("result")
    if not isinstance(result, dict):
        return ["attempts list result must be an object"]
    problems = documents.validate_attempts_list(result)
    if problems:
        return problems
    attempts = result.get("attempts")
    assert isinstance(attempts, list)
    started = [entry.get("started_at") for entry in attempts if isinstance(entry, dict)]
    if any(not isinstance(value, str) for value in started):
        return ["attempts list entries must carry started_at timestamps"]
    ordered = cast("list[str]", started)
    if ordered != sorted(ordered, reverse=True):
        # The active attempt is newest; allow it first even when its timestamp
        # ties the newest finished one.
        return ["attempts list must be newest first"]
    return []


def _check_attempts_show_result(stdout: str, exit_code: int, attempt_id: str) -> list[str]:
    """Validate an ``attempts show`` result for one attempt."""
    problems = _check_ok_result(stdout, exit_code)
    if problems:
        return problems
    doc = json.loads(stdout)
    result = doc.get("result")
    if not isinstance(result, dict):
        return ["attempts show result must be an object"]
    problems = documents.validate_attempts_show(result)
    if problems:
        return problems
    attempt = result.get("attempt")
    assert isinstance(attempt, dict)
    if attempt.get("attempt_id") != attempt_id:
        return [f"attempts show is for {attempt.get('attempt_id')!r}, not {attempt_id!r}"]
    manifest = result.get("manifest")
    assert isinstance(manifest, list)
    payloads = [
        entry
        for entry in manifest
        if isinstance(entry, dict) and entry.get("path") == "events.jsonl"
    ]
    if not payloads:
        return ["attempts show manifest must include events.jsonl"]
    if any(entry.get("kind") != "events" for entry in payloads):
        return ["attempts show manifest events.jsonl must have kind events"]
    return []


def _paging_checks(ctx: SuiteContext, scenario_id: str) -> ScenarioOutcome:
    """Run the ``attempts list`` paging checks; the caller handles harness cleanup."""
    listed = _exec(scenario_id, ["--contract", "1", "attempts", "list"], ctx)
    if isinstance(listed, ScenarioOutcome):
        return listed
    problems = _check_attempts_list_result(listed.stdout, listed.exit_code)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    default_doc = json.loads(listed.stdout)["result"]
    default_attempts = default_doc["attempts"]
    if len(default_attempts) > 20:
        return ScenarioOutcome(
            scenario_id, "failed", "the default attempts list page holds at most 20"
        )
    first_page = _exec(scenario_id, ["--contract", "1", "attempts", "list", "--limit", "1"], ctx)
    if isinstance(first_page, ScenarioOutcome):
        return first_page
    problems = _check_attempts_list_result(first_page.stdout, first_page.exit_code)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    first_doc = json.loads(first_page.stdout)["result"]
    if len(first_doc["attempts"]) != 1:
        return ScenarioOutcome(scenario_id, "failed", "--limit 1 must return one attempt")
    if first_doc["attempts"][0] != default_attempts[0]:
        return ScenarioOutcome(
            scenario_id, "failed", "the first page must start with the newest attempt"
        )
    cursor = first_doc.get("next_cursor")
    total = len(default_attempts)
    if total > 1:
        if not isinstance(cursor, str) or not cursor:
            return ScenarioOutcome(
                scenario_id, "failed", "a partial page must carry an opaque next_cursor"
            )
        if cursor in ("0", "1"):
            return ScenarioOutcome(
                scenario_id, "failed", "the cursor must be opaque, not a plain offset"
            )
        second_page = _exec(
            scenario_id,
            ["--contract", "1", "attempts", "list", "--limit", "1", "--before", cursor],
            ctx,
        )
        if isinstance(second_page, ScenarioOutcome):
            return second_page
        problems = _check_attempts_list_result(second_page.stdout, second_page.exit_code)
        if problems:
            return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
        second_doc = json.loads(second_page.stdout)["result"]
        if not second_doc["attempts"]:
            return ScenarioOutcome(
                scenario_id, "failed", "following next_cursor must return more attempts"
            )
        if second_doc["attempts"][0] == first_doc["attempts"][0]:
            return ScenarioOutcome(
                scenario_id, "failed", "pages must not repeat the cursor boundary"
            )
    rejected = _exec(
        scenario_id,
        ["--contract", "1", "attempts", "list", "--limit", "101"],
        ctx,
    )
    if isinstance(rejected, ScenarioOutcome):
        return rejected
    problems = _check_error(rejected.stdout, rejected.exit_code, "invalid_argument")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    bad_cursor = _exec(
        scenario_id,
        ["--contract", "1", "attempts", "list", "--before", "not-a-cursor"],
        ctx,
    )
    if isinstance(bad_cursor, ScenarioOutcome):
        return bad_cursor
    problems = _check_error(bad_cursor.stdout, bad_cursor.exit_code, "invalid_argument")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    return ScenarioOutcome(scenario_id, "passed")


def scenario_attempts_list_paging(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``attempts list`` pages newest first with a default of 20, max 100, opaque cursor."""
    scenario_id = "attempts-list-paging"
    issue = 109
    attempt_id, outcome, created = _attempt_id_for_reads(ctx, harness_cmd, scenario_id, issue)
    if outcome is not None or attempt_id is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no attempt to read")
    result = _paging_checks(ctx, scenario_id)
    if created:
        cleanup = _drop_issue(ctx, harness_cmd, scenario_id, issue)
        if result.status == "passed" and cleanup is not None:
            return cleanup
    return result


def scenario_attempts_show(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``attempts show`` returns the archive with its manifest for a known attempt."""
    scenario_id = "attempts-show"
    issue = 110
    attempt_id, outcome, created = _attempt_id_for_reads(ctx, harness_cmd, scenario_id, issue)
    if outcome is not None or attempt_id is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no attempt to read")
    answered = _exec(scenario_id, ["--contract", "1", "attempts", "show", attempt_id], ctx)
    if isinstance(answered, ScenarioOutcome):
        result: ScenarioOutcome = answered
    else:
        problems = _check_attempts_show_result(answered.stdout, answered.exit_code, attempt_id)
        if problems:
            result = ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
        else:
            result = ScenarioOutcome(scenario_id, "passed")
    if created:
        cleanup = _drop_issue(ctx, harness_cmd, scenario_id, issue)
        if result.status == "passed" and cleanup is not None:
            return cleanup
    return result


def scenario_attempts_not_found(ctx: SuiteContext) -> ScenarioOutcome:
    """``attempts show`` of an unknown attempt answers ``not_found``."""
    scenario_id = "attempts-not-found"
    answered = _exec(scenario_id, ["--contract", "1", "attempts", "show", "no-such-attempt"], ctx)
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_error(answered.stdout, answered.exit_code, "not_found")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    return ScenarioOutcome(scenario_id, "passed")


def _export_events_payload(
    ctx: SuiteContext, scenario_id: str, attempt_id: str
) -> tuple[bytes | None, ScenarioOutcome | None]:
    """Stream one export and return its raw tar bytes."""
    try:
        proc = _popen_export(
            ctx.docker,
            ctx.container,
            ctx.exec_user,
            ["--contract", "1", "attempts", "export", attempt_id],
        )
    except OSError as exc:
        return None, ScenarioOutcome(scenario_id, "failed", f"export did not start: {exc}")
    try:
        data, _ = proc.communicate(timeout=ctx.timeout)
    except subprocess.TimeoutExpired:
        _stop_export(proc)
        return None, ScenarioOutcome(scenario_id, "failed", "export did not finish")
    if proc.returncode != 0:
        return None, ScenarioOutcome(scenario_id, "failed", f"export exited {proc.returncode}")
    return data, None


def _contract_events_checks(
    ctx: SuiteContext, scenario_id: str, attempt_id: str
) -> ScenarioOutcome:
    """Check ``recent_events`` and the exported ``events.jsonl``; caller cleans up."""
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status snapshot")
    events = snapshot.get("recent_events")
    if not isinstance(events, list):
        return ScenarioOutcome(scenario_id, "failed", "recent_events must be an array")
    for index, entry in enumerate(events):
        problems = documents.validate_contract_event(entry)
        if problems:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"recent_events[{index}] is not a Contract Event: {'; '.join(problems)}",
            )
    data, outcome = _export_events_payload(ctx, scenario_id, attempt_id)
    if outcome is not None or data is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no export stream")
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            try:
                member = archive.getmember("events.jsonl")
            except KeyError:
                return ScenarioOutcome(scenario_id, "failed", "the export lacks events.jsonl")
            extracted = archive.extractfile(member)
            payload = extracted.read() if extracted is not None else b""
    except (tarfile.TarError, OSError) as exc:
        return ScenarioOutcome(scenario_id, "failed", f"export is not a tar: {exc}")
    for number, line in enumerate(payload.decode("utf-8").splitlines()):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            return ScenarioOutcome(scenario_id, "failed", f"events.jsonl line {number} is not JSON")
        problems = documents.validate_contract_event(entry)
        if problems:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"events.jsonl line {number} is not a Contract Event: {'; '.join(problems)}",
            )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_contract_events(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """Every ``recent_events`` entry and every ``events.jsonl`` line is a Contract Event."""
    scenario_id = "contract-events"
    issue = 111
    attempt_id, outcome, created = _attempt_id_for_reads(ctx, harness_cmd, scenario_id, issue)
    if outcome is not None or attempt_id is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no attempt to read")
    result = _contract_events_checks(ctx, scenario_id, attempt_id)
    if created:
        cleanup = _drop_issue(ctx, harness_cmd, scenario_id, issue)
        if result.status == "passed" and cleanup is not None:
            return cleanup
    return result


def scenario_budget_export(
    ctx: SuiteContext,
    harness_cmd: Sequence[str],
    *,
    budget_s: float = EXPORT_FIRST_BYTE_BUDGET_S,
) -> ScenarioOutcome:
    """``attempts export`` starts streaming within 5 s."""
    scenario_id = "budget-export"
    issue = 108
    attempt_id, outcome, created = _attempt_id_for_reads(ctx, harness_cmd, scenario_id, issue)
    if outcome is not None or attempt_id is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no attempt to export")
    argv = ["--contract", "1", "attempts", "export", attempt_id]
    try:
        proc = _popen_export(ctx.docker, ctx.container, ctx.exec_user, argv)
    except OSError as exc:
        result = ScenarioOutcome(scenario_id, "failed", f"export did not start: {exc}")
    else:
        first, elapsed = _read_first_byte(proc, budget_s)
        if not first:
            _stop_export(proc)
            result = ScenarioOutcome(
                scenario_id,
                "failed",
                f"export sent no bytes within {budget_s:.2f}s",
            )
        elif elapsed > budget_s:
            _stop_export(proc)
            result = ScenarioOutcome(
                scenario_id,
                "failed",
                f"export started after {elapsed:.2f}s (budget {budget_s:.2f}s)",
            )
        else:
            try:
                rest, _ = proc.communicate(timeout=ctx.timeout)
            except subprocess.TimeoutExpired:
                _stop_export(proc)
                result = ScenarioOutcome(scenario_id, "failed", "export did not finish")
            else:
                if proc.returncode != 0:
                    result = ScenarioOutcome(
                        scenario_id,
                        "failed",
                        f"export exited {proc.returncode}: {rest[:200]!r}",
                    )
                else:
                    problems = _check_export_stream(first + rest, attempt_id)
                    if problems:
                        result = ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
                    else:
                        result = ScenarioOutcome(scenario_id, "passed")
    if created:
        cleanup = _drop_issue(ctx, harness_cmd, scenario_id, issue)
        if result.status == "passed" and cleanup is not None:
            return cleanup
    return result


def _drain_export(
    ctx: SuiteContext, scenario_id: str, attempt_id: str, paths: Sequence[str] = ()
) -> tuple[bytes | None, ScenarioOutcome | None]:
    """Drain one ``attempts export`` tar stream, optionally narrowed by ``paths``."""
    argv = ["--contract", "1", "attempts", "export", attempt_id]
    for path in paths:
        argv += ["--path", path]
    try:
        proc = _popen_export(ctx.docker, ctx.container, ctx.exec_user, argv)
    except OSError as exc:
        return None, ScenarioOutcome(scenario_id, "failed", f"export did not start: {exc}")
    try:
        data, _ = proc.communicate(timeout=ctx.timeout)
    except subprocess.TimeoutExpired:
        _stop_export(proc)
        return None, ScenarioOutcome(scenario_id, "failed", "export did not finish")
    if proc.returncode != 0:
        return None, ScenarioOutcome(scenario_id, "failed", f"export exited {proc.returncode}")
    return data, None


def _drain_export_offline(
    ctx: SuiteContext, scenario_id: str, attempt_id: str, paths: Sequence[str] = ()
) -> tuple[bytes | None, ScenarioOutcome | None]:
    """Drain one offline ``attempts export`` tar through ``docker run``."""
    argv = ["--contract", "1", "--offline", "attempts", "export", attempt_id]
    for path in paths:
        argv += ["--path", path]
    if not ctx.image:
        return None, ScenarioOutcome(scenario_id, "failed", "no image for the offline read")
    try:
        proc = _popen_run(ctx, argv)
    except OSError as exc:
        return None, ScenarioOutcome(scenario_id, "failed", f"export did not start: {exc}")
    try:
        data, _ = proc.communicate(timeout=ctx.timeout)
    except subprocess.TimeoutExpired:
        _stop_export(proc)
        return None, ScenarioOutcome(scenario_id, "failed", "export did not finish")
    if proc.returncode != 0:
        return None, ScenarioOutcome(scenario_id, "failed", f"export exited {proc.returncode}")
    return data, None


def _read_attempts_show(
    stdout: str, exit_code: int, attempt_id: str
) -> tuple[dict[str, Any] | None, list[str]]:
    """Check an ``attempts show`` read in producer mode; return its result."""
    problems = _check_ok_result(stdout, exit_code)
    if problems:
        return None, problems
    try:
        doc = json.loads(stdout)
    except ValueError:
        return None, ["attempts show is not valid JSON"]
    result = doc.get("result")
    if not isinstance(result, dict):
        return None, ["attempts show result must be an object"]
    problems = documents.validate_attempts_show(result)
    if problems:
        return None, [f"attempts show result is invalid: {problems[0]}"]
    attempt = result.get("attempt")
    if not isinstance(attempt, dict) or attempt.get("attempt_id") != attempt_id:
        return None, [f"attempts show is not for attempt {attempt_id!r}"]
    return result, []


def scenario_sealed_after_outcome(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """Outcome recorded while finalization is still writing (spec section 14).

    The archive reports ``sealed: false`` with its outcome after the hold,
    then a ``sealed`` event follows the release and the manifest stays
    immutable across two sealed reads.
    """
    scenario_id = "sealed-after-outcome"
    issue = 113
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": issue, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status snapshot")
    current = snapshot.get("current_attempt")
    if not isinstance(current, dict) or not isinstance(current.get("attempt_id"), str):
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    attempt_id = str(current["attempt_id"])
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "hold", {"point": "after-outcome", "scope": "active"}
    )
    if outcome is not None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome
    answered = _exec(scenario_id, ["--contract", "1", "attempts", "show", attempt_id], ctx)
    if isinstance(answered, ScenarioOutcome):
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return answered
    result, problems = _read_attempts_show(answered.stdout, answered.exit_code, attempt_id)
    if problems or result is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    attempt = result["attempt"]
    assert isinstance(attempt, dict)
    if attempt.get("sealed") is not False:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "archive must stay unsealed while held")
    if not isinstance(attempt.get("outcome"), dict):
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "the outcome must be recorded while held")
    data, outcome = _drain_export(ctx, scenario_id, attempt_id)
    if outcome is not None or data is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome or ScenarioOutcome(scenario_id, "failed", "no export stream")
    problems = _check_export_stream(data, attempt_id)
    if problems:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status snapshot")
    sealed_events = [
        event
        for event in snapshot.get("recent_events", [])
        if isinstance(event, dict)
        and event.get("event") == "sealed"
        and event.get("attempt_id") == attempt_id
    ]
    if not sealed_events:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "no sealed event follows the release")
    answered = _exec(scenario_id, ["--contract", "1", "attempts", "show", attempt_id], ctx)
    if isinstance(answered, ScenarioOutcome):
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return answered
    sealed_result, problems = _read_attempts_show(answered.stdout, answered.exit_code, attempt_id)
    if problems or sealed_result is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    sealed_attempt = sealed_result["attempt"]
    assert isinstance(sealed_attempt, dict)
    if sealed_attempt.get("sealed") is not True or sealed_attempt.get("sealed_at") is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "the archive must be sealed after release")
    repeat = _exec(scenario_id, ["--contract", "1", "attempts", "show", attempt_id], ctx)
    if isinstance(repeat, ScenarioOutcome):
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return repeat
    repeat_result, problems = _read_attempts_show(repeat.stdout, repeat.exit_code, attempt_id)
    if problems or repeat_result is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    if repeat_result.get("manifest") != sealed_result.get("manifest"):
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "the sealed manifest must be immutable")
    first, outcome = _drain_export(ctx, scenario_id, attempt_id)
    if outcome is not None or first is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome or ScenarioOutcome(scenario_id, "failed", "no export stream")
    second, outcome = _drain_export(ctx, scenario_id, attempt_id)
    if outcome is not None or second is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome or ScenarioOutcome(scenario_id, "failed", "no export stream")
    if first != second:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "the sealed export must be byte-identical")
    cleanup = _drop_issue(ctx, harness_cmd, scenario_id, issue)
    if cleanup is not None:
        return cleanup
    return ScenarioOutcome(scenario_id, "passed")


def scenario_export_torn_read(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """Export of an unsealed archive while a file grows (spec section 14).

    The first export validates, then ``grow-archive`` moves the bytes under
    the snapshot manifest and the second export carries the torn read: a file
    whose ``sha256`` no longer matches its manifest entry.
    """
    scenario_id = "export-torn-read"
    issue = 114
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": issue, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status snapshot")
    current = snapshot.get("current_attempt")
    if not isinstance(current, dict) or not isinstance(current.get("attempt_id"), str):
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    attempt_id = str(current["attempt_id"])
    data, outcome = _drain_export(ctx, scenario_id, attempt_id)
    if outcome is not None or data is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome or ScenarioOutcome(scenario_id, "failed", "no export stream")
    problems = _check_export_stream(data, attempt_id)
    if problems:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    _, outcome = _harness(
        ctx,
        harness_cmd,
        scenario_id,
        "grow-archive",
        {"attempt_id": attempt_id, "append_bytes": 64},
    )
    if outcome is not None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome
    grown, outcome = _drain_export(ctx, scenario_id, attempt_id)
    if outcome is not None or grown is None:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return outcome or ScenarioOutcome(scenario_id, "failed", "no export stream")
    problems = _check_export_stream(grown, attempt_id)
    if not problems:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "expected a torn read after the archive grew")
    # A grown file tears the read either by size or by hash: both mean the
    # streamed bytes no longer match the snapshot manifest entry.
    torn = any("sha256" in problem or "bytes, not" in problem for problem in problems)
    if not torn:
        _drop_issue(ctx, harness_cmd, scenario_id, issue)
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    cleanup = _drop_issue(ctx, harness_cmd, scenario_id, issue)
    if cleanup is not None:
        return cleanup
    return ScenarioOutcome(scenario_id, "passed")


def _snapshot_size_bytes(snapshot: dict[str, Any]) -> int:
    """Measure a snapshot the way spec section 9.1 bounds it: compact JSON."""
    return len(json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def scenario_snapshot_bounds(ctx: SuiteContext) -> ScenarioOutcome:
    """The snapshot fits 256 KiB with oldest-first truncation and sorted order."""
    scenario_id = "snapshot-bounds"
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status snapshot")
    size = _snapshot_size_bytes(snapshot)
    if size > SNAPSHOT_MAX_BYTES:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"snapshot is {size} bytes (bound {SNAPSHOT_MAX_BYTES})",
        )
    recent_commands = snapshot.get("recent_commands")
    if not isinstance(recent_commands, list) or len(recent_commands) > 20:
        return ScenarioOutcome(scenario_id, "failed", "recent_commands holds at most 20 records")
    recent_events = snapshot.get("recent_events")
    if not isinstance(recent_events, list) or len(recent_events) > 50:
        return ScenarioOutcome(scenario_id, "failed", "recent_events holds at most 50 events")
    capabilities = snapshot["identity"].get("capabilities")
    if not isinstance(capabilities, list) or capabilities != sorted(capabilities):
        return ScenarioOutcome(
            scenario_id, "failed", f"capabilities {capabilities!r} are not sorted"
        )
    sequences = [record.get("sequence") for record in recent_commands if isinstance(record, dict)]
    if any(not isinstance(sequence, int) for sequence in sequences):
        return ScenarioOutcome(
            scenario_id, "failed", "recent_commands are not oldest-first (newest last)"
        )
    if sequences != sorted(cast("list[int]", sequences)):
        return ScenarioOutcome(
            scenario_id, "failed", "recent_commands are not oldest-first (newest last)"
        )
    stamped = [event.get("at") for event in recent_events if isinstance(event, dict)]
    if any(not isinstance(at, str) for at in stamped):
        return ScenarioOutcome(
            scenario_id, "failed", "recent_events are not oldest-first (newest last)"
        )
    if stamped != sorted(cast("list[str]", stamped)):
        return ScenarioOutcome(
            scenario_id, "failed", "recent_events are not oldest-first (newest last)"
        )
    if snapshot.get("truncated") is True and recent_events:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            "a truncated snapshot must drop the oldest recent_events first",
        )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_command_not_found(ctx: SuiteContext) -> ScenarioOutcome:
    """``command`` of an unknown request ID answers ``not_found``."""
    scenario_id = "command-not-found"
    answered = _exec(scenario_id, ["--contract", "1", "command", _new_request_id()], ctx)
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_error(answered.stdout, answered.exit_code, "not_found")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    return ScenarioOutcome(scenario_id, "passed")


def scenario_repository_mismatch(ctx: SuiteContext) -> ScenarioOutcome:
    """A wrong ``--expect-repo`` on any mutation is rejected and stores nothing."""
    scenario_id = "repository-mismatch"
    wrong = f"{ctx.repository}-wrong"
    for operation in ("stop", "resume"):
        request_id = _new_request_id()
        answered = _exec(
            scenario_id,
            [
                "--contract",
                "1",
                operation,
                "--request-id",
                request_id,
                "--expect-repo",
                wrong,
            ],
            ctx,
        )
        if isinstance(answered, ScenarioOutcome):
            return answered
        problems = _check_error_request(
            answered.stdout, answered.exit_code, "repository_mismatch", request_id
        )
        if problems:
            return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
        try:
            doc = json.loads(answered.stdout)
        except ValueError:
            return ScenarioOutcome(scenario_id, "failed", "repository_mismatch is not JSON")
        if doc["error"].get("details", {}).get("repository") != ctx.repository:
            return ScenarioOutcome(
                scenario_id,
                "failed",
                f"details.repository is not {ctx.repository!r}: {answered.stdout.strip()}",
            )
        stored = _exec(scenario_id, ["--contract", "1", "command", request_id], ctx)
        if isinstance(stored, ScenarioOutcome):
            return stored
        nothing = _check_error(stored.stdout, stored.exit_code, "not_found")
        if nothing:
            return ScenarioOutcome(
                scenario_id, "failed", f"rejected {operation} stored {request_id}"
            )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_stop_replay(ctx: SuiteContext) -> ScenarioOutcome:
    """An identical resend replays the stored record with ``replayed: true``."""
    scenario_id = "stop-replay"
    steer = _ensure_intake(ctx, scenario_id, "open")
    if steer is not None:
        return steer
    request_id = _new_request_id()
    argv = [
        "--contract",
        "1",
        "stop",
        "--request-id",
        request_id,
        "--expect-repo",
        ctx.repository,
    ]
    first = _exec(scenario_id, argv, ctx)
    if isinstance(first, ScenarioOutcome):
        return first
    problems = _check_mutation(first.stdout, first.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    committed = json.loads(first.stdout)["result"]["command"]
    if committed.get("kind") != "stop" or committed.get("request_id") != request_id:
        return ScenarioOutcome(
            scenario_id, "failed", f"committed record is not this stop: {first.stdout.strip()}"
        )
    second = _exec(scenario_id, argv, ctx)
    if isinstance(second, ScenarioOutcome):
        return second
    problems = _check_mutation(second.stdout, second.exit_code, replayed=True)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    replayed_record = json.loads(second.stdout)["result"]["command"]
    if replayed_record != committed:
        return ScenarioOutcome(scenario_id, "failed", "the replay changed the stored record")
    return ScenarioOutcome(scenario_id, "passed")


def scenario_payload_mismatch(ctx: SuiteContext) -> ScenarioOutcome:
    """The same request ID with a different payload is rejected; nothing stored."""
    scenario_id = "payload-mismatch"
    steer = _ensure_intake(ctx, scenario_id, "open")
    if steer is not None:
        return steer
    request_id = _new_request_id()
    committed = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "stop",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(committed, ScenarioOutcome):
        return committed
    problems = _check_mutation(committed.stdout, committed.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    record = json.loads(committed.stdout)["result"]["command"]
    different = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(different, ScenarioOutcome):
        return different
    problems = _check_error_request(
        different.stdout, different.exit_code, "payload_mismatch", request_id
    )
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    stored = _exec(scenario_id, ["--contract", "1", "command", request_id], ctx)
    if isinstance(stored, ScenarioOutcome):
        return stored
    problems = _check_command(stored.stdout, stored.exit_code, request_id=request_id, kind="stop")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    if json.loads(stored.stdout)["result"]["command"] != record:
        return ScenarioOutcome(scenario_id, "failed", "the mismatched payload changed the record")
    return ScenarioOutcome(scenario_id, "passed")


def scenario_resume(ctx: SuiteContext) -> ScenarioOutcome:
    """``resume`` reopens closed Intake; a second ``resume`` is ``rejected``."""
    scenario_id = "resume"
    steer = _ensure_intake(ctx, scenario_id, "closed")
    if steer is not None:
        return steer
    request_id = _new_request_id()
    reopened = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(reopened, ScenarioOutcome):
        return reopened
    problems = _check_mutation(reopened.stdout, reopened.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    record = json.loads(reopened.stdout)["result"]["command"]
    if record.get("kind") != "resume" or record.get("request_id") != request_id:
        return ScenarioOutcome(
            scenario_id, "failed", f"committed record is not this resume: {reopened.stdout.strip()}"
        )
    state, outcome = _intake_state(ctx, scenario_id)
    if outcome is not None:
        return outcome
    if state != "open":
        return ScenarioOutcome(scenario_id, "failed", f"intake is {state!r} after resume")
    refused_id = _new_request_id()
    refused = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            refused_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(refused, ScenarioOutcome):
        return refused
    problems = _check_error_request(refused.stdout, refused.exit_code, "rejected", refused_id)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    stored = _exec(scenario_id, ["--contract", "1", "command", refused_id], ctx)
    if isinstance(stored, ScenarioOutcome):
        return stored
    nothing = _check_error(stored.stdout, stored.exit_code, "not_found")
    if nothing:
        return ScenarioOutcome(scenario_id, "failed", f"rejected resume stored {refused_id}")
    return ScenarioOutcome(scenario_id, "passed")


def scenario_crash_between_commit_and_reply(
    ctx: SuiteContext, harness_cmd: Sequence[str]
) -> ScenarioOutcome:
    """Mutation commits, the process dies before replying, the retry replays it.

    The harness arms a one-shot crash between commit and reply; the first
    mutation gets no reply. The suite restarts the instance (spec section 14:
    the consumer retries "after restart"), and the retry with the same
    request ID returns the stored record with ``replayed: true`` while the
    command store shows the single committed effect.
    """
    scenario_id = "crash-between-commit-and-reply"
    steer = _ensure_intake(ctx, scenario_id, "open")
    if steer is not None:
        return steer
    armed = _call_harness(harness_cmd, "crash", {"point": "between-commit-and-reply"}, ctx.timeout)
    if armed.status == "unsupported":
        return ScenarioOutcome(scenario_id, "not_run", f"harness cannot crash ({armed.detail})")
    if armed.status != "ok" or armed.result is None:
        return ScenarioOutcome(scenario_id, "failed", armed.detail)
    request_id = _new_request_id()
    argv = [
        "--contract",
        "1",
        "stop",
        "--request-id",
        request_id,
        "--expect-repo",
        ctx.repository,
    ]
    first = _exec(scenario_id, argv, ctx)
    if isinstance(first, ScenarioOutcome):
        return first
    try:
        doc = json.loads(first.stdout)
    except ValueError:
        doc = None
    if isinstance(doc, dict) and doc.get("ok") is True:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected no reply to the crash, got: {first.stdout.strip()}",
        )
    restarted = _restart_after_crash(ctx, harness_cmd, scenario_id)
    if restarted is not None:
        return restarted
    second = _exec(scenario_id, argv, ctx)
    if isinstance(second, ScenarioOutcome):
        return second
    problems = _check_mutation(second.stdout, second.exit_code, replayed=True)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    committed = json.loads(second.stdout)["result"]["command"]
    if committed.get("request_id") != request_id or committed.get("kind") != "stop":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"the replay is not this stop: {second.stdout.strip()}",
        )
    stored = _exec(scenario_id, ["--contract", "1", "command", request_id], ctx)
    if isinstance(stored, ScenarioOutcome):
        return stored
    problems = _check_command(stored.stdout, stored.exit_code, request_id=request_id, kind="stop")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    if json.loads(stored.stdout)["result"]["command"] != committed:
        return ScenarioOutcome(scenario_id, "failed", "the crash committed more than one effect")
    return ScenarioOutcome(scenario_id, "passed")


def scenario_command_found(ctx: SuiteContext) -> ScenarioOutcome:
    """``command`` of a known request ID returns the stored record."""
    scenario_id = "command-found"
    steer = _ensure_intake(ctx, scenario_id, "open")
    if steer is not None:
        return steer
    request_id = _new_request_id()
    committed = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "stop",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(committed, ScenarioOutcome):
        return committed
    problems = _check_mutation(committed.stdout, committed.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    record = json.loads(committed.stdout)["result"]["command"]
    stored = _exec(scenario_id, ["--contract", "1", "command", request_id], ctx)
    if isinstance(stored, ScenarioOutcome):
        return stored
    problems = _check_command(stored.stdout, stored.exit_code, request_id=request_id, kind="stop")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    if json.loads(stored.stdout)["result"]["command"] != record:
        return ScenarioOutcome(scenario_id, "failed", "command read differs from the commit")
    return ScenarioOutcome(scenario_id, "passed")


def _harness(
    ctx: SuiteContext,
    harness_cmd: Sequence[str],
    scenario_id: str,
    verb: str,
    params: dict[str, Any],
) -> tuple[dict[str, Any] | None, ScenarioOutcome | None]:
    """Call one harness verb; an unsupported verb becomes a not-run outcome."""
    result = _call_harness(harness_cmd, verb, params, ctx.timeout)
    if result.status == "unsupported":
        return None, ScenarioOutcome(
            scenario_id, "not_run", f"harness {verb} is not supported ({result.detail})"
        )
    if result.status != "ok" or result.result is None:
        return None, ScenarioOutcome(
            scenario_id, "failed", f"harness {verb} failed: {result.detail}"
        )
    return result.result, None


def _snapshot(
    ctx: SuiteContext, scenario_id: str
) -> tuple[dict[str, Any] | None, ScenarioOutcome | None]:
    """Read and validate one online ``status`` snapshot."""
    answered = _exec(scenario_id, ["--contract", "1", "status"], ctx)
    if isinstance(answered, ScenarioOutcome):
        return None, answered
    problems = _check_status(answered.stdout, answered.exit_code, ctx)
    if problems:
        return None, ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    try:
        doc = json.loads(answered.stdout)
    except ValueError:
        return None, ScenarioOutcome(scenario_id, "failed", "status is not valid JSON")
    return doc["result"], None


def _read_command(
    ctx: SuiteContext, scenario_id: str, request_id: str
) -> tuple[dict[str, Any] | None, ScenarioOutcome | None]:
    """Read and validate one ``command`` record."""
    answered = _exec(scenario_id, ["--contract", "1", "command", request_id], ctx)
    if isinstance(answered, ScenarioOutcome):
        return None, answered
    problems = _check_command(answered.stdout, answered.exit_code, request_id=request_id)
    if problems:
        return None, ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    return json.loads(answered.stdout)["result"]["command"], None


def _submit(
    ctx: SuiteContext, scenario_id: str, operation: str, request_id: str
) -> tuple[dict[str, Any] | None, ScenarioOutcome | None]:
    """Commit one ``stop`` or ``resume`` and validate the mutation reply."""
    answered = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            operation,
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(answered, ScenarioOutcome):
        return None, answered
    problems = _check_mutation(answered.stdout, answered.exit_code, replayed=False)
    if problems:
        return None, ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    return json.loads(answered.stdout)["result"]["command"], None


def _restart_after_crash(
    ctx: SuiteContext, harness_cmd: Sequence[str], scenario_id: str
) -> ScenarioOutcome | None:
    """Start the crashed instance again and wait until it is ready (spec 2.3).

    Releases the startup hold the harness may keep after a start, so the next
    call reaches a ready engine and later scenarios inherit a live instance.
    """
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    for _ in range(RESTART_RELEASES):
        snapshot, outcome = _snapshot(ctx, scenario_id)
        if outcome is not None or snapshot is None:
            return outcome or ScenarioOutcome(scenario_id, "failed", "no status after restart")
        if snapshot["instance"]["state"] not in ("starting", "recovering"):
            return None
        _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
        if outcome is not None:
            return outcome
    return ScenarioOutcome(scenario_id, "failed", "the instance did not become ready after restart")


def _settle(
    ctx: SuiteContext, harness_cmd: Sequence[str], scenario_id: str
) -> ScenarioOutcome | None:
    """Settle to intake open with no active Attempt and no holds or recovery.

    Each harness-driven scenario starts here so scenarios stay independent of
    the order they run in. An unsupported harness verb becomes not-run.
    """
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    for _ in range(6):
        snapshot, outcome = _snapshot(ctx, scenario_id)
        if outcome is not None or snapshot is None:
            return outcome
        state = snapshot["instance"]["state"]
        if state == "starting":
            _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
            if outcome is not None:
                return outcome
            continue
        if state == "recovering":
            _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
            if outcome is not None:
                return outcome
            continue
        if snapshot["current_attempt"] is not None:
            _, outcome = _harness(
                ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
            )
            if outcome is not None:
                return outcome
            continue
        break
    return _ensure_intake(ctx, scenario_id, "open")


def scenario_stop_before_claim_crash(
    ctx: SuiteContext, harness_cmd: Sequence[str]
) -> ScenarioOutcome:
    """``stop`` before the Claim boundary, then a crash: Intake stays closed."""
    scenario_id = "stop-before-claim-crash"
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 101, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "hold", {"point": "before-claim", "scope": "next"}
    )
    if outcome is not None:
        return outcome
    stop_id = _new_request_id()
    record, outcome = _submit(ctx, scenario_id, "stop", stop_id)
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop committed nothing")
    if record.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"stop before the claim must complete at once, got {record.get('acknowledgement')!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "crash", {"point": "now"})
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after restart")
    if snapshot["instance"]["state"] != "starting":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected starting after restart, got {snapshot['instance']['state']!r}",
        )
    if snapshot["instance"]["startup_check"] is None:
        return ScenarioOutcome(scenario_id, "failed", "starting must name its startup_check")
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after recovery")
    if snapshot["intake"]["state"] != "closed":
        return ScenarioOutcome(
            scenario_id, "failed", f"intake must stay closed, got {snapshot['intake']['state']!r}"
        )
    if snapshot["current_attempt"] is not None:
        return ScenarioOutcome(scenario_id, "failed", "no claim may open after the stop")
    stored, outcome = _read_command(ctx, scenario_id, stop_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop record is missing")
    if stored.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"stop must stay completed, got {stored.get('acknowledgement')!r}",
        )
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 101, "eligible": False}
    )
    if outcome is not None:
        return outcome
    resume_id = _new_request_id()
    _, outcome = _submit(ctx, scenario_id, "resume", resume_id)
    if outcome is not None:
        return outcome
    return ScenarioOutcome(scenario_id, "passed")


def scenario_stop_after_claim_sigterm(
    ctx: SuiteContext, harness_cmd: Sequence[str]
) -> ScenarioOutcome:
    """``stop`` after the Claim boundary, then SIGTERM: the Attempt finishes."""
    scenario_id = "stop-after-claim-sigterm"
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 102, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    if snapshot["current_attempt"] is None:
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    if snapshot["instance"]["state"] != "active":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected active with an attempt, got {snapshot['instance']['state']!r}",
        )
    attempt_id = str(snapshot["current_attempt"]["attempt_id"])
    stop_id = _new_request_id()
    record, outcome = _submit(ctx, scenario_id, "stop", stop_id)
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop committed nothing")
    if record.get("acknowledgement") != "accepted":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"stop after the claim must be accepted, got {record.get('acknowledgement')!r}",
        )
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status while draining")
    if snapshot["instance"]["state"] != "draining":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected draining after the stop, got {snapshot['instance']['state']!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "terminate", {})
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after restart")
    if snapshot["instance"]["state"] != "starting":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected starting after restart, got {snapshot['instance']['state']!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status while recovering")
    if snapshot["instance"]["state"] != "recovering":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected recovering after startup, got {snapshot['instance']['state']!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after recovery")
    if snapshot["current_attempt"] is not None:
        return ScenarioOutcome(scenario_id, "failed", "the interrupted attempt must finish")
    finished = snapshot["last_finished"]
    if not isinstance(finished, dict) or finished.get("attempt_id") != attempt_id:
        return ScenarioOutcome(
            scenario_id, "failed", f"attempt {attempt_id} must reach its final lifecycle state"
        )
    stored, outcome = _read_command(ctx, scenario_id, stop_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop record is missing")
    if stored.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"stop must complete when the attempt finishes, got {stored.get('acknowledgement')!r}",
        )
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 102, "eligible": False}
    )
    if outcome is not None:
        return outcome
    resume_id = _new_request_id()
    _, outcome = _submit(ctx, scenario_id, "resume", resume_id)
    if outcome is not None:
        return outcome
    return ScenarioOutcome(scenario_id, "passed")


def scenario_stop_during_starting(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``stop`` during ``starting``: the first Claim never opens."""
    scenario_id = "stop-during-starting"
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    before, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or before is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status before restart")
    last_before = before["last_finished"]
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "stop-container", {})
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    # The issue becomes eligible only once the instance is starting, so a
    # running engine cannot open a Claim before the restart.
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 103, "eligible": True}
    )
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status while starting")
    if snapshot["instance"]["state"] != "starting":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected starting after restart, got {snapshot['instance']['state']!r}",
        )
    stop_id = _new_request_id()
    record, outcome = _submit(ctx, scenario_id, "stop", stop_id)
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop committed nothing")
    if record.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"stop with no active attempt must complete, got {record.get('acknowledgement')!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after startup")
    if snapshot["current_attempt"] is not None:
        return ScenarioOutcome(scenario_id, "failed", "the first claim must never open")
    if snapshot["intake"]["state"] != "closed":
        return ScenarioOutcome(scenario_id, "failed", "intake must stay closed after the stop")
    if snapshot["last_finished"] != last_before:
        return ScenarioOutcome(scenario_id, "failed", "no attempt may finish without a claim")
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 103, "eligible": False}
    )
    if outcome is not None:
        return outcome
    resume_id = _new_request_id()
    _, outcome = _submit(ctx, scenario_id, "resume", resume_id)
    if outcome is not None:
        return outcome
    return ScenarioOutcome(scenario_id, "passed")


def scenario_resume_supersedes_stop(
    ctx: SuiteContext, harness_cmd: Sequence[str]
) -> ScenarioOutcome:
    """``resume`` while a stop is ``accepted``: the stop is ``superseded``."""
    scenario_id = "resume-supersedes-stop"
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 104, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    if snapshot["current_attempt"] is None:
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    # Ineligible again at once, so finishing this Attempt opens no next Claim.
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 104, "eligible": False}
    )
    if outcome is not None:
        return outcome
    stop_id = _new_request_id()
    record, outcome = _submit(ctx, scenario_id, "stop", stop_id)
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop committed nothing")
    if record.get("acknowledgement") != "accepted":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"stop with an active attempt must be accepted, got {record.get('acknowledgement')!r}",
        )
    resume_id = _new_request_id()
    reopened, outcome = _submit(ctx, scenario_id, "resume", resume_id)
    if outcome is not None or reopened is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "resume committed nothing")
    if reopened.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"resume must complete, got {reopened.get('acknowledgement')!r}",
        )
    stored, outcome = _read_command(ctx, scenario_id, stop_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop record is missing")
    if stored.get("acknowledgement") != "superseded":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"the accepted stop must be superseded, got {stored.get('acknowledgement')!r}",
        )
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after resume")
    if snapshot["intake"]["state"] != "open":
        return ScenarioOutcome(scenario_id, "failed", "intake must stay open after resume")
    if snapshot["instance"]["state"] != "active":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected active after resume, got {snapshot['instance']['state']!r}",
        )
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after finishing")
    if snapshot["current_attempt"] is not None:
        return ScenarioOutcome(scenario_id, "failed", "the attempt must finish")
    return ScenarioOutcome(scenario_id, "passed")


def scenario_stop_after_invalid(ctx: SuiteContext) -> ScenarioOutcome:
    """``stop --after 0`` is ``invalid_argument``; nothing stored, ID reusable."""
    scenario_id = "stop-after-invalid"
    gated = _require_capability(ctx, scenario_id, "stop-after")
    if gated is not None:
        return gated
    steer = _ensure_intake(ctx, scenario_id, "open")
    if steer is not None:
        return steer
    request_id = _new_request_id()
    answered = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "stop",
            "--after",
            "0",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_error_request(
        answered.stdout, answered.exit_code, "invalid_argument", request_id
    )
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    stored = _exec(scenario_id, ["--contract", "1", "command", request_id], ctx)
    if isinstance(stored, ScenarioOutcome):
        return stored
    nothing = _check_error(stored.stdout, stored.exit_code, "not_found")
    if nothing:
        return ScenarioOutcome(scenario_id, "failed", f"invalid --after stored {request_id}")
    reuse = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "stop",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(reuse, ScenarioOutcome):
        return reuse
    problems = _check_mutation(reuse.stdout, reuse.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            "the request ID of an invalid --after is not reusable: " + "; ".join(problems),
        )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_stop_after_restart(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``stop --after N`` in progress, restart: the counter survives and continues."""
    scenario_id = "stop-after-restart"
    gated = _require_capability(ctx, scenario_id, "stop-after")
    if gated is not None:
        return gated
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 111, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    if snapshot["current_attempt"] is None:
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    plan_id = _new_request_id()
    planned = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "stop",
            "--after",
            "2",
            "--request-id",
            plan_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(planned, ScenarioOutcome):
        return planned
    problems = _check_mutation(planned.stdout, planned.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    record = json.loads(planned.stdout)["result"]["command"]
    if record.get("kind") != "stop_after" or record.get("acknowledgement") != "accepted":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"stop --after with an active attempt must be accepted, got: {planned.stdout.strip()}",
        )
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 111, "eligible": False}
    )
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after finalizing")
    plan = snapshot["intake"]["stop_plan"]
    if not isinstance(plan, dict) or plan.get("remaining") != 2 or plan.get("counted") != 1:
        return ScenarioOutcome(
            scenario_id, "failed", f"the plan must count one of two, got {plan!r}"
        )
    if snapshot["intake"]["state"] != "open":
        return ScenarioOutcome(
            scenario_id, "failed", "intake must stay open until the plan completes"
        )
    stored, outcome = _read_command(ctx, scenario_id, plan_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop plan record is missing")
    if stored.get("acknowledgement") != "accepted":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"the plan must stay accepted, got {stored.get('acknowledgement')!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "stop-container", {})
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after restart")
    if snapshot["instance"]["state"] != "starting":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected starting after restart, got {snapshot['instance']['state']!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after startup")
    plan = snapshot["intake"]["stop_plan"]
    if not isinstance(plan, dict) or plan.get("remaining") != 2 or plan.get("counted") != 1:
        return ScenarioOutcome(
            scenario_id, "failed", f"the counter must survive the restart, got {plan!r}"
        )
    stored, outcome = _read_command(ctx, scenario_id, plan_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop plan record is missing")
    if stored.get("acknowledgement") != "accepted":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"the plan must stay accepted across the restart, got "
            f"{stored.get('acknowledgement')!r}",
        )
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 112, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the next claim")
    if snapshot["current_attempt"] is None:
        return ScenarioOutcome(scenario_id, "failed", "a second eligible issue must open a claim")
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after the plan")
    if snapshot["intake"]["stop_plan"] is not None:
        return ScenarioOutcome(scenario_id, "failed", "the completed plan must clear from intake")
    if snapshot["intake"]["state"] != "closed":
        return ScenarioOutcome(scenario_id, "failed", "intake must close when the plan completes")
    stored, outcome = _read_command(ctx, scenario_id, plan_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop plan record is missing")
    if stored.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"the plan must complete, got {stored.get('acknowledgement')!r}",
        )
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 112, "eligible": False}
    )
    if outcome is not None:
        return outcome
    resume_id = _new_request_id()
    _, outcome = _submit(ctx, scenario_id, "resume", resume_id)
    if outcome is not None:
        return outcome
    return ScenarioOutcome(scenario_id, "passed")


def scenario_next_issue_restart(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``next issue N`` committed, restart before Claim: priority survives and applies."""
    scenario_id = "next-issue-restart"
    gated = _require_capability(ctx, scenario_id, "next-issue")
    if gated is not None:
        return gated
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "hold", {"point": "before-claim", "scope": "next"}
    )
    if outcome is not None:
        return outcome
    for issue in (121, 122):
        _, outcome = _harness(
            ctx, harness_cmd, scenario_id, "set-issue", {"issue": issue, "eligible": True}
        )
        if outcome is not None:
            return outcome
    priority_id = _new_request_id()
    prioritized = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "next",
            "issue",
            "122",
            "--request-id",
            priority_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(prioritized, ScenarioOutcome):
        return prioritized
    problems = _check_mutation(prioritized.stdout, prioritized.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    record = json.loads(prioritized.stdout)["result"]["command"]
    if record.get("kind") != "next_issue" or record.get("acknowledgement") != "accepted":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"next issue must be accepted while held, got: {prioritized.stdout.strip()}",
        )
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status while held")
    priority = snapshot["intake"]["priority"]
    if (
        not isinstance(priority, dict)
        or priority.get("request_id") != priority_id
        or priority.get("issue") != 122
    ):
        return ScenarioOutcome(
            scenario_id, "failed", f"the priority must wait while held, got {priority!r}"
        )
    if snapshot["current_attempt"] is not None:
        return ScenarioOutcome(scenario_id, "failed", "no claim may open while held")
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "stop-container", {})
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after restart")
    if snapshot["instance"]["state"] != "starting":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected starting after restart, got {snapshot['instance']['state']!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after startup")
    current = snapshot["current_attempt"]
    if not isinstance(current, dict) or current.get("issue", {}).get("number") != 122:
        return ScenarioOutcome(
            scenario_id, "failed", "the priority must still apply after the restart"
        )
    if snapshot["intake"]["priority"] is not None:
        return ScenarioOutcome(scenario_id, "failed", "a consumed priority must clear from intake")
    stored, outcome = _read_command(ctx, scenario_id, priority_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "priority record is missing")
    if stored.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"the applied priority must complete, got {stored.get('acknowledgement')!r}",
        )
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None:
        return outcome
    for issue in (121, 122):
        _, outcome = _harness(
            ctx, harness_cmd, scenario_id, "set-issue", {"issue": issue, "eligible": False}
        )
        if outcome is not None:
            return outcome
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "hold", {"point": "before-claim", "scope": "next"}
    )
    if outcome is not None:
        return outcome
    for issue in (123, 124):
        _, outcome = _harness(
            ctx, harness_cmd, scenario_id, "set-issue", {"issue": issue, "eligible": True}
        )
        if outcome is not None:
            return outcome
    stale_id = _new_request_id()
    stale = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "next",
            "issue",
            "124",
            "--request-id",
            stale_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(stale, ScenarioOutcome):
        return stale
    problems = _check_mutation(stale.stdout, stale.exit_code, replayed=False)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 124, "eligible": False}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    current = snapshot["current_attempt"]
    if not isinstance(current, dict) or current.get("issue", {}).get("number") != 123:
        return ScenarioOutcome(
            scenario_id, "failed", "normal selection must proceed past an ineligible priority"
        )
    stored, outcome = _read_command(ctx, scenario_id, stale_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "priority record is missing")
    if stored.get("acknowledgement") != "not_fulfilled":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"an ineligible priority must be not_fulfilled, got {stored.get('acknowledgement')!r}",
        )
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None:
        return outcome
    for issue in (123, 124):
        _, outcome = _harness(
            ctx, harness_cmd, scenario_id, "set-issue", {"issue": issue, "eligible": False}
        )
        if outcome is not None:
            return outcome
    return ScenarioOutcome(scenario_id, "passed")


def scenario_halt_records_last_halt(
    ctx: SuiteContext, harness_cmd: Sequence[str]
) -> ScenarioOutcome:
    """Halt: a ``halted`` event is recorded and the next start reports it."""
    scenario_id = "halt-records-last-halt"
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 105, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "halt", {"reason": "stub-halt-test"})
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after restart")
    halt = snapshot["instance"]["last_halt"]
    if not isinstance(halt, dict) or halt.get("reason") != "stub-halt-test":
        return ScenarioOutcome(
            scenario_id, "failed", f"the next start must report last_halt, got {halt!r}"
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after startup")
    halt = snapshot["instance"]["last_halt"]
    if not isinstance(halt, dict) or halt.get("reason") != "stub-halt-test":
        return ScenarioOutcome(scenario_id, "failed", "last_halt must survive startup")
    halted = [
        event
        for event in snapshot["recent_events"]
        if isinstance(event, dict) and event.get("event") == "halted"
    ]
    if not halted or halted[-1].get("reason") != "stub-halt-test":
        return ScenarioOutcome(scenario_id, "failed", "a halted event must be recorded")
    if ctx.image:
        _, outcome = _harness(ctx, harness_cmd, scenario_id, "stop-container", {})
        if outcome is not None:
            return outcome
        answered = _exec_offline(ctx, scenario_id, ["--contract", "1", "--offline", "status"])
        if isinstance(answered, ScenarioOutcome):
            _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
            if restart_outcome is not None:
                return restart_outcome
            return answered
        problems = _check_offline_status(answered.stdout, answered.exit_code, ctx)
        if problems:
            _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
            if restart_outcome is not None:
                return restart_outcome
            return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
        try:
            offline_snapshot = json.loads(answered.stdout)["result"]
        except ValueError:
            _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
            if restart_outcome is not None:
                return restart_outcome
            return ScenarioOutcome(scenario_id, "failed", "offline status is not valid JSON")
        offline_halt = offline_snapshot["instance"]["last_halt"]
        if not isinstance(offline_halt, dict) or offline_halt.get("reason") != "stub-halt-test":
            _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
            if restart_outcome is not None:
                return restart_outcome
            return ScenarioOutcome(
                scenario_id, "failed", f"offline read must report last_halt, got {offline_halt!r}"
            )
        _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if outcome is not None:
            return outcome
        _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
        if outcome is not None:
            return outcome
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 105, "eligible": False}
    )
    if outcome is not None:
        return outcome
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    return ScenarioOutcome(scenario_id, "passed")


def _exec_offline(
    ctx: SuiteContext, scenario_id: str, agentctl_argv: list[str]
) -> DockerResult | ScenarioOutcome:
    """Run one Offline Read through ``docker run``; transport failure is an outcome."""
    if not ctx.image:
        return ScenarioOutcome(scenario_id, "failed", "no image for the offline read")
    try:
        return _docker_run(ctx, agentctl_argv)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return ScenarioOutcome(scenario_id, "failed", f"docker run failed: {exc}")


def _offline_unsupported_checks(
    ctx: SuiteContext, scenario_id: str
) -> tuple[ScenarioOutcome | None, list[str]]:
    """Check every mutation answers ``unsupported`` offline; return their IDs.

    Storage is checked after the restart (``docker exec`` cannot reach a
    stopped container), so the caller verifies each returned request ID is
    still ``not_found`` online.
    """
    mutations: list[list[str]] = [
        ["stop"],
        ["resume"],
        ["stop", "--after", "2"],
        ["next", "issue", "7"],
        ["handoff", "now"],
        ["recovery", "retry", "attempt-1"],
        ["recovery", "release", "attempt-1", "--saved-at", "somewhere"],
        ["errors", "reset"],
    ]
    request_ids: list[str] = []
    for operation in mutations:
        request_id = _new_request_id()
        argv = ["--contract", "1", "--offline", *operation]
        argv += ["--request-id", request_id, "--expect-repo", ctx.repository]
        answered = _exec_offline(ctx, scenario_id, argv)
        if isinstance(answered, ScenarioOutcome):
            return answered, []
        problems = _check_error(answered.stdout, answered.exit_code, "unsupported")
        if problems:
            return ScenarioOutcome(scenario_id, "failed", "; ".join(problems)), []
        try:
            doc = json.loads(answered.stdout)
        except ValueError:
            return ScenarioOutcome(scenario_id, "failed", "unsupported is not JSON"), []
        details = doc.get("error", {}).get("details")
        if details != {"mode": "offline"}:
            return (
                ScenarioOutcome(
                    scenario_id,
                    "failed",
                    f"offline details must be mode offline, got {details!r}",
                ),
                [],
            )
        if doc.get("error", {}).get("request_id") != request_id:
            return (
                ScenarioOutcome(
                    scenario_id, "failed", "offline unsupported must echo the request ID"
                ),
                [],
            )
        request_ids.append(request_id)
    return None, request_ids


def scenario_offline_reads(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """Offline Read of a stopped instance serves every read and refuses mutations.

    The container is stopped, then every read of section 6 (``identity``,
    ``status``, ``command``, ``attempts list|show|export``) is served offline
    from the volume with ``instance.state`` stopped; mutations offline answer
    ``unsupported`` and store nothing.
    """
    scenario_id = "offline-reads"
    if not ctx.image:
        return ScenarioOutcome(scenario_id, "not_run", "harness setup did not report an image")
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 115, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    current = snapshot.get("current_attempt")
    if not isinstance(current, dict) or not isinstance(current.get("attempt_id"), str):
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    attempt_id = str(current["attempt_id"])
    stop_id = _new_request_id()
    record, outcome = _submit(ctx, scenario_id, "stop", stop_id)
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop committed nothing")
    if record.get("acknowledgement") != "accepted":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"stop with an active attempt must be accepted, got {record.get('acknowledgement')!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "stop-container", {})
    if outcome is not None:
        return outcome
    # Every read offline, from the stopped volume.
    answered = _exec_offline(ctx, scenario_id, ["--contract", "1", "--offline", "identity"])
    if isinstance(answered, ScenarioOutcome):
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return answered
    problems = _check_offline_identity(answered.stdout, answered.exit_code, ctx)
    if problems:
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    answered = _exec_offline(ctx, scenario_id, ["--contract", "1", "--offline", "status"])
    if isinstance(answered, ScenarioOutcome):
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return answered
    problems = _check_offline_status(answered.stdout, answered.exit_code, ctx)
    if problems:
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    answered = _exec_offline(ctx, scenario_id, ["--contract", "1", "--offline", "command", stop_id])
    if isinstance(answered, ScenarioOutcome):
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return answered
    problems = _check_command(answered.stdout, answered.exit_code, request_id=stop_id, kind="stop")
    if problems:
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    listed = _exec_offline(ctx, scenario_id, ["--contract", "1", "--offline", "attempts", "list"])
    if isinstance(listed, ScenarioOutcome):
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return listed
    problems = _check_attempts_list_result(listed.stdout, listed.exit_code)
    if problems:
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    shown = _exec_offline(
        ctx, scenario_id, ["--contract", "1", "--offline", "attempts", "show", attempt_id]
    )
    if isinstance(shown, ScenarioOutcome):
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return shown
    problems = _check_attempts_show_result(shown.stdout, shown.exit_code, attempt_id)
    if problems:
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    data, outcome = _drain_export_offline(ctx, scenario_id, attempt_id)
    if outcome is not None or data is None:
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return outcome or ScenarioOutcome(scenario_id, "failed", "no export stream")
    problems = _check_export_stream(data, attempt_id)
    if problems:
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    unsupported, offline_ids = _offline_unsupported_checks(ctx, scenario_id)
    if unsupported is not None:
        _, restart_outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
        if restart_outcome is not None:
            return restart_outcome
        return unsupported
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    for offline_id in offline_ids:
        stored_check = _exec(scenario_id, ["--contract", "1", "command", offline_id], ctx)
        if isinstance(stored_check, ScenarioOutcome):
            return stored_check
        nothing = _check_error(stored_check.stdout, stored_check.exit_code, "not_found")
        if nothing:
            return ScenarioOutcome(scenario_id, "failed", f"offline mutation stored {offline_id}")
    stored, outcome = _read_command(ctx, scenario_id, stop_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "stop record is missing")
    resume_id = _new_request_id()
    _, outcome = _submit(ctx, scenario_id, "resume", resume_id)
    if outcome is not None:
        return outcome
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 115, "eligible": False}
    )
    if outcome is not None:
        return outcome
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    return ScenarioOutcome(scenario_id, "passed")


def scenario_offline_while_running(
    ctx: SuiteContext,
    harness_cmd: Sequence[str],
    *,
    duration_ms: int = 5000,
    settle_s: float = 1.0,
) -> ScenarioOutcome:
    """Offline Read while the container runs is refused with ``instance_running``.

    The harness holds the engine lock while an offline ``identity`` (and
    ``status``) read is attempted; the producer must answer ``rejected`` with
    ``details.reason`` ``instance_running`` instead of serving the volume.
    """
    scenario_id = "offline-while-running"
    if not ctx.image:
        return ScenarioOutcome(scenario_id, "not_run", "harness setup did not report an image")
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    held: list[HarnessResult] = []

    def _hold() -> None:
        held.append(
            _call_harness(harness_cmd, "hold-lock", {"duration_ms": duration_ms}, ctx.timeout)
        )

    holder = threading.Thread(target=_hold, daemon=True)
    holder.start()
    time.sleep(min(settle_s, duration_ms / 1000.0 / 4))
    refused = _exec_offline(ctx, scenario_id, ["--contract", "1", "--offline", "identity"])
    still_held = holder.is_alive()
    holder.join(timeout=max(ctx.timeout, duration_ms / 1000.0 + 5.0))
    if isinstance(refused, ScenarioOutcome):
        return refused
    if not held:
        return ScenarioOutcome(scenario_id, "failed", "hold-lock never answered")
    if held[0].status == "unsupported":
        return ScenarioOutcome(
            scenario_id, "not_run", f"harness verb is not supported ({held[0].detail})"
        )
    if held[0].status != "ok":
        return ScenarioOutcome(scenario_id, "failed", f"harness hold-lock failed: {held[0].detail}")
    problems = _check_error(refused.stdout, refused.exit_code, "rejected")
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    try:
        doc = json.loads(refused.stdout)
    except ValueError:
        return ScenarioOutcome(scenario_id, "failed", "rejected is not JSON")
    if doc.get("error", {}).get("details", {}).get("reason") != "instance_running":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"details.reason must be instance_running: {refused.stdout.strip()}",
        )
    if not still_held:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            "hold-lock finished before the offline read; the lock was not held during the read",
        )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_publication_pending_recovery(
    ctx: SuiteContext, harness_cmd: Sequence[str]
) -> ScenarioOutcome:
    """A pending publication recovers before new Claims; the verdict sticks."""
    scenario_id = "publication-pending-recovery"
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 106, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    if snapshot["current_attempt"] is None:
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    attempt_id = str(snapshot["current_attempt"]["attempt_id"])
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "defer-publication", {})
    if outcome is not None:
        return outcome
    finished, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None or finished is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "finish-attempt recorded nothing")
    if finished.get("attempt_id") != attempt_id:
        return ScenarioOutcome(
            scenario_id, "failed", "finish-attempt must finish the active attempt"
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "stop-container", {})
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after restart")
    if snapshot["instance"]["state"] != "starting":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected starting after restart, got {snapshot['instance']['state']!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status while recovering")
    if snapshot["instance"]["state"] != "recovering":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected recovering until recovery finishes, got {snapshot['instance']['state']!r}",
        )
    current = snapshot["current_attempt"]
    if not isinstance(current, dict) or current.get("attempt_id") != attempt_id:
        return ScenarioOutcome(
            scenario_id, "failed", "recovery must finish the pending attempt before new claims"
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after recovery")
    finished_doc = snapshot["last_finished"]
    if not isinstance(finished_doc, dict) or finished_doc.get("attempt_id") != attempt_id:
        return ScenarioOutcome(scenario_id, "failed", "recovery must finalize the attempt")
    outcome_doc = finished_doc.get("outcome")
    if not isinstance(outcome_doc, dict) or outcome_doc.get("native") != "complete":
        return ScenarioOutcome(
            scenario_id, "failed", f"no command may change the recorded verdict: {outcome_doc!r}"
        )
    stop_id = _new_request_id()
    _, outcome = _submit(ctx, scenario_id, "stop", stop_id)
    if outcome is not None:
        return outcome
    resume_id = _new_request_id()
    _, outcome = _submit(ctx, scenario_id, "resume", resume_id)
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after commands")
    finished_doc = snapshot["last_finished"]
    if not isinstance(finished_doc, dict):
        return ScenarioOutcome(scenario_id, "failed", "the finished attempt is missing")
    outcome_doc = finished_doc.get("outcome")
    if not isinstance(outcome_doc, dict) or outcome_doc.get("native") != "complete":
        return ScenarioOutcome(scenario_id, "failed", "commands must not change a recorded verdict")
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 106, "eligible": False}
    )
    if outcome is not None:
        return outcome
    return ScenarioOutcome(scenario_id, "passed")


def _submit_operation(
    ctx: SuiteContext, scenario_id: str, argv: list[str]
) -> tuple[dict[str, Any] | None, ScenarioOutcome | None]:
    """Commit one arbitrary mutation and validate the mutation reply."""
    answered = _exec(scenario_id, argv, ctx)
    if isinstance(answered, ScenarioOutcome):
        return None, answered
    problems = _check_mutation(answered.stdout, answered.exit_code, replayed=False)
    if problems:
        return None, ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    return json.loads(answered.stdout)["result"]["command"], None


def _expect_rejection(
    ctx: SuiteContext, scenario_id: str, argv: list[str], code: str, request_id: str
) -> ScenarioOutcome | None:
    """Submit a mutation that must fail with ``code`` and store nothing."""
    answered = _exec(scenario_id, argv, ctx)
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_error_request(answered.stdout, answered.exit_code, code, request_id)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    stored = _exec(scenario_id, ["--contract", "1", "command", request_id], ctx)
    if isinstance(stored, ScenarioOutcome):
        return stored
    nothing = _check_error(stored.stdout, stored.exit_code, "not_found")
    if nothing:
        return ScenarioOutcome(scenario_id, "failed", f"rejected call stored {request_id}")
    return None


def scenario_handoff(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``handoff now`` completes the active Attempt and seals it as handoff."""
    scenario_id = "handoff"
    gated = _require_capability(ctx, scenario_id, "operator-handoff")
    if gated is not None:
        return gated
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    idle_id = _new_request_id()
    idle = _expect_rejection(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "handoff",
            "now",
            "--request-id",
            idle_id,
            "--expect-repo",
            ctx.repository,
        ],
        "rejected",
        idle_id,
    )
    if idle is not None:
        return idle
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 201, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    current = snapshot.get("current_attempt")
    if not isinstance(current, dict) or not isinstance(current.get("attempt_id"), str):
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    attempt_id = str(current["attempt_id"])
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 201, "eligible": False}
    )
    if outcome is not None:
        return outcome
    request_id = _new_request_id()
    record, outcome = _submit_operation(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "handoff",
            "now",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
    )
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "handoff committed nothing")
    if record.get("kind") != "handoff" or record.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"handoff must complete at once, got {record.get('acknowledgement')!r}",
        )
    if record.get("arguments") != {}:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"handoff arguments must be empty, got {record.get('arguments')!r}",
        )
    replayed = _exec(
        scenario_id,
        [
            "--contract",
            "1",
            "handoff",
            "now",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(replayed, ScenarioOutcome):
        return replayed
    problems = _check_mutation(replayed.stdout, replayed.exit_code, replayed=True)
    if problems:
        return ScenarioOutcome(scenario_id, "failed", "; ".join(problems))
    if json.loads(replayed.stdout)["result"]["command"] != record:
        return ScenarioOutcome(scenario_id, "failed", "the replay changed the stored record")
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after handoff")
    if snapshot["current_attempt"] is not None:
        return ScenarioOutcome(scenario_id, "failed", "the handoff must clear the active attempt")
    finished = snapshot["last_finished"]
    if not isinstance(finished, dict) or finished.get("attempt_id") != attempt_id:
        return ScenarioOutcome(
            scenario_id, "failed", f"attempt {attempt_id} must reach its final lifecycle state"
        )
    outcome_doc = finished.get("outcome")
    if not isinstance(outcome_doc, dict) or outcome_doc.get("native") != "handoff":
        return ScenarioOutcome(
            scenario_id, "failed", f"the handoff verdict must be handoff: {outcome_doc!r}"
        )
    if finished.get("sealed") is not True:
        return ScenarioOutcome(scenario_id, "failed", "the handoff must seal its attempt")
    stored, outcome = _read_command(ctx, scenario_id, request_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "handoff record is missing")
    if stored.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"handoff must stay completed, got {stored.get('acknowledgement')!r}",
        )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_handoff_crash(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``handoff now`` in progress, crash: ``not_fulfilled``, partial work retained."""
    scenario_id = "handoff-crash"
    gated = _require_capability(ctx, scenario_id, "operator-handoff")
    if gated is not None:
        return gated
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 202, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    current = snapshot.get("current_attempt")
    if not isinstance(current, dict) or not isinstance(current.get("attempt_id"), str):
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    attempt_id = str(current["attempt_id"])
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "crash", {"point": "during-finalization"})
    if outcome is not None:
        return outcome
    request_id = _new_request_id()
    record, outcome = _submit_operation(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "handoff",
            "now",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
    )
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "handoff committed nothing")
    if record.get("kind") != "handoff" or record.get("acknowledgement") != "not_fulfilled":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"an interrupted handoff must be not_fulfilled, got {record.get('acknowledgement')!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "stop-container", {})
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "start-container", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after restart")
    if snapshot["instance"]["state"] != "starting":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected starting after restart, got {snapshot['instance']['state']!r}",
        )
    stored, outcome = _read_command(ctx, scenario_id, request_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "handoff record is missing")
    if stored.get("acknowledgement") != "not_fulfilled":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"handoff must stay not_fulfilled, got {stored.get('acknowledgement')!r}",
        )
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status while recovering")
    if snapshot["instance"]["state"] != "recovering":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"expected recovering after startup, got {snapshot['instance']['state']!r}",
        )
    current = snapshot.get("current_attempt")
    if not isinstance(current, dict) or current.get("attempt_id") != attempt_id:
        return ScenarioOutcome(
            scenario_id, "failed", "recovery must retain the interrupted attempt"
        )
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 202, "eligible": False}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after recovery")
    finished = snapshot["last_finished"]
    if not isinstance(finished, dict) or finished.get("attempt_id") != attempt_id:
        return ScenarioOutcome(scenario_id, "failed", "recovery must finalize the attempt")
    stored, outcome = _read_command(ctx, scenario_id, request_id)
    if outcome is not None or stored is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "handoff record is missing")
    if stored.get("acknowledgement") != "not_fulfilled":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"recovery must not complete the handoff, got {stored.get('acknowledgement')!r}",
        )
    return ScenarioOutcome(scenario_id, "passed")


def scenario_recovery_retry(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``recovery retry`` retries safe finalization without touching the verdict."""
    scenario_id = "recovery-retry"
    gated = _require_capability(ctx, scenario_id, "recovery-retry")
    if gated is not None:
        return gated
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 203, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    current = snapshot.get("current_attempt")
    if not isinstance(current, dict) or not isinstance(current.get("attempt_id"), str):
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    attempt_id = str(current["attempt_id"])
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 203, "eligible": False}
    )
    if outcome is not None:
        return outcome
    request_id = _new_request_id()
    record, outcome = _submit_operation(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "recovery",
            "retry",
            attempt_id,
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
    )
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "recovery retry committed nothing")
    if record.get("kind") != "recovery_retry" or record.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"recovery retry must complete, got {record.get('acknowledgement')!r}",
        )
    if record.get("arguments") != {"attempt_id": attempt_id}:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"recovery retry arguments must name the attempt, got {record.get('arguments')!r}",
        )
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after the retry")
    finished = snapshot["last_finished"]
    if not isinstance(finished, dict) or finished.get("attempt_id") != attempt_id:
        return ScenarioOutcome(scenario_id, "failed", "the retry must keep the finished attempt")
    outcome_doc = finished.get("outcome")
    if not isinstance(outcome_doc, dict) or outcome_doc.get("native") != "complete":
        return ScenarioOutcome(
            scenario_id, "failed", f"no command may change the recorded verdict: {outcome_doc!r}"
        )
    unknown_id = _new_request_id()
    unknown = _expect_rejection(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "recovery",
            "retry",
            "no-such-attempt",
            "--request-id",
            unknown_id,
            "--expect-repo",
            ctx.repository,
        ],
        "not_found",
        unknown_id,
    )
    if unknown is not None:
        return unknown
    return ScenarioOutcome(scenario_id, "passed")


def scenario_recovery_release(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``recovery release`` records ``--saved-at``, releases and finalizes."""
    scenario_id = "recovery-release"
    gated = _require_capability(ctx, scenario_id, "recovery-release")
    if gated is not None:
        return gated
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 204, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    current = snapshot.get("current_attempt")
    if not isinstance(current, dict) or not isinstance(current.get("attempt_id"), str):
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    active_id = str(current["attempt_id"])
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 204, "eligible": False}
    )
    if outcome is not None:
        return outcome
    request_id = _new_request_id()
    record, outcome = _submit_operation(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "recovery",
            "release",
            active_id,
            "--saved-at",
            "shelf-204",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
    )
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(
            scenario_id, "failed", "recovery release committed nothing"
        )
    if record.get("kind") != "recovery_release" or record.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"recovery release must complete, got {record.get('acknowledgement')!r}",
        )
    if record.get("arguments") != {"attempt_id": active_id, "saved_at": "shelf-204"}:
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"recovery release arguments must echo the attempt and location, "
            f"got {record.get('arguments')!r}",
        )
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after the release")
    if snapshot["current_attempt"] is not None:
        return ScenarioOutcome(scenario_id, "failed", "the release must clear the active attempt")
    finished = snapshot["last_finished"]
    if not isinstance(finished, dict) or finished.get("attempt_id") != active_id:
        return ScenarioOutcome(scenario_id, "failed", "the release must finalize the attempt")
    if finished.get("lifecycle_state") != "released":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"the released attempt must be released, got {finished.get('lifecycle_state')!r}",
        )
    if finished.get("sealed") is not True:
        return ScenarioOutcome(scenario_id, "failed", "the release must seal its attempt")
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 205, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the next claim")
    if snapshot.get("current_attempt") is None:
        return ScenarioOutcome(scenario_id, "failed", "a second eligible issue must open a claim")
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "finish-attempt", {"native_outcome": "complete"}
    )
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after finishing")
    finished = snapshot["last_finished"]
    if not isinstance(finished, dict) or not isinstance(finished.get("attempt_id"), str):
        return ScenarioOutcome(scenario_id, "failed", "no finished attempt to release")
    finished_id = str(finished["attempt_id"])
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 205, "eligible": False}
    )
    if outcome is not None:
        return outcome
    second_id = _new_request_id()
    second, outcome = _submit_operation(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "recovery",
            "release",
            finished_id,
            "--saved-at",
            "shelf-205",
            "--request-id",
            second_id,
            "--expect-repo",
            ctx.repository,
        ],
    )
    if outcome is not None or second is None:
        return outcome or ScenarioOutcome(
            scenario_id, "failed", "recovery release committed nothing"
        )
    if second.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"recovery release must complete, got {second.get('acknowledgement')!r}",
        )
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status after the release")
    finished = snapshot["last_finished"]
    if not isinstance(finished, dict) or finished.get("attempt_id") != finished_id:
        return ScenarioOutcome(scenario_id, "failed", "the release must keep the finished attempt")
    if finished.get("lifecycle_state") != "released":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"the released attempt must be released, got {finished.get('lifecycle_state')!r}",
        )
    outcome_doc = finished.get("outcome")
    if not isinstance(outcome_doc, dict) or outcome_doc.get("native") != "complete":
        return ScenarioOutcome(
            scenario_id, "failed", f"no command may change the recorded verdict: {outcome_doc!r}"
        )
    unknown_id = _new_request_id()
    unknown = _expect_rejection(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "recovery",
            "release",
            "no-such-attempt",
            "--saved-at",
            "shelf-999",
            "--request-id",
            unknown_id,
            "--expect-repo",
            ctx.repository,
        ],
        "not_found",
        unknown_id,
    )
    if unknown is not None:
        return unknown
    return ScenarioOutcome(scenario_id, "passed")


def scenario_errors_reset(ctx: SuiteContext, harness_cmd: Sequence[str]) -> ScenarioOutcome:
    """``errors reset`` resets the durable infrastructure-error guard."""
    scenario_id = "errors-reset"
    gated = _require_capability(ctx, scenario_id, "errors-reset")
    if gated is not None:
        return gated
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
    clear_id = _new_request_id()
    clear = _expect_rejection(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "errors",
            "reset",
            "--request-id",
            clear_id,
            "--expect-repo",
            ctx.repository,
        ],
        "rejected",
        clear_id,
    )
    if clear is not None:
        return clear
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 206, "eligible": True}
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(ctx, harness_cmd, scenario_id, "release", {})
    if outcome is not None:
        return outcome
    snapshot, outcome = _snapshot(ctx, scenario_id)
    if outcome is not None or snapshot is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "no status for the claim")
    if snapshot.get("current_attempt") is None:
        return ScenarioOutcome(scenario_id, "failed", "an eligible issue must open a claim")
    _, outcome = _harness(
        ctx,
        harness_cmd,
        scenario_id,
        "finish-attempt",
        {"native_outcome": "infrastructure_error", "reason": "boom"},
    )
    if outcome is not None:
        return outcome
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 206, "eligible": False}
    )
    if outcome is not None:
        return outcome
    request_id = _new_request_id()
    record, outcome = _submit_operation(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "errors",
            "reset",
            "--request-id",
            request_id,
            "--expect-repo",
            ctx.repository,
        ],
    )
    if outcome is not None or record is None:
        return outcome or ScenarioOutcome(scenario_id, "failed", "errors reset committed nothing")
    if record.get("kind") != "errors_reset" or record.get("acknowledgement") != "completed":
        return ScenarioOutcome(
            scenario_id,
            "failed",
            f"errors reset must complete, got {record.get('acknowledgement')!r}",
        )
    spent_id = _new_request_id()
    spent = _expect_rejection(
        ctx,
        scenario_id,
        [
            "--contract",
            "1",
            "errors",
            "reset",
            "--request-id",
            spent_id,
            "--expect-repo",
            ctx.repository,
        ],
        "rejected",
        spent_id,
    )
    if spent is not None:
        return spent
    return ScenarioOutcome(scenario_id, "passed")


SCENARIOS: tuple[Scenario, ...] = (
    Scenario("identity", "online identity matches the harness setup"),
    Scenario("status", "online status validates and matches the harness setup"),
    Scenario("argv-usage", "malformed argv answers usage with exit 2"),
    Scenario("contract-mismatch", "--contract 2 answers contract_mismatch and executes nothing"),
    Scenario("unsupported-capability", "an unadvertised capability answers unsupported"),
    Scenario("command-not-found", "command of an unknown request ID answers not_found"),
    Scenario(
        "repository-mismatch",
        "a wrong --expect-repo on any mutation is rejected and stores nothing",
    ),
    Scenario("stop-replay", "an identical resend replays the stored record"),
    Scenario(
        "payload-mismatch",
        "the same request ID with a different payload is rejected and stores nothing",
    ),
    Scenario("resume", "resume reopens closed intake and a second resume is rejected"),
    Scenario("command-found", "command of a known request ID returns the stored record"),
    Scenario(
        "crash-between-commit-and-reply",
        "a crash between commit and reply replays the stored record on retry",
    ),
    Scenario("hold-lock", "the hold-lock harness verb answers with the time held"),
    Scenario(
        "status-during-lock",
        "status answers within 3 s with prepared state while the engine lock is held",
    ),
    Scenario("budget-reads", "identity, status and command answer within 3 s"),
    Scenario("budget-mutations", "stop and resume commit within 10 s"),
    Scenario("budget-attempts", "attempts list and show answer within 5 s"),
    Scenario("budget-export", "attempts export starts streaming within 5 s"),
    Scenario(
        "attempts-list-paging",
        "attempts list pages newest first with default 20, max 100 and opaque cursor",
    ),
    Scenario("attempts-show", "attempts show returns the archive with its manifest"),
    Scenario("attempts-not-found", "attempts show of an unknown attempt answers not_found"),
    Scenario(
        "contract-events",
        "every recent_events entry and every events.jsonl line is a Contract Event",
    ),
    Scenario(
        "snapshot-bounds",
        "the snapshot fits 256 KiB with oldest-first truncation and sorted order",
    ),
    Scenario(
        "stop-before-claim-crash",
        "stop before the claim boundary, then a crash: intake stays closed",
    ),
    Scenario(
        "stop-after-claim-sigterm",
        "stop after the claim boundary, then SIGTERM: the attempt finishes",
    ),
    Scenario(
        "stop-during-starting",
        "stop during starting: the first claim never opens",
    ),
    Scenario(
        "resume-supersedes-stop",
        "resume while a stop is accepted: the stop is superseded",
    ),
    Scenario(
        "stop-after-invalid",
        "stop --after 0 is invalid_argument; nothing stored, ID reusable",
    ),
    Scenario(
        "stop-after-restart",
        "stop --after N in progress, restart: the counter survives and continues",
    ),
    Scenario(
        "next-issue-restart",
        "next issue N committed, restart before Claim: priority survives and applies",
    ),
    Scenario(
        "sealed-after-outcome",
        "outcome recorded, finalization still writing: unsealed, then the sealed event",
    ),
    Scenario(
        "export-torn-read",
        "export of an unsealed archive while a file grows: torn read detected",
    ),
    Scenario(
        "halt-records-last-halt",
        "halt: a halted event is recorded and the next start reports it",
    ),
    Scenario(
        "publication-pending-recovery",
        "a pending publication recovers before new claims; the verdict sticks",
    ),
    Scenario(
        "handoff",
        "handoff now completes the active attempt and seals it as handoff",
    ),
    Scenario(
        "handoff-crash",
        "handoff now in progress, crash: not_fulfilled, partial work retained",
    ),
    Scenario(
        "recovery-retry",
        "recovery retry retries safe finalization without touching the verdict",
    ),
    Scenario(
        "recovery-release",
        "recovery release records --saved-at, releases the issue and finalizes",
    ),
    Scenario(
        "errors-reset",
        "errors reset resets the durable infrastructure-error guard",
    ),
    Scenario(
        "offline-reads",
        "offline read of a stopped instance serves every read and refuses mutations",
    ),
    Scenario(
        "offline-while-running",
        "offline read while the container runs is refused with instance_running",
    ),
)
"""Every scenario of this suite version, with stable identifiers."""


def scenario_ids() -> list[str]:
    """Return the stable identifiers of every scenario."""
    return [scenario.id for scenario in SCENARIOS]


SECTION_14_COVERAGE: dict[str, tuple[str, ...]] = {
    "Mutation commits, the process dies before replying": ("crash-between-commit-and-reply",),
    "Same request ID, different payload": ("payload-mismatch",),
    "`stop` before the Claim boundary, then a crash": ("stop-before-claim-crash",),
    "`stop` after the Claim boundary, then SIGTERM": ("stop-after-claim-sigterm",),
    "`stop` during `starting`": ("stop-during-starting",),
    "`resume` while a stop is `accepted`": ("resume-supersedes-stop",),
    "`next issue N` committed, restart before Claim": ("next-issue-restart",),
    "`stop --after N` in progress, restart": ("stop-after-restart",),
    "`handoff now` in progress, crash": ("handoff-crash",),
    "Unadvertised capability invoked": ("unsupported-capability",),
    "`--contract 2` against a major-1 producer": ("contract-mismatch",),
    "Wrong `--expect-repo` on any mutation": ("repository-mismatch",),
    "Status during a long-held engine lock": ("status-during-lock",),
    "Outcome recorded, finalization still writing": ("sealed-after-outcome",),
    "Export of an unsealed archive while a file grows": ("export-torn-read",),
    "Halt (process exit by engine decision)": ("halt-records-last-halt",),
    "Offline Read while the container is running": ("offline-while-running",),
    "Publication effect pending at restart": ("publication-pending-recovery",),
}
"""Which scenarios cover each row of spec section 14, keyed by the row's first cell.

A coverage gate in this repository's CI pins the keys to the rows of the
spec table, so a new row without a scenario fails the build.
"""

OPERATION_COVERAGE: dict[str, tuple[str, ...]] = {
    "identity": ("identity", "offline-reads"),
    "status": ("status", "status-during-lock", "snapshot-bounds", "offline-reads"),
    "command <request-id>": ("command-found", "command-not-found", "offline-reads"),
    "attempts list [--limit N] [--before <cursor>]": ("attempts-list-paging", "offline-reads"),
    "attempts show <attempt-id>": ("attempts-show", "attempts-not-found", "offline-reads"),
    "attempts export <attempt-id> [--path <path>]...": (
        "budget-export",
        "export-torn-read",
        "offline-reads",
    ),
    "stop": ("stop-replay", "stop-before-claim-crash", "stop-after-claim-sigterm"),
    "stop --after N": ("stop-after-invalid", "stop-after-restart"),
    "resume": ("resume", "resume-supersedes-stop"),
    "next issue N": ("next-issue-restart",),
    "handoff now": ("handoff", "handoff-crash"),
    "recovery retry <attempt-id>": ("recovery-retry",),
    "recovery release <attempt-id> --saved-at <location>": ("recovery-release",),
    "errors reset": ("errors-reset",),
}
"""Which scenarios exercise each operation of spec section 6, keyed by its table cell.

``unsupported-capability`` additionally covers every optional operation whose
capability the producer does not advertise.
"""


def run_all(ctx: SuiteContext, harness_cmd: Sequence[str]) -> list[ScenarioOutcome]:
    """Run every scenario against the instance in ``ctx``."""
    return [
        scenario_identity(ctx),
        scenario_status(ctx),
        scenario_argv_usage(ctx),
        scenario_contract_mismatch(ctx),
        scenario_unsupported_capability(ctx),
        scenario_command_not_found(ctx),
        scenario_repository_mismatch(ctx),
        scenario_stop_replay(ctx),
        scenario_payload_mismatch(ctx),
        scenario_resume(ctx),
        scenario_command_found(ctx),
        scenario_crash_between_commit_and_reply(ctx, harness_cmd),
        scenario_hold_lock(ctx, harness_cmd),
        scenario_status_during_lock(ctx, harness_cmd),
        scenario_budget_reads(ctx),
        scenario_budget_mutations(ctx),
        scenario_budget_attempts(ctx, harness_cmd),
        scenario_budget_export(ctx, harness_cmd),
        scenario_attempts_list_paging(ctx, harness_cmd),
        scenario_attempts_show(ctx, harness_cmd),
        scenario_attempts_not_found(ctx),
        scenario_contract_events(ctx, harness_cmd),
        scenario_snapshot_bounds(ctx),
        scenario_stop_before_claim_crash(ctx, harness_cmd),
        scenario_stop_after_claim_sigterm(ctx, harness_cmd),
        scenario_stop_during_starting(ctx, harness_cmd),
        scenario_resume_supersedes_stop(ctx, harness_cmd),
        scenario_stop_after_invalid(ctx),
        scenario_stop_after_restart(ctx, harness_cmd),
        scenario_next_issue_restart(ctx, harness_cmd),
        scenario_sealed_after_outcome(ctx, harness_cmd),
        scenario_export_torn_read(ctx, harness_cmd),
        scenario_halt_records_last_halt(ctx, harness_cmd),
        scenario_publication_pending_recovery(ctx, harness_cmd),
        scenario_handoff(ctx, harness_cmd),
        scenario_handoff_crash(ctx, harness_cmd),
        scenario_recovery_retry(ctx, harness_cmd),
        scenario_recovery_release(ctx, harness_cmd),
        scenario_errors_reset(ctx, harness_cmd),
        scenario_offline_reads(ctx, harness_cmd),
        scenario_offline_while_running(ctx, harness_cmd),
    ]


def apply_expect_fail(
    outcomes: Sequence[ScenarioOutcome],
    expect_fail: Sequence[str],
    *,
    require_all: bool = False,
) -> tuple[list[str], int]:
    """Render the report lines and the suite exit code.

    A listed scenario that fails becomes an expected failure and does not
    fail the run. A listed scenario that passes, does not run or does not
    exist is reported as unexpected and fails the run. With ``require_all``
    a scenario that does not run fails the run too.
    """
    expected = set(expect_fail)
    titles = {scenario.id: scenario.title for scenario in SCENARIOS}
    lines: list[str] = []
    failed = 0
    unexpected = 0
    for outcome in outcomes:
        scenario = titles.get(outcome.scenario_id, outcome.scenario_id)
        if outcome.status == "passed" and outcome.scenario_id in expected:
            lines.append(f"UNEXPECTED PASS {outcome.scenario_id}: {scenario}")
            unexpected += 1
        elif outcome.status == "failed" and outcome.scenario_id in expected:
            lines.append(f"EXPECTED FAIL {outcome.scenario_id}: {outcome.detail}")
        elif outcome.status == "passed":
            lines.append(f"PASS {outcome.scenario_id}: {scenario}")
        elif outcome.status == "failed":
            lines.append(f"FAIL {outcome.scenario_id}: {outcome.detail}")
            failed += 1
        else:
            reason = f": {outcome.detail}" if outcome.detail else ""
            lines.append(f"NOT RUN {outcome.scenario_id}: {scenario}{reason}")
            if require_all and outcome.scenario_id not in expected:
                failed += 1
            if outcome.scenario_id in expected:
                lines.append(
                    f"UNEXPECTED PASS {outcome.scenario_id}: listed in --expect-fail "
                    "but it did not fail"
                )
                unexpected += 1
    known = {outcome.scenario_id for outcome in outcomes}
    for missing in sorted(expected - known):
        lines.append(f"UNEXPECTED: --expect-fail {missing!r} matched no scenario")
        unexpected += 1
    exit_code = 0 if failed == 0 and unexpected == 0 else 1
    return lines, exit_code


def build_parser() -> argparse.ArgumentParser:
    """Build the suite command line."""
    parser = argparse.ArgumentParser(
        prog="producer-suite",
        description="Black-box producer scenarios over docker exec and the harness.",
    )
    parser.add_argument("--harness", required=True, help="harness executable")
    parser.add_argument("--docker", default="docker", help="docker executable")
    parser.add_argument(
        "--repository", default=DEFAULT_REPOSITORY, help="Target Repository for setup"
    )
    parser.add_argument(
        "--capabilities",
        default=",".join(DEFAULT_CAPABILITIES),
        help="comma-separated capabilities to request at setup",
    )
    parser.add_argument(
        "--expect-fail",
        action="append",
        default=[],
        help="scenario ID expected to fail (repeatable, comma-separated)",
    )
    parser.add_argument(
        "--require-all",
        action="store_true",
        help="fail the run when any scenario does not run",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="per-call timeout")
    return parser


def _parse_expect_fail(values: Sequence[str]) -> list[str]:
    wanted: list[str] = []
    for value in values:
        wanted.extend(piece.strip() for piece in value.split(",") if piece.strip())
    return wanted


def main(argv: Sequence[str] | None = None) -> int:
    """Set up, run every scenario, tear down and report."""
    args = build_parser().parse_args(argv)
    harness_cmd = [args.harness]
    capabilities = [piece.strip() for piece in args.capabilities.split(",") if piece.strip()]

    setup = _call_harness(
        harness_cmd,
        "setup",
        {"repository": args.repository, "capabilities": capabilities},
        args.timeout,
    )
    if setup.status != "ok" or setup.result is None:
        print(f"FAIL setup: {setup.detail}")
        return 1
    raw_volumes = setup.result.get("volumes", ())
    volumes = tuple(
        {"name": str(item.get("name", "")), "path": str(item.get("path", ""))}
        for item in raw_volumes
        if isinstance(item, dict)
    )
    ctx = SuiteContext(
        container=str(setup.result.get("container", "")),
        exec_user=str(setup.result.get("exec_user", "")),
        repository=str(setup.result.get("repository", args.repository)),
        capabilities=list(setup.result.get("capabilities", capabilities)),
        docker=args.docker,
        timeout=args.timeout,
        image=str(setup.result.get("image", "")),
        volumes=volumes,
    )
    try:
        outcomes = run_all(ctx, harness_cmd)
    finally:
        teardown = _call_harness(harness_cmd, "teardown", {}, args.timeout)
    lines, exit_code = apply_expect_fail(
        outcomes, _parse_expect_fail(args.expect_fail), require_all=args.require_all
    )
    for line in lines:
        print(line)
    if teardown.status != "ok":
        print(f"FAIL teardown: {teardown.detail}")
        return 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
