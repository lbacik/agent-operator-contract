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
that passes (or is missing) is reported and fails the run.

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
from typing import Any, Literal

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
    """Start ``agentctl attempts export`` streaming; the caller times first bytes."""
    return subprocess.Popen(
        [docker, "exec", "--user", exec_user, container, AGENTCTL, *agentctl_argv],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
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


def scenario_unsupported_capability(ctx: SuiteContext) -> ScenarioOutcome:
    """An unadvertised capability answers ``unsupported`` with exit 3."""
    answered = _exec(
        "unsupported-capability",
        [
            "--contract",
            "1",
            "handoff",
            "now",
            "--request-id",
            _new_request_id(),
            "--expect-repo",
            ctx.repository,
        ],
        ctx,
    )
    if isinstance(answered, ScenarioOutcome):
        return answered
    problems = _check_error(answered.stdout, answered.exit_code, "unsupported")
    if problems:
        return ScenarioOutcome("unsupported-capability", "failed", "; ".join(problems))
    return ScenarioOutcome("unsupported-capability", "passed")


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
    if any(not isinstance(sequence, int) for sequence in sequences) or sequences != sorted(
        sequences
    ):
        return ScenarioOutcome(
            scenario_id, "failed", "recent_commands are not oldest-first (newest last)"
        )
    stamped = [event.get("at") for event in recent_events if isinstance(event, dict)]
    if any(not isinstance(at, str) for at in stamped) or stamped != sorted(stamped):
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
    mutation gets no reply, and the retry with the same request ID returns
    the stored record with ``replayed: true`` while the command store shows
    the single committed effect.
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
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 103, "eligible": True}
    )
    if outcome is not None:
        return outcome
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
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 104, "eligible": False}
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
    _, outcome = _harness(
        ctx, harness_cmd, scenario_id, "set-issue", {"issue": 105, "eligible": False}
    )
    if outcome is not None:
        return outcome
    settled = _settle(ctx, harness_cmd, scenario_id)
    if settled is not None:
        return settled
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
        "halt-records-last-halt",
        "halt: a halted event is recorded and the next start reports it",
    ),
    Scenario(
        "publication-pending-recovery",
        "a pending publication recovers before new claims; the verdict sticks",
    ),
)
"""Every scenario of this suite version, with stable identifiers."""


def scenario_ids() -> list[str]:
    """Return the stable identifiers of every scenario."""
    return [scenario.id for scenario in SCENARIOS]


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
        scenario_snapshot_bounds(ctx),
        scenario_stop_before_claim_crash(ctx, harness_cmd),
        scenario_stop_after_claim_sigterm(ctx, harness_cmd),
        scenario_stop_during_starting(ctx, harness_cmd),
        scenario_resume_supersedes_stop(ctx, harness_cmd),
        scenario_halt_records_last_halt(ctx, harness_cmd),
        scenario_publication_pending_recovery(ctx, harness_cmd),
    ]


def apply_expect_fail(
    outcomes: Sequence[ScenarioOutcome], expect_fail: Sequence[str]
) -> tuple[list[str], int]:
    """Render the report lines and the suite exit code.

    A listed scenario that fails becomes an expected failure and does not
    fail the run. A listed scenario that passes, does not run or does not
    exist is reported as unexpected and fails the run.
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
    ctx = SuiteContext(
        container=str(setup.result.get("container", "")),
        exec_user=str(setup.result.get("exec_user", "")),
        repository=str(setup.result.get("repository", args.repository)),
        capabilities=list(setup.result.get("capabilities", capabilities)),
        docker=args.docker,
        timeout=args.timeout,
    )
    try:
        outcomes = run_all(ctx, harness_cmd)
    finally:
        teardown = _call_harness(harness_cmd, "teardown", {}, args.timeout)
    lines, exit_code = apply_expect_fail(outcomes, _parse_expect_fail(args.expect_fail))
    for line in lines:
        print(line)
    if teardown.status != "ok":
        print(f"FAIL teardown: {teardown.detail}")
        return 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
