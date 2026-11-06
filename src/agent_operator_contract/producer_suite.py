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
import json
import secrets
import subprocess
import sys
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
    if doc.get("contract", "").split(".")[0] != str(CONTRACT_MAJOR):
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
    """The ``hold-lock`` harness verb answers with the time held."""
    result = _call_harness(harness_cmd, "hold-lock", {"duration_ms": 100}, ctx.timeout)
    if result.status == "unsupported":
        return ScenarioOutcome(
            "hold-lock", "not_run", f"harness verb is not supported ({result.detail})"
        )
    if result.status != "ok" or result.result is None:
        return ScenarioOutcome("hold-lock", "failed", result.detail)
    held_ms = result.result.get("held_ms")
    if not isinstance(held_ms, int) or held_ms < 1:
        return ScenarioOutcome("hold-lock", "failed", f"invalid held_ms: {held_ms!r}")
    return ScenarioOutcome("hold-lock", "passed")


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
