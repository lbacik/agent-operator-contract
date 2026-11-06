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


SCENARIOS: tuple[Scenario, ...] = (
    Scenario("identity", "online identity matches the harness setup"),
    Scenario("status", "online status validates and matches the harness setup"),
    Scenario("argv-usage", "malformed argv answers usage with exit 2"),
    Scenario("contract-mismatch", "--contract 2 answers contract_mismatch and executes nothing"),
    Scenario("unsupported-capability", "an unadvertised capability answers unsupported"),
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
