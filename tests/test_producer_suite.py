"""Unit tests for the producer suite runner (no Docker, no engine).

The docker and harness seams are faked through monkeypatching, so these
tests cover the suite's own logic: scenario checks, the not-run path for an
unsupported harness verb, and the expected-failures list.
"""

import json
from typing import Any

import pytest

from agent_operator_contract import producer_suite, shared

CTX = producer_suite.SuiteContext(
    container="stub-abc123",
    exec_user="agent",
    repository="example-owner/example-repo",
    capabilities=["next-issue", "stop-after"],
)

IDENTITY = {
    "backend": "stub",
    "agent_version": "0.0.0",
    "contract": {"major": 1, "minor": 0},
    "repository": "example-owner/example-repo",
    "mode": "online",
    "process_started_at": "2026-11-06T08:00:00Z",
    "capabilities": ["next-issue", "stop-after"],
}


def _ok(result: dict[str, Any]) -> str:
    return shared.render_success(result)


def _fail(code: str, details: Any = None) -> str:
    doc: dict[str, Any] = {
        "contract": "1.0",
        "ok": False,
        "error": {"code": code, "message": "stub", "retryable": False},
    }
    if details is not None:
        doc["error"]["details"] = details
    return shared.render_document(doc)


def _fake_docker(
    docker: str, container: str, exec_user: str, argv: list[str], timeout: float
) -> producer_suite.DockerResult:
    if argv == ["--contract", "1", "identity"]:
        return producer_suite.DockerResult(0, _ok(IDENTITY), "")
    if argv == ["--contract", "1", "stop", "--after"]:
        return producer_suite.DockerResult(2, _fail("usage"), "")
    if argv == ["--contract", "2", "status"]:
        return producer_suite.DockerResult(4, _fail("contract_mismatch", {"supported": [1]}), "")
    if argv[:3] == ["--contract", "1", "handoff"]:
        return producer_suite.DockerResult(
            3, _fail("unsupported", {"capability": "operator-handoff"}), ""
        )
    raise AssertionError(f"unexpected agentctl argv: {argv!r}")


def _fake_harness(
    cmd: Any, verb: str, params: dict[str, Any], timeout: float
) -> producer_suite.HarnessResult:
    if verb == "setup":
        return producer_suite.HarnessResult(
            "ok",
            {
                "container": CTX.container,
                "image": "sha256:abc",
                "volumes": [{"name": "stub-data", "path": "/data"}],
                "exec_user": CTX.exec_user,
                "repository": CTX.repository,
                "capabilities": CTX.capabilities,
            },
            "",
        )
    if verb == "teardown":
        return producer_suite.HarnessResult("ok", {"removed": True}, "")
    if verb == "hold-lock":
        return producer_suite.HarnessResult("unsupported", None, "verb:hold-lock")
    raise AssertionError(f"unexpected harness verb: {verb!r}")


@pytest.fixture
def fakes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(producer_suite, "_docker_exec", _fake_docker)
    monkeypatch.setattr(producer_suite, "_call_harness", _fake_harness)


def test_scenario_ids_are_stable() -> None:
    assert producer_suite.scenario_ids() == [
        "identity",
        "argv-usage",
        "contract-mismatch",
        "unsupported-capability",
        "hold-lock",
    ]


def test_run_all_passes_with_hold_lock_not_run(fakes: None) -> None:
    outcomes = producer_suite.run_all(CTX, ["fake-harness"])
    by_id = {outcome.scenario_id: outcome for outcome in outcomes}

    assert by_id["identity"].status == "passed"
    assert by_id["argv-usage"].status == "passed"
    assert by_id["contract-mismatch"].status == "passed"
    assert by_id["unsupported-capability"].status == "passed"
    assert by_id["hold-lock"].status == "not_run"


def test_hold_lock_supported_passes(fakes: None, monkeypatch: pytest.MonkeyPatch) -> None:
    def hold_ok(cmd: Any, verb: str, params: dict[str, Any], timeout: float) -> Any:
        if verb == "hold-lock":
            return producer_suite.HarnessResult("ok", {"point": "x", "held_ms": 100}, "")
        return _fake_harness(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", hold_ok)

    outcome = producer_suite.scenario_hold_lock(CTX, ["fake-harness"])

    assert outcome.status == "passed"


def test_supported_but_broken_producer_fails_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        return producer_suite.DockerResult(0, "not json\n", "")

    monkeypatch.setattr(producer_suite, "_docker_exec", broken)

    assert producer_suite.scenario_identity(CTX).status == "failed"


def test_apply_expect_fail_listing_a_failure_keeps_the_run_green() -> None:
    outcomes = [
        producer_suite.ScenarioOutcome("identity", "passed"),
        producer_suite.ScenarioOutcome("argv-usage", "failed", "boom"),
    ]

    lines, exit_code = producer_suite.apply_expect_fail(outcomes, ["argv-usage"])

    assert exit_code == 0
    assert any(line.startswith("EXPECTED FAIL argv-usage") for line in lines)


def test_apply_expect_fail_listing_a_pass_fails_the_run() -> None:
    outcomes = [producer_suite.ScenarioOutcome("identity", "passed")]

    lines, exit_code = producer_suite.apply_expect_fail(outcomes, ["identity"])

    assert exit_code == 1
    assert any(line.startswith("UNEXPECTED PASS identity") for line in lines)


def test_apply_expect_fail_listing_a_not_run_fails_the_run() -> None:
    outcomes = [producer_suite.ScenarioOutcome("hold-lock", "not_run", "unsupported")]

    lines, exit_code = producer_suite.apply_expect_fail(outcomes, ["hold-lock"])

    assert exit_code == 1
    assert any("hold-lock" in line and "did not fail" in line for line in lines)


def test_apply_expect_fail_with_an_unknown_id_fails_the_run() -> None:
    outcomes = [producer_suite.ScenarioOutcome("identity", "passed")]

    lines, exit_code = producer_suite.apply_expect_fail(outcomes, ["no-such-scenario"])

    assert exit_code == 1
    assert any("no-such-scenario" in line for line in lines)


def test_main_reports_per_scenario_and_exits_zero(
    fakes: None, capsys: pytest.CaptureFixture[str]
) -> None:
    assert producer_suite.main(["--harness", "fake-harness"]) == 0

    out = capsys.readouterr().out
    assert "PASS identity" in out
    assert "PASS argv-usage" in out
    assert "PASS contract-mismatch" in out
    assert "PASS unsupported-capability" in out
    assert "NOT RUN hold-lock" in out


def test_main_with_expect_fail_for_a_not_run_fails(
    fakes: None, capsys: pytest.CaptureFixture[str]
) -> None:
    assert producer_suite.main(["--harness", "fake-harness", "--expect-fail", "hold-lock"]) == 1

    assert "did not fail" in capsys.readouterr().out


def test_main_fails_when_setup_fails(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def setup_failed(cmd: Any, verb: str, params: dict[str, Any], timeout: float) -> Any:
        return producer_suite.HarnessResult("error", None, "no instance today")

    monkeypatch.setattr(producer_suite, "_call_harness", setup_failed)

    assert producer_suite.main(["--harness", "fake-harness"]) == 1
    assert "FAIL setup" in capsys.readouterr().out


def test_every_producer_reply_is_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def record(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        result = _fake_docker(docker, container, exec_user, argv, timeout)
        seen.append(result.stdout)
        return result

    monkeypatch.setattr(producer_suite, "_docker_exec", record)
    monkeypatch.setattr(producer_suite, "_call_harness", _fake_harness)

    producer_suite.run_all(CTX, ["fake-harness"])

    assert seen
    for stdout in seen:
        assert stdout.endswith("\n")
        assert stdout.count("\n") == 1
        assert json.loads(stdout)
