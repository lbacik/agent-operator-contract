"""Unit tests for the producer suite runner (no Docker, no engine).

The docker and harness seams are faked through monkeypatching, so these
tests cover the suite's own logic: scenario checks, the not-run path for an
unsupported harness verb, and the expected-failures list. The docker fake is a
small in-memory producer that mirrors the contract's mutation semantics
(repository guard, idempotency, state) so the mutation scenarios run green.
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


def _fail_with_request(code: str, request_id: str, details: Any = None) -> str:
    doc: dict[str, Any] = {
        "contract": "1.0",
        "ok": False,
        "error": {
            "code": code,
            "message": "stub",
            "retryable": False,
            "request_id": request_id,
        },
    }
    if details is not None:
        doc["error"]["details"] = details
    return shared.render_document(doc)


TIMESTAMP = "2026-11-06T09:12:03Z"


class _FakeProducer:
    """A minimal in-memory producer for the suite's own tests.

    Serves ``identity`` and ``status`` from canned documents and ``stop``,
    ``resume`` and ``command`` with the contract's mutation semantics: the
    ``--expect-repo`` guard, idempotent replay, ``payload_mismatch`` and the
    ``rejected`` state guard. Anything else answers as the fixed seams below.
    """

    def __init__(self, ctx: producer_suite.SuiteContext) -> None:
        self._ctx = ctx
        self.intake = "open"
        self.sequence = 1
        self.commands: dict[str, dict[str, Any]] = {}
        self.crash_armed = False

    def _record(self, request_id: str, kind: str, acknowledgement: str) -> dict[str, Any]:
        record = {
            "request_id": request_id,
            "sequence": self.sequence,
            "kind": kind,
            "arguments": {},
            "repository": self._ctx.repository,
            "acknowledgement": acknowledgement,
            "detail": f"{kind} {acknowledgement}",
            "created_at": TIMESTAMP,
            "updated_at": TIMESTAMP,
        }
        self.commands[request_id] = {"kind": kind, "arguments": {}, "record": record}
        self.sequence += 1
        return record

    def _status_doc(self) -> dict[str, Any]:
        identity = dict(IDENTITY)
        records = sorted(
            (entry["record"] for entry in self.commands.values()),
            key=lambda record: record["sequence"],
        )
        return {
            "generated_at": "2026-11-06T09:12:05Z",
            "identity": identity,
            "instance": {
                "state": "idle" if self.intake == "open" else "paused",
                "startup_check": None,
                "last_halt": None,
                "observed_at": "2026-11-06T09:12:05Z",
            },
            "intake": {
                "state": self.intake,
                "pending_stop": None,
                "stop_plan": None,
                "priority": None,
                "observed_at": "2026-11-06T09:12:05Z",
            },
            "current_attempt": None,
            "last_finished": None,
            "recent_commands": records[-20:],
            "recent_events": [],
            "truncated": False,
        }

    def _mutate(
        self, operation: str, request_id: str, expect_repo: str
    ) -> producer_suite.DockerResult:
        if expect_repo.lower() != self._ctx.repository.lower():
            return producer_suite.DockerResult(
                1,
                _fail_with_request(
                    "repository_mismatch",
                    request_id,
                    {"repository": self._ctx.repository},
                ),
                "",
            )
        stored = self.commands.get(request_id)
        if stored is not None:
            if stored["kind"] == operation and stored["arguments"] == {}:
                return producer_suite.DockerResult(
                    0,
                    _ok({"command": stored["record"], "replayed": True}),
                    "",
                )
            return producer_suite.DockerResult(
                1, _fail_with_request("payload_mismatch", request_id), ""
            )
        if operation == "stop":
            if self.intake == "closed":
                return producer_suite.DockerResult(
                    1, _fail_with_request("rejected", request_id), ""
                )
            self.intake = "closed"
            record = self._record(request_id, "stop", "completed")
        else:
            if self.intake == "open":
                return producer_suite.DockerResult(
                    1, _fail_with_request("rejected", request_id), ""
                )
            self.intake = "open"
            record = self._record(request_id, "resume", "completed")
        return producer_suite.DockerResult(0, _ok({"command": record, "replayed": False}), "")

    def handle(
        self,
        docker: str,
        container: str,
        exec_user: str,
        argv: list[str],
        timeout: float,
    ) -> producer_suite.DockerResult:
        if argv == ["--contract", "1", "identity"]:
            return producer_suite.DockerResult(0, _ok(IDENTITY), "")
        if argv == ["--contract", "1", "status"]:
            return producer_suite.DockerResult(0, _ok(self._status_doc()), "")
        if argv == ["--contract", "1", "stop", "--after"]:
            return producer_suite.DockerResult(2, _fail("usage"), "")
        if argv == ["--contract", "2", "status"]:
            return producer_suite.DockerResult(
                4, _fail("contract_mismatch", {"supported": [1]}), ""
            )
        if argv[:3] == ["--contract", "1", "handoff"]:
            return producer_suite.DockerResult(
                3, _fail("unsupported", {"capability": "operator-handoff"}), ""
            )
        if (
            len(argv) == 7
            and argv[:3] == ["--contract", "1", argv[2]]
            and argv[2] in ("stop", "resume")
            and argv[3] == "--request-id"
            and argv[5] == "--expect-repo"
        ):
            if self.crash_armed:
                # Commit durably, then die before replying like the stub does.
                self.crash_armed = False
                self._mutate(argv[2], argv[4], argv[6])
                return producer_suite.DockerResult(137, "", "")
            return self._mutate(argv[2], argv[4], argv[6])
        if len(argv) == 4 and argv[:3] == ["--contract", "1", "command"]:
            stored = self.commands.get(argv[3])
            if stored is None:
                return producer_suite.DockerResult(5, _fail("not_found"), "")
            return producer_suite.DockerResult(0, _ok({"command": stored["record"]}), "")
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
def fakes(monkeypatch: pytest.MonkeyPatch) -> _FakeProducer:
    producer = _FakeProducer(CTX)

    def harness(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        if verb == "crash":
            if params.get("point") == "between-commit-and-reply":
                producer.crash_armed = True
                return producer_suite.HarnessResult("ok", {"point": "between-commit-and-reply"}, "")
            return producer_suite.HarnessResult(
                "unsupported", None, f"crash-point:{params.get('point')}"
            )
        return _fake_harness(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_docker_exec", producer.handle)
    monkeypatch.setattr(producer_suite, "_call_harness", harness)
    return producer


def test_scenario_ids_are_stable() -> None:
    assert producer_suite.scenario_ids() == [
        "identity",
        "status",
        "argv-usage",
        "contract-mismatch",
        "unsupported-capability",
        "command-not-found",
        "repository-mismatch",
        "stop-replay",
        "payload-mismatch",
        "resume",
        "command-found",
        "crash-between-commit-and-reply",
        "hold-lock",
    ]


def test_run_all_passes_with_hold_lock_not_run(fakes: _FakeProducer) -> None:
    outcomes = producer_suite.run_all(CTX, ["fake-harness"])
    by_id = {outcome.scenario_id: outcome for outcome in outcomes}

    assert [outcome.scenario_id for outcome in outcomes] == producer_suite.scenario_ids()
    for outcome in outcomes:
        assert outcome.status in ("passed", "not_run"), (outcome.scenario_id, outcome.detail)
    assert by_id["identity"].status == "passed"
    assert by_id["status"].status == "passed"
    assert by_id["argv-usage"].status == "passed"
    assert by_id["contract-mismatch"].status == "passed"
    assert by_id["unsupported-capability"].status == "passed"
    assert by_id["command-not-found"].status == "passed"
    assert by_id["repository-mismatch"].status == "passed"
    assert by_id["stop-replay"].status == "passed"
    assert by_id["payload-mismatch"].status == "passed"
    assert by_id["resume"].status == "passed"
    assert by_id["command-found"].status == "passed"
    assert by_id["crash-between-commit-and-reply"].status == "passed"
    assert by_id["hold-lock"].status == "not_run"


def test_mutation_scenarios_fail_when_replay_changes_the_record(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_handle = fakes.handle

    def replay_changes_record(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        result = real_handle(docker, container, exec_user, argv, timeout)
        if argv[:2] == ["--contract", "1"] and argv[2:3] == ["stop"] and result.exit_code == 0:
            doc = json.loads(result.stdout)
            if doc["result"].get("replayed") is True:
                doc["result"]["command"]["sequence"] += 100
                return producer_suite.DockerResult(0, shared.render_document(doc), result.stderr)
        return result

    monkeypatch.setattr(producer_suite, "_docker_exec", replay_changes_record)

    assert producer_suite.scenario_stop_replay(CTX).status == "failed"


def test_mutation_scenarios_fail_when_a_rejection_stores(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_handle = fakes.handle

    def rejection_stores(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        result = real_handle(docker, container, exec_user, argv, timeout)
        if result.exit_code == 5 and len(argv) == 4 and argv[2] == "command":
            stored = fakes.commands.get(argv[3])
            if stored is None:
                record = fakes._record(argv[3], "stop", "completed")
                return producer_suite.DockerResult(0, _ok({"command": record}), "")
        return result

    monkeypatch.setattr(producer_suite, "_docker_exec", rejection_stores)

    assert producer_suite.scenario_repository_mismatch(CTX).status == "failed"


def test_hold_lock_supported_passes(fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch) -> None:
    def hold_ok(cmd: Any, verb: str, params: dict[str, Any], timeout: float) -> Any:
        if verb == "hold-lock":
            return producer_suite.HarnessResult("ok", {"point": "x", "held_ms": 100}, "")
        return _fake_harness(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", hold_ok)

    outcome = producer_suite.scenario_hold_lock(CTX, ["fake-harness"])

    assert outcome.status == "passed"


def test_crash_scenario_is_not_run_when_harness_cannot_crash(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_crash(cmd: Any, verb: str, params: dict[str, Any], timeout: float) -> Any:
        if verb == "crash":
            return producer_suite.HarnessResult("unsupported", None, "crash-point:now")
        return _fake_harness(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", no_crash)

    outcome = producer_suite.scenario_crash_between_commit_and_reply(CTX, ["fake-harness"])

    assert outcome.status == "not_run"


def test_crash_scenario_fails_when_the_first_attempt_replies(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    def claim_without_arming(cmd: Any, verb: str, params: dict[str, Any], timeout: float) -> Any:
        if verb == "crash":
            return producer_suite.HarnessResult("ok", {"point": "between-commit-and-reply"}, "")
        return _fake_harness(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", claim_without_arming)

    outcome = producer_suite.scenario_crash_between_commit_and_reply(CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "no reply" in outcome.detail


def test_supported_but_broken_producer_fails_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        return producer_suite.DockerResult(0, "not json\n", "")

    monkeypatch.setattr(producer_suite, "_docker_exec", broken)

    assert producer_suite.scenario_identity(CTX).status == "failed"


def test_supported_but_broken_producer_fails_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        return producer_suite.DockerResult(0, "not json\n", "")

    monkeypatch.setattr(producer_suite, "_docker_exec", broken)

    assert producer_suite.scenario_status(CTX).status == "failed"


def test_supported_but_broken_producer_fails_stop_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        return producer_suite.DockerResult(0, "not json\n", "")

    monkeypatch.setattr(producer_suite, "_docker_exec", broken)

    assert producer_suite.scenario_stop_replay(CTX).status == "failed"


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
    fakes: _FakeProducer, capsys: pytest.CaptureFixture[str]
) -> None:
    assert producer_suite.main(["--harness", "fake-harness"]) == 0

    out = capsys.readouterr().out
    assert "PASS identity" in out
    assert "PASS status" in out
    assert "PASS argv-usage" in out
    assert "PASS contract-mismatch" in out
    assert "PASS unsupported-capability" in out
    assert "PASS command-not-found" in out
    assert "PASS repository-mismatch" in out
    assert "PASS stop-replay" in out
    assert "PASS payload-mismatch" in out
    assert "PASS resume" in out
    assert "PASS command-found" in out
    assert "PASS crash-between-commit-and-reply" in out
    assert "NOT RUN hold-lock" in out


def test_main_with_expect_fail_for_a_not_run_fails(
    fakes: _FakeProducer, capsys: pytest.CaptureFixture[str]
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


def test_every_producer_reply_is_validated(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[str] = []
    real_handle = fakes.handle

    def record(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        result = real_handle(docker, container, exec_user, argv, timeout)
        seen.append(result.stdout)
        return result

    monkeypatch.setattr(producer_suite, "_docker_exec", record)

    producer_suite.run_all(CTX, ["fake-harness"])

    assert seen
    # The crash attempt gets no reply by design; every other reply validates.
    assert seen.count("") == 1
    for stdout in [reply for reply in seen if reply]:
        assert stdout.endswith("\n")
        assert stdout.count("\n") == 1
        assert json.loads(stdout)
