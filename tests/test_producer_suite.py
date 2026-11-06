"""Unit tests for the producer suite runner (no Docker, no engine).

The docker and harness seams are faked through monkeypatching, so these
tests cover the suite's own logic: scenario checks, the not-run path for an
unsupported harness verb, and the expected-failures list. The docker fake is a
small in-memory producer that mirrors the contract's mutation semantics
(repository guard, idempotency, state) so the mutation scenarios run green.
"""

import io
import json
import tarfile
import threading
import time
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


def _fake_encode_cursor(offset: int) -> str:
    import base64

    raw = json.dumps({"offset": offset}, sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _fake_decode_cursor(cursor: str) -> int | None:
    import base64

    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        doc = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(doc, dict) or set(doc) != {"offset"}:
        return None
    offset = doc.get("offset")
    if type(offset) is not int or offset < 0:
        return None
    return offset


def _is_int_in(value: str, low: int, high: int | None) -> bool:
    """Mirror the shared value check: canonical decimal digits within bounds."""
    try:
        number = int(value, 10)
    except ValueError:
        return False
    if str(number) != value:
        return False
    return number >= low and (high is None or number <= high)


def _probe_request_id(argv: list[str]) -> str:
    """Echo the probe's ``--request-id`` value, like a producer ``unsupported``."""
    if "--request-id" in argv:
        return argv[argv.index("--request-id") + 1]
    return "req-unused"


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
    ``rejected`` state guard. Opens a claim on ``status`` for an eligible
    issue and serves ``attempts list``/``show`` from it. ``status`` serves
    the last prepared snapshot while ``engine_lock`` is held, like a real
    engine; ``blocking_reads`` makes it wait on the lock instead, and
    ``always_fresh`` makes it re-read under the hold. Anything else answers
    as the fixed seams below.
    """

    def __init__(self, ctx: producer_suite.SuiteContext) -> None:
        self._ctx = ctx
        self.intake = "open"
        self.sequence = 1
        self.commands: dict[str, dict[str, Any]] = {}
        self.crash_armed = False
        self.latency: dict[str, float] = {}
        self.engine_lock = threading.Lock()
        self.prepared: dict[str, Any] | None = None
        self.blocking_reads = False
        self.always_fresh = False
        self.eligible: set[int] = set()
        self.active_attempt: dict[str, Any] | None = None
        self.finished_attempts: list[dict[str, Any]] = []
        self.attempt_counter = 0
        self.tick = 0
        self.extra_events: list[dict[str, Any]] = []

    def _record(
        self,
        request_id: str,
        kind: str,
        acknowledgement: str,
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {} if arguments is None else arguments
        record = {
            "request_id": request_id,
            "sequence": self.sequence,
            "kind": kind,
            "arguments": payload,
            "repository": self._ctx.repository,
            "acknowledgement": acknowledgement,
            "detail": f"{kind} {acknowledgement}",
            "created_at": TIMESTAMP,
            "updated_at": TIMESTAMP,
        }
        self.commands[request_id] = {"kind": kind, "arguments": payload, "record": record}
        self.sequence += 1
        return record

    def _observed_at(self) -> str:
        return f"2026-11-06T09:12:{5 + self.tick:02d}Z"

    def _attempt_doc(self, active: dict[str, Any], observed_at: str) -> dict[str, Any]:
        unknown: dict[str, Any] = {
            "value": None,
            "unit": "tokens",
            "basis": "unavailable",
            "reason": "fake has no telemetry",
        }
        return {
            "attempt_id": active["attempt_id"],
            "issue": {"number": active["issue_number"], "title": active["issue_title"]},
            "native_phase": "claimed",
            "started_at": active["started_at"],
            "last_activity_at": active["started_at"],
            "observed_at": observed_at,
            "budget": {
                "raw_tokens": dict(unknown),
                "effective_tokens": dict(unknown),
                "enforced_tokens": "none",
                "limits": [],
            },
            "figures": {},
        }

    def _summary_doc(self, active: dict[str, Any], finished_at: str | None) -> dict[str, Any]:
        return {
            "attempt_id": active["attempt_id"],
            "issue": {"number": active["issue_number"], "title": active["issue_title"]},
            "started_at": active["started_at"],
            "finished_at": finished_at,
            "lifecycle_state": "open" if finished_at is None else "finished",
            "outcome": None
            if finished_at is None
            else {"native": "complete", "reason": None, "label": "Complete"},
            "delivery": None,
            "attention_required": False,
            "sealed": finished_at is not None,
            "sealed_at": finished_at,
        }

    def _fresh_doc(self) -> dict[str, Any]:
        self.tick += 1
        observed_at = self._observed_at()
        if self.intake == "open" and self.active_attempt is None and self.eligible:
            self.attempt_counter += 1
            issue = min(self.eligible)
            self.active_attempt = {
                "attempt_id": f"fake-{self.attempt_counter}",
                "issue_number": issue,
                "issue_title": None,
                "started_at": observed_at,
            }
        identity = dict(IDENTITY)
        records = sorted(
            (entry["record"] for entry in self.commands.values()),
            key=lambda record: record["sequence"],
        )
        current = None
        if self.active_attempt is not None:
            current = self._attempt_doc(self.active_attempt, observed_at)
        return {
            "generated_at": observed_at,
            "identity": identity,
            "instance": {
                "state": "idle" if self.intake == "open" else "paused",
                "startup_check": None,
                "last_halt": None,
                "observed_at": observed_at,
            },
            "intake": {
                "state": self.intake,
                "pending_stop": None,
                "stop_plan": None,
                "priority": None,
                "observed_at": observed_at,
            },
            "current_attempt": current,
            "last_finished": self.finished_attempts[-1] if self.finished_attempts else None,
            "recent_commands": records[-20:],
            "recent_events": list(self.extra_events),
            "truncated": False,
        }

    def _status_reply(self) -> producer_suite.DockerResult:
        if self.blocking_reads:
            with self.engine_lock:
                return producer_suite.DockerResult(0, _ok(self._fresh_doc()), "")
        if self.always_fresh:
            return producer_suite.DockerResult(0, _ok(self._fresh_doc()), "")
        if self.engine_lock.acquire(blocking=False):
            try:
                doc = self._fresh_doc()
            finally:
                self.engine_lock.release()
            self.prepared = doc
            return producer_suite.DockerResult(0, _ok(doc), "")
        if self.prepared is None:
            with self.engine_lock:
                doc = self._fresh_doc()
            self.prepared = doc
            return producer_suite.DockerResult(0, _ok(doc), "")
        return producer_suite.DockerResult(0, _ok(self.prepared), "")

    @staticmethod
    def _op_key(argv: list[str]) -> str:
        if len(argv) >= 4 and argv[2] == "attempts":
            return f"attempts-{argv[3]}"
        if len(argv) >= 3:
            return str(argv[2])
        return ""

    def _mutate(
        self,
        operation: str,
        request_id: str,
        expect_repo: str,
        arguments: dict[str, Any] | None = None,
    ) -> producer_suite.DockerResult:
        payload = {} if arguments is None else arguments
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
            if stored["kind"] == operation and stored["arguments"] == payload:
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
        elif operation == "resume":
            if self.intake == "open":
                return producer_suite.DockerResult(
                    1, _fail_with_request("rejected", request_id), ""
                )
            self.intake = "open"
            record = self._record(request_id, "resume", "completed")
        elif operation in ("stop_after", "next_issue"):
            record = self._record(request_id, operation, "accepted", payload)
        else:
            raise AssertionError(f"unexpected fake mutation: {operation!r}")
        return producer_suite.DockerResult(0, _ok({"command": record, "replayed": False}), "")

    def handle(
        self,
        docker: str,
        container: str,
        exec_user: str,
        argv: list[str],
        timeout: float,
    ) -> producer_suite.DockerResult:
        delay = self.latency.get(self._op_key(argv), 0.0)
        if delay:
            time.sleep(delay)
        if argv == ["--contract", "1", "identity"]:
            return producer_suite.DockerResult(0, _ok(IDENTITY), "")
        if argv == ["--contract", "1", "status"]:
            return self._status_reply()
        if len(argv) >= 4 and argv[:3] == ["--contract", "1", "attempts"] and argv[3] == "list":
            summaries: list[dict[str, Any]] = []
            if self.active_attempt is not None:
                summaries.append(self._summary_doc(self.active_attempt, None))
            summaries.extend(reversed(self.finished_attempts))
            rest = argv[4:]
            limit = 20
            offset = 0
            index = 0
            while index < len(rest):
                flag = rest[index]
                if flag == "--limit" and index + 1 < len(rest):
                    try:
                        limit = int(rest[index + 1], 10)
                    except ValueError:
                        return producer_suite.DockerResult(1, _fail("invalid_argument"), "")
                    if limit < 1 or limit > 100:
                        return producer_suite.DockerResult(1, _fail("invalid_argument"), "")
                    index += 2
                elif flag == "--before" and index + 1 < len(rest):
                    decoded = _fake_decode_cursor(rest[index + 1])
                    if decoded is None:
                        return producer_suite.DockerResult(1, _fail("invalid_argument"), "")
                    offset = decoded
                    index += 2
                else:
                    return producer_suite.DockerResult(2, _fail("usage"), "")
            page = summaries[offset : offset + limit]
            if offset + limit < len(summaries):
                next_cursor: str | None = _fake_encode_cursor(offset + limit)
            else:
                next_cursor = None
            return producer_suite.DockerResult(
                0, _ok({"attempts": page, "next_cursor": next_cursor}), ""
            )
        if len(argv) == 5 and argv[:4] == ["--contract", "1", "attempts", "show"]:
            summaries = []
            if self.active_attempt is not None:
                summaries.append(self._summary_doc(self.active_attempt, None))
            summaries.extend(self.finished_attempts)
            for summary in summaries:
                if summary["attempt_id"] == argv[4]:
                    manifest = [
                        {
                            "path": "events.jsonl",
                            "kind": "events",
                            "media_type": "application/x-ndjson",
                            "size": 0,
                            "sha256": "e3b0c44298fc1c149afbf4c8996fb924"
                            "27ae41e4649b934ca495991b7852b855",
                        }
                    ]
                    return producer_suite.DockerResult(
                        0, _ok({"attempt": summary, "manifest": manifest}), ""
                    )
            return producer_suite.DockerResult(5, _fail("not_found"), "")
        if argv == ["--contract", "1", "stop", "--after"]:
            return producer_suite.DockerResult(2, _fail("usage"), "")
        if argv == ["--contract", "2", "status"]:
            return producer_suite.DockerResult(
                4, _fail("contract_mismatch", {"supported": [1]}), ""
            )
        if argv[:3] == ["--contract", "1", "handoff"]:
            return producer_suite.DockerResult(
                3,
                _fail_with_request(
                    "unsupported",
                    _probe_request_id(argv),
                    {"capability": "operator-handoff"},
                ),
                "",
            )
        if (
            len(argv) == 9
            and argv[:3] == ["--contract", "1", "stop"]
            and argv[3] == "--after"
            and argv[5] == "--request-id"
            and argv[7] == "--expect-repo"
        ):
            if "stop-after" not in self._ctx.capabilities:
                return producer_suite.DockerResult(
                    3,
                    _fail_with_request("unsupported", argv[6], {"capability": "stop-after"}),
                    "",
                )
            if not _is_int_in(argv[4], 1, 999):
                return producer_suite.DockerResult(
                    1, _fail_with_request("invalid_argument", argv[6]), ""
                )
            return self._mutate("stop_after", argv[6], argv[8], {"after": int(argv[4])})
        if (
            len(argv) == 9
            and argv[:4] == ["--contract", "1", "next", "issue"]
            and argv[5] == "--request-id"
            and argv[7] == "--expect-repo"
        ):
            if "next-issue" not in self._ctx.capabilities:
                return producer_suite.DockerResult(
                    3,
                    _fail_with_request("unsupported", argv[6], {"capability": "next-issue"}),
                    "",
                )
            if not _is_int_in(argv[4], 1, None):
                return producer_suite.DockerResult(
                    1, _fail_with_request("invalid_argument", argv[6]), ""
                )
            return self._mutate("next_issue", argv[6], argv[8], {"issue": int(argv[4])})
        if len(argv) >= 4 and argv[:3] == ["--contract", "1", "recovery"]:
            capability = (
                "recovery-retry"
                if argv[3] == "retry"
                else "recovery-release"
                if argv[3] == "release"
                else None
            )
            if capability is not None:
                return producer_suite.DockerResult(
                    3,
                    _fail_with_request(
                        "unsupported",
                        _probe_request_id(argv),
                        {"capability": capability},
                    ),
                    "",
                )
        if len(argv) >= 3 and argv[:3] == ["--contract", "1", "errors"]:
            return producer_suite.DockerResult(
                3,
                _fail_with_request(
                    "unsupported",
                    _probe_request_id(argv),
                    {"capability": "errors-reset"},
                ),
                "",
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
    return producer_suite.HarnessResult("unsupported", None, f"verb:{verb}")


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
        "status-during-lock",
        "budget-reads",
        "budget-mutations",
        "budget-attempts",
        "budget-export",
        "attempts-list-paging",
        "attempts-show",
        "attempts-not-found",
        "contract-events",
        "snapshot-bounds",
        "stop-before-claim-crash",
        "stop-after-claim-sigterm",
        "stop-during-starting",
        "resume-supersedes-stop",
        "stop-after-invalid",
        "stop-after-restart",
        "next-issue-restart",
        "halt-records-last-halt",
        "publication-pending-recovery",
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
    assert by_id["status-during-lock"].status == "not_run"
    assert by_id["budget-reads"].status == "passed"
    assert by_id["budget-mutations"].status == "passed"
    assert by_id["budget-attempts"].status == "not_run"
    assert by_id["budget-export"].status == "not_run"
    assert by_id["attempts-list-paging"].status == "not_run"
    assert by_id["attempts-show"].status == "not_run"
    assert by_id["attempts-not-found"].status == "passed"
    assert by_id["contract-events"].status == "not_run"
    assert by_id["snapshot-bounds"].status == "passed"
    assert by_id["stop-before-claim-crash"].status == "not_run"
    assert by_id["stop-after-claim-sigterm"].status == "not_run"
    assert by_id["stop-during-starting"].status == "not_run"
    assert by_id["resume-supersedes-stop"].status == "not_run"
    assert by_id["stop-after-invalid"].status == "passed"
    assert by_id["stop-after-restart"].status == "not_run"
    assert by_id["next-issue-restart"].status == "not_run"
    assert by_id["halt-records-last-halt"].status == "not_run"
    assert by_id["publication-pending-recovery"].status == "not_run"


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
    assert "NOT RUN status-during-lock" in out
    assert "PASS budget-reads" in out
    assert "PASS budget-mutations" in out
    assert "NOT RUN budget-attempts" in out
    assert "NOT RUN budget-export" in out
    assert "PASS snapshot-bounds" in out
    assert "PASS stop-after-invalid" in out
    assert "NOT RUN stop-after-restart" in out
    assert "NOT RUN next-issue-restart" in out


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


def _support_claims(monkeypatch: pytest.MonkeyPatch, producer: _FakeProducer) -> None:
    """Harness override that supports set-issue, release and finish-attempt."""

    def harness(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        if verb == "set-issue":
            issue = int(params["issue"])
            if params["eligible"]:
                producer.eligible.add(issue)
            else:
                producer.eligible.discard(issue)
            return producer_suite.HarnessResult(
                "ok", {"issue": issue, "eligible": params["eligible"]}, ""
            )
        if verb == "release":
            return producer_suite.HarnessResult("ok", {"released": True}, "")
        if verb == "finish-attempt":
            active = producer.active_attempt
            if active is None:
                return producer_suite.HarnessResult("error", None, "no active attempt")
            producer.finished_attempts.append(producer._summary_doc(active, "2026-11-06T09:13:00Z"))
            producer.active_attempt = None
            return producer_suite.HarnessResult(
                "ok",
                {"attempt_id": active["attempt_id"], "native_outcome": "complete"},
                "",
            )
        return _fake_harness(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", harness)


def _holding_harness(monkeypatch: pytest.MonkeyPatch, producer: _FakeProducer) -> None:
    """Harness override whose hold-lock really holds the fake engine lock."""

    def harness(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        if verb == "hold-lock":
            with producer.engine_lock:
                time.sleep(params["duration_ms"] / 1000.0)
            return producer_suite.HarnessResult("ok", {"held_ms": params["duration_ms"]}, "")
        return _fake_harness(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", harness)


def test_status_during_lock_passes_with_prepared_state(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _holding_harness(monkeypatch, fakes)

    outcome = producer_suite.scenario_status_during_lock(
        CTX, ["fake-harness"], duration_ms=800, settle_s=0.05
    )

    assert outcome.status == "passed", outcome.detail


def test_status_during_lock_fails_when_reads_wait_on_the_lock(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fakes.blocking_reads = True
    _holding_harness(monkeypatch, fakes)

    outcome = producer_suite.scenario_status_during_lock(
        CTX, ["fake-harness"], duration_ms=600, budget_s=0.15, settle_s=0.05
    )

    assert outcome.status == "failed"
    assert "budget" in outcome.detail


def test_status_during_lock_fails_when_status_rereads_under_the_hold(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fakes.always_fresh = True
    _holding_harness(monkeypatch, fakes)

    outcome = producer_suite.scenario_status_during_lock(
        CTX, ["fake-harness"], duration_ms=600, settle_s=0.05
    )

    assert outcome.status == "failed"
    assert "prepared state" in outcome.detail


def test_status_during_lock_is_not_run_without_hold_lock(fakes: _FakeProducer) -> None:
    outcome = producer_suite.scenario_status_during_lock(
        CTX, ["fake-harness"], duration_ms=200, settle_s=0.01
    )

    assert outcome.status == "not_run"


def test_budget_reads_passes(fakes: _FakeProducer) -> None:
    assert producer_suite.scenario_budget_reads(CTX).status == "passed"


def test_budget_reads_fails_when_identity_is_slow(fakes: _FakeProducer) -> None:
    fakes.latency = {"identity": 0.3}

    outcome = producer_suite.scenario_budget_reads(CTX, budget_s=0.1)

    assert outcome.status == "failed"
    assert "identity" in outcome.detail


def test_budget_mutations_passes(fakes: _FakeProducer) -> None:
    assert producer_suite.scenario_budget_mutations(CTX).status == "passed"


def test_budget_mutations_fails_when_stop_is_slow(fakes: _FakeProducer) -> None:
    fakes.latency = {"stop": 0.3}

    outcome = producer_suite.scenario_budget_mutations(CTX, budget_s=0.1)

    assert outcome.status == "failed"
    assert "stop" in outcome.detail


def test_budget_attempts_passes_with_a_claim(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, fakes)

    outcome = producer_suite.scenario_budget_attempts(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert fakes.eligible == set()


def test_budget_attempts_is_not_run_without_harness_support(
    fakes: _FakeProducer,
) -> None:
    outcome = producer_suite.scenario_budget_attempts(CTX, ["fake-harness"])

    assert outcome.status == "not_run"


def test_budget_attempts_fails_when_list_is_slow(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, fakes)
    fakes.latency = {"attempts-list": 0.3}

    outcome = producer_suite.scenario_budget_attempts(CTX, ["fake-harness"], budget_s=0.1)

    assert outcome.status == "failed"
    assert "attempts list" in outcome.detail


def _seed_finished_attempt(
    producer: _FakeProducer, attempt_id: str, issue: int, started_at: str
) -> None:
    producer.finished_attempts.append(
        producer._summary_doc(
            {
                "attempt_id": attempt_id,
                "issue_number": issue,
                "issue_title": f"Issue {issue}",
                "started_at": started_at,
            },
            "2026-11-06T09:13:00Z",
        )
    )


def test_attempts_list_paging_passes_with_finished_attempts(fakes: _FakeProducer) -> None:
    _seed_finished_attempt(fakes, "fake-old", 109, "2026-11-06T08:50:00Z")
    _seed_finished_attempt(fakes, "fake-new", 110, "2026-11-06T08:51:00Z")

    outcome = producer_suite.scenario_attempts_list_paging(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert fakes.eligible == set()


def test_attempts_show_passes_with_a_finished_attempt(fakes: _FakeProducer) -> None:
    _seed_finished_attempt(fakes, "fake-1", 110, "2026-11-06T08:51:00Z")

    outcome = producer_suite.scenario_attempts_show(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert fakes.eligible == set()


def test_contract_events_passes_with_a_finished_attempt(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_finished_attempt(fakes, "fake-1", 111, "2026-11-06T08:51:00Z")
    fakes.extra_events.append(
        {
            "at": "2026-11-06T08:51:00Z",
            "level": "info",
            "event": "claim_opened",
            "text": "claimed issue #111",
            "attempt_id": "fake-1",
            "issue": 111,
        }
    )

    def fake_export(
        docker: str, container: str, exec_user: str, argv: list[str]
    ) -> _FakeExportProc:
        attempt_id = argv[-1]
        line = (
            json.dumps(
                {
                    "at": "2026-11-06T08:51:00Z",
                    "level": "info",
                    "event": "claim_opened",
                    "text": "claimed issue #111",
                    "attempt_id": attempt_id,
                    "issue": 111,
                },
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as archive:
            manifest = {"contract": "1.0", "attempt": {"attempt_id": attempt_id}, "files": []}
            payload = (json.dumps(manifest, sort_keys=True) + "\n").encode()
            info = tarfile.TarInfo("manifest.json")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
            info = tarfile.TarInfo("events.jsonl")
            info.size = len(line)
            archive.addfile(info, io.BytesIO(line))
        return _FakeExportProc(io.BytesIO(stream.getvalue()))

    monkeypatch.setattr(producer_suite, "_popen_export", fake_export)

    outcome = producer_suite.scenario_contract_events(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert fakes.eligible == set()


def _export_tar(attempt_id: str) -> bytes:
    manifest = {"contract": "1.0", "attempt": {"attempt_id": attempt_id}, "files": []}
    payload = (json.dumps(manifest, sort_keys=True) + "\n").encode()
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        info = tarfile.TarInfo("manifest.json")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
    return stream.getvalue()


class _FakeExportProc:
    """Stand-in for the streaming export process."""

    def __init__(self, stream: Any) -> None:
        self.stdout = stream
        self.stderr = io.BytesIO(b"")
        self.returncode: int | None = 0

    def kill(self) -> None:
        return None

    def wait(self, timeout: float | None = None) -> int:
        return 0

    def communicate(self, timeout: float | None = None) -> tuple[bytes, bytes]:
        return self.stdout.read(), b""


class _SlowStream:
    """First read blocks past the budget, then yields a valid export."""

    def __init__(self, delay_s: float, payload: bytes) -> None:
        self._delay_s = delay_s
        self._payload = payload

    def read(self, size: int = -1) -> bytes:
        delay, self._delay_s = self._delay_s, 0.0
        if delay:
            time.sleep(delay)
        if size < 0:
            size = len(self._payload)
        out, self._payload = self._payload[:size], self._payload[size:]
        return out


def test_budget_export_passes_with_a_claim(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, fakes)

    def fake_export(
        docker: str, container: str, exec_user: str, argv: list[str]
    ) -> _FakeExportProc:
        return _FakeExportProc(io.BytesIO(_export_tar(argv[-1])))

    monkeypatch.setattr(producer_suite, "_popen_export", fake_export)

    outcome = producer_suite.scenario_budget_export(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert fakes.eligible == set()


def test_budget_export_is_not_run_without_harness_support(
    fakes: _FakeProducer,
) -> None:
    outcome = producer_suite.scenario_budget_export(CTX, ["fake-harness"])

    assert outcome.status == "not_run"


def test_budget_export_fails_on_a_slow_start(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, fakes)

    def slow_export(
        docker: str, container: str, exec_user: str, argv: list[str]
    ) -> _FakeExportProc:
        return _FakeExportProc(_SlowStream(0.4, _export_tar(argv[-1])))

    monkeypatch.setattr(producer_suite, "_popen_export", slow_export)

    outcome = producer_suite.scenario_budget_export(CTX, ["fake-harness"], budget_s=0.1)

    assert outcome.status == "failed"
    assert "no bytes" in outcome.detail


def test_budget_export_fails_when_manifest_is_for_another_attempt(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, fakes)

    def wrong_export(
        docker: str, container: str, exec_user: str, argv: list[str]
    ) -> _FakeExportProc:
        return _FakeExportProc(io.BytesIO(_export_tar("fake-other")))

    monkeypatch.setattr(producer_suite, "_popen_export", wrong_export)

    outcome = producer_suite.scenario_budget_export(CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "not for attempt" in outcome.detail


def test_snapshot_bounds_passes(fakes: _FakeProducer) -> None:
    assert producer_suite.scenario_snapshot_bounds(CTX).status == "passed"


def test_snapshot_bounds_fails_when_oversized(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fakes._mutate("stop", "req-big-1", CTX.repository)
    real_handle = fakes.handle

    def oversized(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        result = real_handle(docker, container, exec_user, argv, timeout)
        doc = json.loads(result.stdout)
        for record in doc["result"]["recent_commands"]:
            record["detail"] = "x" * (300 * 1024)
        return producer_suite.DockerResult(0, shared.render_document(doc), "")

    monkeypatch.setattr(producer_suite, "_docker_exec", oversized)

    outcome = producer_suite.scenario_snapshot_bounds(CTX)

    assert outcome.status == "failed"
    assert "bytes" in outcome.detail


def test_snapshot_bounds_fails_on_unsorted_capabilities(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_handle = fakes.handle

    def unsorted(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        result = real_handle(docker, container, exec_user, argv, timeout)
        doc = json.loads(result.stdout)
        doc["result"]["identity"]["capabilities"] = ["stop-after", "next-issue"]
        return producer_suite.DockerResult(0, shared.render_document(doc), "")

    monkeypatch.setattr(producer_suite, "_docker_exec", unsorted)

    outcome = producer_suite.scenario_snapshot_bounds(CTX)

    assert outcome.status == "failed"
    assert "sorted" in outcome.detail


def test_snapshot_bounds_fails_when_commands_are_not_oldest_first(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fakes._mutate("stop", "req-order-1", CTX.repository)
    fakes._mutate("resume", "req-order-2", CTX.repository)
    real_handle = fakes.handle

    def shuffled(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        result = real_handle(docker, container, exec_user, argv, timeout)
        doc = json.loads(result.stdout)
        if argv == ["--contract", "1", "status"]:
            commands = doc["result"]["recent_commands"]
            commands[0], commands[1] = commands[1], commands[0]
            return producer_suite.DockerResult(0, shared.render_document(doc), "")
        return result

    monkeypatch.setattr(producer_suite, "_docker_exec", shuffled)

    outcome = producer_suite.scenario_snapshot_bounds(CTX)

    assert outcome.status == "failed"
    assert "oldest-first" in outcome.detail


def test_snapshot_bounds_fails_when_truncated_keeps_events(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    fakes.extra_events = [
        {
            "at": "2026-11-06T09:12:03Z",
            "level": "info",
            "event": "kept",
            "text": "kept",
        }
    ]
    real_handle = fakes.handle

    def truncated(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        result = real_handle(docker, container, exec_user, argv, timeout)
        doc = json.loads(result.stdout)
        if argv == ["--contract", "1", "status"]:
            doc["result"]["truncated"] = True
            return producer_suite.DockerResult(0, shared.render_document(doc), "")
        return result

    monkeypatch.setattr(producer_suite, "_docker_exec", truncated)

    outcome = producer_suite.scenario_snapshot_bounds(CTX)

    assert outcome.status == "failed"
    assert "recent_events" in outcome.detail
