"""Unit tests for the producer suite runner (no Docker, no engine).

The docker and harness seams are faked through monkeypatching, so these
tests cover the suite's own logic: scenario checks, the not-run path for an
unsupported harness verb, and the expected-failures list. The docker fake is a
small in-memory producer that mirrors the contract's mutation semantics
(repository guard, idempotency, state) so the mutation scenarios run green.
"""

import dataclasses
import hashlib
import io
import json
import tarfile
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from agent_operator_contract import documents, producer_suite, shared

CTX = producer_suite.SuiteContext(
    container="stub-abc123",
    exec_user="agent",
    repository="example-owner/example-repo",
    capabilities=["next-issue", "stop-after"],
    image="sha256:abc",
    volumes=({"name": "stub-data", "path": "/data"},),
)

FULL_CTX = producer_suite.SuiteContext(
    container="stub-abc123",
    exec_user="agent",
    repository="example-owner/example-repo",
    capabilities=[
        "errors-reset",
        "next-issue",
        "operator-handoff",
        "recovery-release",
        "recovery-retry",
        "stop-after",
    ],
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
        "error": {
            "code": code,
            "message": "stub",
            "retryable": documents.error_table()[code]["retryable"],
        },
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
            "retryable": documents.error_table()[code]["retryable"],
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
    issue, or only on the harness ``release`` with ``claims_on_release_only``
    like a real engine, and serves ``attempts list``/``show`` from it.
    ``status`` serves the last prepared snapshot while ``engine_lock`` is
    held, like a real engine; ``blocking_reads`` makes it wait on the lock
    instead, and ``always_fresh`` makes it re-read under the hold. Anything
    else answers as the fixed seams below.
    """

    def __init__(self, ctx: producer_suite.SuiteContext) -> None:
        self._ctx = ctx
        self.intake = "open"
        self.sequence = 1
        self.commands: dict[str, dict[str, Any]] = {}
        self.crash_armed = False
        self.alive = True
        self.startup = False
        self.recovery_pending = False
        self.crash_finalize = False
        self.crash_before_reply = False
        self.handoff_finish_after: int | None = None
        self.pending_handoff: dict[str, Any] | None = None
        self.recovery_handoff_ack = "not_fulfilled"
        self.error_guard = False
        self.fail_finalization = False
        self.accept_active_recovery = False
        self.release_finalized_accepted = False
        self.retry_finalized_changes = False
        self.latency: dict[str, float] = {}
        self.engine_lock = threading.Lock()
        self.prepared: dict[str, Any] | None = None
        self.blocking_reads = False
        self.always_fresh = False
        self.eligible: set[int] = set()
        self.claims_on_release_only = False
        self.active_attempt: dict[str, Any] | None = None
        self.finished_attempts: list[dict[str, Any]] = []
        self.attempt_counter = 0
        self.tick = 0
        self.extra_events: list[dict[str, Any]] = []
        self.hold: dict[str, Any] | None = None
        self.snapshots: dict[str, list[dict[str, Any]]] = {}
        self.grown: dict[str, bytes] = {}

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

    def _summary_doc(
        self,
        active: dict[str, Any],
        finished_at: str | None,
        lifecycle: str = "finished",
    ) -> dict[str, Any]:
        if finished_at is not None or active.get("outcome_native") is not None:
            native = str(active.get("outcome_native") or "complete")
            outcome: dict[str, Any] | None = {
                "native": native,
                "reason": active.get("outcome_reason"),
                "label": native.replace("_", " ").title(),
            }
        else:
            outcome = None
        return {
            "attempt_id": active["attempt_id"],
            "issue": {"number": active["issue_number"], "title": active["issue_title"]},
            "started_at": active["started_at"],
            "finished_at": finished_at,
            "lifecycle_state": "open" if finished_at is None else lifecycle,
            "outcome": outcome,
            "delivery": None,
            "attention_required": bool(active.get("retained")),
            "sealed": finished_at is not None,
            "sealed_at": finished_at,
        }

    def _finish_active(self, lifecycle: str, native: str | None) -> dict[str, Any] | None:
        """Move the active attempt to a sealed finished summary; None when idle."""
        active = self.active_attempt
        if not isinstance(active, dict):
            return None
        if native is None:
            raw = active.get("outcome_native")
            native = raw if isinstance(raw, str) and raw else "handoff"
        active["outcome_native"] = native
        if "outcome_reason" not in active:
            active["outcome_reason"] = None
        summary = self._summary_doc(active, TIMESTAMP, lifecycle=lifecycle)
        self.finished_attempts.append(summary)
        self.active_attempt = None
        return summary

    def _find_attempt(self, attempt_id: str) -> str | None:
        """Return where an attempt lives: retained, active, finished or None."""
        active = self.active_attempt
        if isinstance(active, dict) and active.get("attempt_id") == attempt_id:
            return "retained" if active.get("retained") else "active"
        if any(summary["attempt_id"] == attempt_id for summary in self.finished_attempts):
            return "finished"
        return None

    def _archive_files(self, attempt_id: str) -> dict[str, bytes]:
        return {
            "events.jsonl": b"",
            "transcript.jsonl": b"transcript-seed\n" + self.grown.get(attempt_id, b""),
        }

    def _fresh_manifest(self, attempt_id: str) -> list[dict[str, Any]]:
        return [
            {
                "path": name,
                "kind": "events" if name == "events.jsonl" else "transcript",
                "media_type": "application/x-ndjson",
                "size": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
            for name, data in self._archive_files(attempt_id).items()
        ]

    def _manifest_for(self, attempt_id: str) -> list[dict[str, Any]]:
        snapshot = self.snapshots.get(attempt_id)
        if snapshot is not None:
            return [dict(entry) for entry in snapshot]
        return self._fresh_manifest(attempt_id)

    def open_claim(self, observed_at: str, *, over_retained: bool = False) -> None:
        """Open a Claim for the lowest eligible issue when the engine is free.

        ``over_retained`` models a broken engine that claims even while a
        Retained Attempt exists, replacing it.
        """
        active = self.active_attempt
        free = active is None and not self.recovery_pending
        retained = isinstance(active, dict) and active.get("retained") is True
        if not (
            self.alive
            and not self.startup
            and self.intake == "open"
            and self.eligible
            and (free or (over_retained and retained))
        ):
            return
        self.attempt_counter += 1
        issue = min(self.eligible)
        self.active_attempt = {
            "attempt_id": f"fake-{self.attempt_counter}",
            "issue_number": issue,
            "issue_title": None,
            "started_at": observed_at,
        }
        attempt_id = str(self.active_attempt["attempt_id"])
        self.snapshots[attempt_id] = self._fresh_manifest(attempt_id)

    def _fresh_doc(self) -> dict[str, Any]:
        self.tick += 1
        observed_at = self._observed_at()
        if not self.claims_on_release_only:
            self.open_claim(observed_at)
        identity = {**IDENTITY, "capabilities": sorted(self._ctx.capabilities)}
        records = sorted(
            (entry["record"] for entry in self.commands.values()),
            key=lambda record: record["sequence"],
        )
        current = None
        if self.active_attempt is not None:
            current = self._attempt_doc(self.active_attempt, observed_at)
        if self.startup:
            instance_state, startup_check = "starting", "fake_startup"
        elif self.recovery_pending:
            instance_state, startup_check = "recovering", None
        elif self.active_attempt is not None:
            instance_state = "active" if self.intake == "open" else "draining"
            startup_check = None
        else:
            instance_state = "idle" if self.intake == "open" else "paused"
            startup_check = None
        return {
            "generated_at": observed_at,
            "identity": identity,
            "instance": {
                "state": instance_state,
                "startup_check": startup_check,
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
            # Like the stub, a stop with an active attempt only closes the
            # intake: it is accepted and finishes with the attempt.
            acknowledgement = "accepted" if self.active_attempt is not None else "completed"
            record = self._record(request_id, "stop", acknowledgement)
        elif operation == "resume":
            if self.intake == "open":
                return producer_suite.DockerResult(
                    1, _fail_with_request("rejected", request_id), ""
                )
            self.intake = "open"
            record = self._record(request_id, "resume", "completed")
        elif operation in ("stop_after", "next_issue"):
            record = self._record(request_id, operation, "accepted", payload)
        elif operation == "handoff":
            if self.active_attempt is None:
                return producer_suite.DockerResult(
                    1, _fail_with_request("rejected", request_id), ""
                )
            if self.crash_finalize:
                self.crash_finalize = False
                self.alive = False
                self.recovery_pending = True
                if self.crash_before_reply:
                    # Commit the handoff, then die before replying: the
                    # consumer sees agent_unreachable and recovery resolves it.
                    self._record(request_id, operation, "accepted", payload)
                    return producer_suite.DockerResult(6, _fail("agent_unreachable"), "")
                record = self._record(request_id, operation, "not_fulfilled", payload)
            elif self.handoff_finish_after is not None:
                # Cooperative handoff: committed now, completed by a later read.
                record = self._record(request_id, operation, "accepted", payload)
                self.pending_handoff = {"request_id": request_id, "reads": 0}
            else:
                record = self._record(request_id, operation, "completed", payload)
                self._finish_active("finished", "handoff")
        elif operation in ("recovery_retry", "recovery_release"):
            attempt_id = str(payload["attempt_id"])
            where = self._find_attempt(attempt_id)
            if where is None:
                return producer_suite.DockerResult(
                    5, _fail_with_request("not_found", request_id), ""
                )
            if where == "active" and not self.accept_active_recovery:
                return producer_suite.DockerResult(
                    1, _fail_with_request("rejected", request_id), ""
                )
            release = operation == "recovery_release"
            if where == "finished" and release and not self.release_finalized_accepted:
                return producer_suite.DockerResult(
                    1, _fail_with_request("rejected", request_id), ""
                )
            record = self._record(request_id, operation, "completed", payload)
            if where in ("retained", "active"):
                self._finish_active("released" if release else "finished", None)
                self.recovery_pending = False
            elif release or self.retry_finalized_changes:
                for summary in self.finished_attempts:
                    if summary["attempt_id"] == attempt_id:
                        summary["lifecycle_state"] = "released"
        elif operation == "errors_reset":
            if not self.error_guard:
                return producer_suite.DockerResult(
                    1, _fail_with_request("rejected", request_id), ""
                )
            self.error_guard = False
            record = self._record(request_id, operation, "completed", payload)
        else:
            raise AssertionError(f"unexpected fake mutation: {operation!r}")
        return producer_suite.DockerResult(0, _ok({"command": record, "replayed": False}), "")

    def _advance_handoff(self, request_id: str) -> None:
        """Complete a cooperative handoff once enough ``command`` reads passed."""
        pending = self.pending_handoff
        if pending is None or pending["request_id"] != request_id:
            return
        pending["reads"] += 1
        assert self.handoff_finish_after is not None
        if pending["reads"] < self.handoff_finish_after:
            return
        self.pending_handoff = None
        record = self.commands[request_id]["record"]
        record["acknowledgement"] = "completed"
        record["detail"] = "handoff completed"
        self._finish_active("finished", "handoff")

    def resolve_interrupted_handoffs(self) -> None:
        """Recovery resolves every still-accepted handoff record."""
        for entry in self.commands.values():
            record = entry["record"]
            if record["kind"] == "handoff" and record["acknowledgement"] == "accepted":
                record["acknowledgement"] = self.recovery_handoff_ack
                record["detail"] = f"handoff {self.recovery_handoff_ack}"

    def handle(
        self,
        docker: str,
        container: str,
        exec_user: str,
        argv: list[str],
        timeout: float,
        *,
        offline: bool = False,
    ) -> producer_suite.DockerResult:
        if not offline and not self.alive:
            # A dead engine answers like the stub: agent_unreachable, exit 6.
            return producer_suite.DockerResult(6, _fail("agent_unreachable"), "")
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
                    manifest = self._manifest_for(summary["attempt_id"])
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
        if (
            len(argv) == 8
            and argv[:4] == ["--contract", "1", "handoff", "now"]
            and argv[4] == "--request-id"
            and argv[6] == "--expect-repo"
        ):
            if "operator-handoff" not in self._ctx.capabilities:
                return producer_suite.DockerResult(
                    3,
                    _fail_with_request(
                        "unsupported",
                        argv[5],
                        {"capability": "operator-handoff"},
                    ),
                    "",
                )
            return self._mutate("handoff", argv[5], argv[7])
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
        if (
            len(argv) == 9
            and argv[:4] == ["--contract", "1", "recovery", "retry"]
            and argv[5] == "--request-id"
            and argv[7] == "--expect-repo"
        ):
            if "recovery-retry" not in self._ctx.capabilities:
                return producer_suite.DockerResult(
                    3,
                    _fail_with_request(
                        "unsupported",
                        argv[6],
                        {"capability": "recovery-retry"},
                    ),
                    "",
                )
            return self._mutate("recovery_retry", argv[6], argv[8], {"attempt_id": argv[4]})
        if (
            len(argv) == 11
            and argv[:4] == ["--contract", "1", "recovery", "release"]
            and argv[5] == "--saved-at"
            and argv[7] == "--request-id"
            and argv[9] == "--expect-repo"
        ):
            if "recovery-release" not in self._ctx.capabilities:
                return producer_suite.DockerResult(
                    3,
                    _fail_with_request(
                        "unsupported",
                        argv[8],
                        {"capability": "recovery-release"},
                    ),
                    "",
                )
            return self._mutate(
                "recovery_release",
                argv[8],
                argv[10],
                {"attempt_id": argv[4], "saved_at": argv[6]},
            )
        if (
            len(argv) == 8
            and argv[:4] == ["--contract", "1", "errors", "reset"]
            and argv[4] == "--request-id"
            and argv[6] == "--expect-repo"
        ):
            if "errors-reset" not in self._ctx.capabilities:
                return producer_suite.DockerResult(
                    3,
                    _fail_with_request(
                        "unsupported",
                        argv[5],
                        {"capability": "errors-reset"},
                    ),
                    "",
                )
            return self._mutate("errors_reset", argv[5], argv[7])
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
                self.alive = False
                return producer_suite.DockerResult(137, "", "")
            return self._mutate(argv[2], argv[4], argv[6])
        if len(argv) == 4 and argv[:3] == ["--contract", "1", "command"]:
            self._advance_handoff(argv[3])
            stored = self.commands.get(argv[3])
            if stored is None:
                return producer_suite.DockerResult(5, _fail("not_found"), "")
            return producer_suite.DockerResult(0, _ok({"command": stored["record"]}), "")
        raise AssertionError(f"unexpected agentctl argv: {argv!r}")

    def _offline_identity_doc(self) -> dict[str, Any]:
        """The offline identity: the setup document with no running process."""
        identity: dict[str, Any] = dict(IDENTITY)
        identity["mode"] = "offline"
        identity["process_started_at"] = None
        return identity

    def _offline_status_doc(self) -> dict[str, Any]:
        """The offline status: stopped, no current attempt, and no clock tick."""
        observed_at = f"2026-11-06T09:12:{5 + self.tick:02d}Z"
        records = sorted(
            (entry["record"] for entry in self.commands.values()),
            key=lambda record: record["sequence"],
        )
        return {
            "generated_at": observed_at,
            "identity": self._offline_identity_doc(),
            "instance": {
                "state": "stopped",
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
            "current_attempt": None,
            "last_finished": self.finished_attempts[-1] if self.finished_attempts else None,
            "recent_commands": records[-20:],
            "recent_events": list(self.extra_events),
            "truncated": False,
        }

    def handle_offline(self, argv: list[str]) -> producer_suite.DockerResult:
        """Serve one Offline Read ``docker run`` argv without touching the volume.

        While the fake engine lock is held a live engine owns the volume, so the
        read is refused with ``rejected``/``instance_running`` like the stub.
        Mutations answer ``unsupported`` with the offline mode and store nothing;
        every other read is served from memory with no clock tick.
        """
        if self.engine_lock.acquire(blocking=False):
            self.engine_lock.release()
        else:
            return producer_suite.DockerResult(
                1, _fail("rejected", {"reason": "instance_running"}), ""
            )
        if argv == ["--contract", "1", "--offline", "identity"]:
            return producer_suite.DockerResult(0, _ok(self._offline_identity_doc()), "")
        if argv == ["--contract", "1", "--offline", "status"]:
            return producer_suite.DockerResult(0, _ok(self._offline_status_doc()), "")
        if len(argv) >= 4 and argv[:3] == ["--contract", "1", "--offline"]:
            tail = argv[3:]
            is_mutation = tail[:1] in (
                ["stop"],
                ["resume"],
                ["handoff"],
                ["recovery"],
                ["errors"],
            ) or tail[:2] == ["next", "issue"]
            if is_mutation:
                request_id = _probe_request_id(argv)
                return producer_suite.DockerResult(
                    3,
                    _fail_with_request("unsupported", request_id, {"mode": "offline"}),
                    "",
                )
            return self.handle("", "", "", [*argv[:2], *argv[3:]], 0.0, offline=True)
        raise AssertionError(f"unexpected offline argv: {argv!r}")


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
        if verb == "start-container":
            producer.alive = True
            producer.startup = True
            return producer_suite.HarnessResult("ok", {"running": True}, "")
        if verb == "release":
            released = producer.startup
            producer.startup = False
            return producer_suite.HarnessResult("ok", {"released": released}, "")
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
        "sealed-after-outcome",
        "export-torn-read",
        "halt-records-last-halt",
        "publication-pending-recovery",
        "handoff",
        "handoff-crash",
        "recovery-retry",
        "recovery-release",
        "errors-reset",
        "offline-reads",
        "offline-while-running",
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
    assert by_id["sealed-after-outcome"].status == "not_run"
    assert by_id["export-torn-read"].status == "not_run"
    assert by_id["halt-records-last-halt"].status == "not_run"
    assert by_id["publication-pending-recovery"].status == "not_run"
    assert by_id["handoff"].status == "not_run"
    assert by_id["handoff-crash"].status == "not_run"
    assert by_id["recovery-retry"].status == "not_run"
    assert by_id["recovery-release"].status == "not_run"
    assert by_id["errors-reset"].status == "not_run"
    assert by_id["offline-reads"].status == "not_run"
    assert by_id["offline-while-running"].status == "not_run"


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


def test_crash_scenario_restarts_before_the_retry_and_leaves_a_live_instance(
    fakes: _FakeProducer,
) -> None:
    outcome = producer_suite.scenario_crash_between_commit_and_reply(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert fakes.alive
    assert not fakes.startup


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


def test_apply_expect_fail_tolerates_not_run_by_default() -> None:
    outcomes = [producer_suite.ScenarioOutcome("handoff", "not_run", "not advertised")]

    lines, exit_code = producer_suite.apply_expect_fail(outcomes, [])

    assert exit_code == 0
    assert any(line.startswith("NOT RUN handoff") for line in lines)


def test_apply_expect_fail_require_all_fails_on_not_run() -> None:
    outcomes = [
        producer_suite.ScenarioOutcome("identity", "passed"),
        producer_suite.ScenarioOutcome("handoff", "not_run", "not advertised"),
    ]

    _, exit_code = producer_suite.apply_expect_fail(outcomes, [], require_all=True)

    assert exit_code == 1


def test_main_require_all_fails_when_a_scenario_does_not_run(
    fakes: _FakeProducer, capsys: pytest.CaptureFixture[str]
) -> None:
    # The minimal fakes leave harness-driven scenarios not run.
    assert producer_suite.main(["--harness", "fake-harness", "--require-all"]) == 1
    assert "NOT RUN" in capsys.readouterr().out


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
    assert "NOT RUN handoff" in out
    assert "NOT RUN handoff-crash" in out
    assert "NOT RUN recovery-retry" in out
    assert "NOT RUN recovery-release" in out
    assert "NOT RUN errors-reset" in out


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
    """Harness override that supports claims plus archive finality.

    Beyond set-issue, release and finish-attempt it stages ``hold``,
    ``grow-archive`` and the after-outcome seal: finishing under an
    ``after-outcome`` hold records the outcome with a manifest snapshot while
    the archive stays unsealed, and the release seals it with that snapshot.
    """

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
        if verb == "hold":
            producer.hold = {"point": params["point"], "scope": params.get("scope")}
            active = producer.active_attempt
            attempt_id = active["attempt_id"] if isinstance(active, dict) else None
            return producer_suite.HarnessResult(
                "ok", {"point": params["point"], "attempt_id": attempt_id}, ""
            )
        if verb == "crash":
            if params.get("point") == "between-commit-and-reply":
                producer.crash_armed = True
                return producer_suite.HarnessResult("ok", {"point": "between-commit-and-reply"}, "")
            if params.get("point") == "during-finalization":
                producer.crash_finalize = True
                return producer_suite.HarnessResult("ok", {"point": "during-finalization"}, "")
            return producer_suite.HarnessResult(
                "unsupported", None, f"crash-point:{params.get('point')}"
            )
        if verb == "stop-container":
            producer.alive = False
            return producer_suite.HarnessResult("ok", {"running": False}, "")
        if verb == "start-container":
            producer.alive = True
            producer.startup = True
            return producer_suite.HarnessResult("ok", {"running": True}, "")
        if verb == "fail-finalization":
            producer.fail_finalization = True
            return producer_suite.HarnessResult("ok", {"armed": True}, "")
        if verb == "release":
            hold, producer.hold = producer.hold, None
            if producer.startup:
                producer.startup = False
                return producer_suite.HarnessResult("ok", {"released": True}, "")
            retained = producer.active_attempt
            if (
                producer.recovery_pending
                and isinstance(retained, dict)
                and retained.get("retained")
            ):
                # An operator decides a Retained Attempt: a release never clears it.
                return producer_suite.HarnessResult("ok", {"released": False}, "")
            if producer.recovery_pending:
                producer.recovery_pending = False
                producer.resolve_interrupted_handoffs()
                active = producer.active_attempt
                if isinstance(active, dict):
                    if not isinstance(active.get("outcome_native"), str):
                        active["outcome_native"] = "infrastructure_error"
                        active["outcome_reason"] = "terminated"
                    summary = producer._summary_doc(active, "2026-11-06T09:13:00Z")
                    producer.finished_attempts.append(summary)
                    producer.active_attempt = None
                return producer_suite.HarnessResult("ok", {"released": True}, "")
            active = producer.active_attempt
            if (
                isinstance(hold, dict)
                and hold.get("point") == "after-outcome"
                and isinstance(active, dict)
                and isinstance(active.get("outcome_native"), str)
            ):
                attempt_id = str(active["attempt_id"])
                summary = producer._summary_doc(active, "2026-11-06T09:13:00Z")
                producer.finished_attempts.append(summary)
                producer.active_attempt = None
                producer.extra_events.append(
                    {
                        "at": "2026-11-06T09:13:00Z",
                        "level": "info",
                        "event": "sealed",
                        "text": f"sealed {attempt_id}",
                        "attempt_id": attempt_id,
                    }
                )
                return producer_suite.HarnessResult("ok", {"released": True}, "")
            return producer_suite.HarnessResult("ok", {"released": True}, "")
        if verb == "finish-attempt":
            active = producer.active_attempt
            if active is None:
                return producer_suite.HarnessResult("error", None, "no active attempt")
            if producer.crash_finalize:
                # Like the stub, an armed finalization crash fires here too.
                producer.crash_finalize = False
                producer.alive = False
                producer.recovery_pending = True
                return producer_suite.HarnessResult(
                    "ok",
                    {
                        "attempt_id": active["attempt_id"],
                        "native_outcome": params["native_outcome"],
                    },
                    "",
                )
            active["outcome_native"] = params["native_outcome"]
            active["outcome_reason"] = params.get("reason")
            if params["native_outcome"] == "infrastructure_error":
                producer.error_guard = True
            if producer.fail_finalization:
                # The verdict is recorded, then finalization fails at its boundary.
                producer.fail_finalization = False
                active["retained"] = True
                producer.recovery_pending = True
                return producer_suite.HarnessResult(
                    "ok",
                    {
                        "attempt_id": active["attempt_id"],
                        "native_outcome": params["native_outcome"],
                    },
                    "",
                )
            if isinstance(producer.hold, dict) and producer.hold.get("point") == "after-outcome":
                attempt_id = str(active["attempt_id"])
                producer.snapshots[attempt_id] = producer._fresh_manifest(attempt_id)
                return producer_suite.HarnessResult(
                    "ok",
                    {"attempt_id": attempt_id, "native_outcome": params["native_outcome"]},
                    "",
                )
            producer.finished_attempts.append(producer._summary_doc(active, "2026-11-06T09:13:00Z"))
            producer.active_attempt = None
            return producer_suite.HarnessResult(
                "ok",
                {
                    "attempt_id": active["attempt_id"],
                    "native_outcome": params["native_outcome"],
                },
                "",
            )
        if verb == "grow-archive":
            attempt_id = params.get("attempt_id")
            if not isinstance(attempt_id, str):
                active = producer.active_attempt
                attempt_id = active["attempt_id"] if isinstance(active, dict) else None
            if not isinstance(attempt_id, str) or not attempt_id:
                return producer_suite.HarnessResult("error", None, "no active attempt to grow")
            if any(summary["attempt_id"] == attempt_id for summary in producer.finished_attempts):
                return producer_suite.HarnessResult(
                    "error", None, "the archive is sealed and immutable"
                )
            append_bytes = int(params["append_bytes"])
            grown = producer.grown.get(attempt_id, b"") + b"0" * append_bytes
            producer.grown[attempt_id] = grown
            return producer_suite.HarnessResult(
                "ok", {"path": "transcript.jsonl", "appended_bytes": append_bytes}, ""
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


def _export_summary(attempt_id: str) -> dict[str, Any]:
    """A full AttemptSummary for a standalone export tar."""
    return {
        "attempt_id": attempt_id,
        "issue": {"number": 42, "title": "Issue 42"},
        "started_at": TIMESTAMP,
        "finished_at": None,
        "lifecycle_state": "open",
        "outcome": None,
        "delivery": None,
        "attention_required": False,
        "sealed": False,
        "sealed_at": None,
    }


def _export_tar(attempt_id: str, files: dict[str, bytes] | None = None) -> bytes:
    payload_files = {"events.jsonl": b""} if files is None else files
    entries = [
        {
            "path": name,
            "kind": "events" if name == "events.jsonl" else "transcript",
            "media_type": "application/x-ndjson",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        for name, data in payload_files.items()
    ]
    manifest = {"contract": "1.0", "attempt": _export_summary(attempt_id), "files": entries}
    payload = (json.dumps(manifest, sort_keys=True) + "\n").encode()
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        info = tarfile.TarInfo("manifest.json")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
        for name, data in payload_files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
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


def _serve_export(producer: _FakeProducer, argv: list[str]) -> _FakeExportProc:
    """Build the fake's export tar: snapshot manifest plus the live bytes.

    Mirrors the stub producer: ``--path`` narrows the stream (an unknown
    path answers ``not_found`` before streaming), the manifest is the stored
    snapshot when one exists, and the file bytes are always current, so a
    file that grew while unsealed no longer matches its manifest entry.
    """
    attempt_id = argv[4]
    paths = [argv[index + 1] for index, arg in enumerate(argv) if arg == "--path"]
    summaries: list[dict[str, Any]] = []
    if producer.active_attempt is not None:
        summaries.append(producer._summary_doc(producer.active_attempt, None))
    summaries.extend(producer.finished_attempts)
    summary = next((entry for entry in summaries if entry["attempt_id"] == attempt_id), None)
    if summary is None:
        proc = _FakeExportProc(io.BytesIO(b""))
        proc.returncode = 5
        return proc
    entries = producer._manifest_for(attempt_id)
    if paths:
        known = {entry["path"] for entry in entries}
        if any(path not in known for path in paths):
            proc = _FakeExportProc(io.BytesIO(b""))
            proc.returncode = 5
            return proc
        entries = [entry for entry in entries if entry["path"] in paths]
    manifest = {"contract": "1.0", "attempt": summary, "files": entries}
    payload = (json.dumps(manifest, sort_keys=True) + "\n").encode()
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        info = tarfile.TarInfo("manifest.json")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
        for entry in entries:
            data = producer._archive_files(attempt_id)[str(entry["path"])]
            info = tarfile.TarInfo(str(entry["path"]))
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return _FakeExportProc(io.BytesIO(stream.getvalue()))


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


def test_first_byte_read_loses_nothing_from_a_real_export_pipe(tmp_path: Path) -> None:
    payload = bytes(range(256)) * 512
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        f"sys.stdout.buffer.write(bytes(range(256)) * {len(payload) // 256})\n"
    )
    fake_docker.chmod(0o755)

    proc = producer_suite._popen_export(str(fake_docker), "c", "agent", ["--contract", "1"])
    first, _ = producer_suite._read_first_byte(proc, 5.0)
    rest, _ = proc.communicate(timeout=10.0)

    assert first + rest == payload


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


def test_sealed_after_outcome_passes_with_hold_and_release(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, fakes)

    def finality_export(
        docker: str, container: str, exec_user: str, argv: list[str]
    ) -> _FakeExportProc:
        return _serve_export(fakes, argv)

    monkeypatch.setattr(producer_suite, "_popen_export", finality_export)

    outcome = producer_suite.scenario_sealed_after_outcome(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert fakes.eligible == set()


def test_sealed_after_outcome_fails_without_a_sealed_event(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, fakes)
    inner = producer_suite._call_harness

    def no_seal(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        if verb == "release":
            fakes.hold = None
            return producer_suite.HarnessResult("ok", {"released": True}, "")
        return inner(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", no_seal)

    def finality_export(
        docker: str, container: str, exec_user: str, argv: list[str]
    ) -> _FakeExportProc:
        return _serve_export(fakes, argv)

    monkeypatch.setattr(producer_suite, "_popen_export", finality_export)

    outcome = producer_suite.scenario_sealed_after_outcome(CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "sealed event" in outcome.detail


def test_export_torn_read_passes_when_a_file_grows(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, fakes)

    def finality_export(
        docker: str, container: str, exec_user: str, argv: list[str]
    ) -> _FakeExportProc:
        return _serve_export(fakes, argv)

    monkeypatch.setattr(producer_suite, "_popen_export", finality_export)

    outcome = producer_suite.scenario_export_torn_read(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert fakes.eligible == set()


def test_export_torn_read_fails_when_the_archive_never_grows(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, fakes)
    inner = producer_suite._call_harness

    def no_grow(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        if verb == "grow-archive":
            return producer_suite.HarnessResult(
                "ok", {"path": "transcript.jsonl", "appended_bytes": 0}, ""
            )
        return inner(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", no_grow)

    def finality_export(
        docker: str, container: str, exec_user: str, argv: list[str]
    ) -> _FakeExportProc:
        return _serve_export(fakes, argv)

    monkeypatch.setattr(producer_suite, "_popen_export", finality_export)

    outcome = producer_suite.scenario_export_torn_read(CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "torn read" in outcome.detail


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


def _serve_export_offline(producer: _FakeProducer, argv: list[str]) -> _FakeExportProc:
    """Serve one offline export: strip ``--offline`` and stream the same tar."""
    return _serve_export(producer, [*argv[:2], *argv[3:]])


def _support_offline(monkeypatch: pytest.MonkeyPatch, producer: _FakeProducer) -> None:
    """Harness plus ``docker run`` fakes for the two offline scenarios.

    The claims lifecycle comes from ``_support_claims``; stopping interrupts
    the active Attempt, restart needs startup then recovery releases,
    ``hold-lock`` really holds the fake engine lock, and Offline Reads
    dispatch to the fake without touching a volume.
    """
    _support_claims(monkeypatch, producer)
    inner = producer_suite._call_harness

    def harness(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        if verb == "hold-lock":
            with producer.engine_lock:
                time.sleep(params["duration_ms"] / 1000.0)
            return producer_suite.HarnessResult("ok", {"held_ms": params["duration_ms"]}, "")
        if verb == "stop-container" and producer.active_attempt is not None:
            producer.recovery_pending = True
        result = inner(cmd, verb, params, timeout)
        if verb == "release" and not producer.recovery_pending and producer.active_attempt is None:
            for entry in producer.commands.values():
                record = entry["record"]
                if record["kind"] == "stop" and record["acknowledgement"] == "accepted":
                    record["acknowledgement"] = "completed"
        return result

    monkeypatch.setattr(producer_suite, "_call_harness", harness)

    def fake_run(
        ctx: producer_suite.SuiteContext,
        agentctl_argv: list[str],
        timeout: float | None = None,
    ) -> producer_suite.DockerResult:
        return producer.handle_offline(agentctl_argv)

    def fake_popen_run(
        ctx: producer_suite.SuiteContext, agentctl_argv: list[str]
    ) -> _FakeExportProc:
        return _serve_export_offline(producer, agentctl_argv)

    monkeypatch.setattr(producer_suite, "_docker_run", fake_run)
    monkeypatch.setattr(producer_suite, "_popen_run", fake_popen_run)


def test_offline_reads_passes_with_a_stopped_container(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_offline(monkeypatch, fakes)

    outcome = producer_suite.scenario_offline_reads(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert fakes.eligible == set()


def test_offline_reads_waits_for_startup_and_recovery_before_resume(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_offline(monkeypatch, fakes)
    real_handle = fakes.handle

    def reject_resume_during_recovery(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        if argv[2:3] == ["resume"] and (fakes.startup or fakes.recovery_pending):
            request_id = argv[argv.index("--request-id") + 1]
            return producer_suite.DockerResult(1, _fail_with_request("rejected", request_id), "")
        return real_handle(docker, container, exec_user, argv, timeout)

    monkeypatch.setattr(producer_suite, "_docker_exec", reject_resume_during_recovery)

    outcome = producer_suite.scenario_offline_reads(CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert not fakes.startup
    assert not fakes.recovery_pending
    assert fakes.active_attempt is None
    assert fakes.finished_attempts[0]["outcome"]["native"] == "infrastructure_error"
    assert fakes.intake == "open"


def test_offline_reads_is_not_run_without_an_image(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_offline(monkeypatch, fakes)
    ctx = producer_suite.SuiteContext(
        container=CTX.container,
        exec_user=CTX.exec_user,
        repository=CTX.repository,
        capabilities=list(CTX.capabilities),
    )

    outcome = producer_suite.scenario_offline_reads(ctx, ["fake-harness"])

    assert outcome.status == "not_run"


@pytest.mark.parametrize(
    ("defect", "detail"),
    [
        ("unfinished", "finalized with a recorded outcome"),
        ("no-outcome", "finalized with a recorded outcome"),
        ("stop-pending", "stop must complete"),
        ("intake-open", "intake must stay closed"),
        ("resume-not-fulfilled", "resume must complete"),
        ("resume-keeps-intake-closed", "resume must reopen intake"),
    ],
)
def test_offline_reads_checks_recovery_and_resume(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch, defect: str, detail: str
) -> None:
    _support_offline(monkeypatch, fakes)
    real_handle = fakes.handle

    def broken_recovery(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        answered = real_handle(docker, container, exec_user, argv, timeout)
        if not fakes.finished_attempts or answered.exit_code != 0:
            return answered
        doc = json.loads(answered.stdout)
        result = doc["result"]
        if argv[2:4] == ["attempts", "show"]:
            if defect == "unfinished":
                result["attempt"]["finished_at"] = None
            elif defect == "no-outcome":
                result["attempt"]["outcome"] = None
        elif argv[2:3] == ["command"] and defect == "stop-pending":
            result["command"]["acknowledgement"] = "accepted"
        elif argv[2:3] == ["resume"] and defect == "resume-not-fulfilled":
            result["command"]["acknowledgement"] = "not_fulfilled"
        elif argv == list(producer_suite.STATUS_ARGV):
            if defect == "intake-open":
                result["intake"]["state"] = "open"
            elif defect == "resume-keeps-intake-closed":
                result["intake"]["state"] = "closed"
        return producer_suite.DockerResult(0, shared.render_document(doc), "")

    monkeypatch.setattr(producer_suite, "_docker_exec", broken_recovery)

    outcome = producer_suite.scenario_offline_reads(CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert detail in outcome.detail


def test_offline_while_running_passes_with_a_held_lock(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_offline(monkeypatch, fakes)

    outcome = producer_suite.scenario_offline_while_running(
        CTX, ["fake-harness"], duration_ms=800, settle_s=0.05
    )

    assert outcome.status == "passed", outcome.detail


def test_offline_while_running_fails_when_the_read_is_served(
    fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_offline(monkeypatch, fakes)

    def serve_despite_lock(
        ctx: producer_suite.SuiteContext,
        agentctl_argv: list[str],
        timeout: float | None = None,
    ) -> producer_suite.DockerResult:
        del ctx, timeout
        return producer_suite.DockerResult(0, _ok(IDENTITY), "")

    monkeypatch.setattr(producer_suite, "_docker_run", serve_despite_lock)

    outcome = producer_suite.scenario_offline_while_running(
        CTX, ["fake-harness"], duration_ms=800, settle_s=0.05
    )

    assert outcome.status == "failed"


@pytest.fixture
def full_fakes(monkeypatch: pytest.MonkeyPatch) -> _FakeProducer:
    """A fake producer advertising every optional capability."""
    producer = _FakeProducer(FULL_CTX)

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


def test_unsupported_capability_passes_when_everything_advertised(
    full_fakes: _FakeProducer,
) -> None:
    assert producer_suite.scenario_unsupported_capability(FULL_CTX).status == "passed"


def test_handoff_passes_with_claim_replay_and_sealed_verdict(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)

    outcome = producer_suite.scenario_handoff(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert full_fakes.eligible == set()


def test_handoff_rejected_without_an_active_attempt(
    full_fakes: _FakeProducer,
) -> None:
    answered = full_fakes.handle(
        "docker",
        "container",
        "agent",
        [
            "--contract",
            "1",
            "handoff",
            "now",
            "--request-id",
            "req-000000000030",
            "--expect-repo",
            FULL_CTX.repository,
        ],
        30.0,
    )

    assert answered.exit_code == 1
    assert json.loads(answered.stdout)["error"]["code"] == "rejected"


@pytest.mark.parametrize(
    ("scenario", "capability"),
    [
        (producer_suite.scenario_handoff, "operator-handoff"),
        (producer_suite.scenario_handoff_crash, "operator-handoff"),
        (producer_suite.scenario_recovery_retry, "recovery-retry"),
        (producer_suite.scenario_recovery_release, "recovery-release"),
        (producer_suite.scenario_errors_reset, "errors-reset"),
        (producer_suite.scenario_stop_after_restart, "stop-after"),
        (producer_suite.scenario_next_issue_restart, "next-issue"),
    ],
)
def test_capability_scenario_is_not_run_when_capability_missing(
    fakes: _FakeProducer,
    monkeypatch: pytest.MonkeyPatch,
    scenario: Any,
    capability: str,
) -> None:
    _support_claims(monkeypatch, fakes)
    ctx = dataclasses.replace(
        FULL_CTX, capabilities=[name for name in FULL_CTX.capabilities if name != capability]
    )

    outcome = scenario(ctx, ["fake-harness"])

    assert outcome.status == "not_run"
    assert capability in outcome.detail


def test_stop_after_invalid_is_not_run_without_stop_after(fakes: _FakeProducer) -> None:
    ctx = dataclasses.replace(CTX, capabilities=["next-issue"])

    outcome = producer_suite.scenario_stop_after_invalid(ctx)

    assert outcome.status == "not_run"
    assert "stop-after" in outcome.detail


def test_handoff_crash_passes_with_retained_attempt(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)

    outcome = producer_suite.scenario_handoff_crash(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert full_fakes.eligible == set()


def test_handoff_crash_is_not_run_without_crash_support(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    inner = producer_suite._call_harness

    def no_finalize_crash(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        if verb == "crash":
            return producer_suite.HarnessResult(
                "unsupported", None, "crash-point:during-finalization"
            )
        return inner(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", no_finalize_crash)

    outcome = producer_suite.scenario_handoff_crash(FULL_CTX, ["fake-harness"])

    assert outcome.status == "not_run"


def test_handoff_passes_when_accepted_completes_later(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    monkeypatch.setattr(producer_suite, "POLL_INTERVAL_S", 0.0)
    full_fakes.handoff_finish_after = 3

    outcome = producer_suite.scenario_handoff(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    (record,) = (entry["record"] for entry in full_fakes.commands.values())
    assert record["acknowledgement"] == "completed"


def test_handoff_fails_when_the_record_never_leaves_accepted(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    monkeypatch.setattr(producer_suite, "POLL_INTERVAL_S", 0.01)
    full_fakes.handoff_finish_after = 10**9
    ctx = dataclasses.replace(FULL_CTX, timeout=0.2)

    outcome = producer_suite.scenario_handoff(ctx, ["fake-harness"])

    assert outcome.status == "failed"
    assert "accepted" in outcome.detail


def test_handoff_crash_passes_when_the_producer_dies_before_replying(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    monkeypatch.setattr(producer_suite, "POLL_INTERVAL_S", 0.0)
    full_fakes.crash_before_reply = True

    outcome = producer_suite.scenario_handoff_crash(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    (record,) = (entry["record"] for entry in full_fakes.commands.values())
    assert record["acknowledgement"] == "not_fulfilled"
    assert full_fakes.finished_attempts[-1]["attempt_id"] == "fake-1"


def test_handoff_crash_fails_when_recovery_completes_the_handoff(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    monkeypatch.setattr(producer_suite, "POLL_INTERVAL_S", 0.0)
    full_fakes.crash_before_reply = True
    full_fakes.recovery_handoff_ack = "completed"

    outcome = producer_suite.scenario_handoff_crash(FULL_CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "completed" in outcome.detail


def _arm_crash_under_a_claim(producer: _FakeProducer) -> None:
    """Leave what a failed crash scenario leaves: a claim and a crash still armed."""
    producer.eligible.add(299)
    producer._status_reply()
    producer.eligible.discard(299)
    assert producer.active_attempt is not None
    producer.crash_finalize = True


def test_settle_restarts_an_unreachable_instance_so_the_next_scenario_passes(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    _arm_crash_under_a_claim(full_fakes)

    outcome = producer_suite.scenario_handoff(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert full_fakes.alive


def test_settle_fails_when_the_instance_stays_unreachable_after_restart(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    inner = producer_suite._call_harness

    def start_stays_dead(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        result = inner(cmd, verb, params, timeout)
        if verb == "start-container":
            full_fakes.alive = False
        return result

    monkeypatch.setattr(producer_suite, "_call_harness", start_stays_dead)
    _arm_crash_under_a_claim(full_fakes)

    outcome = producer_suite.scenario_handoff(FULL_CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "agent_unreachable" in outcome.detail


def test_recovery_retry_passes_on_a_retained_attempt_and_keeps_the_verdict(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)

    outcome = producer_suite.scenario_recovery_retry(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert full_fakes.eligible == set()


def test_recovery_retry_is_not_found_for_an_unknown_attempt(
    full_fakes: _FakeProducer,
) -> None:
    answered = full_fakes.handle(
        "docker",
        "container",
        "agent",
        [
            "--contract",
            "1",
            "recovery",
            "retry",
            "no-such-attempt",
            "--request-id",
            "req-000000000031",
            "--expect-repo",
            FULL_CTX.repository,
        ],
        30.0,
    )

    assert answered.exit_code == 5
    assert json.loads(answered.stdout)["error"]["code"] == "not_found"


def test_recovery_release_passes_on_a_retained_attempt_and_rejects_the_others(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)

    outcome = producer_suite.scenario_recovery_release(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert full_fakes.eligible == set()


def test_recovery_release_is_not_found_for_an_unknown_attempt(
    full_fakes: _FakeProducer,
) -> None:
    answered = full_fakes.handle(
        "docker",
        "container",
        "agent",
        [
            "--contract",
            "1",
            "recovery",
            "release",
            "no-such-attempt",
            "--saved-at",
            "shelf-999",
            "--request-id",
            "req-000000000032",
            "--expect-repo",
            FULL_CTX.repository,
        ],
        30.0,
    )

    assert answered.exit_code == 5
    assert json.loads(answered.stdout)["error"]["code"] == "not_found"


def test_errors_reset_passes_with_guard_and_rejects_when_clear(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)

    outcome = producer_suite.scenario_errors_reset(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert full_fakes.eligible == set()


def test_errors_reset_is_rejected_when_the_guard_is_clear(
    full_fakes: _FakeProducer,
) -> None:
    answered = full_fakes.handle(
        "docker",
        "container",
        "agent",
        [
            "--contract",
            "1",
            "errors",
            "reset",
            "--request-id",
            "req-000000000033",
            "--expect-repo",
            FULL_CTX.repository,
        ],
        30.0,
    )

    assert answered.exit_code == 1
    assert json.loads(answered.stdout)["error"]["code"] == "rejected"


_RECOVERY_SCENARIOS = [
    pytest.param(producer_suite.scenario_recovery_retry, id="recovery-retry"),
    pytest.param(producer_suite.scenario_recovery_release, id="recovery-release"),
]


@pytest.mark.parametrize("scenario", _RECOVERY_SCENARIOS)
def test_recovery_scenarios_leave_a_clean_instance(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch, scenario: Any
) -> None:
    _support_claims(monkeypatch, full_fakes)

    outcome = scenario(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert not full_fakes.recovery_pending
    assert full_fakes.eligible == set()


@pytest.mark.parametrize("scenario", _RECOVERY_SCENARIOS)
def test_recovery_scenarios_fail_when_an_active_attempt_is_recovered(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch, scenario: Any
) -> None:
    _support_claims(monkeypatch, full_fakes)
    full_fakes.accept_active_recovery = True

    outcome = scenario(FULL_CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "rejected" in outcome.detail


@pytest.mark.parametrize("scenario", _RECOVERY_SCENARIOS)
def test_recovery_scenarios_fail_when_a_retained_attempt_is_last_finished(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch, scenario: Any
) -> None:
    _support_claims(monkeypatch, full_fakes)
    real_handle = full_fakes.handle

    def retained_as_last_finished(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        answered = real_handle(docker, container, exec_user, argv, timeout)
        active = full_fakes.active_attempt
        if argv == list(producer_suite.STATUS_ARGV) and active and active.get("retained"):
            doc = json.loads(answered.stdout)
            doc["result"]["last_finished"] = full_fakes._summary_doc(active, None)
            return producer_suite.DockerResult(0, json.dumps(doc) + "\n", "")
        return answered

    monkeypatch.setattr(producer_suite, "_docker_exec", retained_as_last_finished)

    outcome = scenario(FULL_CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "last_finished" in outcome.detail


def _claim_on_release_only(
    monkeypatch: pytest.MonkeyPatch, producer: _FakeProducer, *, over_retained: bool = False
) -> None:
    """Like a real engine harness: a Claim opens only on ``release``.

    Never on ``set-issue`` or a ``status`` read. Wraps the harness installed
    by ``_support_claims``. ``over_retained`` makes the engine wrongly claim
    on ``release`` while a Retained Attempt exists.
    """
    producer.claims_on_release_only = True
    inner = producer_suite._call_harness

    def harness(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        result = inner(cmd, verb, params, timeout)
        if verb == "release" and result.status == "ok":
            producer.open_claim(producer._observed_at(), over_retained=over_retained)
        return result

    monkeypatch.setattr(producer_suite, "_call_harness", harness)


@pytest.mark.parametrize("scenario", _RECOVERY_SCENARIOS)
def test_recovery_scenarios_pass_when_claims_open_only_on_release(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch, scenario: Any
) -> None:
    _support_claims(monkeypatch, full_fakes)
    _claim_on_release_only(monkeypatch, full_fakes)

    outcome = scenario(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert full_fakes.eligible == set()


@pytest.mark.parametrize("scenario", _RECOVERY_SCENARIOS)
def test_recovery_scenarios_fail_when_a_release_claims_over_a_retained_attempt(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch, scenario: Any
) -> None:
    _support_claims(monkeypatch, full_fakes)
    _claim_on_release_only(monkeypatch, full_fakes, over_retained=True)

    outcome = scenario(FULL_CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "no Claim may open while a Retained Attempt exists" in outcome.detail


def test_recovery_release_fails_when_a_finalized_attempt_is_released(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    full_fakes.release_finalized_accepted = True

    outcome = producer_suite.scenario_recovery_release(FULL_CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "rejected" in outcome.detail


def test_recovery_retry_fails_when_a_finalized_attempt_is_changed(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    full_fakes.retry_finalized_changes = True

    outcome = producer_suite.scenario_recovery_retry(FULL_CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert "change nothing" in outcome.detail


@pytest.mark.parametrize("scenario", _RECOVERY_SCENARIOS)
def test_recovery_scenarios_are_not_run_without_fail_finalization(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch, scenario: Any
) -> None:
    _support_claims(monkeypatch, full_fakes)
    inner = producer_suite._call_harness

    def cannot_fail_finalization(
        cmd: Any, verb: str, params: dict[str, Any], timeout: float
    ) -> producer_suite.HarnessResult:
        if verb == "fail-finalization":
            return producer_suite.HarnessResult("unsupported", None, "verb:fail-finalization")
        return inner(cmd, verb, params, timeout)

    monkeypatch.setattr(producer_suite, "_call_harness", cannot_fail_finalization)

    outcome = scenario(FULL_CTX, ["fake-harness"])

    assert outcome.status == "not_run"
    assert "fail-finalization" in outcome.detail


def _leave_a_retained_attempt(producer: _FakeProducer) -> str:
    """Leave what a failed recovery scenario leaves: a Retained Attempt, no operator."""
    producer.eligible.add(298)
    producer._status_reply()
    producer.eligible.discard(298)
    active = producer.active_attempt
    assert active is not None
    active["outcome_native"] = "complete"
    active["retained"] = True
    producer.recovery_pending = True
    return str(active["attempt_id"])


@pytest.mark.parametrize("capability", ["recovery-release", "recovery-retry"])
@pytest.mark.parametrize("crashed", [False, True])
def test_settle_clears_a_retained_attempt_so_the_next_scenario_passes(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch, capability: str, crashed: bool
) -> None:
    _support_claims(monkeypatch, full_fakes)
    ctx = dataclasses.replace(
        FULL_CTX,
        capabilities=[
            name
            for name in FULL_CTX.capabilities
            if not name.startswith("recovery-") or name == capability
        ],
    )
    producer = _FakeProducer(ctx)
    monkeypatch.setattr(producer_suite, "_docker_exec", producer.handle)
    _support_claims(monkeypatch, producer)
    retained_id = _leave_a_retained_attempt(producer)
    producer.alive = not crashed

    outcome = producer_suite.scenario_handoff(ctx, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert not producer.recovery_pending
    kinds = [entry["record"]["kind"] for entry in producer.commands.values()]
    assert kinds[0] == capability.replace("-", "_")
    first = next(iter(producer.commands.values()))["record"]
    assert first["arguments"]["attempt_id"] == retained_id
    if capability == "recovery-release":
        assert first["arguments"]["saved_at"] == "suite-settle"


def test_settle_fails_naming_the_retained_attempt_without_a_recovery_capability(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = dataclasses.replace(
        FULL_CTX,
        capabilities=[n for n in FULL_CTX.capabilities if not n.startswith("recovery-")],
    )
    producer = _FakeProducer(ctx)
    monkeypatch.setattr(producer_suite, "_docker_exec", producer.handle)
    _support_claims(monkeypatch, producer)
    retained_id = _leave_a_retained_attempt(producer)

    outcome = producer_suite.scenario_handoff(ctx, ["fake-harness"])

    assert outcome.status == "failed"
    assert f"Retained Attempt {retained_id}" in outcome.detail
    assert "recovery-release" in outcome.detail


def test_settle_waits_for_accepted_recovery_before_checking_the_retained_attempt(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    retained_id = _leave_a_retained_attempt(full_fakes)
    real_handle = full_fakes.handle
    recovery_id: str | None = None
    reads = 0

    def delayed_recovery(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        nonlocal recovery_id, reads
        if argv[2:4] == ["recovery", "release"]:
            recovery_id = argv[argv.index("--request-id") + 1]
            record = full_fakes._record(
                recovery_id,
                "recovery_release",
                "accepted",
                {"attempt_id": retained_id, "saved_at": "suite-settle"},
            )
            return producer_suite.DockerResult(0, _ok({"command": record, "replayed": False}), "")
        if recovery_id is not None and argv[2:] == ["command", recovery_id]:
            reads += 1
            if reads == 2:
                full_fakes._finish_active("released", None)
                full_fakes.recovery_pending = False
                full_fakes.commands[recovery_id]["record"]["acknowledgement"] = "completed"
        return real_handle(docker, container, exec_user, argv, timeout)

    monkeypatch.setattr(producer_suite, "_docker_exec", delayed_recovery)

    outcome = producer_suite.scenario_handoff(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert reads == 2


def test_settle_fails_when_the_recovery_command_does_not_clear_the_attempt(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _support_claims(monkeypatch, full_fakes)
    retained_id = _leave_a_retained_attempt(full_fakes)
    real_handle = full_fakes.handle

    def release_that_keeps_it(
        docker: str, container: str, exec_user: str, argv: list[str], timeout: float
    ) -> producer_suite.DockerResult:
        if argv[2:4] == ["recovery", "release"]:
            request_id = argv[argv.index("--request-id") + 1]
            record = full_fakes._record(
                request_id,
                "recovery_release",
                "completed",
                {"attempt_id": argv[4], "saved_at": argv[6]},
            )
            return producer_suite.DockerResult(0, _ok({"command": record, "replayed": False}), "")
        return real_handle(docker, container, exec_user, argv, timeout)

    monkeypatch.setattr(producer_suite, "_docker_exec", release_that_keeps_it)

    outcome = producer_suite.scenario_handoff(FULL_CTX, ["fake-harness"])

    assert outcome.status == "failed"
    assert f"Retained Attempt {retained_id}" in outcome.detail
    assert "still the current_attempt" in outcome.detail


def test_settle_leaves_a_recovering_instance_without_a_retained_attempt_alone(
    full_fakes: _FakeProducer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pending recovery the engine finishes by itself is not an operator decision."""
    _support_claims(monkeypatch, full_fakes)
    full_fakes.eligible.add(297)
    full_fakes._status_reply()
    full_fakes.eligible.discard(297)
    full_fakes.recovery_pending = True

    outcome = producer_suite.scenario_handoff(FULL_CTX, ["fake-harness"])

    assert outcome.status == "passed", outcome.detail
    assert not any(
        entry["record"]["kind"].startswith("recovery_") for entry in full_fakes.commands.values()
    )
