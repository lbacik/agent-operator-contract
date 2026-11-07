"""Unit tests for the stub producer scripts without Docker.

``agentctl`` is executed directly with the package on ``PYTHONPATH``; the
harness verbs that need no container run as-is, while ``setup`` against a
missing Docker CLI must answer ``failed`` instead of crashing.
"""

import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from agent_operator_contract import documents

REPO_ROOT = Path(__file__).parent.parent
AGENTCTL = REPO_ROOT / "stub-producer" / "agentctl"
HARNESS = REPO_ROOT / "stub-producer" / "harness"
PACKAGE_SRC = REPO_ROOT / "src"


def _agentctl_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(PACKAGE_SRC) + os.pathsep + env.get("PYTHONPATH", "")
    env.setdefault("CONTRACT_REPOSITORY", "example-owner/example-repo")
    env.setdefault("CONTRACT_CAPABILITIES", "next-issue,stop-after")
    if extra:
        env.update(extra)
    return env


def _run_agentctl(argv: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(AGENTCTL), *argv],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )


def _durable_env(tmp_path: Path) -> dict[str, str]:
    state_dir = tmp_path / "volume"
    state_dir.mkdir()
    env = _agentctl_env()
    env["CONTRACT_STATE_DIR"] = str(state_dir)
    return env


def _stop(argv_repo: str, request_id: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return _run_agentctl(
        [
            "--contract",
            "1",
            "stop",
            "--request-id",
            request_id,
            "--expect-repo",
            argv_repo,
        ],
        env,
    )


def _command(request_id: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return _run_agentctl(["--contract", "1", "command", request_id], env)


def test_agentctl_identity() -> None:
    completed = _run_agentctl(["--contract", "1", "identity"], _agentctl_env())

    assert completed.returncode == 0
    assert completed.stdout.endswith("\n")
    assert completed.stdout.count("\n") == 1
    doc = json.loads(completed.stdout)
    assert documents.validate_envelope(doc) == []
    assert documents.validate_identity(doc["result"]) == []
    assert doc["result"]["repository"] == "example-owner/example-repo"
    assert doc["result"]["mode"] == "online"
    assert doc["result"]["capabilities"] == ["next-issue", "stop-after"]


def test_agentctl_identity_offline_has_null_started_at() -> None:
    completed = _run_agentctl(["--contract", "1", "--offline", "identity"], _agentctl_env())

    assert completed.returncode == 0
    doc = json.loads(completed.stdout)
    assert doc["result"]["mode"] == "offline"
    assert doc["result"]["process_started_at"] is None


def test_agentctl_status_serves_an_idle_snapshot() -> None:
    completed = _run_agentctl(["--contract", "1", "status"], _agentctl_env())

    assert completed.returncode == 0
    assert completed.stdout.endswith("\n")
    assert completed.stdout.count("\n") == 1
    doc = json.loads(completed.stdout)
    assert documents.validate_envelope(doc) == []
    snapshot = doc["result"]
    assert documents.validate_status(snapshot) == []
    assert snapshot["identity"]["repository"] == "example-owner/example-repo"
    assert snapshot["identity"]["mode"] == "online"
    assert snapshot["instance"]["state"] == "idle"
    assert snapshot["current_attempt"] is None


def test_agentctl_status_offline_serves_a_stopped_snapshot() -> None:
    completed = _run_agentctl(["--contract", "1", "--offline", "status"], _agentctl_env())

    assert completed.returncode == 0
    doc = json.loads(completed.stdout)
    assert documents.validate_envelope(doc) == []
    snapshot = doc["result"]
    assert documents.validate_status(snapshot) == []
    assert snapshot["identity"]["mode"] == "offline"
    assert snapshot["identity"]["process_started_at"] is None
    assert snapshot["instance"]["state"] == "stopped"


def test_agentctl_contract_mismatch() -> None:
    completed = _run_agentctl(["--contract", "2", "status"], _agentctl_env())

    assert completed.returncode == 4
    doc = json.loads(completed.stdout)
    assert doc["error"]["code"] == "contract_mismatch"
    assert doc["error"]["details"] == {"supported": [1]}


def test_agentctl_usage() -> None:
    completed = _run_agentctl(["--contract", "1", "stop", "--after"], _agentctl_env())

    assert completed.returncode == 2
    assert json.loads(completed.stdout)["error"]["code"] == "usage"


def test_agentctl_unsupported_capability() -> None:
    completed = _run_agentctl(
        [
            "--contract",
            "1",
            "handoff",
            "now",
            "--request-id",
            "req-ABC123",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        _agentctl_env(),
    )

    assert completed.returncode == 3
    doc = json.loads(completed.stdout)
    assert doc["error"]["code"] == "unsupported"
    assert doc["error"]["details"] == {"capability": "operator-handoff"}


def test_stub_stop_commits_and_closes_intake(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    completed = _stop("example-owner/example-repo", "req-000000000001", env)

    assert completed.returncode == 0
    doc = json.loads(completed.stdout)
    assert documents.validate_envelope(doc) == []
    assert documents.validate_mutation_result(doc["result"]) == []
    assert doc["result"]["replayed"] is False
    record = doc["result"]["command"]
    assert record["request_id"] == "req-000000000001"
    assert record["kind"] == "stop"
    assert record["acknowledgement"] == "completed"

    status = _run_agentctl(["--contract", "1", "status"], env)
    snapshot = json.loads(status.stdout)["result"]
    assert documents.validate_status(snapshot) == []
    assert snapshot["intake"]["state"] == "closed"
    assert [command["request_id"] for command in snapshot["recent_commands"]] == [
        "req-000000000001"
    ]


def test_stub_identical_resend_replays_without_side_effects(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    first = _stop("example-owner/example-repo", "req-000000000002", env)
    second = _stop("example-owner/example-repo", "req-000000000002", env)

    assert second.returncode == 0
    replayed = json.loads(second.stdout)
    assert replayed["result"]["replayed"] is True
    assert replayed["result"]["command"] == json.loads(first.stdout)["result"]["command"]


def test_stub_same_request_id_different_payload_is_rejected(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    committed = _stop("example-owner/example-repo", "req-000000000003", env)

    assert committed.returncode == 0
    different = _run_agentctl(
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            "req-000000000003",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )

    assert different.returncode == 1
    doc = json.loads(different.stdout)
    assert doc["error"]["code"] == "payload_mismatch"

    stored = _command("req-000000000003", env)
    assert (
        json.loads(stored.stdout)["result"]["command"]
        == json.loads(committed.stdout)["result"]["command"]
    )


def test_stub_wrong_expect_repo_on_any_mutation_stores_nothing(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    for operation in ("stop", "resume"):
        argv = [operation] if operation == "stop" else ["resume"]
        completed = _run_agentctl(
            [
                "--contract",
                "1",
                *argv,
                "--request-id",
                f"req-00000000{operation}",
                "--expect-repo",
                "example-owner/other-repo",
            ],
            env,
        )

        assert completed.returncode == 1
        doc = json.loads(completed.stdout)
        assert doc["error"]["code"] == "repository_mismatch"
        assert doc["error"]["request_id"] == f"req-00000000{operation}"

        stored = _command(f"req-00000000{operation}", env)
        assert stored.returncode == 5
        assert json.loads(stored.stdout)["error"]["code"] == "not_found"


def test_stub_resume_reopens_and_second_resume_is_rejected(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    assert _stop("example-owner/example-repo", "req-000000000004", env).returncode == 0

    reopened = _run_agentctl(
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            "req-000000000005",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )

    assert reopened.returncode == 0
    doc = json.loads(reopened.stdout)
    assert documents.validate_mutation_result(doc["result"]) == []
    assert doc["result"]["command"]["kind"] == "resume"

    refused = _run_agentctl(
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            "req-000000000006",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )

    assert refused.returncode == 1
    assert json.loads(refused.stdout)["error"]["code"] == "rejected"
    assert _command("req-000000000006", env).returncode == 5


def test_stub_resume_supersedes_a_pending_stop_plan(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    planned = _run_agentctl(
        [
            "--contract",
            "1",
            "stop",
            "--after",
            "3",
            "--request-id",
            "req-000000000010",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )

    assert planned.returncode == 0
    assert json.loads(planned.stdout)["result"]["command"]["kind"] == "stop_after"

    reopened = _run_agentctl(
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            "req-000000000011",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )

    assert reopened.returncode == 0
    doc = json.loads(reopened.stdout)
    assert doc["result"]["command"]["kind"] == "resume"
    assert (
        json.loads(_command("req-000000000010", env).stdout)["result"]["command"]["acknowledgement"]
        == "superseded"
    )


def _next_issue(
    issue: int, request_id: str, env: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return _run_agentctl(
        [
            "--contract",
            "1",
            "next",
            "issue",
            str(issue),
            "--request-id",
            request_id,
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )


def test_stub_stop_after_zero_is_invalid_argument(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    refused = _run_agentctl(
        [
            "--contract",
            "1",
            "stop",
            "--after",
            "0",
            "--request-id",
            "req-000000000020",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )

    assert refused.returncode == 1
    assert json.loads(refused.stdout)["error"]["code"] == "invalid_argument"
    assert _command("req-000000000020", env).returncode == 5
    assert _stop("example-owner/example-repo", "req-000000000020", env).returncode == 0


def test_stub_next_issue_commits_priority_and_claims_it(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 122, True)
    _patch_state(env, "doc['hold'] = {'point': 'before-claim', 'scope': 'next'}")

    prioritized = _next_issue(122, "req-000000000021", env)

    assert prioritized.returncode == 0
    record = json.loads(prioritized.stdout)["result"]["command"]
    assert record["kind"] == "next_issue"
    assert record["acknowledgement"] == "accepted"
    held = _status(env)
    assert held["current_attempt"] is None
    assert held["intake"]["priority"]["issue"] == 122  # type: ignore[index]

    _patch_state(env, "doc['hold'] = None")
    snapshot = _status(env)

    assert snapshot["current_attempt"]["issue"]["number"] == 122  # type: ignore[index]
    assert snapshot["intake"]["priority"] is None  # type: ignore[index]
    stored = _command("req-000000000021", env)
    assert json.loads(stored.stdout)["result"]["command"]["acknowledgement"] == "completed"


def test_stub_next_issue_supersedes_the_previous_priority(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 122, True)
    _patch_state(env, "doc['hold'] = {'point': 'before-claim', 'scope': 'next'}")

    assert _next_issue(121, "req-000000000022", env).returncode == 0
    assert _next_issue(122, "req-000000000023", env).returncode == 0

    _patch_state(env, "doc['hold'] = None")
    snapshot = _status(env)

    assert snapshot["current_attempt"]["issue"]["number"] == 122  # type: ignore[index]
    assert (
        json.loads(_command("req-000000000022", env).stdout)["result"]["command"]["acknowledgement"]
        == "superseded"
    )
    assert (
        json.loads(_command("req-000000000023", env).stdout)["result"]["command"]["acknowledgement"]
        == "completed"
    )


def test_stub_next_issue_ineligible_at_commit_is_rejected(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 124, False)

    refused = _next_issue(124, "req-000000000024", env)

    assert refused.returncode == 1
    assert json.loads(refused.stdout)["error"]["code"] == "rejected"
    assert _command("req-000000000024", env).returncode == 5
    assert _stop("example-owner/example-repo", "req-000000000024", env).returncode == 0


def test_stub_next_issue_ineligible_at_claim_is_not_fulfilled(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 123, True)
    _set_issue(env, 124, True)
    _patch_state(env, "doc['hold'] = {'point': 'before-claim', 'scope': 'next'}")

    assert _next_issue(124, "req-000000000025", env).returncode == 0
    _set_issue(env, 124, False)
    _patch_state(env, "doc['hold'] = None")
    snapshot = _status(env)

    assert snapshot["current_attempt"]["issue"]["number"] == 123  # type: ignore[index]
    stored = _command("req-000000000025", env)
    assert json.loads(stored.stdout)["result"]["command"]["acknowledgement"] == "not_fulfilled"


def test_stub_command_found_and_not_found(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    assert _stop("example-owner/example-repo", "req-000000000007", env).returncode == 0

    found = _command("req-000000000007", env)

    assert found.returncode == 0
    doc = json.loads(found.stdout)
    assert documents.validate_envelope(doc) == []
    assert documents.validate_command_result(doc["result"]) == []
    assert doc["result"]["command"]["request_id"] == "req-000000000007"

    missing = _command("req-000000000008", env)

    assert missing.returncode == 5
    assert json.loads(missing.stdout)["error"]["code"] == "not_found"


def test_stub_invalid_request_id_is_invalid_argument(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    completed = _stop("example-owner/example-repo", "not a valid id!", env)

    assert completed.returncode == 1
    assert json.loads(completed.stdout)["error"]["code"] == "invalid_argument"


def test_stub_intake_is_durable_offline(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    assert _stop("example-owner/example-repo", "req-000000000009", env).returncode == 0

    offline = _run_agentctl(["--contract", "1", "--offline", "status"], env)
    snapshot = json.loads(offline.stdout)["result"]

    assert offline.returncode == 0
    assert snapshot["instance"]["state"] == "stopped"
    assert snapshot["intake"]["state"] == "closed"

    read = _run_agentctl(["--contract", "1", "--offline", "command", "req-000000000009"], env)

    assert read.returncode == 0
    assert json.loads(read.stdout)["result"]["command"]["kind"] == "stop"

    state_files = list((tmp_path / "volume").glob("*.json"))
    assert state_files, "the stub keeps its Intake and command store on its volume"


@pytest.mark.parametrize(
    ("operation", "request_id"),
    [
        (["stop"], "req-000000000041"),
        (["resume"], "req-000000000042"),
        (["stop", "--after", "2"], "req-000000000043"),
        (["next", "issue", "7"], "req-000000000044"),
        (["handoff", "now"], "req-000000000046"),
        (["recovery", "retry", "attempt-1"], "req-000000000047"),
        (
            ["recovery", "release", "attempt-1", "--saved-at", "somewhere"],
            "req-000000000048",
        ),
        (["errors", "reset"], "req-000000000049"),
    ],
)
def test_stub_offline_mutations_are_unsupported_and_store_nothing(
    tmp_path: Path, operation: list[str], request_id: str
) -> None:
    env = _durable_env(tmp_path)
    argv = [
        "--contract",
        "1",
        "--offline",
        *operation,
        "--request-id",
        request_id,
        "--expect-repo",
        "example-owner/example-repo",
    ]
    refused = _run_agentctl(argv, env)

    assert refused.returncode == 3, refused.stdout + refused.stderr
    doc = json.loads(refused.stdout)
    assert documents.validate_envelope(doc) == []
    assert doc["error"]["code"] == "unsupported"
    assert doc["error"]["details"] == {"mode": "offline"}
    assert doc["error"]["request_id"] == request_id

    missing = _run_agentctl(["--contract", "1", "command", request_id], env)
    assert missing.returncode == 5
    assert json.loads(missing.stdout)["error"]["code"] == "not_found"


def test_stub_offline_reads_serve_every_read_from_the_volume(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 302, True)
    claimed = _status(env)
    current = claimed["current_attempt"]
    assert isinstance(current, dict)
    assert isinstance(current["attempt_id"], str)
    attempt_id: str = current["attempt_id"]
    assert _stop("example-owner/example-repo", "req-000000000045", env).returncode == 0

    identity = _run_agentctl(["--contract", "1", "--offline", "identity"], env)
    assert identity.returncode == 0, identity.stdout + identity.stderr
    identity_doc = json.loads(identity.stdout)
    assert documents.validate_envelope(identity_doc) == []
    assert documents.validate_identity(identity_doc["result"]) == []
    assert identity_doc["result"]["mode"] == "offline"
    assert identity_doc["result"]["process_started_at"] is None

    offline_status = _run_agentctl(["--contract", "1", "--offline", "status"], env)
    assert offline_status.returncode == 0, offline_status.stdout + offline_status.stderr
    snapshot = json.loads(offline_status.stdout)["result"]
    assert documents.validate_status(snapshot) == []
    assert snapshot["identity"]["mode"] == "offline"
    assert snapshot["identity"]["process_started_at"] is None
    assert snapshot["instance"]["state"] == "stopped"
    assert snapshot["current_attempt"] is None

    read = _run_agentctl(["--contract", "1", "--offline", "command", "req-000000000045"], env)
    assert read.returncode == 0, read.stdout + read.stderr
    assert json.loads(read.stdout)["result"]["command"]["kind"] == "stop"

    listed = _run_agentctl(["--contract", "1", "--offline", "attempts", "list"], env)
    assert listed.returncode == 0, listed.stdout + listed.stderr
    listed_doc = json.loads(listed.stdout)
    assert documents.validate_envelope(listed_doc) == []
    assert attempt_id in [entry["attempt_id"] for entry in listed_doc["result"]["attempts"]]

    shown = _run_agentctl(["--contract", "1", "--offline", "attempts", "show", attempt_id], env)
    assert shown.returncode == 0, shown.stdout + shown.stderr
    shown_doc = json.loads(shown.stdout)
    assert documents.validate_envelope(shown_doc) == []
    assert shown_doc["result"]["attempt"]["attempt_id"] == attempt_id

    exported = _run_agentctl_raw(
        ["--contract", "1", "--offline", "attempts", "export", attempt_id], env
    )
    assert exported.returncode == 0, exported.stderr.decode("utf-8", "replace")
    with tarfile.open(fileobj=io.BytesIO(exported.stdout), mode="r:") as archive:
        names = archive.getnames()
        assert names
        assert names[0] == "manifest.json"
        manifest_file = archive.extractfile("manifest.json")
        assert manifest_file is not None
        manifest = json.loads(manifest_file.read())
        assert manifest["attempt"]["attempt_id"] == attempt_id


def test_stub_offline_reads_write_nothing(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 303, True)
    claimed = _status(env)
    current = claimed["current_attempt"]
    assert isinstance(current, dict)
    assert isinstance(current["attempt_id"], str)
    attempt_id: str = current["attempt_id"]
    assert _stop("example-owner/example-repo", "req-000000000046", env).returncode == 0

    # Prime one offline read: probing the engine lock may create the lock file.
    assert _run_agentctl(["--contract", "1", "--offline", "identity"], env).returncode == 0
    volume = tmp_path / "volume"

    def _tree() -> dict[str, bytes]:
        tree: dict[str, bytes] = {}
        for path in sorted(volume.rglob("*")):
            key = str(path.relative_to(volume))
            tree[key] = path.read_bytes() if path.is_file() else b"<dir>"
        return tree

    before = _tree()

    assert _run_agentctl(["--contract", "1", "--offline", "identity"], env).returncode == 0
    assert _run_agentctl(["--contract", "1", "--offline", "status"], env).returncode == 0
    assert (
        _run_agentctl(
            ["--contract", "1", "--offline", "command", "req-000000000046"], env
        ).returncode
        == 0
    )
    assert _run_agentctl(["--contract", "1", "--offline", "attempts", "list"], env).returncode == 0
    assert (
        _run_agentctl(
            ["--contract", "1", "--offline", "attempts", "show", attempt_id], env
        ).returncode
        == 0
    )
    assert (
        _run_agentctl_raw(
            ["--contract", "1", "--offline", "attempts", "export", attempt_id], env
        ).returncode
        == 0
    )

    after = _tree()
    assert after == before, "offline reads must not write to the volume"


def test_stub_offline_read_refused_while_engine_holds_the_lock(tmp_path: Path) -> None:
    fcntl = pytest.importorskip("fcntl")
    env = _durable_env(tmp_path)
    lock_path = tmp_path / "volume" / "agent-operator-contract.lock"
    with open(lock_path, "a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            refused = _run_agentctl(["--contract", "1", "--offline", "identity"], env)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    fixture_path = (
        REPO_ROOT
        / "src"
        / "agent_operator_contract"
        / "fixtures"
        / "agentctl"
        / "offline-identity-running.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))

    assert refused.returncode == 1
    assert refused.returncode == fixture["exit_code"]
    assert refused.stdout == fixture["stdout"]
    assert refused.stderr == fixture["stderr"]


def test_stub_crash_between_commit_and_reply_replays_on_retry(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _patch_state(env, "doc['crash_armed'] = {'point': 'between-commit-and-reply'}")
    crashed = _stop("example-owner/example-repo", "req-000000000020", env)

    assert crashed.returncode == 6
    doc = json.loads(crashed.stdout)
    assert documents.validate_envelope(doc) == []
    assert doc["error"]["code"] == "agent_unreachable"

    _patch_state(env, "doc['engine_alive'] = True")
    retry = _stop("example-owner/example-repo", "req-000000000020", env)

    assert retry.returncode == 0
    doc = json.loads(retry.stdout)
    assert documents.validate_envelope(doc) == []
    assert documents.validate_mutation_result(doc["result"]) == []
    assert doc["result"]["replayed"] is True
    stored = _command("req-000000000020", env)
    assert json.loads(stored.stdout)["result"]["command"] == doc["result"]["command"]
    status = _run_agentctl(["--contract", "1", "status"], env)
    snapshot = json.loads(status.stdout)["result"]
    assert snapshot["intake"]["state"] == "closed"
    assert [command["request_id"] for command in snapshot["recent_commands"]] == [
        "req-000000000020"
    ]


def test_stub_crash_flag_is_one_shot(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _patch_state(env, "doc['crash_armed'] = {'point': 'between-commit-and-reply'}")
    crashed = _stop("example-owner/example-repo", "req-000000000021", env)

    assert crashed.returncode == 6
    assert json.loads(crashed.stdout)["error"]["code"] == "agent_unreachable"

    # The crash commit closed the intake, so after a restart a resume commits
    # normally: the one-shot was consumed and the process replies.
    _patch_state(env, "doc['engine_alive'] = True")
    reopened = _run_agentctl(
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            "req-000000000022",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )

    assert reopened.returncode == 0
    assert json.loads(reopened.stdout)["result"]["replayed"] is False


def _run_harness(
    argv: list[str], env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(HARNESS), *argv],
        capture_output=True,
        text=True,
        timeout=60,
        env=env or dict(os.environ),
    )


def test_harness_without_a_verb_answers_usage() -> None:
    completed = _run_harness([])

    assert completed.returncode == 2
    doc = json.loads(completed.stdout)
    assert "verb" not in doc
    assert documents.validate_harness_reply(doc) == []


def test_harness_unknown_verb_answers_usage() -> None:
    completed = _run_harness(["frobnicate", json.dumps({"verb": "frobnicate", "params": {}})])

    assert completed.returncode == 2
    assert json.loads(completed.stdout)["error"]["code"] == "usage"


def test_harness_verb_mismatch_answers_usage() -> None:
    completed = _run_harness(["setup", json.dumps({"verb": "teardown", "params": {}})])

    assert completed.returncode == 2


def test_harness_hold_without_instance_answers_failed(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["STUB_HARNESS_STATE"] = str(tmp_path / "state.json")

    completed = _run_harness(["hold", json.dumps({"verb": "hold", "params": {}})], env)

    assert completed.returncode == 1
    doc = json.loads(completed.stdout)
    assert documents.validate_harness_reply(doc) == []
    assert doc["error"]["code"] == "failed"


def test_harness_crash_points_match_library() -> None:
    import runpy

    namespace = runpy.run_path(str(HARNESS), run_name="stub_harness_consts")

    assert tuple(namespace["CRASH_POINTS"]) == documents.CRASH_POINTS


def test_harness_crash_unknown_point_answers_usage(tmp_path: Path) -> None:
    env = _harness_env(tmp_path)
    completed = _run_harness(
        ["crash", json.dumps({"verb": "crash", "params": {"point": "mid-sentence"}})], env
    )

    assert completed.returncode == 2
    doc = json.loads(completed.stdout)
    assert documents.validate_harness_reply(doc) == []
    assert doc["error"]["code"] == "usage"


def test_harness_crash_known_points_are_supported(tmp_path: Path) -> None:
    import runpy

    namespace = runpy.run_path(str(HARNESS), run_name="stub_harness_consts")
    for point in namespace["CRASH_POINTS"]:
        env = _harness_env(tmp_path)
        completed = _run_harness(
            ["crash", json.dumps({"verb": "crash", "params": {"point": point}})], env
        )
        # The stub harness implements every crash point; without a real
        # container the mutation fails at the Docker step, not as unsupported.
        assert completed.returncode == 1, point
        doc = json.loads(completed.stdout)
        assert documents.validate_harness_reply(doc) == []
        assert doc["error"]["code"] == "failed", point


def test_harness_crash_without_setup_answers_failed(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["STUB_HARNESS_STATE"] = str(tmp_path / "state.json")

    completed = _run_harness(
        ["crash", json.dumps({"verb": "crash", "params": {"point": "between-commit-and-reply"}})],
        env,
    )

    assert completed.returncode == 1
    doc = json.loads(completed.stdout)
    assert documents.validate_harness_reply(doc) == []
    assert doc["error"]["code"] == "failed"


def test_harness_crash_arms_the_stub_flag(tmp_path: Path) -> None:
    import runpy

    namespace = runpy.run_path(str(HARNESS), run_name="stub_harness_consts")
    engine_state = tmp_path / "engine-state.json"
    engine_state.write_text(namespace["_FRESH_DOC"], encoding="utf-8")
    # Fake `docker exec ... python3 -` by running the stdin script against a
    # local engine state file instead of a container volume.
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import sys\n"
        "if len(sys.argv) < 2 or sys.argv[1] != 'exec':\n"
        "    sys.exit(0)\n"
        "target = os.environ['FAKE_ENGINE_STATE']\n"
        "script = sys.stdin.read().replace('/data/agent-operator-contract.json', target)\n"
        "exec(compile(script, '<engine>', 'exec'), {})\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"container": "stub-1", "volume": "stub-1-data"}), encoding="utf-8")
    env = dict(os.environ)
    env["DOCKER"] = str(fake_docker)
    env["STUB_HARNESS_STATE"] = str(state)
    env["FAKE_ENGINE_STATE"] = str(engine_state)

    completed = _run_harness(
        ["crash", json.dumps({"verb": "crash", "params": {"point": "between-commit-and-reply"}})],
        env,
    )

    assert completed.returncode == 0
    doc = json.loads(completed.stdout)
    assert documents.validate_harness_reply(doc) == []
    assert doc["result"] == {"point": "between-commit-and-reply"}
    engine = json.loads(engine_state.read_text(encoding="utf-8"))
    assert engine["crash_armed"] == {"point": "between-commit-and-reply"}


def test_harness_teardown_without_state_removes_nothing(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["STUB_HARNESS_STATE"] = str(tmp_path / "state.json")

    completed = _run_harness(["teardown", json.dumps({"verb": "teardown", "params": {}})], env)

    assert completed.returncode == 0
    assert json.loads(completed.stdout)["result"] == {"removed": False}


def test_harness_setup_without_docker_answers_failed(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["STUB_HARNESS_STATE"] = str(tmp_path / "state.json")
    env["DOCKER"] = "nonexistent-docker-binary-for-tests"

    completed = _run_harness(
        [
            "setup",
            json.dumps(
                {
                    "verb": "setup",
                    "params": {
                        "repository": "example-owner/example-repo",
                        "capabilities": ["next-issue", "stop-after"],
                    },
                }
            ),
        ],
        env,
    )

    assert completed.returncode == 1
    doc = json.loads(completed.stdout)
    assert documents.validate_harness_reply(doc) == []
    assert doc["error"]["code"] == "failed"


def _state_file(env: dict[str, str]) -> Path:
    return Path(env["CONTRACT_STATE_DIR"]) / "agent-operator-contract.json"


def _patch_state(env: dict[str, str], mutator: str) -> None:
    path = _state_file(env)
    lines = [
        "import json",
        f"path = {str(path)!r}",
        "try:",
        "    doc = json.load(open(path))",
        "    assert isinstance(doc, dict)",
        "except Exception:",
        "    doc = {}",
        mutator,
        'tmp = path + ".tmp"',
        'json.dump(doc, open(tmp, "w"), sort_keys=True)',
        "import os",
        "os.replace(tmp, path)",
    ]
    script = "\n".join(lines) + "\n"
    subprocess.run([sys.executable, "-c", script], check=True, timeout=60)


def _status(env: dict[str, str]) -> dict[str, object]:
    completed = _run_agentctl(["--contract", "1", "status"], env)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    doc = json.loads(completed.stdout)
    assert documents.validate_envelope(doc) == []
    snapshot = doc["result"]
    assert documents.validate_status(snapshot) == []
    assert isinstance(snapshot, dict)
    return snapshot


def _set_issue(env: dict[str, str], issue: int, eligible: bool) -> None:
    _patch_state(
        env,
        f"doc.setdefault('issues', {{}})[str({issue})]"
        f" = {{'eligible': {eligible!r}, 'title': 'Issue {issue}'}}",
    )


def test_stub_claim_opens_for_an_eligible_issue(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 101, True)

    snapshot = _status(env)

    assert snapshot["current_attempt"] is not None
    assert snapshot["instance"]["state"] == "active"  # type: ignore[index]


def test_stub_hold_before_claim_blocks_the_claim(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 101, True)
    _patch_state(env, "doc['hold'] = {'point': 'before-claim', 'scope': 'next'}")

    snapshot = _status(env)

    assert snapshot["current_attempt"] is None
    assert snapshot["instance"]["state"] == "idle"  # type: ignore[index]


def test_stub_stop_before_claim_then_crash_stays_closed(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 101, True)
    _patch_state(env, "doc['hold'] = {'point': 'before-claim', 'scope': 'next'}")
    assert _stop("example-owner/example-repo", "req-before-claim-1", env).returncode == 0

    _patch_state(env, "doc['engine_alive'] = False\ndoc['recovery_pending'] = False")
    crashed = _run_agentctl(["--contract", "1", "status"], env)
    assert crashed.returncode == 6
    assert json.loads(crashed.stdout)["error"]["code"] == "agent_unreachable"

    _patch_state(env, "doc['engine_alive'] = True\ndoc['startup_hold'] = True")
    starting = _status(env)
    assert starting["instance"]["state"] == "starting"  # type: ignore[index]
    assert starting["current_attempt"] is None

    _patch_state(env, "doc['startup_hold'] = False\ndoc['hold'] = None")
    snapshot = _status(env)

    assert snapshot["intake"]["state"] == "closed"  # type: ignore[index]
    assert snapshot["current_attempt"] is None
    stored = _command("req-before-claim-1", env)
    assert json.loads(stored.stdout)["result"]["command"]["acknowledgement"] == "completed"


def test_stub_stop_after_claim_is_accepted_until_the_attempt_finishes(
    tmp_path: Path,
) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 102, True)
    claimed = _status(env)
    assert claimed["current_attempt"] is not None
    attempt_id = claimed["current_attempt"]["attempt_id"]  # type: ignore[index]

    stopped = _run_agentctl(
        [
            "--contract",
            "1",
            "stop",
            "--request-id",
            "req-after-claim-1",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )
    assert stopped.returncode == 0
    record = json.loads(stopped.stdout)["result"]["command"]
    assert record["acknowledgement"] == "accepted"

    draining = _status(env)
    assert draining["instance"]["state"] == "draining"  # type: ignore[index]
    assert draining["intake"]["pending_stop"] is not None  # type: ignore[index]

    _patch_state(
        env,
        "active = doc.pop('active_attempt')\n"
        "import datetime\n"
        "now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)"
        ".isoformat().replace('+00:00', 'Z')\n"
        "summary = {'attempt_id': active['attempt_id'], "
        "'issue': {'number': active['issue_number'], 'title': active['issue_title']}, "
        "'started_at': active['started_at'], 'finished_at': now, "
        "'lifecycle_state': 'finished', "
        "'outcome': {'native': 'complete', 'reason': None, 'label': 'Complete'}, "
        "'delivery': None, 'attention_required': False, 'sealed': True, "
        "'sealed_at': now}\n"
        "doc.setdefault('attempts', {})[summary['attempt_id']] = summary\n"
        "doc.setdefault('attempt_order', []).append(summary['attempt_id'])\n"
        "doc['last_finished'] = summary",
    )
    finished = _status(env)

    assert finished["current_attempt"] is None
    assert finished["last_finished"]["attempt_id"] == attempt_id  # type: ignore[index]
    stored = _command("req-after-claim-1", env)
    assert json.loads(stored.stdout)["result"]["command"]["acknowledgement"] == "completed"


def test_stub_stop_during_starting_never_opens_a_claim(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 103, True)
    _patch_state(env, "doc['startup_hold'] = True")

    starting = _status(env)
    assert starting["instance"]["state"] == "starting"  # type: ignore[index]

    stopped = _stop("example-owner/example-repo", "req-starting-1", env)
    assert stopped.returncode == 0
    assert json.loads(stopped.stdout)["result"]["command"]["acknowledgement"] == "completed"

    _patch_state(env, "doc['startup_hold'] = False")
    snapshot = _status(env)

    assert snapshot["current_attempt"] is None
    assert snapshot["intake"]["state"] == "closed"  # type: ignore[index]
    assert snapshot["instance"]["state"] == "paused"  # type: ignore[index]


def test_stub_resume_supersedes_an_accepted_stop(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 104, True)
    assert _status(env)["current_attempt"] is not None

    assert _stop("example-owner/example-repo", "req-supersede-stop", env).returncode == 0
    reopened = _run_agentctl(
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            "req-supersede-resume",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )
    assert reopened.returncode == 0

    assert (
        json.loads(_command("req-supersede-stop", env).stdout)["result"]["command"][
            "acknowledgement"
        ]
        == "superseded"
    )
    snapshot = _status(env)
    assert snapshot["intake"]["state"] == "open"  # type: ignore[index]
    assert snapshot["instance"]["state"] == "active"  # type: ignore[index]


def test_stub_halt_is_reported_after_restart_and_offline(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _patch_state(
        env,
        "import datetime\n"
        "now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)"
        ".isoformat().replace('+00:00', 'Z')\n"
        "doc['last_halt'] = {'reason': 'stub-halt', 'detail': '', 'at': now}\n"
        "doc.setdefault('recent_events', []).append({'at': now, 'level': 'error', "
        "'event': 'halted', 'reason': 'stub-halt', 'text': 'halted stub-halt'})\n"
        "doc['engine_alive'] = False",
    )
    dead = _run_agentctl(["--contract", "1", "status"], env)
    assert dead.returncode == 6

    _patch_state(env, "doc['engine_alive'] = True\ndoc['startup_hold'] = True")
    starting = _status(env)
    assert starting["instance"]["last_halt"]["reason"] == "stub-halt"  # type: ignore[index]

    _patch_state(env, "doc['startup_hold'] = False")
    snapshot = _status(env)
    assert snapshot["instance"]["last_halt"]["reason"] == "stub-halt"  # type: ignore[index]
    recent_events = snapshot["recent_events"]
    assert isinstance(recent_events, list)
    events = [
        event
        for event in recent_events
        if isinstance(event, dict) and event.get("event") == "halted"
    ]
    assert events
    last = events[-1]
    assert isinstance(last, dict)
    assert last.get("reason") == "stub-halt"

    offline = _run_agentctl(["--contract", "1", "--offline", "status"], env)
    assert offline.returncode == 0
    offline_snapshot = json.loads(offline.stdout)["result"]
    assert documents.validate_status(offline_snapshot) == []
    assert offline_snapshot["instance"]["state"] == "stopped"
    assert offline_snapshot["instance"]["last_halt"]["reason"] == "stub-halt"


def test_stub_publication_pending_recovers_without_changing_the_verdict(
    tmp_path: Path,
) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 106, True)
    claimed = _status(env)
    assert claimed["current_attempt"] is not None
    attempt_id = claimed["current_attempt"]["attempt_id"]  # type: ignore[index]

    _patch_state(
        env,
        "doc['active_attempt']['outcome_native'] = 'complete'\n"
        "doc['active_attempt']['publication_pending'] = True\n"
        "doc['defer_armed'] = False\n"
        "doc['recovery_pending'] = True",
    )
    recovering = _status(env)
    assert recovering["instance"]["state"] == "recovering"  # type: ignore[index]
    assert recovering["current_attempt"]["attempt_id"] == attempt_id  # type: ignore[index]

    _patch_state(
        env,
        "import datetime\n"
        "now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)"
        ".isoformat().replace('+00:00', 'Z')\n"
        "active = doc['active_attempt']\n"
        "summary = {'attempt_id': active['attempt_id'], "
        "'issue': {'number': active['issue_number'], 'title': active['issue_title']}, "
        "'started_at': active['started_at'], 'finished_at': now, "
        "'lifecycle_state': 'finished', "
        "'outcome': {'native': active['outcome_native'], 'reason': None, "
        "'label': 'Complete'}, 'delivery': None, 'attention_required': False, "
        "'sealed': True, 'sealed_at': now}\n"
        "doc.setdefault('attempts', {})[summary['attempt_id']] = summary\n"
        "doc.setdefault('attempt_order', []).append(summary['attempt_id'])\n"
        "doc['last_finished'] = summary\n"
        "doc['active_attempt'] = None\n"
        "doc['recovery_pending'] = False",
    )
    done = _status(env)
    assert done["last_finished"]["outcome"]["native"] == "complete"  # type: ignore[index]

    assert _stop("example-owner/example-repo", "req-verdict-stop", env).returncode == 0
    resumed = _run_agentctl(
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            "req-verdict-resume",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        env,
    )
    assert resumed.returncode == 0
    again = _status(env)
    assert again["last_finished"]["outcome"]["native"] == "complete"  # type: ignore[index]


def test_stub_attempts_list_and_show_validate(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 107, True)
    claimed = _status(env)
    assert claimed["current_attempt"] is not None
    attempt_id = claimed["current_attempt"]["attempt_id"]  # type: ignore[index]

    listed = _run_agentctl(["--contract", "1", "attempts", "list"], env)
    assert listed.returncode == 0
    assert json.loads(listed.stdout)["result"]["attempts"]

    shown = _run_agentctl(["--contract", "1", "attempts", "show", attempt_id], env)
    assert shown.returncode == 0
    doc = json.loads(shown.stdout)
    assert doc["result"]["attempt"]["attempt_id"] == attempt_id

    missing = _run_agentctl(["--contract", "1", "attempts", "show", "no-such-attempt"], env)
    assert missing.returncode == 5
    assert json.loads(missing.stdout)["error"]["code"] == "not_found"


def test_stub_attempts_list_pages_newest_first_with_opaque_cursor(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _patch_state(
        env,
        "doc.setdefault('attempts', {})\n"
        "doc.setdefault('attempt_order', [])\n"
        "for i in range(1, 26):\n"
        "    started = '2026-11-06T07:' + ('%02d' % i) + ':00Z'\n"
        "    summary = {'attempt_id': 'seeded-' + str(i), "
        "'issue': {'number': 40 + i, 'title': 'Issue ' + str(40 + i)}, "
        "'started_at': started, 'finished_at': '2026-11-06T09:00:00Z', "
        "'lifecycle_state': 'finished', "
        "'outcome': {'native': 'complete', 'reason': None, 'label': 'Complete'}, "
        "'delivery': None, 'attention_required': False, "
        "'sealed': True, 'sealed_at': '2026-11-06T09:00:00Z'}\n"
        "    doc['attempts'][summary['attempt_id']] = summary\n"
        "    doc['attempt_order'].append(summary['attempt_id'])\n"
        "doc['last_finished'] = doc['attempts']['seeded-25']",
    )

    listed = _run_agentctl(["--contract", "1", "attempts", "list"], env)
    assert listed.returncode == 0, listed.stderr
    result = json.loads(listed.stdout)["result"]
    assert documents.validate_attempts_list(result) == []
    assert next(entry["attempt_id"] for entry in result["attempts"]) == "seeded-25"
    assert len(result["attempts"]) == 20
    assert result["next_cursor"] is None or isinstance(result["next_cursor"], str)

    first = _run_agentctl(["--contract", "1", "attempts", "list", "--limit", "1"], env)
    assert first.returncode == 0, first.stderr
    first_doc = json.loads(first.stdout)["result"]
    assert [entry["attempt_id"] for entry in first_doc["attempts"]] == ["seeded-25"]
    cursor = first_doc["next_cursor"]
    assert isinstance(cursor, str)
    assert cursor
    assert cursor not in ("0", "1")

    second = _run_agentctl(
        ["--contract", "1", "attempts", "list", "--limit", "1", "--before", cursor], env
    )
    assert second.returncode == 0, second.stderr
    second_doc = json.loads(second.stdout)["result"]
    assert [entry["attempt_id"] for entry in second_doc["attempts"]] == ["seeded-24"]

    over = _run_agentctl(["--contract", "1", "attempts", "list", "--limit", "101"], env)
    assert over.returncode == 1
    assert json.loads(over.stdout)["error"]["code"] == "invalid_argument"

    bad = _run_agentctl(["--contract", "1", "attempts", "list", "--before", "not-a-cursor"], env)
    assert bad.returncode == 1
    assert json.loads(bad.stdout)["error"]["code"] == "invalid_argument"


def test_stub_attempt_archives_carry_valid_contract_events(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 109, True)
    snapshot = _status(env)
    current = snapshot["current_attempt"]
    assert isinstance(current, dict)
    assert isinstance(current["attempt_id"], str)
    attempt_id: str = current["attempt_id"]

    events = snapshot["recent_events"]
    assert isinstance(events, list)
    assert events
    for entry in events:
        assert documents.validate_contract_event(entry) == []

    shown = _run_agentctl(["--contract", "1", "attempts", "show", attempt_id], env)
    assert shown.returncode == 0, shown.stderr
    manifest = json.loads(shown.stdout)["result"]["manifest"]
    events_entries = [entry for entry in manifest if entry["path"] == "events.jsonl"]
    assert events_entries
    assert all(entry["kind"] == "events" for entry in events_entries)

    completed = _run_agentctl_raw(["--contract", "1", "attempts", "export", attempt_id], env)
    assert completed.returncode == 0, completed.stderr
    with tarfile.open(fileobj=io.BytesIO(completed.stdout), mode="r:") as archive:
        assert archive.getnames()[0] == "manifest.json"
        member = archive.extractfile("events.jsonl")
        assert member is not None
        payload = member.read()
    lines = [line for line in payload.decode("utf-8").splitlines() if line.strip()]
    assert lines
    for line in lines:
        assert documents.validate_contract_event(json.loads(line)) == []


def test_stub_crash_between_commit_and_reply_replays(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _patch_state(env, "doc['crash_armed'] = {'point': 'between-commit-and-reply'}")

    crashed = _stop("example-owner/example-repo", "req-armed-1", env)
    assert crashed.returncode == 6
    assert json.loads(crashed.stdout)["error"]["code"] == "agent_unreachable"

    _patch_state(env, "doc['engine_alive'] = True")
    replayed = _stop("example-owner/example-repo", "req-armed-1", env)
    assert replayed.returncode == 0
    assert json.loads(replayed.stdout)["result"]["replayed"] is True


def _harness_env(tmp_path: Path, container: str = "stub-test-container") -> dict[str, str]:
    env = dict(os.environ)
    state_file = tmp_path / "harness-state.json"
    state_file.write_text(json.dumps({"container": container, "volume": "stub-test-data"}))
    env["STUB_HARNESS_STATE"] = str(state_file)
    env["DOCKER"] = "nonexistent-docker-binary-for-tests"
    return env


def _stub_stack(tmp_path: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Wire the real agentctl and the real harness to one local volume.

    A fake ``docker`` routes the harness ``exec`` snippets at the local engine
    state file (no daemon, no container), while agentctl serves the same
    volume through ``CONTRACT_STATE_DIR``. Returns ``(agentctl_env,
    harness_env)``.
    """
    agentctl_env = _durable_env(tmp_path)
    volume = tmp_path / "volume"
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import sys\n"
        "if len(sys.argv) < 2 or sys.argv[1] != 'exec':\n"
        "    sys.exit(0)\n"
        "state = os.environ['FAKE_ENGINE_STATE']\n"
        "archives = os.environ['FAKE_ENGINE_ARCHIVES']\n"
        "script = sys.stdin.read()\n"
        "script = script.replace('/data/archives', archives)\n"
        "script = script.replace('/data/agent-operator-contract.json', state)\n"
        "exec(compile(script, '<engine>', 'exec'), {})\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    harness_state = tmp_path / "harness-state.json"
    harness_state.write_text(
        json.dumps({"container": "stub-test", "volume": "stub-test-data"}),
        encoding="utf-8",
    )
    harness_env = dict(os.environ)
    harness_env["DOCKER"] = str(fake_docker)
    harness_env["STUB_HARNESS_STATE"] = str(harness_state)
    harness_env["FAKE_ENGINE_STATE"] = str(volume / "agent-operator-contract.json")
    harness_env["FAKE_ENGINE_ARCHIVES"] = str(volume / "archives")
    return agentctl_env, harness_env


def _harness_verb(harness_env: dict[str, str], verb: str, params: dict[str, Any]) -> dict[str, Any]:
    completed = _run_harness([verb, json.dumps({"verb": verb, "params": params})], harness_env)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    doc = json.loads(completed.stdout)
    assert documents.validate_harness_reply(doc) == []
    assert doc["ok"] is True, completed.stdout
    assert isinstance(doc["result"], dict)
    return doc["result"]


def _export_stream(argv: list[str], env: dict[str, str]) -> tuple[int, bytes, bytes]:
    completed = _run_agentctl_raw(argv, env)
    return completed.returncode, completed.stdout, completed.stderr


def _read_export_tar(data: bytes) -> tuple[dict[str, Any], dict[str, bytes], list[str]]:
    members: dict[str, bytes] = {}
    order: list[str] = []
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        for member in archive:
            extracted = archive.extractfile(member)
            order.append(member.name)
            members[member.name] = extracted.read() if extracted is not None else b""
    assert order
    assert order[0] == "manifest.json"
    manifest = json.loads(members["manifest.json"])
    assert isinstance(manifest, dict)
    return manifest, members, order


def _mismatches(manifest: dict[str, Any], members: dict[str, bytes]) -> list[str]:
    problems: list[str] = []
    entries = manifest["files"]
    assert isinstance(entries, list)
    for entry in entries:
        assert isinstance(entry, dict)
        content = members.get(str(entry["path"]))
        assert content is not None, f"tar lacks {entry['path']!r}"
        if len(content) != entry["size"] or hashlib.sha256(content).hexdigest() != entry["sha256"]:
            problems.append(str(entry["path"]))
    return problems


def test_harness_verbs_without_a_container_answer_failed(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["STUB_HARNESS_STATE"] = str(tmp_path / "missing-state.json")
    for verb in (
        "set-issue",
        "hold",
        "hold-lock",
        "release",
        "finish-attempt",
        "crash",
        "terminate",
        "stop-container",
        "start-container",
        "halt",
        "grow-archive",
        "defer-publication",
    ):
        completed = _run_harness([verb, json.dumps({"verb": verb, "params": {}})], env)
        assert completed.returncode == 1, verb
        assert json.loads(completed.stdout)["error"]["code"] == "failed"


def test_harness_hold_lock_validates_duration(tmp_path: Path) -> None:
    env = _harness_env(tmp_path)
    completed = _run_harness(
        ["hold-lock", json.dumps({"verb": "hold-lock", "params": {"duration_ms": 0}})],
        env,
    )

    assert completed.returncode == 2
    assert json.loads(completed.stdout)["error"]["code"] == "usage"


def test_harness_set_issue_validates_params(tmp_path: Path) -> None:
    env = _harness_env(tmp_path)
    completed = _run_harness(
        ["set-issue", json.dumps({"verb": "set-issue", "params": {"issue": 0, "eligible": True}})],
        env,
    )

    assert completed.returncode == 2
    assert json.loads(completed.stdout)["error"]["code"] == "usage"


def test_harness_finish_unknown_outcome_is_unsupported(tmp_path: Path) -> None:
    env = _harness_env(tmp_path)
    completed = _run_harness(
        [
            "finish-attempt",
            json.dumps({"verb": "finish-attempt", "params": {"native_outcome": "bogus"}}),
        ],
        env,
    )

    assert completed.returncode == 3
    doc = json.loads(completed.stdout)
    assert documents.validate_harness_reply(doc) == []
    assert doc["error"]["code"] == "unsupported"
    assert doc["error"]["details"] == {"what": "native-outcome", "name": "bogus"}


def test_harness_setup_unknown_capability_is_unsupported(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["STUB_HARNESS_STATE"] = str(tmp_path / "state.json")
    env["DOCKER"] = "nonexistent-docker-binary-for-tests"
    completed = _run_harness(
        [
            "setup",
            json.dumps(
                {
                    "verb": "setup",
                    "params": {
                        "repository": "example-owner/example-repo",
                        "capabilities": ["time-travel"],
                    },
                }
            ),
        ],
        env,
    )

    assert completed.returncode == 3
    doc = json.loads(completed.stdout)
    assert doc["error"]["details"] == {"what": "capability", "name": "time-travel"}


def _run_agentctl_raw(argv: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[bytes]:
    """Run ``agentctl`` capturing raw bytes (``attempts export`` streams a tar)."""
    return subprocess.run(
        [sys.executable, str(AGENTCTL), *argv],
        capture_output=True,
        timeout=60,
        env=env,
    )


def _agentctl_module(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Load the stub ``agentctl`` as a module so tests can drive its bounds."""
    import runpy

    monkeypatch.syspath_prepend(str(PACKAGE_SRC))
    return runpy.run_path(str(AGENTCTL), run_name="stub_agentctl_bounds")


def _sized_event(index: int, size: int) -> dict[str, Any]:
    return {
        "at": f"2026-01-01T00:{index // 60:02d}:{index % 60:02d}Z",
        "level": "info",
        "event": "note",
        "text": f"event-{index}-" + "x" * size,
    }


def _sized_command(sequence: int, size: int) -> dict[str, Any]:
    return {
        "request_id": f"req-bound-{sequence:06d}",
        "sequence": sequence,
        "kind": "stop",
        "detail": "y" * size,
    }


def test_stub_apply_bounds_drops_oldest_events_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import copy

    namespace = _agentctl_module(monkeypatch)
    events = [_sized_event(index, 6000) for index in range(50)]
    commands = [_sized_command(1, 100)]
    figures = {"load": "small"}
    snapshot: dict[str, Any] = {
        "recent_events": events,
        "recent_commands": commands,
        "current_attempt": {"attempt_id": "stub-1", "figures": figures},
        "truncated": False,
    }
    saved_events = copy.deepcopy(events)
    saved_commands = copy.deepcopy(commands)

    bounded: dict[str, Any] = namespace["_apply_bounds"](snapshot)

    assert namespace["_snapshot_size_bytes"](bounded) <= namespace["SNAPSHOT_MAX_BYTES"]
    assert bounded["truncated"] is True
    assert events == saved_events
    assert commands == saved_commands
    assert bounded["recent_commands"] == commands
    assert bounded["current_attempt"]["figures"] == figures
    remaining = bounded["recent_events"]
    assert len(remaining) < len(events)
    assert remaining == events[len(events) - len(remaining) :]
    assert remaining[-1] == events[-1]


def test_stub_apply_bounds_drops_figures_before_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _agentctl_module(monkeypatch)
    commands = [_sized_command(1, 100), _sized_command(2, 100)]
    snapshot: dict[str, Any] = {
        "recent_events": [_sized_event(0, 100)],
        "recent_commands": commands,
        "current_attempt": {
            "attempt_id": "stub-1",
            "figures": {f"figure-{index}": "y" * 100_000 for index in range(3)},
        },
        "truncated": False,
    }

    bounded: dict[str, Any] = namespace["_apply_bounds"](snapshot)

    assert namespace["_snapshot_size_bytes"](bounded) <= namespace["SNAPSHOT_MAX_BYTES"]
    assert bounded["truncated"] is True
    assert bounded["recent_events"] == []
    assert len(bounded["current_attempt"]["figures"]) < 3
    assert bounded["recent_commands"] == commands


def test_stub_apply_bounds_drops_oldest_commands_last(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = _agentctl_module(monkeypatch)
    commands = [_sized_command(sequence, 60_000) for sequence in range(1, 6)]
    snapshot: dict[str, Any] = {
        "recent_events": [_sized_event(0, 100)],
        "recent_commands": commands,
        "current_attempt": {"attempt_id": "stub-1", "figures": {"load": "small"}},
        "truncated": False,
    }

    bounded: dict[str, Any] = namespace["_apply_bounds"](snapshot)

    assert namespace["_snapshot_size_bytes"](bounded) <= namespace["SNAPSHOT_MAX_BYTES"]
    assert bounded["truncated"] is True
    assert bounded["recent_events"] == []
    assert bounded["current_attempt"]["figures"] == {}
    remaining = bounded["recent_commands"]
    assert 1 <= len(remaining) < len(commands)
    assert remaining == commands[len(commands) - len(remaining) :]
    assert [record["sequence"] for record in remaining] == sorted(
        record["sequence"] for record in remaining
    )


def test_stub_recent_commands_capped_at_twenty(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    for index in range(11):
        assert _stop("example-owner/example-repo", f"req-cap-{index:06d}-stop", env).returncode == 0
        reopened = _run_agentctl(
            [
                "--contract",
                "1",
                "resume",
                "--request-id",
                f"req-cap-{index:06d}-resume",
                "--expect-repo",
                "example-owner/example-repo",
            ],
            env,
        )
        assert reopened.returncode == 0

    snapshot = _status(env)
    recent = snapshot["recent_commands"]
    assert isinstance(recent, list)
    assert len(recent) == 20
    sequences = [record["sequence"] for record in recent if isinstance(record, dict)]
    assert sequences == list(range(3, 23))


def test_stub_capabilities_are_sorted(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    env["CONTRACT_CAPABILITIES"] = "stop-after,next-issue"

    identity = _run_agentctl(["--contract", "1", "identity"], env)

    assert identity.returncode == 0
    assert json.loads(identity.stdout)["result"]["capabilities"] == [
        "next-issue",
        "stop-after",
    ]

    snapshot = _status(env)
    identity_doc = snapshot["identity"]
    assert isinstance(identity_doc, dict)
    assert identity_doc["capabilities"] == ["next-issue", "stop-after"]


def test_stub_status_under_held_lock_serves_prepared_state(tmp_path: Path) -> None:
    fcntl = pytest.importorskip("fcntl")
    env = _durable_env(tmp_path)
    baseline = _status(env)
    baseline_instance = baseline["instance"]
    baseline_intake = baseline["intake"]
    assert isinstance(baseline_instance, dict)
    assert isinstance(baseline_intake, dict)

    lock_path = tmp_path / "volume" / "agent-operator-contract.lock"
    with open(lock_path, "a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            started = time.monotonic()
            answered = _run_agentctl(["--contract", "1", "status"], env)
            elapsed = time.monotonic() - started
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    assert answered.returncode == 0, answered.stderr
    doc = json.loads(answered.stdout)
    assert documents.validate_envelope(doc) == []
    assert documents.validate_status(doc["result"]) == []
    assert elapsed < 3.0
    during = doc["result"]
    assert isinstance(during, dict)
    assert isinstance(during["instance"], dict)
    assert isinstance(during["intake"], dict)
    assert during["instance"]["observed_at"] <= baseline_instance["observed_at"]
    assert during["intake"]["observed_at"] <= baseline_intake["observed_at"]


def test_stub_mutation_waits_for_the_engine_lock(tmp_path: Path) -> None:
    fcntl = pytest.importorskip("fcntl")
    env = _durable_env(tmp_path)
    lock_path = tmp_path / "volume" / "agent-operator-contract.lock"
    lock_path.write_text("", encoding="utf-8")
    outcome: dict[str, Any] = {}

    def _commit() -> None:
        started = time.monotonic()
        completed = _stop("example-owner/example-repo", "req-lock-wait-1", env)
        outcome["elapsed"] = time.monotonic() - started
        outcome["returncode"] = completed.returncode
        outcome["stdout"] = completed.stdout

    with open(lock_path, encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        worker = threading.Thread(target=_commit)
        worker.start()
        time.sleep(1.5)
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    worker.join(timeout=60)

    assert outcome["elapsed"] >= 1.0
    assert outcome["returncode"] == 0
    assert json.loads(outcome["stdout"])["result"]["command"]["request_id"] == "req-lock-wait-1"


def test_stub_attempts_export_streams_manifest_first(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 201, True)
    snapshot = _status(env)
    current = snapshot["current_attempt"]
    assert isinstance(current, dict)
    assert isinstance(current["attempt_id"], str)
    attempt_id: str = current["attempt_id"]

    completed = _run_agentctl_raw(["--contract", "1", "attempts", "export", attempt_id], env)

    assert completed.returncode == 0, completed.stderr
    with tarfile.open(fileobj=io.BytesIO(completed.stdout), mode="r:") as archive:
        names = archive.getnames()
        assert names
        assert names[0] == "manifest.json"
        manifest_file = archive.extractfile("manifest.json")
        assert manifest_file is not None
        manifest = json.loads(manifest_file.read())
        assert manifest["attempt"]["attempt_id"] == attempt_id
        for entry in manifest["files"]:
            member = archive.extractfile(entry["path"])
            assert member is not None
            data = member.read()
            assert len(data) == entry["size"]
            assert hashlib.sha256(data).hexdigest() == entry["sha256"]


def test_stub_attempts_export_unknown_attempt_is_not_found(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)

    completed = _run_agentctl(["--contract", "1", "attempts", "export", "no-such-attempt"], env)

    assert completed.returncode == 5
    assert json.loads(completed.stdout)["error"]["code"] == "not_found"


def test_stub_attempts_export_unknown_path_is_not_found(tmp_path: Path) -> None:
    env = _durable_env(tmp_path)
    _set_issue(env, 202, True)
    snapshot = _status(env)
    current = snapshot["current_attempt"]
    assert isinstance(current, dict)
    assert isinstance(current["attempt_id"], str)

    completed = _run_agentctl(
        ["--contract", "1", "attempts", "export", current["attempt_id"], "--path", "nope.bin"],
        env,
    )

    assert completed.returncode == 5
    assert json.loads(completed.stdout)["error"]["code"] == "not_found"


def _stub_claim(agentctl_env: dict[str, str], harness_env: dict[str, str], issue: int) -> str:
    """Open a claim through the harness and return its attempt id."""
    _harness_verb(harness_env, "set-issue", {"issue": issue, "eligible": True})
    snapshot = _status(agentctl_env)
    current = snapshot["current_attempt"]
    assert isinstance(current, dict)
    attempt_id = current["attempt_id"]
    assert isinstance(attempt_id, str)
    return attempt_id


def _show_attempt(agentctl_env: dict[str, str], attempt_id: str) -> dict[str, Any]:
    shown = _run_agentctl(["--contract", "1", "attempts", "show", attempt_id], agentctl_env)
    assert shown.returncode == 0, shown.stdout + shown.stderr
    doc = json.loads(shown.stdout)
    assert documents.validate_envelope(doc) == []
    result = doc["result"]
    assert documents.validate_attempts_show(result) == []
    assert isinstance(result, dict)
    return result


def test_stub_sealed_after_outcome_freezes_the_manifest(tmp_path: Path) -> None:
    """An outcome recorded mid-finalization seals only after the release."""
    agentctl_env, harness_env = _stub_stack(tmp_path)
    attempt_id = _stub_claim(agentctl_env, harness_env, 301)

    _harness_verb(harness_env, "hold", {"point": "after-outcome", "scope": "active"})
    _harness_verb(harness_env, "finish-attempt", {"native_outcome": "complete"})

    held = _show_attempt(agentctl_env, attempt_id)
    assert held["attempt"]["sealed"] is False
    assert isinstance(held["attempt"]["outcome"], dict)

    code, data, err = _export_stream(
        ["--contract", "1", "attempts", "export", attempt_id], agentctl_env
    )
    assert code == 0, err
    manifest, members, _order = _read_export_tar(data)
    assert documents.validate_export_manifest(manifest) == []
    assert _mismatches(manifest, members) == []

    code, narrowed, err = _export_stream(
        ["--contract", "1", "attempts", "export", attempt_id, "--path", "transcript.jsonl"],
        agentctl_env,
    )
    assert code == 0, err
    narrowed_manifest, narrowed_members, _order = _read_export_tar(narrowed)
    assert [entry["path"] for entry in narrowed_manifest["files"]] == ["transcript.jsonl"]
    assert _mismatches(narrowed_manifest, narrowed_members) == []

    _harness_verb(harness_env, "release", {})

    snapshot = _status(agentctl_env)
    recent_events = snapshot["recent_events"]
    assert isinstance(recent_events, list)
    sealed_events = [
        event
        for event in recent_events
        if isinstance(event, dict)
        and event.get("event") == "sealed"
        and event.get("attempt_id") == attempt_id
    ]
    assert sealed_events, "no sealed event follows the release"

    first_show = _show_attempt(agentctl_env, attempt_id)
    assert first_show["attempt"]["sealed"] is True
    assert first_show["attempt"]["sealed_at"] is not None
    second_show = _show_attempt(agentctl_env, attempt_id)
    assert second_show["manifest"] == first_show["manifest"]

    first_code, first, first_err = _export_stream(
        ["--contract", "1", "attempts", "export", attempt_id], agentctl_env
    )
    assert first_code == 0, first_err
    second_code, second, second_err = _export_stream(
        ["--contract", "1", "attempts", "export", attempt_id], agentctl_env
    )
    assert second_code == 0, second_err
    assert first == second, "the sealed export must be byte-identical"
    sealed_manifest, sealed_members, _order = _read_export_tar(first)
    assert documents.validate_export_manifest(sealed_manifest) == []
    assert _mismatches(sealed_manifest, sealed_members) == []
    sealed_lines = sealed_members["events.jsonl"].decode("utf-8").splitlines()
    assert any('"event": "sealed"' in line for line in sealed_lines)

    sealed_grow = _run_harness(
        [
            "grow-archive",
            json.dumps(
                {
                    "verb": "grow-archive",
                    "params": {"attempt_id": attempt_id, "append_bytes": 16},
                }
            ),
        ],
        harness_env,
    )
    assert sealed_grow.returncode == 1
    assert json.loads(sealed_grow.stdout)["error"]["code"] == "failed"


def test_stub_export_torn_read_after_archive_growth(tmp_path: Path) -> None:
    """A file grown while unsealed tears the next export against its snapshot."""
    agentctl_env, harness_env = _stub_stack(tmp_path)
    attempt_id = _stub_claim(agentctl_env, harness_env, 302)

    code, data, err = _export_stream(
        ["--contract", "1", "attempts", "export", attempt_id], agentctl_env
    )
    assert code == 0, err
    manifest, members, _order = _read_export_tar(data)
    assert documents.validate_export_manifest(manifest) == []
    assert _mismatches(manifest, members) == []

    grown = _harness_verb(
        harness_env, "grow-archive", {"attempt_id": attempt_id, "append_bytes": 64}
    )
    assert grown["path"] == "transcript.jsonl"

    torn_code, torn_data, torn_err = _export_stream(
        ["--contract", "1", "attempts", "export", attempt_id], agentctl_env
    )
    assert torn_code == 0, torn_err
    torn_manifest, torn_members, _order = _read_export_tar(torn_data)
    assert documents.validate_export_manifest(torn_manifest) == []
    assert "transcript.jsonl" in _mismatches(torn_manifest, torn_members)


def test_harness_hold_lock_holds_the_engine_lock(tmp_path: Path) -> None:
    fcntl = pytest.importorskip("fcntl")
    lock_file = tmp_path / "engine.lock"
    lock_file.write_text("", encoding="utf-8")
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import sys\n"
        "if len(sys.argv) < 2 or sys.argv[1] != 'exec':\n"
        "    sys.exit(0)\n"
        "target = os.environ['FAKE_LOCK_PATH']\n"
        "script = sys.stdin.read().replace('/data/agent-operator-contract.lock', target)\n"
        "exec(compile(script, '<engine>', 'exec'), {})\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"container": "stub-1", "volume": "stub-1-data"}))
    env = dict(os.environ)
    env["DOCKER"] = str(fake_docker)
    env["STUB_HARNESS_STATE"] = str(state)
    env["FAKE_LOCK_PATH"] = str(lock_file)
    replies: list[subprocess.CompletedProcess[str]] = []

    def _hold() -> None:
        replies.append(
            _run_harness(
                [
                    "hold-lock",
                    json.dumps({"verb": "hold-lock", "params": {"duration_ms": 2000}}),
                ],
                env,
            )
        )

    holder = threading.Thread(target=_hold)
    started = time.monotonic()
    holder.start()
    deadline = time.monotonic() + 30.0
    saw_locked = False
    while time.monotonic() < deadline:
        with open(lock_file, encoding="utf-8") as probe:
            try:
                fcntl.flock(probe.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                saw_locked = True
                break
            fcntl.flock(probe.fileno(), fcntl.LOCK_UN)
        if replies:
            break
        time.sleep(0.05)
    holder.join(timeout=60)
    elapsed = time.monotonic() - started

    assert saw_locked, "hold-lock never held the engine lock"
    assert elapsed >= 1.5
    assert len(replies) == 1
    assert replies[0].returncode == 0, replies[0].stdout
    doc = json.loads(replies[0].stdout)
    assert documents.validate_harness_reply(doc) == []
    assert doc["result"]["held_ms"] >= 2000
