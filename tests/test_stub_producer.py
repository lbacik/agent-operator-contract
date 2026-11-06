"""Unit tests for the stub producer scripts without Docker.

``agentctl`` is executed directly with the package on ``PYTHONPATH``; the
harness verbs that need no container run as-is, while ``setup`` against a
missing Docker CLI must answer ``failed`` instead of crashing.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

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


def test_harness_unimplemented_verb_answers_unsupported() -> None:
    completed = _run_harness(["hold", json.dumps({"verb": "hold", "params": {}})])

    assert completed.returncode == 3
    doc = json.loads(completed.stdout)
    assert documents.validate_harness_reply(doc) == []
    assert doc["error"]["code"] == "unsupported"
    assert doc["error"]["details"] == {"what": "verb", "name": "hold"}


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
