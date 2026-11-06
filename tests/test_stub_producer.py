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
