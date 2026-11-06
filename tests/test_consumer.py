"""Consumer suite: `agentfleet` against the replaying fake `docker`.

The consumer boundary is the `docker` executable: `agentfleet` talks to
Docker only through the configurable `docker` CLI via subprocess. The fake
(`tests/consumer/fake_docker`) stands in for `docker`: it serves a described
Fleet for listing and inspection, answers `exec` of `agentctl` by replaying
fixtures matched on argv, and records every invocation so these tests can
assert what the consumer sent. No real daemon, container or network is used.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from agent_operator_contract import documents

REPO_ROOT = Path(__file__).parent.parent
FAKE_DOCKER = REPO_ROOT / "tests" / "consumer" / "fake_docker"
FIXTURES_DIR = REPO_ROOT / "src" / "agent_operator_contract" / "fixtures" / "agentctl"

CLIENT_ENVELOPE_SCHEMA = documents.load_schema("client-envelope")
LS_SCHEMA = documents.load_schema("ls")
CLIENT_ENVELOPE_VALIDATOR = Draft202012Validator(CLIENT_ENVELOPE_SCHEMA)
LS_VALIDATOR = Draft202012Validator(LS_SCHEMA)

FLEET: dict[str, Any] = {
    "containers": [
        {
            "id": "a" * 64,
            "name": "web-1",
            "backend": "coding-agent",
            "contract": "1",
            "exec_user": "agent",
            "state": "running",
        },
        {
            "id": "b" * 64,
            "name": "web-2",
            "backend": "simple-coding-agent",
            "contract": "1",
            "exec_user": "agent",
            "state": "exited",
        },
        {
            "id": "c" * 64,
            "name": "dup",
            "backend": "coding-agent",
            "contract": "1",
            "exec_user": "agent",
            "state": "running",
        },
        {
            "id": "d" * 64,
            "name": "dup",
            "backend": "coding-agent",
            "contract": "1",
            "exec_user": "agent",
            "state": "exited",
        },
        {
            "id": "e" * 64,
            "name": "worker-run-1",
            "backend": "coding-agent",
            "contract": "1",
            "exec_user": "agent",
            "state": "running",
            "oneoff": True,
        },
    ]
}


@pytest.fixture
def consumer_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fleet file, fixture dir and invocation log wired to the fake docker."""
    fleet_file = tmp_path / "fleet.json"
    fleet_file.write_text(json.dumps(FLEET), encoding="utf-8")
    log_file = tmp_path / "docker.log"
    log_file.write_text("", encoding="utf-8")
    env = {
        **os.environ,
        "AGENTFLEET_DOCKER": str(FAKE_DOCKER),
        "FAKE_DOCKER_FLEET": str(fleet_file),
        "FAKE_DOCKER_FIXTURES": str(FIXTURES_DIR),
        "FAKE_DOCKER_LOG": str(log_file),
    }
    monkeypatch.setenv("AGENTFLEET_DOCKER", str(FAKE_DOCKER))
    monkeypatch.setenv("FAKE_DOCKER_FLEET", str(fleet_file))
    monkeypatch.setenv("FAKE_DOCKER_FIXTURES", str(FIXTURES_DIR))
    monkeypatch.setenv("FAKE_DOCKER_LOG", str(log_file))
    return {"fleet": str(fleet_file), "log": str(log_file), "env": env}


def _run_agentfleet(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [sys.executable, "-m", "agent_operator_contract.agentfleet", *args],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        env=env,
        cwd=REPO_ROOT,
        timeout=60,
    )


def _recorded(env: dict[str, Any]) -> list[dict[str, Any]]:
    log = Path(env["log"])
    entries = []
    for line in log.read_text(encoding="utf-8").splitlines():
        if line.strip():
            entries.append(json.loads(line))
    return entries


def test_ls_lists_members_ignores_oneoff_and_flags_duplicates(
    consumer_env: dict[str, Any],
) -> None:
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet(["ls"], env)

    assert completed.returncode == 0
    out = completed.stdout.decode("utf-8")
    assert "web-1" in out
    assert "coding-agent" in out
    assert "web-2" in out
    assert "worker-run-1" not in out
    assert "oneoff" not in out.lower().replace("worker-run-1", "")
    dup_lines = [line for line in out.splitlines() if "dup" in line.split()]
    assert len(dup_lines) == 2
    assert all("duplicate" in line.lower() for line in dup_lines)


def test_ls_uses_only_labels_and_no_exec(consumer_env: dict[str, Any]) -> None:
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet(["ls"], env)

    assert completed.returncode == 0
    recorded = _recorded(consumer_env)
    assert recorded, "fake must record docker invocations"
    assert all(entry["argv"][0] in ("ps", "inspect") for entry in recorded)
    assert not any(entry["argv"][0] == "exec" for entry in recorded)


def test_ls_json_validates_against_schema(consumer_env: dict[str, Any]) -> None:
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet(["ls", "--json"], env)

    assert completed.returncode == 0
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_ls(doc) == []
    assert LS_VALIDATOR.is_valid(doc)
    names = [item["name"] for item in doc["instances"]]
    assert "web-1" in names
    assert "web-2" in names
    assert "worker-run-1" not in names
    web1 = next(item for item in doc["instances"] if item["name"] == "web-1")
    assert web1["backend"] == "coding-agent"
    assert web1["contract"] == "1"
    assert web1["state"] == "running"
    assert web1["duplicate"] is False
    dups = [item for item in doc["instances"] if item["name"] == "dup"]
    assert len(dups) == 2
    assert all(item["duplicate"] is True for item in dups)


def test_identity_json_passthrough_is_byte_for_byte(
    consumer_env: dict[str, Any],
) -> None:
    env: dict[str, str] = consumer_env["env"]
    fixture = json.loads((FIXTURES_DIR / "identity.json").read_text(encoding="utf-8"))
    expected_stdout: str = fixture["stdout"]
    completed = _run_agentfleet(["identity", "web-1", "--json"], env)

    assert completed.returncode == fixture["exit_code"]
    assert completed.stdout.decode("utf-8") == expected_stdout


def test_identity_exec_argv_has_contract_first_and_no_tty(
    consumer_env: dict[str, Any],
) -> None:
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet(["identity", "web-1", "--json"], env)

    assert completed.returncode == 0
    recorded = _recorded(consumer_env)
    execs = [entry for entry in recorded if entry["argv"][0] == "exec"]
    assert len(execs) == 1
    argv: list[str] = execs[0]["argv"]
    assert "-t" not in argv
    assert "--tty" not in argv
    assert "-i" not in argv
    assert "-it" not in argv
    assert "--user" in argv
    user_index = argv.index("--user")
    assert argv[user_index + 1] == "agent"
    agentctl_index = argv.index("/usr/local/bin/agentctl")
    agentctl_argv = argv[agentctl_index + 1 :]
    assert agentctl_argv[:2] == ["--contract", "1"]
    assert agentctl_argv[2:] == ["identity"]


def test_identity_human_output(consumer_env: dict[str, Any]) -> None:
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet(["identity", "web-1"], env)

    assert completed.returncode == 0
    out = completed.stdout.decode("utf-8")
    assert "coding-agent" in out
    assert "example-owner/example-repo" in out


STATUS_FIXTURES = [
    "status-starting",
    "status-recovering",
    "status-idle",
    "status-active",
    "status-draining",
    "status-paused",
    "status-simple-coding-agent",
    "offline-status-halted",
]


def _status_env(
    consumer_env: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> dict[str, str]:
    """A fixture dir with one status recording: every status argv collides.

    All online status fixtures share ``argv == ["--contract", "1", "status"]``
    and the fake replays the first match, so ``--json`` verbatim and human
    output are tested against an isolated dir with a single recording.
    """
    isolated = tmp_path / "status-fixtures"
    isolated.mkdir(exist_ok=True)
    (isolated / f"{name}.json").write_text(
        (FIXTURES_DIR / f"{name}.json").read_text(encoding="utf-8"), encoding="utf-8"
    )
    env = dict(consumer_env["env"])
    env["FAKE_DOCKER_FIXTURES"] = str(isolated)
    monkeypatch.setenv("FAKE_DOCKER_FIXTURES", str(isolated))
    return env


def test_status_json_passthrough_is_byte_for_byte(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _status_env(consumer_env, tmp_path, monkeypatch, "status-active")
    fixture = json.loads((FIXTURES_DIR / "status-active.json").read_text(encoding="utf-8"))
    expected_stdout: str = fixture["stdout"]
    completed = _run_agentfleet(["status", "web-1", "--json"], env)

    assert completed.returncode == fixture["exit_code"]
    assert completed.stdout.decode("utf-8") == expected_stdout


def test_status_exec_argv_has_contract_first_and_no_tty(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _status_env(consumer_env, tmp_path, monkeypatch, "status-active")
    completed = _run_agentfleet(["status", "web-1", "--json"], env)

    assert completed.returncode == 0
    recorded = _recorded(consumer_env)
    execs = [entry for entry in recorded if entry["argv"][0] == "exec"]
    assert len(execs) == 1
    argv: list[str] = execs[0]["argv"]
    assert "-t" not in argv
    assert "--tty" not in argv
    assert "-i" not in argv
    assert "-it" not in argv
    assert "--user" in argv
    user_index = argv.index("--user")
    assert argv[user_index + 1] == "agent"
    agentctl_index = argv.index("/usr/local/bin/agentctl")
    agentctl_argv = argv[agentctl_index + 1 :]
    assert agentctl_argv == ["--contract", "1", "status"]


def test_status_human_output_shows_units_bases_and_freshness(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _status_env(consumer_env, tmp_path, monkeypatch, "status-active")
    completed = _run_agentfleet(["status", "web-1"], env)

    assert completed.returncode == 0
    out = completed.stdout.decode("utf-8")
    assert "118 count (measured)" in out
    assert "tokens (measured)" in out
    assert "cache_hit_rate: unknown ratio (unavailable)" in out
    assert "unknown" in out
    assert "cache_hit_rate: 0" not in out
    assert "observed 2026-11-06T09:12:05Z" in out
    assert "observed 2026-11-06T09:12:04Z" in out


def test_status_human_output_for_idle_shows_no_attempt(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _status_env(consumer_env, tmp_path, monkeypatch, "status-idle")
    completed = _run_agentfleet(["status", "web-1"], env)

    assert completed.returncode == 0
    out = completed.stdout.decode("utf-8")
    assert "instance: idle" in out
    assert "current_attempt: none" in out


@pytest.mark.parametrize("name", STATUS_FIXTURES)
def test_status_fixtures_are_success_snapshots(name: str) -> None:
    fixture = documents.load_fixture(name)
    doc = json.loads(fixture["stdout"])

    assert documents.validate_envelope(doc) == []
    assert documents.validate_status(doc["result"]) == []
    assert documents.classify_fixture(fixture) == "success"
    assert documents.load_fixture_index()["fixtures"][name]["classification"] == "success"


def test_identity_duplicate_fails_with_client_error_and_no_exec(
    consumer_env: dict[str, Any],
) -> None:
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet(["identity", "dup", "--json"], env)

    table = documents.client_table()
    expected_exit = int(table["client.duplicate_instance"]["exit_code"])
    assert completed.returncode == expected_exit
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert CLIENT_ENVELOPE_VALIDATOR.is_valid(doc)
    assert doc["error"]["code"] == "client.duplicate_instance"
    recorded = _recorded(consumer_env)
    assert not any(entry["argv"][0] == "exec" for entry in recorded)


def test_identity_unknown_fails_with_client_error(
    consumer_env: dict[str, Any],
) -> None:
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet(["identity", "nope", "--json"], env)

    table = documents.client_table()
    assert completed.returncode == int(table["client.unknown_instance"]["exit_code"])
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert doc["error"]["code"] == "client.unknown_instance"
    recorded = _recorded(consumer_env)
    assert not any(entry["argv"][0] == "exec" for entry in recorded)


def test_docker_unavailable_fails_with_client_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fleet_file = tmp_path / "fleet.json"
    fleet_file.write_text(json.dumps(FLEET), encoding="utf-8")
    log_file = tmp_path / "docker.log"
    log_file.write_text("", encoding="utf-8")
    env = {
        **os.environ,
        "AGENTFLEET_DOCKER": "/nonexistent/docker-for-test",
        "FAKE_DOCKER_FLEET": str(fleet_file),
        "FAKE_DOCKER_LOG": str(log_file),
    }
    completed = _run_agentfleet(["ls"], env)

    table = documents.client_table()
    assert completed.returncode == int(table["client.docker_unavailable"]["exit_code"])
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert doc["error"]["code"] == "client.docker_unavailable"


def test_client_table_exit_classes_match_spec_section_4() -> None:
    table = documents.client_table()
    producer = documents.error_table()
    allowed = set(entry["exit_code"] for entry in producer.values()) | {2}
    for code, entry in table.items():
        assert code.startswith("client."), code
        assert entry["exit_code"] in (0, 1, 2, 3, 4, 5, 6, 7), code
        assert entry["exit_code"] in allowed, code
        assert isinstance(entry["retryable"], bool), code

    assert table["client.unknown_instance"]["exit_code"] == 5
    assert table["client.duplicate_instance"]["exit_code"] == 1
    assert table["client.docker_unavailable"]["exit_code"] == 6
    assert table["client.usage"]["exit_code"] == 2


def test_client_envelope_schema_matches_generated_output() -> None:
    script = REPO_ROOT / "scripts" / "generate_schemas.py"
    completed = subprocess.run(
        [sys.executable, str(script), "--check"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout


def test_client_envelope_schema_enum_matches_client_table() -> None:
    enum = CLIENT_ENVELOPE_SCHEMA["properties"]["error"]["properties"]["code"]["enum"]

    assert set(enum) == set(documents.client_table())


def test_ls_schema_patterns_match_library() -> None:
    item = LS_SCHEMA["properties"]["instances"]["items"]["properties"]

    assert item["name"]["pattern"] == documents.INSTANCE_NAME_PATTERN
    assert item["contract"]["pattern"] == documents.CONTRACT_MAJOR_PATTERN


def test_fake_and_fixtures_usable_from_checkout_without_imports(
    tmp_path: Path,
) -> None:
    """The documented invocation: files on disk, no Python imports."""
    fleet_file = tmp_path / "fleet.json"
    fleet_file.write_text(json.dumps(FLEET), encoding="utf-8")
    log_file = tmp_path / "docker.log"
    env = {
        **os.environ,
        "FAKE_DOCKER_FLEET": str(fleet_file),
        "FAKE_DOCKER_FIXTURES": str(FIXTURES_DIR),
        "FAKE_DOCKER_LOG": str(log_file),
    }
    ps = subprocess.run(
        [str(FAKE_DOCKER), "ps", "-a", "--format", "{{.ID}}"],
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO_ROOT,
        check=False,
        timeout=30,
    )
    assert ps.returncode == 0
    assert ("a" * 64) in ps.stdout
    first_id = ps.stdout.splitlines()[0].strip()
    inspect = subprocess.run(
        [str(FAKE_DOCKER), "inspect", first_id],
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO_ROOT,
        check=False,
        timeout=30,
    )
    assert inspect.returncode == 0
    entry = json.loads(inspect.stdout)[0]
    assert entry["Config"]["Labels"]["io.github.lbacik.agent-operator.instance"]
