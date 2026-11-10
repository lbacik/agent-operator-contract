"""Consumer suite: `agentfleet` against the replaying fake `docker`.

The consumer boundary is the `docker` executable: `agentfleet` talks to
Docker only through the configurable `docker` CLI via subprocess. The fake
(`tests/consumer/fake_docker`) stands in for `docker`: it serves a described
Fleet for listing and inspection, answers `exec` of `agentctl` by replaying
fixtures matched on argv, and records every invocation so these tests can
assert what the consumer sent. No real daemon, container or network is used.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from agent_operator_contract import client, documents

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
            "image": "sha256:offline-image-web-2",
            "volumes": [{"name": "stub-data", "path": "/data"}],
            "binds": [{"source": "/host/cfg", "path": "/cfg"}],
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


def _fixture_env(
    consumer_env: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    names: list[str],
    extra: dict[str, Any] | None = None,
) -> dict[str, str]:
    """A fixture dir with exactly the named recordings, plus inline extras.

    Mutation tests need an ``identity`` recording (the precheck) next to the
    mutation recording; ``extra`` maps a file stem to a fixture document that
    is written inline for caller-chosen request IDs and repositories.
    """
    isolated = tmp_path / "isolated-fixtures"
    isolated.mkdir(exist_ok=True)
    for name in names:
        (isolated / f"{name}.json").write_text(
            (FIXTURES_DIR / f"{name}.json").read_text(encoding="utf-8"), encoding="utf-8"
        )
    for stem, doc in (extra or {}).items():
        (isolated / f"{stem}.json").write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    env = dict(consumer_env["env"])
    env["FAKE_DOCKER_FIXTURES"] = str(isolated)
    monkeypatch.setenv("FAKE_DOCKER_FIXTURES", str(isolated))
    return env


def _exec_argvs(recorded: list[dict[str, Any]]) -> list[list[str]]:
    return [
        entry["argv"][entry["argv"].index("/usr/local/bin/agentctl") + 1 :]
        for entry in recorded
        if entry["argv"][0] == "exec"
    ]


def _run_argvs(recorded: list[dict[str, Any]]) -> list[list[str]]:
    """The full docker argv of every recorded Offline Read ``run``."""
    return [entry["argv"] for entry in recorded if entry["argv"][0] == "run"]


def _agentctl_argvs_of_run(run_argv: list[str]) -> list[str]:
    """The agentctl args of one recorded ``run`` (everything after the image)."""
    entrypoint = run_argv.index("--entrypoint")
    assert run_argv[entrypoint + 1] == "/usr/local/bin/agentctl"
    return run_argv[entrypoint + 3 :]


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


def test_status_stopped_reads_offline_with_readonly_mounts(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stopped instance is read through ``docker run``, never ``exec``.

    The Offline Read re-runs the stopped container's image with every volume
    and bind mount re-mounted read-only at the same destination, ``--network
    none``, the exec user, and ``--rm`` (spec section 3.2).
    """
    env = _status_env(consumer_env, tmp_path, monkeypatch, "offline-status-halted")
    fixture = json.loads((FIXTURES_DIR / "offline-status-halted.json").read_text(encoding="utf-8"))
    completed = _run_agentfleet(["status", "web-2", "--json"], env)

    assert completed.returncode == fixture["exit_code"]
    assert completed.stdout.decode("utf-8") == fixture["stdout"]
    recorded = _recorded(consumer_env)
    assert not any(entry["argv"][0] == "exec" for entry in recorded)
    runs = _run_argvs(recorded)
    assert len(runs) == 1
    assert runs[0] == [
        "run",
        "--rm",
        "--network",
        "none",
        "--user",
        "agent",
        "--mount",
        "type=volume,src=stub-data,dst=/data,readonly",
        "--mount",
        "type=bind,src=/host/cfg,dst=/cfg,readonly",
        "--entrypoint",
        "/usr/local/bin/agentctl",
        "sha256:offline-image-web-2",
        "--contract",
        "1",
        "--offline",
        "status",
    ]


def test_stop_stopped_is_refused_client_side_with_no_agentctl(
    consumer_env: dict[str, Any],
) -> None:
    """A mutation on a stopped instance is refused before anything is sent.

    ``agentfleet stop <stopped>`` exits with ``client.instance_stopped`` and
    the fake records no ``agentctl`` invocation at all (neither ``exec`` nor
    an Offline Read ``run``); only discovery (``ps``/``inspect``) runs.
    """
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet(
        ["stop", "web-2", "--expect-repo", "example-owner/example-repo", "--json"], env
    )

    table = documents.client_table()
    assert completed.returncode == int(table["client.instance_stopped"]["exit_code"])
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert CLIENT_ENVELOPE_VALIDATOR.is_valid(doc)
    assert doc["error"]["code"] == "client.instance_stopped"
    recorded = _recorded(consumer_env)
    assert _exec_argvs(recorded) == []
    assert _run_argvs(recorded) == []
    assert all(entry["argv"][0] in ("ps", "inspect") for entry in recorded)


def test_offline_read_of_running_container_is_refused(
    consumer_env: dict[str, Any],
) -> None:
    """Building an Offline Read target for a running container fails client-side."""
    fleet = client.discover(str(FAKE_DOCKER))
    instance = client.resolve(fleet, "web-1")

    with pytest.raises(client.InstanceRunningError) as exc_info:
        client.inspect_offline_target(str(FAKE_DOCKER), instance)

    assert exc_info.value.code == "client.instance_running"


def test_status_paused_is_refused_with_no_run(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A paused container still has a live engine, so no Offline Read runs.

    ``docker ps`` reports paused containers as up, and the engine owns the
    volume while paused; ``agentfleet status <paused>`` exits with
    ``client.instance_running`` and records neither ``exec`` nor ``run``.
    """
    fleet = {
        "containers": [
            {
                "id": "f" * 64,
                "name": "paused-1",
                "backend": "coding-agent",
                "contract": "1",
                "exec_user": "agent",
                "state": "paused",
            }
        ]
    }
    fleet_file = tmp_path / "paused-fleet.json"
    fleet_file.write_text(json.dumps(fleet), encoding="utf-8")
    monkeypatch.setenv("FAKE_DOCKER_FLEET", str(fleet_file))
    env = {**consumer_env["env"], "FAKE_DOCKER_FLEET": str(fleet_file)}
    completed = _run_agentfleet(["status", "paused-1", "--json"], env)

    table = documents.client_table()
    assert completed.returncode == int(table["client.instance_running"]["exit_code"])
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert doc["error"]["code"] == "client.instance_running"
    recorded = _recorded(consumer_env)
    assert _exec_argvs(recorded) == []
    assert _run_argvs(recorded) == []
    assert all(entry["argv"][0] in ("ps", "inspect") for entry in recorded)


def test_offline_docker_argv_mounts_everything_read_only() -> None:
    """The Offline Read argv pins the spec section 3.2 shape exactly."""
    target = client.OfflineTarget(
        image="sha256:abc",
        mounts=(
            client.VolumeMount("volume", "stub-data", "/data"),
            client.VolumeMount("bind", "/host/cfg", "/cfg"),
        ),
        exec_user="agent",
    )

    assert client.offline_docker_argv(target, ["status"]) == [
        "run",
        "--rm",
        "--network",
        "none",
        "--user",
        "agent",
        "--mount",
        "type=volume,src=stub-data,dst=/data,readonly",
        "--mount",
        "type=bind,src=/host/cfg,dst=/cfg,readonly",
        "--entrypoint",
        "/usr/local/bin/agentctl",
        "sha256:abc",
        "--contract",
        "1",
        "--offline",
        "status",
    ]


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


def _masked_stop_fixture(source: str, placeholder: str) -> dict[str, Any]:
    """Copy a fixture, replacing its request ID with a positional placeholder.

    The fake matches ``--request-id`` by position and substitutes the ID the
    consumer actually sent, so a test for a generated ID stages its fixtures
    with a placeholder the consumer never sends.
    """
    doc = dict(json.loads((FIXTURES_DIR / f"{source}.json").read_text(encoding="utf-8")))
    argv = list(doc["argv"])
    old_id = argv[argv.index("--request-id") + 1]
    argv[argv.index("--request-id") + 1] = placeholder
    doc["argv"] = argv
    doc["stdout"] = str(doc["stdout"]).replace(old_id, placeholder)
    doc["stderr"] = str(doc.get("stderr", "")).replace(old_id, placeholder)
    return doc


def _stop_fixture(request_id: str, sequence: int = 7, replayed: bool = False) -> dict[str, Any]:
    record = {
        "acknowledgement": "accepted",
        "arguments": {},
        "created_at": "2026-11-06T09:12:03Z",
        "detail": "finish the active attempt, then close intake",
        "kind": "stop",
        "repository": "example-owner/example-repo",
        "request_id": request_id,
        "sequence": sequence,
        "updated_at": "2026-11-06T09:12:03Z",
    }
    stdout = json.dumps(
        {"contract": "1.0", "ok": True, "result": {"command": record, "replayed": replayed}},
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "argv": [
            "--contract",
            "1",
            "stop",
            "--request-id",
            request_id,
            "--expect-repo",
            "example-owner/example-repo",
        ],
        "exit_code": 0,
        "stdout": stdout + "\n",
        "stderr": "",
    }


@pytest.mark.parametrize("operation", ["stop", "resume"])
def test_mutation_without_expect_repo_is_usage_and_sends_nothing(
    consumer_env: dict[str, Any], operation: str
) -> None:
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet([operation, "web-1"], env)

    assert completed.returncode == 2
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert CLIENT_ENVELOPE_VALIDATOR.is_valid(doc)
    assert doc["error"]["code"] == "client.usage"
    recorded = _recorded(consumer_env)
    assert not any(entry["argv"][0] == "exec" for entry in recorded)


def test_stop_with_supplied_request_id_sends_exactly_that_id(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_id = "req-C0FFEE01"
    env = _fixture_env(
        consumer_env,
        tmp_path,
        monkeypatch,
        ["identity"],
        {"stop-custom": _stop_fixture(request_id)},
    )
    completed = _run_agentfleet(
        [
            "stop",
            "web-1",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            request_id,
        ],
        env,
    )

    assert completed.returncode == 0
    assert request_id in completed.stdout.decode("utf-8")
    exec_argvs = _exec_argvs(_recorded(consumer_env))
    assert exec_argvs[0] == ["--contract", "1", "identity"]
    assert exec_argvs[1] == [
        "--contract",
        "1",
        "stop",
        "--request-id",
        request_id,
        "--expect-repo",
        "example-owner/example-repo",
    ]


def test_resume_with_supplied_request_id_replays_the_fixture(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, ["identity", "resume-accepted"])
    request_id = "req-4d5e6f7a8b9c"
    completed = _run_agentfleet(
        [
            "resume",
            "web-1",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            request_id,
        ],
        env,
    )

    assert completed.returncode == 0
    assert request_id in completed.stdout.decode("utf-8")
    exec_argvs = _exec_argvs(_recorded(consumer_env))
    assert exec_argvs[0] == ["--contract", "1", "identity"]
    assert exec_argvs[1] == [
        "--contract",
        "1",
        "resume",
        "--request-id",
        request_id,
        "--expect-repo",
        "example-owner/example-repo",
    ]


def test_mutation_never_sent_when_contract_major_mismatches(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mismatch = dict(
        json.loads((FIXTURES_DIR / "contract-mismatch.json").read_text(encoding="utf-8"))
    )
    mismatch["argv"] = ["--contract", "1", "identity"]
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, [], {"identity": mismatch})
    completed = _run_agentfleet(
        ["stop", "web-1", "--expect-repo", "example-owner/example-repo"],
        env,
    )

    assert completed.returncode == 4
    exec_argvs = _exec_argvs(_recorded(consumer_env))
    assert exec_argvs == [["--contract", "1", "identity"]]


def test_command_read_replays_the_fixture(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, ["command-found"])
    completed = _run_agentfleet(["command", "web-1", "req-3f1c9a2b7d10", "--json"], env)
    fixture = json.loads((FIXTURES_DIR / "command-found.json").read_text(encoding="utf-8"))

    assert completed.returncode == fixture["exit_code"]
    assert completed.stdout.decode("utf-8") == fixture["stdout"]
    assert _exec_argvs(_recorded(consumer_env)) == [fixture["argv"]]


def test_command_unknown_id_reports_not_found(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, ["command-not-found"])
    completed = _run_agentfleet(["command", "web-1", "req-000000000000"], env)

    assert completed.returncode == 5
    assert "not_found" in completed.stderr.decode("utf-8")
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "command", "req-000000000000"]
    ]


def test_new_request_id_matches_the_contract_pattern() -> None:
    seen = {client.new_request_id() for _ in range(50)}

    assert len(seen) == 50
    for request_id in seen:
        assert re.search(documents.REQUEST_ID_PATTERN, request_id) is not None, request_id


def test_mutation_precheck_allows_core_refuses_major_and_capability() -> None:
    identity: dict[str, Any] = json.loads(
        json.loads((FIXTURES_DIR / "identity.json").read_text(encoding="utf-8"))["stdout"]
    )["result"]

    client.ensure_mutation_allowed(identity, None)

    wrong_major = {**identity, "contract": {"major": 2, "minor": 0}}
    with pytest.raises(client.ClientError) as major_info:
        client.ensure_mutation_allowed(wrong_major, None)
    assert major_info.value.code == "client.contract_mismatch"
    assert major_info.value.exit_code == 4

    with pytest.raises(client.ClientError) as capability_info:
        client.ensure_mutation_allowed(identity, "operator-handoff")
    assert capability_info.value.code == "client.unsupported_capability"
    assert capability_info.value.exit_code == 3


def test_mutation_timeout_defaults_follow_spec_section_3_3() -> None:
    assert client.timeout_for("identity") == 10.0
    assert client.timeout_for("status") == 10.0
    assert client.timeout_for("command") == 10.0
    assert client.timeout_for("stop") == 15.0
    assert client.timeout_for("resume") == 15.0
    assert client.timeout_for("attempts-list") == 15.0
    assert client.timeout_for("attempts-export") == 30.0
    # The only caller is the mutation retry path: an unrecognized operation
    # carries the mutation default so a short timeout cannot manufacture an
    # ambiguous outcome.
    assert client.timeout_for("stop-after") == 15.0
    assert client.timeout_for("mutation") == 15.0


def test_mutation_timeout_override_wins_only_when_positive() -> None:
    assert client.timeout_for("stop", override=0.2) == 0.2
    assert client.timeout_for("stop", override=0.0) == 15.0
    assert client.timeout_for("stop", override=-1.0) == 15.0
    assert client.timeout_for("identity", override=-1.0) == 10.0


def test_mutation_ambiguous_then_replayed_with_same_generated_id(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ambiguous reply is retried with the same generated ID and payload.

    The fixtures carry a placeholder request ID the consumer never sends; the
    fake matches by position and substitutes the generated ID. The first
    attempt is ambiguous, the second replays the commit, and ``agentfleet``
    exits 0 without the caller ever supplying an ID.
    """
    placeholder = "req-PLACEHOLDER0001"
    ambiguous = _masked_stop_fixture("agent-unreachable", placeholder)
    success = _stop_fixture(placeholder, replayed=True)
    env = _fixture_env(
        consumer_env,
        tmp_path,
        monkeypatch,
        ["identity"],
        {"stop-retry-01": ambiguous, "stop-retry-02": success},
    )
    completed = _run_agentfleet(
        ["stop", "web-1", "--expect-repo", "example-owner/example-repo"],
        env,
    )

    assert completed.returncode == 0
    out = completed.stdout.decode("utf-8")
    assert "replayed: True" in out
    exec_argvs = _exec_argvs(_recorded(consumer_env))
    assert exec_argvs[0] == ["--contract", "1", "identity"]
    stops = [argv for argv in exec_argvs[1:] if argv[2:3] == ["stop"]]
    assert len(stops) == 2
    assert stops[0] == stops[1]
    generated = stops[0][stops[0].index("--request-id") + 1]
    assert generated != placeholder
    assert re.search(documents.REQUEST_ID_PATTERN, generated) is not None
    assert generated in out


def test_mutation_delayed_past_timeout_retries_then_reports_request_id(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A reply slower than the timeout is ambiguous; exhaustion prints the ID.

    The single staged reply sleeps past the 0.2 s timeout, so both attempts
    time out with the same generated ID, and ``agentfleet`` exits non-zero
    with the request ID for a ``command`` follow-up instead of resubmitting.
    """
    placeholder = "req-PLACEHOLDER0002"
    slow = _stop_fixture(placeholder)
    slow["delay_seconds"] = 2.0
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, ["identity"], {"stop-slow": slow})
    completed = _run_agentfleet(
        [
            "stop",
            "web-1",
            "--expect-repo",
            "example-owner/example-repo",
            "--timeout",
            "0.2",
            "--max-attempts",
            "2",
        ],
        env,
    )

    assert completed.returncode == 6
    combined = completed.stdout.decode("utf-8") + completed.stderr.decode("utf-8")
    exec_argvs = _exec_argvs(_recorded(consumer_env))
    stops = [argv for argv in exec_argvs if argv[2:3] == ["stop"]]
    assert len(stops) == 2
    assert stops[0] == stops[1]
    generated = stops[0][stops[0].index("--request-id") + 1]
    assert generated in combined


def test_mutation_internal_not_stored_is_not_retried(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``internal`` with ``details.stored: false`` is definitive, not ambiguous."""
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, ["identity", "internal-not-stored"])
    completed = _run_agentfleet(
        [
            "resume",
            "web-1",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            "req-3c4d5e6f7a8b",
        ],
        env,
    )

    assert completed.returncode == 7
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "identity"],
        [
            "--contract",
            "1",
            "resume",
            "--request-id",
            "req-3c4d5e6f7a8b",
            "--expect-repo",
            "example-owner/example-repo",
        ],
    ]


@pytest.mark.parametrize(
    "name",
    [
        "transport-125",
        "transport-126",
        "transport-127",
        "malformed-empty",
        "malformed-truncated",
        "malformed-non-json",
        "contract-unknown-major",
    ],
)
def test_malformed_stdout_is_never_a_success(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    """Malformed stdout falls back to the exit code and never reads as success.

    Exit 125-127 without an envelope is a transport failure (ambiguous for a
    mutation); any other incoherent stdout is a client error. Even the
    ``ok:true`` envelope with an unknown contract major must not exit 0.
    """
    fixture = json.loads((FIXTURES_DIR / f"{name}.json").read_text(encoding="utf-8"))
    request_id = fixture["argv"][fixture["argv"].index("--request-id") + 1]
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, ["identity", name])
    completed = _run_agentfleet(
        [
            "stop",
            "web-1",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            request_id,
            "--max-attempts",
            "1",
        ],
        env,
    )

    assert completed.returncode != 0
    combined = completed.stdout.decode("utf-8") + completed.stderr.decode("utf-8")
    assert request_id in combined


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


def _attempts_env(
    consumer_env: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> dict[str, str]:
    """A fixture dir with a single attempts recording for argv-exact replay.

    Unlike status, attempts fixtures do not share one argv (list takes
    ``--limit``/``--before`` and show takes the attempt ID), so each test
    isolates the one recording it replays.
    """
    isolated = tmp_path / "attempts-fixtures"
    isolated.mkdir(exist_ok=True)
    (isolated / f"{name}.json").write_text(
        (FIXTURES_DIR / f"{name}.json").read_text(encoding="utf-8"), encoding="utf-8"
    )
    env = dict(consumer_env["env"])
    env["FAKE_DOCKER_FIXTURES"] = str(isolated)
    monkeypatch.setenv("FAKE_DOCKER_FIXTURES", str(isolated))
    return env


def test_attempts_list_json_passthrough_is_byte_for_byte(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _attempts_env(consumer_env, tmp_path, monkeypatch, "attempts-list")
    fixture = json.loads((FIXTURES_DIR / "attempts-list.json").read_text(encoding="utf-8"))
    completed = _run_agentfleet(["attempts", "ls", "web-1", "--json"], env)

    assert completed.returncode == fixture["exit_code"]
    assert completed.stdout.decode("utf-8") == fixture["stdout"]
    assert _exec_argvs(_recorded(consumer_env)) == [fixture["argv"]]


def test_attempts_list_human_output_states_sealed_entries(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _attempts_env(consumer_env, tmp_path, monkeypatch, "attempts-list")
    completed = _run_agentfleet(["attempts", "ls", "web-1"], env)

    assert completed.returncode == 0
    out = completed.stdout.decode("utf-8")
    assert "#42/1 issue #42 finished Sealed" in out
    assert "next_cursor: none" in out


def test_attempts_list_paged_reports_opaque_cursor(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _attempts_env(consumer_env, tmp_path, monkeypatch, "attempts-list-paged")
    completed = _run_agentfleet(["attempts", "list", "web-1", "--limit", "1"], env)

    assert completed.returncode == 0
    out = completed.stdout.decode("utf-8")
    assert "#42/1 issue #42 finished Sealed" in out
    assert "next_cursor: eyJvZmZzZXQiOjF9" in out
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "attempts", "list", "--limit", "1"]
    ]


def test_attempts_show_sealed_human_output_states_sealed(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _attempts_env(consumer_env, tmp_path, monkeypatch, "attempts-show-sealed")
    completed = _run_agentfleet(["attempts", "show", "web-1", "#41/1"], env)

    assert completed.returncode == 0
    out = completed.stdout.decode("utf-8")
    assert "attempt_id: #41/1" in out
    assert "archive: Sealed" in out
    assert "sealed_at: 2026-11-06T08:45:01Z" in out
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "attempts", "show", "#41/1"]
    ]


def test_attempts_show_unsealed_human_output_states_not_sealed(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _attempts_env(consumer_env, tmp_path, monkeypatch, "attempts-show-unsealed")
    completed = _run_agentfleet(["attempts", "show", "web-1", "#42/1"], env)

    assert completed.returncode == 0
    out = completed.stdout.decode("utf-8")
    assert "attempt_id: #42/1" in out
    assert "archive: not Sealed" in out
    assert "outcome: none" in out


def test_attempts_show_json_passthrough_is_byte_for_byte(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _attempts_env(consumer_env, tmp_path, monkeypatch, "attempts-show-sealed")
    fixture = json.loads((FIXTURES_DIR / "attempts-show-sealed.json").read_text(encoding="utf-8"))
    completed = _run_agentfleet(["attempts", "show", "web-1", "#41/1", "--json"], env)

    assert completed.returncode == fixture["exit_code"]
    assert completed.stdout.decode("utf-8") == fixture["stdout"]


def test_attempts_show_unknown_id_reports_not_found(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _attempts_env(consumer_env, tmp_path, monkeypatch, "attempts-not-found")
    completed = _run_agentfleet(["attempts", "show", "web-1", "#99/9"], env)

    assert completed.returncode == 5
    assert "not_found" in completed.stderr.decode("utf-8")
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "attempts", "show", "#99/9"]
    ]


def test_attempts_fixture_classifications_cover_list_show_and_not_found() -> None:
    index = documents.load_fixture_index()["fixtures"]
    assert index["attempts-list"]["classification"] == "success"
    assert index["attempts-list-paged"]["classification"] == "success"
    assert index["attempts-show-sealed"]["classification"] == "success"
    assert index["attempts-show-unsealed"]["classification"] == "success"
    assert index["attempts-not-found"]["classification"] == "not_found"


def test_stop_after_with_supplied_request_id_sends_after(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_id = "req-7f2a9b4c1d3e"
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, ["identity", "stop-after-accepted"])
    completed = _run_agentfleet(
        [
            "stop-after",
            "web-1",
            "2",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            request_id,
        ],
        env,
    )

    assert completed.returncode == 0
    assert request_id in completed.stdout.decode("utf-8")
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "identity"],
        [
            "--contract",
            "1",
            "stop",
            "--after",
            "2",
            "--request-id",
            request_id,
            "--expect-repo",
            "example-owner/example-repo",
        ],
    ]


def test_next_issue_with_supplied_request_id_sends_issue(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_id = "req-8e3b0c5d2f4a"
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, ["identity", "next-issue-accepted"])
    completed = _run_agentfleet(
        [
            "next-issue",
            "web-1",
            "122",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            request_id,
        ],
        env,
    )

    assert completed.returncode == 0
    assert request_id in completed.stdout.decode("utf-8")
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "identity"],
        [
            "--contract",
            "1",
            "next",
            "issue",
            "122",
            "--request-id",
            request_id,
            "--expect-repo",
            "example-owner/example-repo",
        ],
    ]


def test_next_issue_refused_when_capability_missing(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``next-issue`` against an instance not advertising it fails client-side.

    The identity precheck refuses with ``client.unsupported_capability``
    before anything is submitted, so the fake records the ``identity`` read
    and no mutation.
    """
    identity_doc = dict(json.loads((FIXTURES_DIR / "identity.json").read_text(encoding="utf-8")))
    narrowed_stdout = json.dumps(
        {
            **json.loads(identity_doc["stdout"]),
            "result": {
                **json.loads(identity_doc["stdout"])["result"],
                "capabilities": ["stop-after"],
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    narrowed = {**identity_doc, "stdout": narrowed_stdout + "\n"}
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, [], {"identity": narrowed})
    completed = _run_agentfleet(
        [
            "next-issue",
            "web-1",
            "122",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            "req-8e3b0c5d2f4a",
            "--json",
        ],
        env,
    )

    table = documents.client_table()
    assert completed.returncode == int(table["client.unsupported_capability"]["exit_code"])
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert doc["error"]["code"] == "client.unsupported_capability"
    assert _exec_argvs(_recorded(consumer_env)) == [["--contract", "1", "identity"]]


def _export_summary(attempt_id: str) -> dict[str, Any]:
    """A full AttemptSummary for an export tar of one attempt."""
    return {
        "attempt_id": attempt_id,
        "issue": {"number": 43, "title": "Issue 43"},
        "started_at": "2026-11-06T08:51:00Z",
        "finished_at": None,
        "lifecycle_state": "open",
        "outcome": None,
        "delivery": None,
        "attention_required": False,
        "sealed": False,
        "sealed_at": None,
    }


def _export_tar_bytes(
    attempt_id: str, files: dict[str, bytes], stream_files: dict[str, bytes] | None = None
) -> bytes:
    """Build a spec section 11.3 export tar: manifest.json first, then files.

    ``stream_files`` overrides the streamed member bytes while the manifest
    still describes ``files`` — a torn read where the bytes no longer match
    the snapshot manifest entry.
    """
    entries = [
        {
            "path": name,
            "kind": "events" if name == "events.jsonl" else "transcript",
            "media_type": "application/x-ndjson",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        for name, data in files.items()
    ]
    manifest = {"contract": "1.0", "attempt": _export_summary(attempt_id), "files": entries}
    payload = (json.dumps(manifest, sort_keys=True) + "\n").encode()
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        info = tarfile.TarInfo("manifest.json")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
        members = stream_files if stream_files is not None else files
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return stream.getvalue()


def _export_fixture(
    attempt_id: str, tar_bytes: bytes, paths: list[str] | None = None
) -> dict[str, Any]:
    """An export fixture replayed as raw bytes: tars are not UTF-8 text.

    The tar leaves the package fixtures alone and lives in the test's own
    isolated fixture dir; ``stdout_base64`` carries it byte for byte.
    """
    argv = ["--contract", "1", "attempts", "export", attempt_id]
    for path in paths or []:
        argv += ["--path", path]
    return {
        "argv": argv,
        "exit_code": 0,
        "stdout_base64": base64.b64encode(tar_bytes).decode("ascii"),
        "stderr": "",
    }


def _export_env(
    consumer_env: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fixtures: dict[str, dict[str, Any]],
) -> dict[str, str]:
    """A fixture dir with inline export recordings for argv-exact replay."""
    isolated = tmp_path / "export-fixtures"
    isolated.mkdir(exist_ok=True)
    for stem, doc in fixtures.items():
        (isolated / f"{stem}.json").write_text(json.dumps(doc) + "\n", encoding="utf-8")
    env = dict(consumer_env["env"])
    env["FAKE_DOCKER_FIXTURES"] = str(isolated)
    monkeypatch.setenv("FAKE_DOCKER_FIXTURES", str(isolated))
    return env


def test_attempts_export_matching_stream_writes_bytes(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tar_bytes = _export_tar_bytes("#43/1", {"events.jsonl": b"", "transcript.jsonl": b"seed\n"})
    env = _export_env(
        consumer_env, tmp_path, monkeypatch, {"export-ok": _export_fixture("#43/1", tar_bytes)}
    )
    completed = _run_agentfleet(["attempts", "export", "web-1", "#43/1"], env)

    assert completed.returncode == 0
    assert completed.stdout == tar_bytes
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "attempts", "export", "#43/1"]
    ]


def test_attempts_export_torn_stream_refetches_then_fails(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bytes that do not match the manifest fail after the bounded refetch.

    The staged tar claims ``b"seed\\n"`` but streams grown bytes, so every
    attempt is torn: ``agentfleet`` refetches exactly three times, writes no
    tar bytes, and exits 7 with ``client.export_torn``.
    """
    manifest_files = {"events.jsonl": b"", "transcript.jsonl": b"seed\n"}
    grown_files = {
        "events.jsonl": b"",
        "transcript.jsonl": b"seed\n-appended-while-unsealed",
    }
    grown = _export_tar_bytes("#43/1", manifest_files, stream_files=grown_files)
    env = _export_env(
        consumer_env, tmp_path, monkeypatch, {"export-torn": _export_fixture("#43/1", grown)}
    )
    completed = _run_agentfleet(["attempts", "export", "web-1", "#43/1"], env)

    table = documents.client_table()
    assert completed.returncode == int(table["client.export_torn"]["exit_code"])
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert CLIENT_ENVELOPE_VALIDATOR.is_valid(doc)
    assert doc["error"]["code"] == "client.export_torn"
    exec_argvs = _exec_argvs(_recorded(consumer_env))
    assert exec_argvs == [["--contract", "1", "attempts", "export", "#43/1"]] * 3


def test_attempts_export_torn_writes_no_partial_file(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_files = {"events.jsonl": b"", "transcript.jsonl": b"seed\n"}
    grown_files = {
        "events.jsonl": b"",
        "transcript.jsonl": b"seed\n-appended-while-unsealed",
    }
    grown = _export_tar_bytes("#43/1", manifest_files, stream_files=grown_files)
    env = _export_env(
        consumer_env, tmp_path, monkeypatch, {"export-torn": _export_fixture("#43/1", grown)}
    )
    completed = _run_agentfleet(
        ["attempts", "export", "web-1", "#43/1", "--output", str(tmp_path / "out.tar")], env
    )

    assert completed.returncode == 7
    assert not (tmp_path / "out.tar").exists()


def test_attempts_export_stalled_stream_trips_the_progress_timeout(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tar_bytes = _export_tar_bytes("#43/1", {"events.jsonl": b"", "transcript.jsonl": b"seed\n"})
    fixture = _export_fixture("#43/1", tar_bytes)
    fixture["delay_seconds"] = 2.0
    env = _export_env(consumer_env, tmp_path, monkeypatch, {"export-slow": fixture})
    completed = _run_agentfleet(["attempts", "export", "web-1", "#43/1", "--timeout", "0.2"], env)

    assert completed.returncode == 6
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "attempts", "export", "#43/1"]
    ]


def test_attempts_export_unknown_id_relays_not_found(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _attempts_env(consumer_env, tmp_path, monkeypatch, "attempts-export-not-found")
    completed = _run_agentfleet(["attempts", "export", "web-1", "#99/9"], env)

    assert completed.returncode == 5
    assert "not_found" in completed.stderr.decode("utf-8")
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "attempts", "export", "#99/9"]
    ]


FULL_OPERATOR_CAPABILITIES = [
    "errors-reset",
    "next-issue",
    "operator-handoff",
    "recovery-release",
    "recovery-retry",
    "stop-after",
]


def _widened_identity_doc() -> dict[str, Any]:
    """The checkout identity advertising every optional mutation capability."""
    base = dict(json.loads((FIXTURES_DIR / "identity.json").read_text(encoding="utf-8")))
    inner = json.loads(base["stdout"])
    widened_stdout = json.dumps(
        {**inner, "result": {**inner["result"], "capabilities": FULL_OPERATOR_CAPABILITIES}},
        sort_keys=True,
        separators=(",", ":"),
    )
    return {**base, "stdout": widened_stdout + "\n"}


def test_handoff_with_supplied_request_id_sends_now(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_id = "req-a1b2c3d4e5f6"
    env = _fixture_env(
        consumer_env,
        tmp_path,
        monkeypatch,
        ["handoff-completed"],
        {"identity": _widened_identity_doc()},
    )
    completed = _run_agentfleet(
        [
            "handoff",
            "web-1",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            request_id,
        ],
        env,
    )

    assert completed.returncode == 0
    assert request_id in completed.stdout.decode("utf-8")
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "identity"],
        [
            "--contract",
            "1",
            "handoff",
            "now",
            "--request-id",
            request_id,
            "--expect-repo",
            "example-owner/example-repo",
        ],
    ]


def test_recovery_retry_with_supplied_request_id_sends_attempt(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_id = "req-c1b2c3d4e5f6"
    env = _fixture_env(
        consumer_env,
        tmp_path,
        monkeypatch,
        ["recovery-retry-completed"],
        {"identity": _widened_identity_doc()},
    )
    completed = _run_agentfleet(
        [
            "recovery-retry",
            "web-1",
            "stub-1",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            request_id,
        ],
        env,
    )

    assert completed.returncode == 0
    assert request_id in completed.stdout.decode("utf-8")
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "identity"],
        [
            "--contract",
            "1",
            "recovery",
            "retry",
            "stub-1",
            "--request-id",
            request_id,
            "--expect-repo",
            "example-owner/example-repo",
        ],
    ]


def test_recovery_release_with_supplied_request_id_sends_saved_at(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_id = "req-e1b2c3d4e5f6"
    env = _fixture_env(
        consumer_env,
        tmp_path,
        monkeypatch,
        ["recovery-release-completed"],
        {"identity": _widened_identity_doc()},
    )
    completed = _run_agentfleet(
        [
            "recovery-release",
            "web-1",
            "stub-1",
            "--saved-at",
            "shelf-303",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            request_id,
        ],
        env,
    )

    assert completed.returncode == 0
    assert request_id in completed.stdout.decode("utf-8")
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "identity"],
        [
            "--contract",
            "1",
            "recovery",
            "release",
            "stub-1",
            "--saved-at",
            "shelf-303",
            "--request-id",
            request_id,
            "--expect-repo",
            "example-owner/example-repo",
        ],
    ]


def test_errors_reset_with_supplied_request_id_sends_reset(
    consumer_env: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request_id = "req-01b2c3d4e5f6"
    env = _fixture_env(
        consumer_env,
        tmp_path,
        monkeypatch,
        ["errors-reset-completed"],
        {"identity": _widened_identity_doc()},
    )
    completed = _run_agentfleet(
        [
            "errors-reset",
            "web-1",
            "--expect-repo",
            "example-owner/example-repo",
            "--request-id",
            request_id,
        ],
        env,
    )

    assert completed.returncode == 0
    assert request_id in completed.stdout.decode("utf-8")
    assert _exec_argvs(_recorded(consumer_env)) == [
        ["--contract", "1", "identity"],
        [
            "--contract",
            "1",
            "errors",
            "reset",
            "--request-id",
            request_id,
            "--expect-repo",
            "example-owner/example-repo",
        ],
    ]


@pytest.mark.parametrize(
    ("cli", "capability"),
    [
        (["handoff", "web-1"], "operator-handoff"),
        (["recovery-retry", "web-1", "stub-1"], "recovery-retry"),
        (["recovery-release", "web-1", "stub-1", "--saved-at", "shelf-303"], "recovery-release"),
        (["errors-reset", "web-1"], "errors-reset"),
    ],
)
def test_new_operations_refused_when_capability_missing(
    consumer_env: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cli: list[str],
    capability: str,
) -> None:
    """Each new operation is gated on its advertised capability.

    The checkout identity advertises only ``next-issue`` and ``stop-after``,
    so the precheck refuses with ``client.unsupported_capability`` and the
    fake records the ``identity`` read and no mutation.
    """
    request_id = "req-9c4d5e6f7a8b"
    env = _fixture_env(consumer_env, tmp_path, monkeypatch, ["identity"])
    completed = _run_agentfleet(
        [*cli, "--expect-repo", "example-owner/example-repo", "--request-id", request_id, "--json"],
        env,
    )

    table = documents.client_table()
    assert completed.returncode == int(table["client.unsupported_capability"]["exit_code"])
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert doc["error"]["code"] == "client.unsupported_capability"
    assert capability in str(doc["error"].get("message", ""))
    assert _exec_argvs(_recorded(consumer_env)) == [["--contract", "1", "identity"]]


@pytest.mark.parametrize(
    "cli",
    [
        ["handoff", "web-1"],
        ["recovery-retry", "web-1", "stub-1"],
        ["recovery-release", "web-1", "stub-1", "--saved-at", "shelf-303"],
        ["errors-reset", "web-1"],
    ],
)
def test_new_operations_without_expect_repo_are_usage_and_send_nothing(
    consumer_env: dict[str, Any], cli: list[str]
) -> None:
    env: dict[str, str] = consumer_env["env"]
    completed = _run_agentfleet(cli, env)

    assert completed.returncode == 2
    doc = json.loads(completed.stdout.decode("utf-8"))
    assert documents.validate_client_envelope(doc) == []
    assert doc["error"]["code"] == "client.usage"
    recorded = _recorded(consumer_env)
    assert not any(entry["argv"][0] == "exec" for entry in recorded)


def test_new_operation_fixture_classifications_cover_success_rejection_and_not_found() -> None:
    index = documents.load_fixture_index()["fixtures"]
    assert index["handoff-accepted"]["classification"] == "success"
    assert index["handoff-completed"]["classification"] == "success"
    assert index["handoff-rejected"]["classification"] == "rejection"
    assert index["recovery-retry-completed"]["classification"] == "success"
    assert index["recovery-retry-not-found"]["classification"] == "not_found"
    assert index["recovery-release-completed"]["classification"] == "success"
    assert index["recovery-release-not-found"]["classification"] == "not_found"
    assert index["errors-reset-completed"]["classification"] == "success"
    assert index["errors-reset-rejected"]["classification"] == "rejection"
