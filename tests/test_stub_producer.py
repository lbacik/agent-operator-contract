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


def test_harness_verbs_without_a_container_answer_failed(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["STUB_HARNESS_STATE"] = str(tmp_path / "missing-state.json")
    for verb in (
        "set-issue",
        "hold",
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
