"""Unit tests for the standard-library document validation.

These tests use synthetic documents and the error table only; agreement with
the JSON Schemas over the whole fixture set is covered in test_schemas.py.
"""

import json
from typing import Any

import pytest

from agent_operator_contract import documents

#: The spec section 4 table, written out so the test pins error-table.json to it.
SPEC_SECTION_4: dict[str, dict[str, object]] = {
    "rejected": {"exit_code": 1, "retryable": False},
    "repository_mismatch": {"exit_code": 1, "retryable": False},
    "payload_mismatch": {"exit_code": 1, "retryable": False},
    "invalid_argument": {"exit_code": 1, "retryable": False},
    "usage": {"exit_code": 2, "retryable": False},
    "unsupported": {"exit_code": 3, "retryable": False},
    "contract_mismatch": {"exit_code": 4, "retryable": False},
    "not_found": {"exit_code": 5, "retryable": False},
    "agent_unreachable": {"exit_code": 6, "retryable": True},
    "internal": {"exit_code": 7, "retryable": True},
}


def test_error_table_matches_spec_section_4() -> None:
    table = documents.error_table()

    assert set(table) == set(SPEC_SECTION_4)
    for code, expected in SPEC_SECTION_4.items():
        assert table[code]["exit_code"] == expected["exit_code"], code
        assert table[code]["retryable"] is expected["retryable"], code


def _ok_envelope() -> dict[str, object]:
    return {"contract": "1.0", "ok": True, "result": {}}


def _error_envelope(**error: object) -> dict[str, object]:
    fields: dict[str, object] = {"code": "internal", "message": "boom", "retryable": True}
    fields.update(error)
    return {"contract": "1.0", "ok": False, "error": fields}


def test_validate_envelope_accepts_success_and_error() -> None:
    assert documents.validate_envelope(_ok_envelope()) == []
    assert documents.validate_envelope(_error_envelope()) == []
    assert documents.validate_envelope(_error_envelope(request_id="req-3f1c9a2b7d10")) == []


def test_validate_envelope_rejects_non_objects() -> None:
    assert documents.validate_envelope(None) != []
    assert documents.validate_envelope([{"contract": "1.0", "ok": True}]) != []
    assert documents.validate_envelope("ok") != []


@pytest.mark.parametrize("doc", [{}, {"ok": True, "result": {}}, {"contract": "1.0"}])
def test_validate_envelope_requires_contract_and_ok(doc: dict[str, object]) -> None:
    assert documents.validate_envelope(doc) != []


@pytest.mark.parametrize("contract", ["1", "v1.0", "1.0.0", "", 1, None])
def test_validate_envelope_rejects_bad_contract(contract: object) -> None:
    assert documents.validate_envelope({"contract": contract, "ok": True, "result": {}}) != []


def test_validate_envelope_rejects_non_boolean_ok() -> None:
    assert documents.validate_envelope({"contract": "1.0", "ok": "yes", "result": {}}) != []


def test_validate_envelope_rejects_missing_result() -> None:
    assert documents.validate_envelope({"contract": "1.0", "ok": True}) != []


def test_validate_envelope_rejects_error_alongside_ok_true() -> None:
    doc = _ok_envelope()
    doc["error"] = {"code": "internal", "message": "boom", "retryable": True}

    assert documents.validate_envelope(doc) != []


def test_validate_envelope_rejects_missing_error() -> None:
    assert documents.validate_envelope({"contract": "1.0", "ok": False}) != []


def test_validate_envelope_rejects_result_alongside_ok_false() -> None:
    doc = _error_envelope()
    doc["result"] = {}

    assert documents.validate_envelope(doc) != []


@pytest.mark.parametrize(
    "error",
    [
        {"code": "timeout", "message": "boom", "retryable": True},
        {"code": ["internal"], "message": "boom", "retryable": True},
        {"code": "internal", "retryable": True},
        {"code": "internal", "message": "", "retryable": "yes"},
        {"code": "internal", "message": 7, "retryable": True},
        {"code": "internal", "message": "boom", "retryable": 1},
        {"code": "internal", "message": "boom", "retryable": True, "request_id": "bad id!"},
        {"code": "internal", "message": "boom", "retryable": True, "details": []},
    ],
)
def test_validate_envelope_rejects_bad_error(error: dict[str, object]) -> None:
    assert documents.validate_envelope({"contract": "1.0", "ok": False, "error": error}) != []


def _identity() -> dict[str, Any]:
    return {
        "backend": "coding-agent",
        "agent_version": "0.9.0",
        "contract": {"major": 1, "minor": 0},
        "repository": "acme/widgets",
        "mode": "online",
        "process_started_at": "2026-11-06T08:00:00Z",
        "capabilities": ["next-issue"],
    }


def test_validate_identity_accepts_online_and_offline() -> None:
    assert documents.validate_identity(_identity()) == []

    offline = _identity()
    offline["mode"] = "offline"
    offline["process_started_at"] = None
    assert documents.validate_identity(offline) == []


def test_validate_identity_rejects_non_objects() -> None:
    assert documents.validate_identity(None) != []
    assert documents.validate_identity([]) != []


def test_validate_identity_requires_every_field() -> None:
    for key in list(_identity()):
        doc = _identity()
        del doc[key]

        assert documents.validate_identity(doc) != [], key


@pytest.mark.parametrize("mode", ["Online", "OFFLINE", "", None, True])
def test_validate_identity_rejects_bad_mode(mode: object) -> None:
    doc = _identity()
    doc["mode"] = mode

    assert documents.validate_identity(doc) != []


def test_validate_identity_requires_null_started_at_offline() -> None:
    doc = _identity()
    doc["mode"] = "offline"

    assert documents.validate_identity(doc) != []


def test_validate_identity_requires_string_started_at_online() -> None:
    doc = _identity()
    doc["process_started_at"] = None

    assert documents.validate_identity(doc) != []


@pytest.mark.parametrize("started_at", ["2026-11-06 08:00:00", "not-a-time", "2026-11-06", 0])
def test_validate_identity_rejects_bad_timestamps(started_at: object) -> None:
    doc = _identity()
    doc["process_started_at"] = started_at

    assert documents.validate_identity(doc) != []


def test_validate_identity_accepts_fractional_timestamps() -> None:
    doc = _identity()
    doc["process_started_at"] = "2026-11-06T08:00:00.123Z"

    assert documents.validate_identity(doc) == []


@pytest.mark.parametrize("repository", ["acme", "acme/widgets/extra", "/widgets", "", None])
def test_validate_identity_rejects_bad_repository(repository: object) -> None:
    doc = _identity()
    doc["repository"] = repository

    assert documents.validate_identity(doc) != []


def test_validate_identity_rejects_bad_contract() -> None:
    for contract in ({"major": 1}, {"major": True, "minor": 0}, {"major": 0, "minor": 0}):
        doc = _identity()
        doc["contract"] = contract

        assert documents.validate_identity(doc) != [], contract


def test_validate_fixture_rejects_shape_mistakes() -> None:
    good: dict[str, object] = {
        "argv": ["--contract", "1", "status"],
        "exit_code": 0,
        "stdout": "{}\n",
        "stderr": "",
    }
    assert documents.validate_fixture(good) == []

    missing = dict(good)
    del missing["stderr"]
    assert documents.validate_fixture(missing) != []

    extra = dict(good)
    extra["note"] = "nope"
    assert documents.validate_fixture(extra) != []

    for bad in ([], ["--contract", 1], "argv", None):
        doc = dict(good)
        doc["argv"] = bad
        assert documents.validate_fixture(doc) != [], bad

    for bad_exit in (True, "0", -1, 256, None):
        doc = dict(good)
        doc["exit_code"] = bad_exit
        assert documents.validate_fixture(doc) != [], bad_exit


@pytest.mark.parametrize("name", ["web-1", "a", "x" * 63, "0abc"])
def test_validate_instance_name_accepts_contract_names(name: str) -> None:
    assert documents.validate_instance_name(name) == []


@pytest.mark.parametrize("name", ["Web-1", "-web", "a" * 64, "", "a b", "a_b", 7, None])
def test_validate_instance_name_rejects_other_names(name: object) -> None:
    assert documents.validate_instance_name(name) != []


def _fixture(exit_code: object, stdout_doc: object) -> dict[str, object]:
    return {
        "argv": ["--contract", "1", "status"],
        "exit_code": exit_code,
        "stdout": json.dumps(stdout_doc) + "\n",
        "stderr": "",
    }


def test_classify_success_and_replay() -> None:
    assert documents.classify_fixture(_fixture(0, _ok_envelope())) == "success"

    replayed = {"contract": "1.0", "ok": True, "result": {"command": {}, "replayed": True}}
    assert documents.classify_fixture(_fixture(0, replayed)) == "replay"


def test_classify_rejections() -> None:
    rejected = {
        "contract": "1.0",
        "ok": False,
        "error": {"code": "rejected", "message": "no", "retryable": False},
    }
    assert documents.classify_fixture(_fixture(1, rejected)) == "rejection"

    usage = {
        "contract": "1.0",
        "ok": False,
        "error": {"code": "usage", "message": "no", "retryable": False},
    }
    assert documents.classify_fixture(_fixture(2, usage)) == "rejection"


@pytest.mark.parametrize(
    ("code", "exit_code", "expected"),
    [
        ("unsupported", 3, "unsupported"),
        ("contract_mismatch", 4, "contract_mismatch"),
        ("not_found", 5, "not_found"),
        ("agent_unreachable", 6, "ambiguous_outcome"),
        ("internal", 7, "ambiguous_outcome"),
    ],
)
def test_classify_special_codes(code: str, exit_code: int, expected: str) -> None:
    doc = {
        "contract": "1.0",
        "ok": False,
        "error": {
            "code": code,
            "message": "no",
            "retryable": code in ("agent_unreachable", "internal"),
        },
    }

    assert documents.classify_fixture(_fixture(exit_code, doc)) == expected


def test_classify_internal_with_stored_false_is_rejection() -> None:
    doc = {
        "contract": "1.0",
        "ok": False,
        "error": {
            "code": "internal",
            "message": "no",
            "retryable": True,
            "details": {"stored": False},
        },
    }

    assert documents.classify_fixture(_fixture(7, doc)) == "rejection"


@pytest.mark.parametrize(
    "fixture",
    [
        None,
        [],
        {"argv": [], "exit_code": 0, "stdout": "{}\n", "stderr": ""},
        {"argv": ["x"], "exit_code": True, "stdout": "{}\n", "stderr": ""},
        {"argv": ["x"], "exit_code": 0, "stdout": "not json\n", "stderr": ""},
        {"argv": ["x"], "exit_code": 0, "stdout": "[1]\n", "stderr": ""},
        {"argv": ["x"], "exit_code": 1, "stdout": json.dumps(_ok_envelope()) + "\n", "stderr": ""},
    ],
)
def test_classify_malformed_stdout(fixture: object) -> None:
    assert documents.classify_fixture(fixture) == "malformed_stdout"


def _command_record() -> dict[str, object]:
    return {
        "request_id": "req-3f1c9a2b7d10",
        "sequence": 7,
        "kind": "stop",
        "arguments": {},
        "repository": "acme/widgets",
        "acknowledgement": "accepted",
        "detail": "finish the active attempt, then close intake",
        "created_at": "2026-11-06T09:12:03Z",
        "updated_at": "2026-11-06T09:12:03Z",
    }


def test_validate_command_record_accepts_a_stop_record() -> None:
    assert documents.validate_command_record(_command_record()) == []


def test_validate_command_record_rejects_bad_enums_and_shapes() -> None:
    assert documents.validate_command_record(None) != []
    assert documents.validate_command_record({}) != []

    bad_kind = _command_record()
    bad_kind["kind"] = "halt"
    assert documents.validate_command_record(bad_kind) != []

    bad_ack = _command_record()
    bad_ack["acknowledgement"] = "done"
    assert documents.validate_command_record(bad_ack) != []

    bad_id = _command_record()
    bad_id["request_id"] = "bad id!"
    assert documents.validate_command_record(bad_id) != []

    bad_sequence = _command_record()
    bad_sequence["sequence"] = -1
    assert documents.validate_command_record(bad_sequence) != []


def test_command_kind_and_acknowledgement_enums_match_the_spec() -> None:
    assert documents.COMMAND_KINDS == (
        "stop",
        "stop_after",
        "resume",
        "next_issue",
        "handoff",
        "recovery_retry",
        "recovery_release",
        "errors_reset",
    )
    assert documents.ACKNOWLEDGEMENTS == (
        "accepted",
        "completed",
        "superseded",
        "not_fulfilled",
    )


def test_validate_command_ref_accepts_a_reference() -> None:
    assert documents.validate_command_ref({"request_id": "req-3f1c9a2b7d10", "sequence": 7}) == []
    assert documents.validate_command_ref({}) != []
    assert documents.validate_command_ref({"request_id": "bad id!", "sequence": 7}) != []


def test_validate_command_result_accepts_a_command_read() -> None:
    assert documents.validate_command_result({"command": _command_record()}) == []
    assert documents.validate_command_result({}) != []
    assert documents.validate_command_result({"command": {}}) != []


def test_validate_mutation_result_accepts_a_commit_and_rejects_shape_mistakes() -> None:
    assert (
        documents.validate_mutation_result({"command": _command_record(), "replayed": False}) == []
    )
    assert (
        documents.validate_mutation_result({"command": _command_record(), "replayed": True}) == []
    )
    assert documents.validate_mutation_result({"command": _command_record()}) != []
    assert (
        documents.validate_mutation_result({"command": _command_record(), "replayed": "no"}) != []
    )
    assert documents.validate_mutation_result({"replayed": False}) != []


def test_resource_loaders_expose_package_data() -> None:
    assert set(documents.SCHEMA_NAMES) == {
        "envelope",
        "identity",
        "fixture",
        "instance-name",
        "harness-request",
        "harness-reply",
        "client-envelope",
        "ls",
        "status",
        "figure",
        "command",
        "mutation-result",
    }
    for name in documents.SCHEMA_NAMES:
        schema = documents.load_schema(name)
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"

    assert "identity" in documents.fixture_names()
    assert documents.negative_names()
    assert documents.harness_example_names()
    assert documents.load_harness_example("setup.request")["verb"] == "setup"

    index = documents.load_fixture_index()
    assert index["version"] == 1
    assert isinstance(index["fixtures"], dict)


def _client_error(code: str = "client.unknown_instance") -> dict[str, object]:
    table = documents.client_table()
    entry = table[code]
    return {
        "contract": "1.0",
        "ok": False,
        "error": {
            "code": code,
            "message": "nope",
            "retryable": entry["retryable"],
        },
    }


def test_client_table_exit_classes_are_spec_section_4_classes() -> None:
    producer_exits = {entry["exit_code"] for entry in documents.error_table().values()}
    allowed = producer_exits | {2}

    for code, entry in documents.client_table().items():
        assert code.startswith("client.")
        assert entry["exit_code"] in allowed, code
        assert isinstance(entry["retryable"], bool), code


def test_validate_client_envelope_accepts_known_codes() -> None:
    for code in documents.client_table():
        assert documents.validate_client_envelope(_client_error(code)) == [], code


def test_validate_client_envelope_rejects_producer_codes_and_ok_true() -> None:
    producer = {
        "contract": "1.0",
        "ok": False,
        "error": {"code": "usage", "message": "x", "retryable": False},
    }
    assert documents.validate_client_envelope(producer) != []

    ok_true = {"contract": "1.0", "ok": True, "result": {}}
    assert documents.validate_client_envelope(ok_true) != []
    assert documents.validate_client_envelope(None) != []
    assert documents.validate_client_envelope({}) != []


def _ls_doc() -> dict[str, object]:
    return {
        "version": 1,
        "instances": [
            {
                "name": "web-1",
                "backend": "coding-agent",
                "contract": "1",
                "state": "running",
                "duplicate": False,
            },
            {
                "name": "dup",
                "backend": "coding-agent",
                "contract": "1",
                "state": "exited",
                "duplicate": True,
            },
        ],
    }


def test_validate_ls_accepts_listing() -> None:
    assert documents.validate_ls(_ls_doc()) == []
    assert documents.validate_ls({"version": 1, "instances": []}) == []


def test_validate_ls_rejects_bad_documents() -> None:
    assert documents.validate_ls(None) != []
    assert documents.validate_ls({}) != []

    missing = _ls_doc()
    del missing["instances"]
    assert documents.validate_ls(missing) != []

    bad_version = _ls_doc()
    bad_version["version"] = 2
    assert documents.validate_ls(bad_version) != []

    bad_name = _ls_doc()
    instances = bad_name["instances"]
    assert isinstance(instances, list)
    first = instances[0]
    assert isinstance(first, dict)
    first["name"] = "Web-1"
    assert documents.validate_ls(bad_name) != []


def _measured_figure(value: object = 118) -> dict[str, object]:
    return {"value": value, "unit": "count", "basis": "measured"}


def test_validate_figure_accepts_a_measured_number() -> None:
    assert documents.validate_figure(_measured_figure()) == []
    assert documents.validate_figure({"value": None, "unit": "ratio", "basis": "unavailable"}) == []
    assert (
        documents.validate_figure({"value": None, "unit": "count", "basis": "not_applicable"}) == []
    )


def test_validate_figure_rejects_a_measured_null() -> None:
    assert documents.validate_figure({"value": None, "unit": "tokens", "basis": "measured"}) != []


def test_validate_figure_rejects_an_unavailable_value() -> None:
    assert documents.validate_figure({"value": 5, "unit": "tokens", "basis": "unavailable"}) != []


def test_validate_figure_rejects_unknown_enums_and_booleans() -> None:
    bad_unit = _measured_figure()
    bad_unit["unit"] = "furlongs"
    assert documents.validate_figure(bad_unit) != []

    bad_basis = _measured_figure()
    bad_basis["basis"] = "guessed"
    assert documents.validate_figure(bad_basis) != []

    assert documents.validate_figure(_measured_figure(True)) != []


def _idle_snapshot() -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(documents.load_fixture("status-idle")["stdout"])
    result: dict[str, Any] = doc["result"]
    return result


def test_validate_status_accepts_an_idle_snapshot() -> None:
    assert documents.validate_status(_idle_snapshot()) == []


def test_validate_status_rejects_an_unknown_state() -> None:
    snapshot = _idle_snapshot()
    instance = snapshot["instance"]
    assert isinstance(instance, dict)
    instance["state"] = "flying"

    assert documents.validate_status(snapshot) != []


def test_validate_status_rejects_a_measured_null_figure() -> None:
    assert documents.validate_status(documents.load_negative("status-unknown-state")) != []
    assert documents.validate_figure(documents.load_negative("figure-measured-null")) != []
    assert documents.validate_figure(documents.load_negative("figure-unavailable-with-value")) != []
