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


def test_resource_loaders_expose_package_data() -> None:
    assert set(documents.SCHEMA_NAMES) == {
        "envelope",
        "identity",
        "fixture",
        "instance-name",
        "harness-request",
        "harness-reply",
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
