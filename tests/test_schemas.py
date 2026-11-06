"""The JSON Schemas agree with the fixtures and with the library validation.

Every positive fixture's stdout must validate against the envelope schema (and
against the Identity schema where the result carries one); every negative
example must be rejected by both the schema and ``documents``; and the schemas
and the library must reach the same verdict on every document.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from agent_operator_contract import documents

REPO_ROOT = Path(__file__).parent.parent

ENVELOPE_SCHEMA: dict[str, Any] = documents.load_schema("envelope")
IDENTITY_SCHEMA: dict[str, Any] = documents.load_schema("identity")
INSTANCE_NAME_SCHEMA: dict[str, Any] = documents.load_schema("instance-name")
HARNESS_REQUEST_SCHEMA: dict[str, Any] = documents.load_schema("harness-request")
HARNESS_REPLY_SCHEMA: dict[str, Any] = documents.load_schema("harness-reply")

ENVELOPE_VALIDATOR = Draft202012Validator(ENVELOPE_SCHEMA)
IDENTITY_VALIDATOR = Draft202012Validator(IDENTITY_SCHEMA)
INSTANCE_NAME_VALIDATOR = Draft202012Validator(INSTANCE_NAME_SCHEMA)
HARNESS_REQUEST_VALIDATOR = Draft202012Validator(HARNESS_REQUEST_SCHEMA)
HARNESS_REPLY_VALIDATOR = Draft202012Validator(HARNESS_REPLY_SCHEMA)

FIXTURE_NAMES = documents.fixture_names()
NEGATIVE_NAMES = documents.negative_names()
HARNESS_NAMES = documents.harness_example_names()


def _harness_validator(name: str) -> Draft202012Validator:
    if name.endswith(".request"):
        return HARNESS_REQUEST_VALIDATOR
    if name.endswith(".reply"):
        return HARNESS_REPLY_VALIDATOR
    raise AssertionError(f"harness example without a schema: {name}")


def _harness_library_errors(name: str, doc: Any) -> list[str]:
    if name.endswith(".request"):
        return documents.validate_harness_request(doc)
    if name.endswith(".reply"):
        return documents.validate_harness_reply(doc)
    raise AssertionError(f"harness example without a validator: {name}")


def _negative_schema(name: str) -> Draft202012Validator:
    if name.startswith("envelope-"):
        return ENVELOPE_VALIDATOR
    if name.startswith("identity-"):
        return Draft202012Validator(IDENTITY_SCHEMA)
    if name.startswith("fixture-"):
        return Draft202012Validator(documents.load_schema("fixture"))
    if name.startswith("harness-request-"):
        return HARNESS_REQUEST_VALIDATOR
    if name.startswith("harness-reply-"):
        return HARNESS_REPLY_VALIDATOR
    raise AssertionError(f"negative example without a schema: {name}")


def _negative_library_errors(name: str, doc: Any) -> list[str]:
    if name.startswith("envelope-"):
        return documents.validate_envelope(doc)
    if name.startswith("identity-"):
        return documents.validate_identity(doc)
    if name.startswith("fixture-"):
        return documents.validate_fixture(doc)
    if name.startswith("harness-request-"):
        return documents.validate_harness_request(doc)
    if name.startswith("harness-reply-"):
        return documents.validate_harness_reply(doc)
    raise AssertionError(f"negative example without a schema: {name}")


def _identities_in_stdout(doc: Any) -> list[Any]:
    """Every Identity carried by a successful stdout: the result itself or result.identity."""
    if not isinstance(doc, dict) or doc.get("ok") is not True:
        return []
    result = doc.get("result")
    if not isinstance(result, dict):
        return []
    found = []
    if "backend" in result and "mode" in result:
        found.append(result)
    nested = result.get("identity")
    if isinstance(nested, dict):
        found.append(nested)
    return found


@pytest.mark.parametrize("name", documents.SCHEMA_NAMES)
def test_schema_validates_against_draft_2020_12_meta_schema(name: str) -> None:
    Draft202012Validator.check_schema(documents.load_schema(name))


def test_every_schema_file_on_disk_is_meta_validated() -> None:
    on_disk = sorted(
        path.stem[: -len(".schema")]
        for path in (REPO_ROOT / "src" / "agent_operator_contract" / "schemas").glob(
            "*.schema.json"
        )
    )

    assert on_disk == sorted(documents.SCHEMA_NAMES)


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_positive_fixture_stdout_validates_against_envelope_schema(name: str) -> None:
    doc = json.loads(documents.load_fixture(name)["stdout"])

    assert ENVELOPE_VALIDATOR.is_valid(doc), name


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_identity_in_stdout_validates_against_identity_schema(name: str) -> None:
    doc = json.loads(documents.load_fixture(name)["stdout"])
    identities = _identities_in_stdout(doc)

    assert all(IDENTITY_VALIDATOR.is_valid(identity) for identity in identities)
    if name in ("identity", "status-draining", "offline-status-halted"):
        assert len(identities) == 1


@pytest.mark.parametrize("name", NEGATIVE_NAMES)
def test_negative_example_rejected_by_schema(name: str) -> None:
    doc = documents.load_negative(name)

    assert not _negative_schema(name).is_valid(doc), name


@pytest.mark.parametrize("name", NEGATIVE_NAMES)
def test_negative_example_rejected_by_library(name: str) -> None:
    assert _negative_library_errors(name, documents.load_negative(name)) != []


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_library_agrees_with_schema_on_positive_stdout(name: str) -> None:
    doc = json.loads(documents.load_fixture(name)["stdout"])

    assert (documents.validate_envelope(doc) == []) == ENVELOPE_VALIDATOR.is_valid(doc)


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_library_agrees_with_schema_on_identity_in_stdout(name: str) -> None:
    doc = json.loads(documents.load_fixture(name)["stdout"])

    for identity in _identities_in_stdout(doc):
        assert (documents.validate_identity(identity) == []) == IDENTITY_VALIDATOR.is_valid(
            identity
        )


def test_envelope_schema_matches_generated_output() -> None:
    script = REPO_ROOT / "scripts" / "generate_schemas.py"
    completed = subprocess.run(
        [sys.executable, str(script), "--check"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout


@pytest.mark.parametrize("name", NEGATIVE_NAMES)
def test_library_agrees_with_schema_on_negatives(name: str) -> None:
    doc = documents.load_negative(name)

    assert (_negative_library_errors(name, doc) == []) == _negative_schema(name).is_valid(doc)


@pytest.mark.parametrize("name", HARNESS_NAMES)
def test_harness_example_validates_against_schema(name: str) -> None:
    doc = documents.load_harness_example(name)

    assert _harness_validator(name).is_valid(doc), name


@pytest.mark.parametrize("name", HARNESS_NAMES)
def test_library_agrees_with_schema_on_harness_examples(name: str) -> None:
    doc = documents.load_harness_example(name)

    assert (_harness_library_errors(name, doc) == []) == _harness_validator(name).is_valid(doc)


def test_harness_verb_enums_match_library() -> None:
    expected = list(documents.HARNESS_VERBS)

    assert HARNESS_REQUEST_SCHEMA["properties"]["verb"]["enum"] == expected
    assert HARNESS_REPLY_SCHEMA["properties"]["verb"]["enum"] == expected


def test_harness_point_enums_match_library() -> None:
    def collect_point_enums(node: Any) -> list[list[str]]:
        found: list[list[str]] = []
        if isinstance(node, dict):
            for key, value in node.items():
                if (
                    key == "properties"
                    and isinstance(value, dict)
                    and isinstance(value.get("point"), dict)
                    and isinstance(value["point"].get("enum"), list)
                ):
                    found.append(value["point"]["enum"])
                found.extend(collect_point_enums(value))
        elif isinstance(node, list):
            for item in node:
                found.extend(collect_point_enums(item))
        return found

    expected = {tuple(documents.HOLD_POINTS), tuple(documents.CRASH_POINTS)}
    request_enums = {tuple(enum) for enum in collect_point_enums(HARNESS_REQUEST_SCHEMA)}
    reply_enums = {tuple(enum) for enum in collect_point_enums(HARNESS_REPLY_SCHEMA)}

    assert request_enums == expected
    assert reply_enums == expected


def test_harness_error_code_enum_matches_library() -> None:
    enum = HARNESS_REPLY_SCHEMA["properties"]["error"]["properties"]["code"]["enum"]

    assert set(enum) == set(documents.HARNESS_ERROR_CODES)
    assert len(enum) == len(set(enum))


def test_harness_unsupported_what_enum_matches_library() -> None:
    details = HARNESS_REPLY_SCHEMA["properties"]["error"]["properties"]["details"]

    assert details["properties"]["what"]["enum"] == list(documents.HARNESS_UNSUPPORTED_WHAT)


def test_harness_setup_repository_pattern_matches_library() -> None:
    params = HARNESS_REQUEST_SCHEMA["allOf"][0]["then"]["properties"]["params"]

    assert params["properties"]["repository"]["pattern"] == documents.REPOSITORY_PATTERN


def test_harness_version_pattern_matches_library() -> None:
    assert HARNESS_REPLY_SCHEMA["properties"]["harness"]["pattern"] == documents.CONTRACT_PATTERN


@pytest.mark.parametrize("name", ["web-1", "a", "0abc"])
def test_library_agrees_with_schema_on_instance_names(name: str) -> None:
    assert (documents.validate_instance_name(name) == []) == INSTANCE_NAME_VALIDATOR.is_valid(name)


@pytest.mark.parametrize("name", ["Web-1", "-web", "", "a b"])
def test_library_agrees_with_schema_on_bad_instance_names(name: str) -> None:
    assert (documents.validate_instance_name(name) == []) == INSTANCE_NAME_VALIDATOR.is_valid(name)
    assert not INSTANCE_NAME_VALIDATOR.is_valid(name)


def test_envelope_error_code_enum_matches_error_table() -> None:
    enum = ENVELOPE_SCHEMA["properties"]["error"]["properties"]["code"]["enum"]

    assert set(enum) == set(documents.error_table())
    assert len(enum) == len(set(enum))


@pytest.mark.parametrize(
    ("schema_name", "path", "pattern"),
    [
        ("envelope", ["properties", "contract", "pattern"], documents.CONTRACT_PATTERN),
        (
            "envelope",
            ["properties", "error", "properties", "request_id", "pattern"],
            documents.REQUEST_ID_PATTERN,
        ),
        ("identity", ["properties", "repository", "pattern"], documents.REPOSITORY_PATTERN),
        (
            "identity",
            ["properties", "process_started_at", "pattern"],
            documents.TIMESTAMP_PATTERN,
        ),
        ("instance-name", ["pattern"], documents.INSTANCE_NAME_PATTERN),
    ],
)
def test_schema_patterns_match_library(schema_name: str, path: list[str], pattern: str) -> None:
    node: Any = documents.load_schema(schema_name)
    for key in path:
        node = node[key]

    assert node == pattern


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_error_fixture_signals_match_error_table(name: str) -> None:
    fixture = documents.load_fixture(name)
    doc = json.loads(fixture["stdout"])
    table = documents.error_table()

    if doc["ok"] is True:
        assert fixture["exit_code"] == 0
    else:
        code: str = doc["error"]["code"]
        assert fixture["exit_code"] == table[code]["exit_code"]
        assert doc["error"]["retryable"] is table[code]["retryable"]
