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

STATUS_SCHEMA: dict[str, Any] = documents.load_schema("status")
FIGURE_SCHEMA: dict[str, Any] = documents.load_schema("figure")

ENVELOPE_VALIDATOR = Draft202012Validator(ENVELOPE_SCHEMA)
IDENTITY_VALIDATOR = Draft202012Validator(IDENTITY_SCHEMA)
STATUS_VALIDATOR = Draft202012Validator(STATUS_SCHEMA)
FIGURE_VALIDATOR = Draft202012Validator(FIGURE_SCHEMA)

COMMAND_SCHEMA: dict[str, Any] = documents.load_schema("command")
MUTATION_RESULT_SCHEMA: dict[str, Any] = documents.load_schema("mutation-result")
COMMAND_VALIDATOR = Draft202012Validator(COMMAND_SCHEMA)
MUTATION_RESULT_VALIDATOR = Draft202012Validator(MUTATION_RESULT_SCHEMA)
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
    if name.startswith("client-envelope-"):
        return Draft202012Validator(documents.load_schema("client-envelope"))
    if name.startswith("ls-"):
        return Draft202012Validator(documents.load_schema("ls"))
    if name.startswith("status-"):
        return STATUS_VALIDATOR
    if name.startswith("figure-"):
        return FIGURE_VALIDATOR
    if name.startswith("attempt-summary-"):
        return Draft202012Validator(documents.load_schema("attempt-summary"))
    if name.startswith("attempts-list-"):
        return Draft202012Validator(documents.load_schema("attempts-list"))
    if name.startswith("attempts-show-"):
        return Draft202012Validator(documents.load_schema("attempts-show"))
    if name.startswith("manifest-entry-"):
        return Draft202012Validator(documents.load_schema("manifest-entry"))
    if name.startswith("export-manifest-"):
        return Draft202012Validator(documents.load_schema("export-manifest"))
    if name.startswith("contract-event-"):
        return Draft202012Validator(documents.load_schema("contract-event"))
    if name.startswith("command-"):
        return Draft202012Validator(documents.load_schema("command"))
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
    if name.startswith("client-envelope-"):
        return documents.validate_client_envelope(doc)
    if name.startswith("ls-"):
        return documents.validate_ls(doc)
    if name.startswith("status-"):
        return documents.validate_status(doc)
    if name.startswith("figure-"):
        return documents.validate_figure(doc)
    if name.startswith("attempt-summary-"):
        return documents.validate_attempt_summary(doc)
    if name.startswith("attempts-list-"):
        return documents.validate_attempts_list(doc)
    if name.startswith("attempts-show-"):
        return documents.validate_attempts_show(doc)
    if name.startswith("manifest-entry-"):
        return documents.validate_manifest_entry(doc)
    if name.startswith("export-manifest-"):
        return documents.validate_export_manifest(doc)
    if name.startswith("contract-event-"):
        return documents.validate_contract_event(doc)
    if name.startswith("command-"):
        return documents.validate_command_result(doc)
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


def _fixture_classification(name: str) -> str:
    index = documents.load_fixture_index()
    fixtures = index.get("fixtures")
    assert isinstance(fixtures, dict)
    entry = fixtures.get(name)
    assert isinstance(entry, dict)
    classification = entry.get("classification")
    assert isinstance(classification, str)
    return classification


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_positive_fixture_stdout_validates_against_envelope_schema(name: str) -> None:
    if _fixture_classification(name) == "malformed_stdout":
        pytest.skip("malformed stdout is not a valid envelope by design")
    doc = json.loads(documents.load_fixture(name)["stdout"])

    assert ENVELOPE_VALIDATOR.is_valid(doc), name


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_identity_in_stdout_validates_against_identity_schema(name: str) -> None:
    if _fixture_classification(name) == "malformed_stdout":
        pytest.skip("malformed stdout carries no identity by design")
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
    if _fixture_classification(name) == "malformed_stdout":
        pytest.skip("malformed stdout is rejected by design")
    doc = json.loads(documents.load_fixture(name)["stdout"])

    assert (documents.validate_envelope(doc) == []) == ENVELOPE_VALIDATOR.is_valid(doc)


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_library_agrees_with_schema_on_identity_in_stdout(name: str) -> None:
    if _fixture_classification(name) == "malformed_stdout":
        pytest.skip("malformed stdout carries no identity by design")
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
    # Only a usage reply may echo an unknown verb, so the enum binds every other reply.
    (verb_rule,) = [
        rule
        for rule in HARNESS_REPLY_SCHEMA["allOf"]
        if "verb" in rule.get("else", {}).get("properties", {})
    ]
    assert verb_rule["else"]["properties"]["verb"]["enum"] == expected


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
    if _fixture_classification(name) == "malformed_stdout":
        pytest.skip("malformed stdout has no error-code signal by design")
    fixture = documents.load_fixture(name)
    doc = json.loads(fixture["stdout"])
    table = documents.error_table()

    if doc["ok"] is True:
        assert fixture["exit_code"] == 0
    else:
        code: str = doc["error"]["code"]
        assert fixture["exit_code"] == table[code]["exit_code"]
        assert doc["error"]["retryable"] is table[code]["retryable"]


def test_client_envelope_error_code_enum_matches_client_table() -> None:
    schema = documents.load_schema("client-envelope")
    enum = schema["properties"]["error"]["properties"]["code"]["enum"]

    assert set(enum) == set(documents.client_table())
    assert len(enum) == len(set(enum))


def test_client_envelope_schema_patterns_match_library() -> None:
    schema = documents.load_schema("client-envelope")

    assert schema["properties"]["contract"]["pattern"] == documents.CONTRACT_PATTERN


def test_ls_schema_patterns_match_library() -> None:
    schema = documents.load_schema("ls")
    item = schema["properties"]["instances"]["items"]["properties"]

    assert item["name"]["pattern"] == documents.INSTANCE_NAME_PATTERN
    assert item["contract"]["pattern"] == documents.CONTRACT_MAJOR_PATTERN


def _mutation_results() -> list[tuple[str, Any]]:
    pairs = []
    for name in FIXTURE_NAMES:
        if _fixture_classification(name) == "malformed_stdout":
            continue
        doc = json.loads(documents.load_fixture(name)["stdout"])
        if doc.get("ok") is True and isinstance(doc.get("result"), dict):
            result = doc["result"]
            if "command" in result and "replayed" in result:
                pairs.append((name, result))
    return pairs


def _command_results() -> list[tuple[str, Any]]:
    pairs = []
    for name in FIXTURE_NAMES:
        if _fixture_classification(name) == "malformed_stdout":
            continue
        doc = json.loads(documents.load_fixture(name)["stdout"])
        if doc.get("ok") is True and isinstance(doc.get("result"), dict):
            result = doc["result"]
            if "command" in result and "replayed" not in result:
                pairs.append((name, result))
    return pairs


def test_mutation_results_validate_against_mutation_result_schema() -> None:
    assert _mutation_results()
    for name, result in _mutation_results():
        assert MUTATION_RESULT_VALIDATOR.is_valid(result), name


def test_mutation_results_validate_against_library() -> None:
    for name, result in _mutation_results():
        assert documents.validate_mutation_result(result) == [], name


def test_library_agrees_with_schema_on_mutation_results() -> None:
    for name, result in _mutation_results():
        assert (documents.validate_mutation_result(result) == []) == (
            MUTATION_RESULT_VALIDATOR.is_valid(result)
        ), name


def test_command_reads_validate_against_command_schema() -> None:
    assert _command_results()
    for name, result in _command_results():
        assert COMMAND_VALIDATOR.is_valid(result), name


def test_command_reads_validate_against_library() -> None:
    for name, result in _command_results():
        assert documents.validate_command_result(result) == [], name


def test_library_agrees_with_schema_on_command_reads() -> None:
    for name, result in _command_results():
        assert (documents.validate_command_result(result) == []) == (
            COMMAND_VALIDATOR.is_valid(result)
        ), name


def test_command_schema_enums_and_patterns_match_library() -> None:
    record = COMMAND_SCHEMA["$defs"]["command_record"]["properties"]
    assert record["kind"]["enum"] == list(documents.COMMAND_KINDS)
    assert record["acknowledgement"]["enum"] == list(documents.ACKNOWLEDGEMENTS)
    assert record["request_id"]["pattern"] == documents.REQUEST_ID_PATTERN
    assert record["repository"]["pattern"] == documents.REPOSITORY_PATTERN
    ref = COMMAND_SCHEMA["$defs"]["command_ref"]["properties"]
    assert ref["request_id"]["pattern"] == documents.REQUEST_ID_PATTERN

    mutation_record = MUTATION_RESULT_SCHEMA["$defs"]["command_record"]["properties"]
    assert mutation_record["kind"]["enum"] == list(documents.COMMAND_KINDS)
    assert mutation_record["acknowledgement"]["enum"] == list(documents.ACKNOWLEDGEMENTS)
    assert mutation_record["request_id"]["pattern"] == documents.REQUEST_ID_PATTERN

    status_record = STATUS_SCHEMA["$defs"]["command_record"]["properties"]
    assert status_record["kind"]["enum"] == list(documents.COMMAND_KINDS)
    assert status_record["acknowledgement"]["enum"] == list(documents.ACKNOWLEDGEMENTS)
    assert status_record["request_id"]["pattern"] == documents.REQUEST_ID_PATTERN


def test_mutation_fixture_signals_match_mutation_schema() -> None:
    replayed = [result["replayed"] for _, result in _mutation_results() if "replayed" in result]

    assert False in replayed
    assert True in replayed


def _status_snapshots() -> list[tuple[str, Any]]:
    pairs = []
    for name in FIXTURE_NAMES:
        if _fixture_classification(name) == "malformed_stdout":
            continue
        doc = json.loads(documents.load_fixture(name)["stdout"])
        if doc.get("ok") is True and isinstance(doc.get("result"), dict):
            result = doc["result"]
            if "instance" in result and "intake" in result and "generated_at" in result:
                pairs.append((name, result))
    return pairs


def test_status_snapshots_validate_against_status_schema() -> None:
    assert _status_snapshots()
    for name, snapshot in _status_snapshots():
        assert STATUS_VALIDATOR.is_valid(snapshot), name


def test_status_snapshots_validate_against_library() -> None:
    for name, snapshot in _status_snapshots():
        assert documents.validate_status(snapshot) == [], name


def test_library_agrees_with_schema_on_status_snapshots() -> None:
    for name, snapshot in _status_snapshots():
        assert (documents.validate_status(snapshot) == []) == STATUS_VALIDATOR.is_valid(snapshot), (
            name
        )


def test_status_schema_enums_match_library() -> None:
    assert STATUS_SCHEMA["properties"]["instance"]["properties"]["state"]["enum"] == list(
        documents.STATUS_STATES
    )
    assert STATUS_SCHEMA["properties"]["intake"]["properties"]["state"]["enum"] == list(
        documents.INTAKE_STATES
    )
    assert STATUS_SCHEMA["$defs"]["budget"]["properties"]["enforced_tokens"]["enum"] == list(
        documents.ENFORCED_TOKENS
    )
    figure = STATUS_SCHEMA["$defs"]["figure"]
    assert figure["properties"]["unit"]["enum"] == list(documents.FIGURE_UNITS)
    assert figure["properties"]["basis"]["enum"] == list(documents.FIGURE_BASES)


def test_figure_schema_enums_match_library() -> None:
    assert FIGURE_SCHEMA["properties"]["unit"]["enum"] == list(documents.FIGURE_UNITS)
    assert FIGURE_SCHEMA["properties"]["basis"]["enum"] == list(documents.FIGURE_BASES)


def test_status_limit_name_enum_matches_library() -> None:
    name_schema = STATUS_SCHEMA["$defs"]["budget"]["properties"]["limits"]["items"]["properties"][
        "name"
    ]
    enum_option = next(option for option in name_schema["anyOf"] if "enum" in option)

    assert enum_option["enum"] == list(documents.LIMIT_NAMES)


def test_status_schema_covers_every_instance_state() -> None:
    seen = {snapshot["instance"]["state"] for _, snapshot in _status_snapshots()}

    assert set(documents.STATUS_STATES) <= seen


ATTEMPT_SUMMARY_SCHEMA: dict[str, Any] = documents.load_schema("attempt-summary")
MANIFEST_ENTRY_SCHEMA: dict[str, Any] = documents.load_schema("manifest-entry")
ATTEMPTS_LIST_SCHEMA: dict[str, Any] = documents.load_schema("attempts-list")
ATTEMPTS_SHOW_SCHEMA: dict[str, Any] = documents.load_schema("attempts-show")
CONTRACT_EVENT_SCHEMA: dict[str, Any] = documents.load_schema("contract-event")
ATTEMPT_SUMMARY_VALIDATOR = Draft202012Validator(ATTEMPT_SUMMARY_SCHEMA)
MANIFEST_ENTRY_VALIDATOR = Draft202012Validator(MANIFEST_ENTRY_SCHEMA)
ATTEMPTS_LIST_VALIDATOR = Draft202012Validator(ATTEMPTS_LIST_SCHEMA)
ATTEMPTS_SHOW_VALIDATOR = Draft202012Validator(ATTEMPTS_SHOW_SCHEMA)
CONTRACT_EVENT_VALIDATOR = Draft202012Validator(CONTRACT_EVENT_SCHEMA)


def _attempts_results() -> list[tuple[str, Any, Any]]:
    pairs = []
    for name in FIXTURE_NAMES:
        if _fixture_classification(name) == "malformed_stdout":
            continue
        fixture = documents.load_fixture(name)
        argv = fixture.get("argv", [])
        if not (
            (len(argv) >= 3 and argv[1:3] == ["1", "attempts"]) or argv[:2] == ["--contract", "1"]
        ):
            continue
        doc = json.loads(fixture["stdout"])
        if doc.get("ok") is not True or not isinstance(doc.get("result"), dict):
            continue
        result = doc["result"]
        if "attempts" in result or "attempt" in result:
            pairs.append((name, result, argv))
    return pairs


def test_attempts_results_validate_against_schemas() -> None:
    assert _attempts_results()
    for name, result, argv in _attempts_results():
        if "attempts" in result:
            assert ATTEMPTS_LIST_VALIDATOR.is_valid(result), name
        else:
            assert ATTEMPTS_SHOW_VALIDATOR.is_valid(result), name
        assert "attempts" in argv, name


def test_attempts_results_validate_against_library() -> None:
    for name, result, _ in _attempts_results():
        if "attempts" in result:
            assert documents.validate_attempts_list(result) == [], name
        else:
            assert documents.validate_attempts_show(result) == [], name


def test_library_agrees_with_schema_on_attempts_results() -> None:
    for name, result, _ in _attempts_results():
        if "attempts" in result:
            assert (documents.validate_attempts_list(result) == []) == (
                ATTEMPTS_LIST_VALIDATOR.is_valid(result)
            ), name
        else:
            assert (documents.validate_attempts_show(result) == []) == (
                ATTEMPTS_SHOW_VALIDATOR.is_valid(result)
            ), name


def test_attempts_fixture_ids_cover_backend_shapes() -> None:
    ids = set()
    for _, result, _ in _attempts_results():
        if "attempts" in result:
            for summary in result["attempts"]:
                ids.add(summary["attempt_id"])
        else:
            ids.add(result["attempt"]["attempt_id"])
    assert any(str(attempt_id).startswith("#") for attempt_id in ids)
    assert any(not str(attempt_id).startswith("#") for attempt_id in ids)


def test_attempts_list_paging_cursor_shapes() -> None:
    paged = json.loads(documents.load_fixture("attempts-list")["stdout"])["result"]
    assert paged["next_cursor"] is None
    first = json.loads(documents.load_fixture("attempts-list-paged")["stdout"])["result"]
    assert isinstance(first["next_cursor"], str)
    assert first["next_cursor"]


def test_manifest_entry_kind_enum_matches_library() -> None:
    assert MANIFEST_ENTRY_SCHEMA["properties"]["kind"]["enum"] == list(documents.MANIFEST_KINDS)
    assert ATTEMPTS_SHOW_SCHEMA["$defs"]["manifest_entry"]["properties"]["kind"]["enum"] == list(
        documents.MANIFEST_KINDS
    )


def test_manifest_entry_schema_patterns_match_library() -> None:
    assert MANIFEST_ENTRY_SCHEMA["properties"]["path"]["pattern"] == documents.MANIFEST_PATH_PATTERN
    assert MANIFEST_ENTRY_SCHEMA["properties"]["sha256"]["pattern"] == documents.SHA256_PATTERN


def test_attempt_summary_sealed_rule_matches_library() -> None:
    sealed_rule = ATTEMPT_SUMMARY_SCHEMA["allOf"]
    assert any("sealed_at" in json.dumps(rule) for rule in sealed_rule)
    for doc_name in ("attempts-list", "attempts-list-paged"):
        result = json.loads(documents.load_fixture(doc_name)["stdout"])["result"]
        for summary in result["attempts"]:
            assert documents.validate_attempt_summary(summary) == [], doc_name


def test_contract_event_enums_match_library() -> None:
    assert CONTRACT_EVENT_SCHEMA["properties"]["level"]["enum"] == list(documents.EVENT_LEVELS)
    assert CONTRACT_EVENT_SCHEMA["properties"]["kind"]["enum"] == list(documents.COMMAND_KINDS)
    assert CONTRACT_EVENT_SCHEMA["properties"]["acknowledgement"]["enum"] == list(
        documents.ACKNOWLEDGEMENTS
    )
    assert CONTRACT_EVENT_SCHEMA["properties"]["result"]["enum"] == list(
        documents.STARTUP_CHECK_RESULTS
    )


def test_contract_event_schema_patterns_match_library() -> None:
    assert CONTRACT_EVENT_SCHEMA["properties"]["at"]["pattern"] == documents.TIMESTAMP_PATTERN
    assert (
        CONTRACT_EVENT_SCHEMA["properties"]["request_id"]["pattern"] == documents.REQUEST_ID_PATTERN
    )


def test_status_recent_events_validate_as_contract_events() -> None:
    for name, snapshot in _status_snapshots():
        for event in snapshot.get("recent_events", []):
            assert CONTRACT_EVENT_VALIDATOR.is_valid(event), (name, event)
            assert documents.validate_contract_event(event) == [], (name, event)


def test_manifest_path_forbids_a_dotdot_segment_not_a_dotdot_substring() -> None:
    entry = {
        "path": "notes/a..b.txt",
        "kind": "other",
        "media_type": "text/plain",
        "size": 1,
        "sha256": "0" * 64,
    }
    assert MANIFEST_ENTRY_VALIDATOR.is_valid(entry)
    assert documents.validate_manifest_entry(entry) == []
    for bad in ("..", "../a", "a/..", "a/../b", "/a"):
        assert not MANIFEST_ENTRY_VALIDATOR.is_valid(dict(entry, path=bad)), bad
        assert documents.validate_manifest_entry(dict(entry, path=bad)) != [], bad


def test_usage_reply_may_echo_an_unknown_verb() -> None:
    reply = {
        "harness": "1.1",
        "verb": "fly",
        "ok": False,
        "error": {"code": "usage", "message": "unknown verb 'fly'"},
    }
    assert HARNESS_REPLY_VALIDATOR.is_valid(reply)
    assert documents.validate_harness_reply(reply) == []


def test_export_manifest_accepts_any_major_1_minor() -> None:
    sealed = json.loads(documents.load_fixture("attempts-show-sealed")["stdout"])["result"]
    manifest = {"contract": "1.3", "attempt": sealed["attempt"], "files": sealed["manifest"]}
    assert Draft202012Validator(documents.load_schema("export-manifest")).is_valid(manifest)
    assert documents.validate_export_manifest(manifest) == []


def test_status_unit_tables_match_library() -> None:
    budget = STATUS_SCHEMA["$defs"]["budget"]
    for key in ("raw_tokens", "effective_tokens"):
        assert budget["properties"][key]["properties"]["unit"]["const"] == "tokens"
    limit_units = {
        rule["if"]["properties"]["name"]["const"]: rule["then"]["properties"]["used"]["properties"][
            "unit"
        ]["const"]
        for rule in budget["properties"]["limits"]["items"]["allOf"]
    }
    assert limit_units == documents.LIMIT_UNITS
    figures = STATUS_SCHEMA["properties"]["current_attempt"]["properties"]["figures"]
    figure_units = {
        name: entry["properties"]["unit"]["const"] for name, entry in figures["properties"].items()
    }
    assert figure_units == documents.FIGURE_NAME_UNITS


@pytest.mark.parametrize("schema_name", ["command", "mutation-result", "status"])
def test_command_argument_rules_match_library(schema_name: str) -> None:
    record = documents.load_schema(schema_name)["$defs"]["command_record"]
    required = {
        rule["if"]["properties"]["kind"]["const"]: tuple(
            rule["then"]["properties"]["arguments"]["required"]
        )
        for rule in record["allOf"]
    }
    assert required == documents.COMMAND_ARGUMENTS


def test_export_manifest_contract_pattern_matches_library() -> None:
    schema = documents.load_schema("export-manifest")
    assert schema["properties"]["contract"]["pattern"] == documents.EXPORT_CONTRACT_PATTERN


def test_only_a_usage_reply_may_echo_an_unknown_verb() -> None:
    failed = {
        "harness": "1.1",
        "verb": "fly",
        "ok": False,
        "error": {"code": "failed", "message": "no instance"},
    }
    assert not HARNESS_REPLY_VALIDATOR.is_valid(failed)
    assert documents.validate_harness_reply(failed) != []


def test_attempts_show_requires_events_jsonl_by_path() -> None:
    show = json.loads(documents.load_fixture("attempts-show-sealed")["stdout"])["result"]
    for entry in show["manifest"]:
        if entry["kind"] == "events":
            entry["path"] = "foo.ndjson"
    assert not ATTEMPTS_SHOW_VALIDATOR.is_valid(show)
    assert documents.validate_attempts_show(show) != []
