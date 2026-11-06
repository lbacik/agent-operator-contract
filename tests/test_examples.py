"""Consumer fixtures: exact recordings, the classification index and the seed checks.

The fixtures live in the package (``agent_operator_contract.fixtures.agentctl``)
so a consumer that pins this repository as a git dependency loads them from the
installed package; they are plain JSON files in this repository too. The
per-fixture expectations live next to them in ``index.json`` so the recording
files themselves stay byte-exact.
"""

import json
from typing import Any

import pytest

from agent_operator_contract import documents

FIXTURE_NAMES = documents.fixture_names()


def _load_fixture(name: str) -> Any:
    return documents.load_fixture(name)


def _fixture_index() -> dict[str, Any]:
    index: dict[str, Any] = documents.load_fixture_index()
    fixtures = index.get("fixtures")
    assert isinstance(fixtures, dict)
    return fixtures


def test_fixtures_exist() -> None:
    assert FIXTURE_NAMES


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_fixture_matches_fixture_format(name: str) -> None:
    assert documents.validate_fixture(_load_fixture(name)) == []


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_fixture_stdout_is_one_json_document_and_a_newline(name: str) -> None:
    if _fixture_index()[name]["classification"] == "malformed_stdout":
        pytest.skip("malformed stdout is not one JSON document by design")
    stdout: str = _load_fixture(name)["stdout"]

    assert stdout.endswith("\n")
    assert stdout.count("\n") == 1
    assert isinstance(json.loads(stdout), dict)


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_positive_fixture_stdout_is_a_valid_envelope(name: str) -> None:
    if _fixture_index()[name]["classification"] == "malformed_stdout":
        pytest.skip("malformed stdout is not a valid envelope by design")
    doc = json.loads(_load_fixture(name)["stdout"])

    assert documents.validate_envelope(doc) == []


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_malformed_fixture_stdout_is_never_a_coherent_envelope(name: str) -> None:
    if _fixture_index()[name]["classification"] != "malformed_stdout":
        pytest.skip("only malformed stdout fixtures")
    fixture = _load_fixture(name)
    stdout: str = fixture["stdout"]
    exit_code: int = fixture["exit_code"]
    try:
        doc = json.loads(stdout)
    except ValueError:
        return
    if not isinstance(doc, dict) or documents.validate_envelope(doc) != []:
        return
    contract = doc.get("contract")
    major = documents.contract_major_of(contract)
    assert major != "1", name
    if doc.get("ok") is True:
        # Never parsed as success: interpret must reject the incoherent reply.
        from agent_operator_contract import client as _client

        with pytest.raises(_client.ClientError):
            _client.interpret(
                _client.ExecResult(
                    exit_code=exit_code,
                    stdout=stdout.encode("utf-8"),
                    stderr=b"",
                )
            )


def test_fixture_index_covers_every_fixture() -> None:
    assert set(_fixture_index()) == set(FIXTURE_NAMES)


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_fixture_index_classification_is_known(name: str) -> None:
    assert _fixture_index()[name]["classification"] in documents.CLASSIFICATIONS


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_library_classification_matches_index(name: str) -> None:
    expected: str = _fixture_index()[name]["classification"]

    assert documents.classify_fixture(_load_fixture(name)) == expected
