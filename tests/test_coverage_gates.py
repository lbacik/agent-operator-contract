"""Release coverage gates for contract v1.0.0 (spec section 15).

Each gate fails the build when the spec grows something the fixtures or the
producer suite do not cover yet:

- every ``error.code`` and every exit code of spec section 4 appears in at
  least one consumer fixture;
- every ``instance.state`` has a Status Snapshot fixture;
- every row of spec section 14 and every operation of spec section 6 has a
  producer scenario;
- CI runs the producer suite against the stub producer with every scenario
  required to pass and an empty expected-failures list.

The spec tables are read from ``docs/spec.md`` so the gates follow the prose.
"""

import json
import re
from pathlib import Path
from typing import Any

from agent_operator_contract import documents, producer_suite

REPO_ROOT = Path(__file__).resolve().parent.parent
SPEC = (REPO_ROOT / "docs" / "spec.md").read_text(encoding="utf-8")
CI_WORKFLOW = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

TRANSPORT_EXIT_CODES = {125, 126, 127}
"""Docker's own exit codes, which spec section 4 classes as transport failures."""


def _section(heading_prefix: str) -> str:
    """Return the body of the level-2 spec section whose heading starts so."""
    match = re.search(rf"^## {re.escape(heading_prefix)}.*?(?=^## |\Z)", SPEC, re.M | re.S)
    assert match is not None, heading_prefix
    return match.group(0)


def _first_cells(section: str) -> list[str]:
    """Return the first cell of every body row of every table in ``section``."""
    cells: list[str] = []
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        cell = re.split(r"(?<!\\)\|", line)[1].strip()
        if not cell or set(cell) <= {"-", " ", ":"}:
            continue
        cells.append(cell)
    return cells


def _table_rows(section: str, header: str) -> list[str]:
    """First cells of the body rows, dropping each table's header row."""
    return [cell for cell in _first_cells(section) if cell != header]


def _fixture_docs() -> list[tuple[str, dict[str, Any], Any]]:
    docs = []
    for name in documents.fixture_names():
        fixture = documents.load_fixture(name)
        try:
            doc = json.loads(fixture["stdout"])
        except ValueError:
            doc = None
        docs.append((name, fixture, doc))
    return docs


def test_every_error_code_appears_in_a_fixture() -> None:
    seen = {
        doc["error"].get("code")
        for _, _, doc in _fixture_docs()
        if isinstance(doc, dict) and isinstance(doc.get("error"), dict)
    }

    assert set(documents.error_table()) - seen == set()


def test_every_exit_code_appears_in_a_fixture() -> None:
    spec_exits = {
        int(cell) for cell in _table_rows(_section("4. "), "Exit") if cell.isdigit()
    } | TRANSPORT_EXIT_CODES
    table_exits = {int(entry["exit_code"]) for entry in documents.error_table().values()}
    seen = {int(fixture["exit_code"]) for _, fixture, _ in _fixture_docs()}

    assert table_exits | {0} <= spec_exits
    assert spec_exits - seen == set()


def test_every_instance_state_has_a_status_snapshot_fixture() -> None:
    seen = {
        doc["result"]["instance"]["state"]
        for _, _, doc in _fixture_docs()
        if isinstance(doc, dict)
        and doc.get("ok") is True
        and isinstance(doc.get("result"), dict)
        and isinstance(doc["result"].get("instance"), dict)
        and "state" in doc["result"]["instance"]
    }

    assert set(documents.STATUS_STATES) - seen == set()


def test_every_section_14_row_has_a_producer_scenario() -> None:
    rows = _table_rows(_section("14. "), "Scenario")

    assert rows
    assert sorted(rows) == sorted(producer_suite.SECTION_14_COVERAGE)


def test_every_section_6_operation_has_a_producer_scenario() -> None:
    operations = [cell.strip("`") for cell in _table_rows(_section("6. "), "Operation")]

    assert operations
    assert sorted(operations) == sorted(producer_suite.OPERATION_COVERAGE)


def test_coverage_maps_name_only_real_scenarios() -> None:
    known = set(producer_suite.scenario_ids())
    for coverage in (producer_suite.SECTION_14_COVERAGE, producer_suite.OPERATION_COVERAGE):
        for key, scenario_ids in coverage.items():
            assert scenario_ids, key
            assert set(scenario_ids) <= known, key


def test_ci_requires_every_scenario_to_pass_against_the_stub_producer() -> None:
    runs = [
        block
        for block in CI_WORKFLOW.split("- name: ")
        if "agent_operator_contract.producer_suite" in block
    ]

    assert runs, "CI must run the producer suite"
    assert all("--expect-fail" not in block for block in runs)
    assert any(
        "--require-all" in block and all(name in block for name in _optional_capabilities())
        for block in runs
    ), "one CI run must advertise every capability and require every scenario"


def _optional_capabilities() -> list[str]:
    cells = _table_rows(_section("5. "), "Capability")
    return [cell.strip("`") for cell in cells if cell.startswith("`")]
