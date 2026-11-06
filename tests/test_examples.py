"""Shape checks for the consumer fixtures under examples/agentctl/.

Full validation against the JSON Schemas replaces these once the schemas exist
(docs/adoption-plan.md, step 1).
"""

import json
from pathlib import Path
from typing import Any

import pytest

EXAMPLES = Path(__file__).parent.parent / "examples" / "agentctl"
FIXTURES = sorted(EXAMPLES.glob("*.json"))


def test_examples_exist() -> None:
    assert FIXTURES


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.stem)
def test_fixture_records_one_invocation(path: Path) -> None:
    fixture: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

    assert set(fixture) == {"argv", "exit_code", "stdout", "stderr"}
    assert all(isinstance(arg, str) for arg in fixture["argv"])
    assert isinstance(fixture["exit_code"], int)
    assert isinstance(fixture["stderr"], str)


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.stem)
def test_fixture_stdout_is_one_json_document_and_a_newline(path: Path) -> None:
    stdout: str = json.loads(path.read_text(encoding="utf-8"))["stdout"]

    assert stdout.endswith("\n")
    assert stdout.count("\n") == 1
    assert isinstance(json.loads(stdout), dict)
