"""Unit tests for the Status Snapshot DTOs (spec sections 5, 7.2 and 7.3).

Producer mode (``strict=True``) rejects unknown fields and unknown enum
values; consumer mode (``strict=False``) ignores unknown fields and keeps
unknown enum values in optional positions as the explicit string
``"unknown"`` so an operator never mistakes them for a real value.
"""

import copy
import json
from typing import Any

import pytest

from agent_operator_contract import documents, status


def _snapshot(name: str) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(documents.load_fixture(name)["stdout"])
    result: dict[str, Any] = doc["result"]
    return result


def _idle_snapshot() -> dict[str, Any]:
    return _snapshot("status-idle")


@pytest.mark.parametrize(
    "name",
    [
        "status-starting",
        "status-recovering",
        "status-recovering-retained",
        "status-idle",
        "status-active",
        "status-active-1-1",
        "status-draining",
        "status-paused",
        "status-simple-coding-agent",
        "offline-status-halted",
    ],
)
def test_strict_parses_every_positive_snapshot(name: str) -> None:
    parsed = status.parse_status_strict(_snapshot(name))

    assert isinstance(parsed, status.StatusSnapshot)


@pytest.mark.parametrize(
    "name",
    [
        "status-starting",
        "status-recovering",
        "status-recovering-retained",
        "status-idle",
        "status-active",
        "status-active-1-1",
        "status-draining",
        "status-paused",
        "status-simple-coding-agent",
        "offline-status-halted",
    ],
)
def test_lenient_parses_every_positive_snapshot(name: str) -> None:
    parsed = status.parse_status_lenient(_snapshot(name))

    assert isinstance(parsed, status.StatusSnapshot)


def test_strict_rejects_a_measured_null_figure() -> None:
    with pytest.raises(status.StatusError):
        status.parse_figure(documents.load_negative("figure-measured-null"), strict=True)


def test_strict_rejects_an_unavailable_figure_with_a_value() -> None:
    with pytest.raises(status.StatusError):
        status.parse_figure(documents.load_negative("figure-unavailable-with-value"), strict=True)


def test_strict_rejects_an_unknown_instance_state() -> None:
    with pytest.raises(status.StatusError):
        status.parse_status_strict(documents.load_negative("status-unknown-state"))


def test_strict_rejects_unknown_fields() -> None:
    snapshot = _idle_snapshot()
    snapshot["surprise"] = "a minor added this"

    with pytest.raises(status.StatusError):
        status.parse_status_strict(snapshot)


def test_strict_rejects_unknown_enum_values() -> None:
    snapshot = _idle_snapshot()
    instance = snapshot["instance"]
    assert isinstance(instance, dict)
    instance["state"] = "flying"

    with pytest.raises(status.StatusError):
        status.parse_status_strict(snapshot)


def test_lenient_keeps_an_unknown_optional_enum_and_ignores_unknown_fields() -> None:
    snapshot = _idle_snapshot()
    instance = snapshot["instance"]
    assert isinstance(instance, dict)
    instance["state"] = "flying"
    snapshot["surprise"] = "a minor added this"

    parsed = status.parse_status_lenient(snapshot)

    assert parsed.instance.state == status.UNKNOWN == "unknown"


def test_lenient_keeps_an_unknown_figure_unit() -> None:
    figure = {"value": 3, "unit": "furlongs", "basis": "measured"}

    parsed = status.parse_figure(figure, strict=False)

    assert parsed.unit == "unknown"


def test_lenient_still_enforces_the_value_basis_rule() -> None:
    with pytest.raises(status.StatusError):
        status.parse_figure(documents.load_negative("figure-measured-null"), strict=False)
    with pytest.raises(status.StatusError):
        status.parse_figure(documents.load_negative("figure-unavailable-with-value"), strict=False)


def test_strict_requires_a_startup_check_while_starting() -> None:
    snapshot = _snapshot("status-starting")
    instance = snapshot["instance"]
    assert isinstance(instance, dict)
    broken = copy.deepcopy(snapshot)
    assert isinstance(broken["instance"], dict)
    broken["instance"]["startup_check"] = None

    with pytest.raises(status.StatusError):
        status.parse_status_strict(broken)


def test_strict_requires_null_started_at_offline() -> None:
    snapshot = _snapshot("offline-status-halted")
    identity = snapshot["identity"]
    assert isinstance(identity, dict)
    broken = copy.deepcopy(snapshot)
    assert isinstance(broken["identity"], dict)
    broken["identity"]["process_started_at"] = "2026-11-06T08:00:00Z"

    with pytest.raises(status.StatusError):
        status.parse_status_strict(broken)


def test_strict_accepts_both_backend_flavours() -> None:
    coding_agent = status.parse_status_strict(_snapshot("status-active"))
    simple = status.parse_status_strict(_snapshot("status-simple-coding-agent"))

    assert coding_agent.current_attempt is not None
    assert coding_agent.current_attempt.attempt_id == "#42/1"
    assert simple.current_attempt is not None
    assert simple.current_attempt.budget.enforced_tokens == "raw"


def _by_origin(snapshot: dict[str, Any]) -> dict[str, Any]:
    by_origin: dict[str, Any] = snapshot["current_attempt"]["budget"]["by_origin"]
    return by_origin


@pytest.mark.parametrize("strict", [True, False])
def test_parses_spend_per_usage_origin(strict: bool) -> None:
    parsed = status.parse_status(_snapshot("status-active-1-1"), strict=strict)

    assert parsed.identity.contract_minor == 1
    assert parsed.current_attempt is not None
    by_origin = parsed.current_attempt.budget.by_origin
    assert by_origin is not None
    assert set(by_origin) == {"main", "subagent", "handoff"}
    assert by_origin["main"].cache_read_tokens.value == 1300000
    assert by_origin["subagent"].effective_tokens.basis == "estimated"
    assert by_origin["handoff"].cost_usd.basis == "unavailable"
    assert by_origin["handoff"].cost_usd.value is None
    figures = parsed.current_attempt.figures
    assert figures["context_threshold"].value == 140000
    assert figures["subagents_running"].unit == "count"
    assert figures["subagents_ended"].value == 3


@pytest.mark.parametrize("strict", [True, False])
def test_a_1_0_budget_has_no_spend_per_usage_origin(strict: bool) -> None:
    parsed = status.parse_status(_snapshot("status-active"), strict=strict)

    assert parsed.current_attempt is not None
    assert parsed.current_attempt.budget.by_origin is None


@pytest.mark.parametrize("strict", [True, False])
def test_an_unknown_origin_name_is_accepted_and_kept(strict: bool) -> None:
    snapshot = _snapshot("status-active-1-1")
    by_origin = _by_origin(snapshot)
    by_origin["compaction"] = copy.deepcopy(by_origin["main"])

    parsed = status.parse_status(snapshot, strict=strict)

    assert parsed.current_attempt is not None
    assert parsed.current_attempt.budget.by_origin is not None
    assert "compaction" in parsed.current_attempt.budget.by_origin
    assert "compaction" not in documents.ORIGIN_NAMES


@pytest.mark.parametrize(
    "name",
    [
        "status-by-origin-cell-missing-figure",
        "status-by-origin-cell-unit-not-the-names-unit",
        "status-by-origin-cell-not-an-object",
        "status-by-origin-not-an-object",
    ],
)
@pytest.mark.parametrize("strict", [True, False])
def test_rejects_a_malformed_usage_origin_cell(name: str, strict: bool) -> None:
    with pytest.raises(status.StatusError):
        status.parse_status(documents.load_negative(name), strict=strict)


@pytest.mark.parametrize("strict", [True, False])
def test_rejects_an_empty_origin_name(strict: bool) -> None:
    snapshot = _snapshot("status-active-1-1")
    by_origin = _by_origin(snapshot)
    by_origin[""] = by_origin.pop("handoff")

    with pytest.raises(status.StatusError):
        status.parse_status(snapshot, strict=strict)


def test_strict_rejects_an_unknown_field_in_a_cell_and_lenient_ignores_it() -> None:
    snapshot = _snapshot("status-active-1-1")
    _by_origin(snapshot)["main"]["response_count"] = 12

    with pytest.raises(status.StatusError):
        status.parse_status_strict(snapshot)
    assert status.parse_status_lenient(snapshot).current_attempt is not None


@pytest.mark.parametrize("figure", ["context_threshold", "subagents_running", "subagents_ended"])
def test_validator_rejects_a_new_figure_in_the_wrong_unit(figure: str) -> None:
    snapshot = _snapshot("status-active-1-1")
    snapshot["current_attempt"]["figures"][figure]["unit"] = "usd"

    assert documents.validate_status(snapshot) != []
