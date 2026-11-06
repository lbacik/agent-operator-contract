"""Validation and classification of the contract's documents.

Covers the reply envelope and ``Identity`` (spec sections 4 and 7.1), the
consumer fixture format (spec section 15), the ``agentfleet`` client error
envelope (``client.`` codes), the ``agentfleet ls --json`` listing and the
conformance harness protocol (spec section 15.1). Standard library only: this
module mirrors the JSON Schemas in ``schemas/`` rule for rule, so engines and
consumers without a JSON Schema engine reach the same verdicts. It is not a
general JSON Schema engine; agreement with the schemas is tested on the whole
fixture set (see ``tests/test_schemas.py``).

The ``error.code`` table lives once, as data, in ``error-table.json``: each
code with its fixed exit code and ``retryable`` flag (spec section 4). The
envelope schema's ``error.code`` enum mirrors these keys (tests enforce
equality); :func:`validate_envelope` and :func:`classify_fixture` read this
file at runtime. The ``client.`` code table lives once, as data, in
``client-table.json``: each client failure with the spec section 4 exit
class it exits with and its ``retryable`` flag. The client-envelope schema's
``error.code`` enum mirrors these keys. Pattern matching uses ``re.search``,
exactly like the JSON Schema ``pattern`` keyword, and every pattern constant
below is pinned to the matching schema pattern by tests.
"""

from __future__ import annotations

import json
import re
from importlib import resources
from typing import Any, Literal

Classification = Literal[
    "success",
    "replay",
    "rejection",
    "unsupported",
    "contract_mismatch",
    "not_found",
    "ambiguous_outcome",
    "malformed_stdout",
]
"""How a consumer reads one fixture: the expected verdict recorded in index.json."""

CLASSIFICATIONS: tuple[Classification, ...] = (
    "success",
    "replay",
    "rejection",
    "unsupported",
    "contract_mismatch",
    "not_found",
    "ambiguous_outcome",
    "malformed_stdout",
)

REQUEST_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
"""Consumer-generated durable request ID (spec section 6.2)."""

INSTANCE_NAME_PATTERN = r"^[a-z0-9][a-z0-9-]{0,62}$"
"""Unique-on-the-host instance name from the discovery labels (spec section 2.1)."""

CONTRACT_PATTERN = r"^[0-9]+\.[0-9]+$"
"""Producer version as major.minor, e.g. 1.0 (spec section 4)."""

TIMESTAMP_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z$"
"""RFC 3339 UTC timestamps with a Z suffix (spec section 7)."""

REPOSITORY_PATTERN = r"^[^/\s]+/[^/\s]+$"
"""Target Repository written owner/name."""

CONTRACT_MAJOR_PATTERN = r"^[0-9]+$"
"""Contract major from the discovery label, e.g. 1 (spec section 2.1)."""

CLIENT_CODE_PATTERN = r"^client\.[a-z_]+$"
"""Shape of an agentfleet client error.code; the exact codes live in client-table.json."""
SCHEMA_NAMES: tuple[str, ...] = (
    "envelope",
    "identity",
    "fixture",
    "instance-name",
    "client-envelope",
    "ls",
    "harness-request",
    "harness-reply",
    "status",
    "figure",
    "command",
    "mutation-result",
)

STATUS_STATES: tuple[str, ...] = (
    "starting",
    "recovering",
    "idle",
    "active",
    "draining",
    "paused",
    "stopped",
)
"""Every ``instance.state`` of a Status Snapshot (spec section 7.2)."""

FIGURE_UNITS: tuple[str, ...] = ("tokens", "usd", "seconds", "count", "ratio")
"""Every Figure ``unit`` of spec section 7.3."""

FIGURE_BASES: tuple[str, ...] = (
    "measured",
    "estimated",
    "configured",
    "assumed",
    "unavailable",
    "not_applicable",
)
"""Every Figure ``basis`` of spec section 7.3."""

NULL_BASES: frozenset[str] = frozenset({"unavailable", "not_applicable"})
"""Bases that require a null Figure value (spec section 7.3)."""

ENFORCED_TOKENS: tuple[str, ...] = ("raw", "effective", "none")
"""Every ``Budget.enforced_tokens`` value (spec section 7.2)."""

LIMIT_NAMES: tuple[str, ...] = (
    "raw_tokens",
    "effective_tokens",
    "cost_usd",
    "wall_clock_seconds",
    "tool_calls",
    "turns",
)
"""Ceiling names that need no ``native.`` prefix (spec section 7.2)."""

INTAKE_STATES: tuple[str, ...] = ("open", "closed")
"""Every ``intake.state`` of a Status Snapshot (spec section 7.2)."""

COMMAND_KINDS: tuple[str, ...] = (
    "stop",
    "stop_after",
    "resume",
    "next_issue",
    "handoff",
    "recovery_retry",
    "recovery_release",
    "errors_reset",
)
"""Every ``CommandRecord.kind`` of spec section 7.5."""

ACKNOWLEDGEMENTS: tuple[str, ...] = (
    "accepted",
    "completed",
    "superseded",
    "not_fulfilled",
)
"""Every command acknowledgement of spec section 8.2."""

EVENT_LEVELS: tuple[str, ...] = ("debug", "info", "warning", "error")
"""Every Contract Event ``level`` of spec section 10."""

HARNESS_VERSION = "1.0"
"""The harness protocol version the suite speaks (spec section 15.1)."""

HARNESS_VERBS: tuple[str, ...] = (
    "setup",
    "teardown",
    "set-issue",
    "hold",
    "release",
    "finish-attempt",
    "hold-lock",
    "crash",
    "terminate",
    "stop-container",
    "start-container",
    "halt",
    "grow-archive",
    "defer-publication",
)
"""Every verb of the conformance harness protocol (spec section 15.1)."""

HOLD_POINTS: tuple[str, ...] = (
    "before-claim",
    "after-claim",
    "native-phase",
    "after-outcome",
)
"""Where the ``hold`` verb can pause the next or active Attempt."""

CRASH_POINTS: tuple[str, ...] = (
    "between-commit-and-reply",
    "before-claim",
    "after-claim",
    "during-finalization",
    "now",
)
"""Where the ``crash`` verb can kill the engine process; ``now`` acts at once,
any other point arms a one-shot crash for the next time it is reached."""

HARNESS_ERROR_CODES: tuple[str, ...] = ("usage", "unsupported", "failed")
"""Machine-readable causes of a harness ``ok:false`` reply."""

HARNESS_EXIT_CODES: dict[str, int] = {"usage": 2, "unsupported": 3, "failed": 1}
"""Process exit per harness error code; ``ok:true`` always exits 0."""

HARNESS_UNSUPPORTED_WHAT: tuple[str, ...] = (
    "verb",
    "hold-point",
    "crash-point",
    "native-outcome",
    "capability",
)
"""Values of ``error.details.what`` on a harness ``unsupported`` reply."""

_PACKAGE = "agent_operator_contract"


def _read_package_text(relative: str) -> str:
    return resources.files(_PACKAGE).joinpath(relative).read_text(encoding="utf-8")


def load_schema(name: str) -> Any:
    """Load one JSON Schema from the installed package data by short name."""
    if name not in SCHEMA_NAMES:
        raise KeyError(f"unknown schema: {name!r}")
    return json.loads(_read_package_text(f"schemas/{name}.schema.json"))


def error_table() -> dict[str, Any]:
    """Load the error.code table (spec section 4) from the package data."""
    table = json.loads(_read_package_text("error-table.json"))
    if not isinstance(table, dict):
        raise ValueError("error-table.json must be an object")
    return table


def client_table() -> dict[str, Any]:
    """Load the client. code table from the package data.

    Each entry maps a ``client.`` error code to the spec section 4 exit
    class ``agentfleet`` exits with for it and its ``retryable`` flag.
    """
    table = json.loads(_read_package_text("client-table.json"))
    if not isinstance(table, dict):
        raise ValueError("client-table.json must be an object")
    return table


def _data_stems(subdirectory: str, exclude: str = "") -> list[str]:
    """Sorted stems of the JSON documents under a package data directory."""
    directory = resources.files(_PACKAGE).joinpath(subdirectory)
    return sorted(
        path.name[: -len(".json")]
        for path in directory.iterdir()
        if path.name.endswith(".json") and path.name != exclude
    )


def fixture_names() -> list[str]:
    """Sorted stems of the consumer fixtures, excluding the index."""
    return _data_stems("fixtures/agentctl", "index.json")


def load_fixture(name: str) -> Any:
    """Load one consumer fixture from the installed package data by stem."""
    return json.loads(_read_package_text(f"fixtures/agentctl/{name}.json"))


def load_fixture_index() -> dict[str, Any]:
    """Load the fixture classification index from the installed package data."""
    index = json.loads(_read_package_text("fixtures/agentctl/index.json"))
    if not isinstance(index, dict):
        raise ValueError("index.json must be an object")
    return index


def negative_names() -> list[str]:
    """Sorted stems of the negative examples (bare documents, not fixtures)."""
    return _data_stems("fixtures/negative")


def load_negative(name: str) -> Any:
    """Load one negative example from the installed package data by stem."""
    return json.loads(_read_package_text(f"fixtures/negative/{name}.json"))


def harness_example_names() -> list[str]:
    """Sorted stems of the harness request and reply examples.

    Each stem ends in ``.request`` (a verb's JSON argument) or ``.reply`` (the
    JSON document the harness writes to stdout for it).
    """
    return _data_stems("fixtures/harness")


def load_harness_example(name: str) -> Any:
    """Load one harness example from the installed package data by stem."""
    return json.loads(_read_package_text(f"fixtures/harness/{name}.json"))


def _matches(pattern: str, value: object) -> bool:
    return isinstance(value, str) and re.search(pattern, value) is not None


def validate_envelope(doc: object) -> list[str]:
    """Check a reply envelope; the empty list means valid (spec section 4)."""
    if not isinstance(doc, dict):
        return ["envelope must be an object"]
    errors: list[str] = []
    if "contract" not in doc:
        errors.append("missing required property: 'contract'")
    elif not _matches(CONTRACT_PATTERN, doc["contract"]):
        errors.append("contract must be a '<major>.<minor>' string")
    if "ok" not in doc:
        errors.append("missing required property: 'ok'")
        return errors
    ok = doc["ok"]
    if type(ok) is not bool:
        return [*errors, "ok must be a boolean"]
    if ok:
        if "result" not in doc:
            errors.append("ok:true envelope requires 'result'")
        elif not isinstance(doc["result"], dict):
            errors.append("result must be an object")
        if "error" in doc:
            errors.append("ok:true envelope must not contain 'error'")
    else:
        if "result" in doc:
            errors.append("ok:false envelope must not contain 'result'")
        if "error" not in doc:
            errors.append("ok:false envelope requires 'error'")
        elif not isinstance(doc["error"], dict):
            errors.append("error must be an object")
        else:
            errors.extend(_validate_error(doc["error"]))
    return errors


def _validate_error(doc: dict[str, Any]) -> list[str]:
    table = error_table()
    errors: list[str] = []
    code = doc.get("code")
    if "code" not in doc:
        errors.append("error is missing required property: 'code'")
    elif not isinstance(code, str) or code not in table:
        errors.append(f"error.code {code!r} is not a known 1.0 code")
    if "message" not in doc:
        errors.append("error is missing required property: 'message'")
    elif not isinstance(doc["message"], str):
        errors.append("error.message must be a string")
    if "retryable" not in doc:
        errors.append("error is missing required property: 'retryable'")
    elif type(doc["retryable"]) is not bool:
        errors.append("error.retryable must be a boolean")
    if "request_id" in doc and not _matches(REQUEST_ID_PATTERN, doc["request_id"]):
        errors.append("error.request_id must match the request ID pattern")
    if "details" in doc and not isinstance(doc["details"], dict):
        errors.append("error.details must be an object")
    return errors


def validate_identity(doc: object) -> list[str]:
    """Check an Identity document; the empty list means valid (spec section 7.1)."""
    if not isinstance(doc, dict):
        return ["identity must be an object"]
    errors: list[str] = []
    for key in (
        "backend",
        "agent_version",
        "contract",
        "repository",
        "mode",
        "process_started_at",
        "capabilities",
    ):
        if key not in doc:
            errors.append(f"identity is missing required property: {key!r}")
    backend = doc.get("backend")
    if "backend" in doc and (not isinstance(backend, str) or not backend):
        errors.append("identity.backend must be a non-empty string")
    agent_version = doc.get("agent_version")
    if "agent_version" in doc and (not isinstance(agent_version, str) or not agent_version):
        errors.append("identity.agent_version must be a non-empty string")
    if "contract" in doc:
        errors.extend(_validate_contract_version(doc["contract"]))
    if "repository" in doc and not _matches(REPOSITORY_PATTERN, doc["repository"]):
        errors.append("identity.repository must be written owner/name")
    mode = doc.get("mode")
    if "mode" in doc and mode not in ("online", "offline"):
        errors.append("identity.mode must be 'online' or 'offline'")
    if "process_started_at" in doc:
        errors.extend(_validate_started_at(mode, doc["process_started_at"]))
    if "capabilities" in doc:
        capabilities = doc["capabilities"]
        if not isinstance(capabilities, list) or any(
            not isinstance(item, str) or not item for item in capabilities
        ):
            errors.append("identity.capabilities must be an array of non-empty strings")
    return errors


def _validate_contract_version(value: object) -> list[str]:
    if not isinstance(value, dict):
        return ["identity.contract must be an object"]
    errors: list[str] = []
    major = value.get("major")
    if "major" not in value:
        errors.append("identity.contract is missing required property: 'major'")
    elif type(major) is not int or major < 1:
        errors.append("identity.contract.major must be an integer >= 1")
    minor = value.get("minor")
    if "minor" not in value:
        errors.append("identity.contract is missing required property: 'minor'")
    elif type(minor) is not int or minor < 0:
        errors.append("identity.contract.minor must be an integer >= 0")
    return errors


def _validate_started_at(mode: object, value: object) -> list[str]:
    if mode == "offline":
        if value is not None:
            return ["identity.process_started_at must be null offline"]
        return []
    if mode == "online":
        if not isinstance(value, str):
            return ["identity.process_started_at must be a string online"]
        if not _matches(TIMESTAMP_PATTERN, value):
            return ["identity.process_started_at must be an RFC 3339 UTC timestamp"]
        return []
    if value is not None and not _matches(TIMESTAMP_PATTERN, value):
        return ["identity.process_started_at must be an RFC 3339 UTC timestamp or null"]
    return []


def validate_figure(doc: object, prefix: str = "figure") -> list[str]:
    """Check a Figure document; the empty list means valid (spec section 7.3).

    ``value`` is null exactly when ``basis`` is ``unavailable`` or
    ``not_applicable``; with any other basis it must be a number.
    Unknown fields are ignored so a minor can add them.
    """
    if not isinstance(doc, dict):
        return [f"{prefix} must be an object"]
    errors: list[str] = []
    for key in ("value", "unit", "basis"):
        if key not in doc:
            errors.append(f"{prefix} is missing required property: {key!r}")
    value = doc.get("value")
    unit = doc.get("unit")
    basis = doc.get("basis")
    if "unit" in doc and unit not in FIGURE_UNITS:
        errors.append(f"{prefix}.unit {unit!r} is not a known 1.0 unit")
    if "basis" in doc and basis not in FIGURE_BASES:
        errors.append(f"{prefix}.basis {basis!r} is not a known 1.0 basis")
    if "reason" in doc and not isinstance(doc["reason"], str):
        errors.append(f"{prefix}.reason must be a string")
    if "value" in doc and "basis" in doc and basis in FIGURE_BASES:
        if basis in NULL_BASES:
            if value is not None:
                errors.append(f"{prefix}.value must be null when basis is {basis!r}")
        elif not isinstance(value, (int, float)) or isinstance(value, bool):
            errors.append(f"{prefix}.value must be a number when basis is {basis!r}")
    return errors


def _validate_timestamp(value: object) -> bool:
    return _matches(TIMESTAMP_PATTERN, value)


def _validate_halt(doc: object, prefix: str) -> list[str]:
    if doc is None:
        return []
    if not isinstance(doc, dict):
        return [f"{prefix} must be an object or null"]
    errors: list[str] = []
    for key in ("reason", "detail", "at"):
        if key not in doc:
            errors.append(f"{prefix} is missing required property: {key!r}")
    if "reason" in doc and (not isinstance(doc["reason"], str) or not doc["reason"]):
        errors.append(f"{prefix}.reason must be a non-empty string")
    if "detail" in doc and not isinstance(doc["detail"], str):
        errors.append(f"{prefix}.detail must be a string")
    if "at" in doc and not _validate_timestamp(doc["at"]):
        errors.append(f"{prefix}.at must be an RFC 3339 UTC timestamp")
    return errors


def _validate_budget(doc: object, prefix: str = "budget") -> list[str]:
    if not isinstance(doc, dict):
        return [f"{prefix} must be an object"]
    errors: list[str] = []
    for key in ("raw_tokens", "effective_tokens", "enforced_tokens", "limits"):
        if key not in doc:
            errors.append(f"{prefix} is missing required property: {key!r}")
    if "raw_tokens" in doc:
        errors.extend(validate_figure(doc["raw_tokens"], f"{prefix}.raw_tokens"))
    if "effective_tokens" in doc:
        errors.extend(validate_figure(doc["effective_tokens"], f"{prefix}.effective_tokens"))
    if "enforced_tokens" in doc and doc["enforced_tokens"] not in ENFORCED_TOKENS:
        errors.append(f"{prefix}.enforced_tokens {doc['enforced_tokens']!r} is not known")
    if "limits" in doc:
        limits = doc["limits"]
        if not isinstance(limits, list):
            errors.append(f"{prefix}.limits must be an array")
        else:
            for index, entry in enumerate(limits):
                item_prefix = f"{prefix}.limits[{index}]"
                if not isinstance(entry, dict):
                    errors.append(f"{item_prefix} must be an object")
                    continue
                for key in ("name", "used", "limit"):
                    if key not in entry:
                        errors.append(f"{item_prefix} is missing required property: {key!r}")
                name = entry.get("name")
                if "name" in entry:
                    if not isinstance(name, str) or not name:
                        errors.append(f"{item_prefix}.name must be a non-empty string")
                    elif name not in LIMIT_NAMES and not (
                        name.startswith("native.") and len(name) > len("native.")
                    ):
                        errors.append(f"{item_prefix}.name {name!r} is not a known 1.0 limit")
                if "used" in entry:
                    errors.extend(validate_figure(entry["used"], f"{item_prefix}.used"))
                if "limit" in entry:
                    errors.extend(validate_figure(entry["limit"], f"{item_prefix}.limit"))
    return errors


def _validate_current_attempt(doc: object) -> list[str]:
    if doc is None:
        return []
    if not isinstance(doc, dict):
        return ["current_attempt must be an object or null"]
    errors: list[str] = []
    for key in (
        "attempt_id",
        "issue",
        "native_phase",
        "started_at",
        "last_activity_at",
        "observed_at",
        "budget",
        "figures",
    ):
        if key not in doc:
            errors.append(f"current_attempt is missing required property: {key!r}")
    if "attempt_id" in doc and (not isinstance(doc["attempt_id"], str) or not doc["attempt_id"]):
        errors.append("current_attempt.attempt_id must be a non-empty string")
    if "issue" in doc:
        issue = doc["issue"]
        if not isinstance(issue, dict):
            errors.append("current_attempt.issue must be an object")
        else:
            if "number" not in issue:
                errors.append("current_attempt.issue is missing required property: 'number'")
            elif type(issue["number"]) is not int or issue["number"] < 1:
                errors.append("current_attempt.issue.number must be an integer >= 1")
            if "title" not in issue:
                errors.append("current_attempt.issue is missing required property: 'title'")
            elif issue["title"] is not None and not isinstance(issue["title"], str):
                errors.append("current_attempt.issue.title must be a string or null")
    if "native_phase" in doc and (
        not isinstance(doc["native_phase"], str) or not doc["native_phase"]
    ):
        errors.append("current_attempt.native_phase must be a non-empty string")
    if "started_at" in doc and not _validate_timestamp(doc["started_at"]):
        errors.append("current_attempt.started_at must be an RFC 3339 UTC timestamp")
    if "last_activity_at" in doc:
        last_activity = doc["last_activity_at"]
        if last_activity is not None and not _validate_timestamp(last_activity):
            errors.append(
                "current_attempt.last_activity_at must be an RFC 3339 UTC timestamp or null"
            )
    if "observed_at" in doc and not _validate_timestamp(doc["observed_at"]):
        errors.append("current_attempt.observed_at must be an RFC 3339 UTC timestamp")
    if "budget" in doc:
        errors.extend(_validate_budget(doc["budget"]))
    if "figures" in doc:
        figures = doc["figures"]
        if not isinstance(figures, dict):
            errors.append("current_attempt.figures must be an object")
        else:
            for name, figure in figures.items():
                errors.extend(validate_figure(figure, f"current_attempt.figures.{name}"))
    return errors


def _validate_attempt_summary(doc: object, prefix: str) -> list[str]:
    if doc is None:
        return []
    if not isinstance(doc, dict):
        return [f"{prefix} must be an object or null"]
    errors: list[str] = []
    for key in (
        "attempt_id",
        "issue",
        "started_at",
        "finished_at",
        "lifecycle_state",
        "outcome",
        "delivery",
        "attention_required",
        "sealed",
        "sealed_at",
    ):
        if key not in doc:
            errors.append(f"{prefix} is missing required property: {key!r}")
    if "attempt_id" in doc and (not isinstance(doc["attempt_id"], str) or not doc["attempt_id"]):
        errors.append(f"{prefix}.attempt_id must be a non-empty string")
    if "issue" in doc:
        issue = doc["issue"]
        if not isinstance(issue, dict):
            errors.append(f"{prefix}.issue must be an object")
        else:
            if "number" not in issue:
                errors.append(f"{prefix}.issue is missing required property: 'number'")
            elif type(issue["number"]) is not int or issue["number"] < 1:
                errors.append(f"{prefix}.issue.number must be an integer >= 1")
            if "title" not in issue:
                errors.append(f"{prefix}.issue is missing required property: 'title'")
            elif issue["title"] is not None and not isinstance(issue["title"], str):
                errors.append(f"{prefix}.issue.title must be a string or null")
    if "started_at" in doc and not _validate_timestamp(doc["started_at"]):
        errors.append(f"{prefix}.started_at must be an RFC 3339 UTC timestamp")
    if "finished_at" in doc:
        finished = doc["finished_at"]
        if finished is not None and not _validate_timestamp(finished):
            errors.append(f"{prefix}.finished_at must be an RFC 3339 UTC timestamp or null")
    if "lifecycle_state" in doc and (
        not isinstance(doc["lifecycle_state"], str) or not doc["lifecycle_state"]
    ):
        errors.append(f"{prefix}.lifecycle_state must be a non-empty string")
    if "outcome" in doc:
        outcome = doc["outcome"]
        if outcome is not None:
            if not isinstance(outcome, dict):
                errors.append(f"{prefix}.outcome must be an object or null")
            else:
                for key in ("native", "reason", "label"):
                    if key not in outcome:
                        errors.append(f"{prefix}.outcome is missing required property: {key!r}")
                if "native" in outcome and (
                    not isinstance(outcome["native"], str) or not outcome["native"]
                ):
                    errors.append(f"{prefix}.outcome.native must be a non-empty string")
                if (
                    "reason" in outcome
                    and outcome["reason"] is not None
                    and not isinstance(outcome["reason"], str)
                ):
                    errors.append(f"{prefix}.outcome.reason must be a string or null")
                if "label" in outcome and (
                    not isinstance(outcome["label"], str) or not outcome["label"]
                ):
                    errors.append(f"{prefix}.outcome.label must be a non-empty string")
    if "delivery" in doc:
        delivery = doc["delivery"]
        if delivery is not None and not isinstance(delivery, dict):
            errors.append(f"{prefix}.delivery must be an object or null")
    if "attention_required" in doc and type(doc["attention_required"]) is not bool:
        errors.append(f"{prefix}.attention_required must be a boolean")
    if "sealed" in doc and type(doc["sealed"]) is not bool:
        errors.append(f"{prefix}.sealed must be a boolean")
    if "sealed_at" in doc:
        sealed_at = doc["sealed_at"]
        if sealed_at is not None and not _validate_timestamp(sealed_at):
            errors.append(f"{prefix}.sealed_at must be an RFC 3339 UTC timestamp or null")
    return errors


def validate_command_record(doc: object, prefix: str = "command") -> list[str]:
    """Check a CommandRecord; the empty list means valid (spec section 7.5)."""
    if not isinstance(doc, dict):
        return [f"{prefix} must be an object"]
    errors: list[str] = []
    for key in (
        "request_id",
        "sequence",
        "kind",
        "arguments",
        "repository",
        "acknowledgement",
        "detail",
        "created_at",
        "updated_at",
    ):
        if key not in doc:
            errors.append(f"{prefix} is missing required property: {key!r}")
    if "request_id" in doc and not _matches(REQUEST_ID_PATTERN, doc["request_id"]):
        errors.append(f"{prefix}.request_id must match the request ID pattern")
    if "sequence" in doc and (type(doc["sequence"]) is not int or doc["sequence"] < 0):
        errors.append(f"{prefix}.sequence must be an integer >= 0")
    if "kind" in doc and doc["kind"] not in COMMAND_KINDS:
        errors.append(f"{prefix}.kind {doc['kind']!r} is not a known 1.0 kind")
    if "arguments" in doc and not isinstance(doc["arguments"], dict):
        errors.append(f"{prefix}.arguments must be an object")
    if "repository" in doc and not _matches(REPOSITORY_PATTERN, doc["repository"]):
        errors.append(f"{prefix}.repository must be written owner/name")
    if "acknowledgement" in doc and doc["acknowledgement"] not in ACKNOWLEDGEMENTS:
        errors.append(f"{prefix}.acknowledgement {doc['acknowledgement']!r} is not known")
    if "detail" in doc and not isinstance(doc["detail"], str):
        errors.append(f"{prefix}.detail must be a string")
    if "created_at" in doc and not _validate_timestamp(doc["created_at"]):
        errors.append(f"{prefix}.created_at must be an RFC 3339 UTC timestamp")
    if "updated_at" in doc and not _validate_timestamp(doc["updated_at"]):
        errors.append(f"{prefix}.updated_at must be an RFC 3339 UTC timestamp")
    return errors


def validate_command_ref(doc: object, prefix: str = "command_ref") -> list[str]:
    """Check a CommandRef; the empty list means valid (spec section 7.5)."""
    if not isinstance(doc, dict):
        return [f"{prefix} must be an object"]
    errors: list[str] = []
    for key in ("request_id", "sequence"):
        if key not in doc:
            errors.append(f"{prefix} is missing required property: {key!r}")
    if "request_id" in doc and not _matches(REQUEST_ID_PATTERN, doc["request_id"]):
        errors.append(f"{prefix}.request_id must match the request ID pattern")
    if "sequence" in doc and (type(doc["sequence"]) is not int or doc["sequence"] < 0):
        errors.append(f"{prefix}.sequence must be an integer >= 0")
    return errors


def validate_command_result(doc: object) -> list[str]:
    """Check a ``command <request-id>`` result; the empty list means valid.

    The result is ``{"command": CommandRecord}`` (spec section 6.1). Unknown
    fields are ignored so a minor can add them.
    """
    if not isinstance(doc, dict):
        return ["command result must be an object"]
    errors: list[str] = []
    if "command" not in doc:
        errors.append("command result is missing required property: 'command'")
    else:
        errors.extend(validate_command_record(doc["command"], "command result.command"))
    return errors


def validate_mutation_result(doc: object) -> list[str]:
    """Check a mutation result; the empty list means valid (spec section 6.2).

    The result is ``{"command": CommandRecord, "replayed": bool}``: ``replayed``
    is true when the request ID already existed with the identical payload and
    nothing new happened. Unknown fields are ignored so a minor can add them.
    """
    if not isinstance(doc, dict):
        return ["mutation result must be an object"]
    errors: list[str] = []
    if "command" not in doc:
        errors.append("mutation result is missing required property: 'command'")
    else:
        errors.extend(validate_command_record(doc["command"], "mutation result.command"))
    if "replayed" not in doc:
        errors.append("mutation result is missing required property: 'replayed'")
    elif type(doc["replayed"]) is not bool:
        errors.append("mutation result.replayed must be a boolean")
    return errors


def _validate_contract_event(doc: object, prefix: str) -> list[str]:
    if not isinstance(doc, dict):
        return [f"{prefix} must be an object"]
    errors: list[str] = []
    for key in ("at", "level", "event", "text"):
        if key not in doc:
            errors.append(f"{prefix} is missing required property: {key!r}")
    if "at" in doc and not _validate_timestamp(doc["at"]):
        errors.append(f"{prefix}.at must be an RFC 3339 UTC timestamp")
    if "level" in doc and doc["level"] not in EVENT_LEVELS:
        errors.append(f"{prefix}.level {doc['level']!r} is not a known 1.0 level")
    if "event" in doc and (not isinstance(doc["event"], str) or not doc["event"]):
        errors.append(f"{prefix}.event must be a non-empty string")
    if "text" in doc:
        text = doc["text"]
        if not isinstance(text, str):
            errors.append(f"{prefix}.text must be a string")
        elif len(text) > 300:
            errors.append(f"{prefix}.text must be at most 300 characters")
    return errors


def validate_status(doc: object) -> list[str]:
    """Check a Status Snapshot; the empty list means valid (spec section 7.2).

    Unknown fields are ignored so a minor can add them. Enum values,
    the Figure value/basis rule and ``process_started_at`` nullability are
    enforced exactly as in ``status.schema.json``.
    """
    if not isinstance(doc, dict):
        return ["status must be an object"]
    errors: list[str] = []
    for key in (
        "generated_at",
        "identity",
        "instance",
        "intake",
        "current_attempt",
        "last_finished",
        "recent_commands",
        "recent_events",
        "truncated",
    ):
        if key not in doc:
            errors.append(f"status is missing required property: {key!r}")
    if "generated_at" in doc and not _validate_timestamp(doc["generated_at"]):
        errors.append("status.generated_at must be an RFC 3339 UTC timestamp")
    if "identity" in doc:
        identity_errors = validate_identity(doc["identity"])
        errors.extend(identity_errors)
    if "instance" in doc:
        instance = doc["instance"]
        if not isinstance(instance, dict):
            errors.append("status.instance must be an object")
        else:
            for key in ("state", "startup_check", "last_halt", "observed_at"):
                if key not in instance:
                    errors.append(f"status.instance is missing required property: {key!r}")
            state = instance.get("state")
            if "state" in instance and state not in STATUS_STATES:
                errors.append(f"status.instance.state {state!r} is not known")
            if "startup_check" in instance:
                check = instance["startup_check"]
                if state == "starting":
                    if not isinstance(check, str) or not check:
                        errors.append(
                            "status.instance.startup_check must be a non-empty "
                            "string while starting"
                        )
                elif check is not None and not isinstance(check, str):
                    errors.append("status.instance.startup_check must be a string or null")
            if "last_halt" in instance:
                errors.extend(_validate_halt(instance["last_halt"], "status.instance.last_halt"))
            if "observed_at" in instance and not _validate_timestamp(instance["observed_at"]):
                errors.append("status.instance.observed_at must be an RFC 3339 UTC timestamp")
    if "intake" in doc:
        intake = doc["intake"]
        if not isinstance(intake, dict):
            errors.append("status.intake must be an object")
        else:
            for key in ("state", "pending_stop", "stop_plan", "priority", "observed_at"):
                if key not in intake:
                    errors.append(f"status.intake is missing required property: {key!r}")
            if "state" in intake and intake["state"] not in INTAKE_STATES:
                errors.append(f"status.intake.state {intake['state']!r} is not known")
            if "pending_stop" in intake:
                pending = intake["pending_stop"]
                if pending is not None:
                    if not isinstance(pending, dict):
                        errors.append("status.intake.pending_stop must be an object or null")
                    else:
                        for key in ("request_id", "sequence"):
                            if key not in pending:
                                errors.append(
                                    "status.intake.pending_stop is missing required "
                                    f"property: {key!r}"
                                )
                        if "request_id" in pending and not _matches(
                            REQUEST_ID_PATTERN, pending["request_id"]
                        ):
                            errors.append(
                                "status.intake.pending_stop.request_id must match "
                                "the request ID pattern"
                            )
                        if "sequence" in pending and (
                            type(pending["sequence"]) is not int or pending["sequence"] < 0
                        ):
                            errors.append(
                                "status.intake.pending_stop.sequence must be an integer >= 0"
                            )
            if "stop_plan" in intake:
                plan = intake["stop_plan"]
                if plan is not None:
                    if not isinstance(plan, dict):
                        errors.append("status.intake.stop_plan must be an object or null")
                    else:
                        for key in ("request_id", "remaining", "counted"):
                            if key not in plan:
                                errors.append(
                                    f"status.intake.stop_plan is missing required property: {key!r}"
                                )
                        if "request_id" in plan and not _matches(
                            REQUEST_ID_PATTERN, plan["request_id"]
                        ):
                            errors.append(
                                "status.intake.stop_plan.request_id must match "
                                "the request ID pattern"
                            )
                        for key in ("remaining", "counted"):
                            if key in plan and (type(plan[key]) is not int or plan[key] < 0):
                                errors.append(
                                    f"status.intake.stop_plan.{key} must be an integer >= 0"
                                )
            if "priority" in intake:
                priority = intake["priority"]
                if priority is not None:
                    if not isinstance(priority, dict):
                        errors.append("status.intake.priority must be an object or null")
                    else:
                        for key in ("request_id", "issue"):
                            if key not in priority:
                                errors.append(
                                    f"status.intake.priority is missing required property: {key!r}"
                                )
                        if "request_id" in priority and not _matches(
                            REQUEST_ID_PATTERN, priority["request_id"]
                        ):
                            errors.append(
                                "status.intake.priority.request_id must match "
                                "the request ID pattern"
                            )
                        if "issue" in priority and (
                            type(priority["issue"]) is not int or priority["issue"] < 1
                        ):
                            errors.append("status.intake.priority.issue must be an integer >= 1")
            if "observed_at" in intake and not _validate_timestamp(intake["observed_at"]):
                errors.append("status.intake.observed_at must be an RFC 3339 UTC timestamp")
    if "current_attempt" in doc:
        errors.extend(_validate_current_attempt(doc["current_attempt"]))
    if "last_finished" in doc:
        errors.extend(_validate_attempt_summary(doc["last_finished"], "status.last_finished"))
    if "recent_commands" in doc:
        commands = doc["recent_commands"]
        if not isinstance(commands, list):
            errors.append("status.recent_commands must be an array")
        elif len(commands) > 20:
            errors.append("status.recent_commands must contain at most 20 entries")
        else:
            for index, entry in enumerate(commands):
                errors.extend(validate_command_record(entry, f"status.recent_commands[{index}]"))
    if "recent_events" in doc:
        events = doc["recent_events"]
        if not isinstance(events, list):
            errors.append("status.recent_events must be an array")
        elif len(events) > 50:
            errors.append("status.recent_events must contain at most 50 entries")
        else:
            for index, entry in enumerate(events):
                errors.extend(_validate_contract_event(entry, f"status.recent_events[{index}]"))
    if "truncated" in doc and type(doc["truncated"]) is not bool:
        errors.append("status.truncated must be a boolean")
    return errors


def validate_fixture(doc: object) -> list[str]:
    """Check a consumer fixture file; the empty list means valid (spec section 15)."""
    if not isinstance(doc, dict):
        return ["fixture must be an object"]
    errors: list[str] = []
    required = ("argv", "exit_code", "stdout", "stderr")
    for key in required:
        if key not in doc:
            errors.append(f"fixture is missing required property: {key!r}")
    for key in doc:
        if key not in required:
            errors.append(f"fixture has an unexpected property: {key!r}")
    if "argv" in doc:
        argv = doc["argv"]
        if not isinstance(argv, list) or not argv or not all(isinstance(arg, str) for arg in argv):
            errors.append("fixture.argv must be a non-empty array of strings")
    if "exit_code" in doc:
        exit_code = doc["exit_code"]
        if type(exit_code) is not int or not 0 <= exit_code <= 255:
            errors.append("fixture.exit_code must be an integer between 0 and 255")
    for key in ("stdout", "stderr"):
        if key in doc and not isinstance(doc[key], str):
            errors.append(f"fixture.{key} must be a string")
    return errors


def validate_instance_name(value: object) -> list[str]:
    """Check an instance name; the empty list means valid (spec section 2.1)."""
    if _matches(INSTANCE_NAME_PATTERN, value):
        return []
    return ["instance name must match ^[a-z0-9][a-z0-9-]{0,62}$"]


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value != ""


def _is_string_list(value: object) -> bool:
    return isinstance(value, list) and all(_is_non_empty_string(item) for item in value)


def _unexpected_harness_params(verb: str, params: dict[str, Any], allowed: set[str]) -> list[str]:
    return [
        f"harness {verb!r} has an unexpected parameter: {key!r}"
        for key in params
        if key not in allowed
    ]


def validate_harness_request(doc: object) -> list[str]:
    """Check a harness request document; the empty list means valid (spec section 15.1)."""
    if not isinstance(doc, dict):
        return ["harness request must be an object"]
    errors: list[str] = []
    for key in doc:
        if key not in ("verb", "params"):
            errors.append(f"harness request has an unexpected property: {key!r}")
    if "verb" not in doc:
        errors.append("harness request is missing required property: 'verb'")
        return errors
    verb = doc["verb"]
    if not isinstance(verb, str) or verb not in HARNESS_VERBS:
        return [*errors, f"harness request verb {verb!r} is not a known 1.0 verb"]
    if "params" not in doc:
        return [*errors, "harness request is missing required property: 'params'"]
    params = doc["params"]
    if not isinstance(params, dict):
        return [*errors, "harness request params must be an object"]
    errors.extend(_validate_harness_params(verb, params))
    return errors


def _validate_harness_params(verb: str, params: dict[str, Any]) -> list[str]:
    if verb in (
        "teardown",
        "release",
        "terminate",
        "stop-container",
        "start-container",
        "defer-publication",
    ):
        if params:
            return [f"harness {verb!r} takes no parameters"]
        return []
    if verb == "setup":
        return _validate_setup_params(params)
    if verb == "set-issue":
        return _validate_set_issue_params(params)
    if verb == "hold":
        return _validate_hold_params(params)
    if verb == "finish-attempt":
        return _validate_finish_attempt_params(params)
    if verb == "hold-lock":
        return _validate_hold_lock_params(params)
    if verb == "crash":
        return _validate_crash_params(params)
    if verb == "halt":
        return _validate_halt_params(params)
    if verb == "grow-archive":
        return _validate_grow_archive_params(params)
    raise AssertionError(f"unreachable harness verb: {verb!r}")


def _validate_setup_params(params: dict[str, Any]) -> list[str]:
    errors = _unexpected_harness_params("setup", params, {"repository", "capabilities", "config"})
    if "repository" not in params:
        errors.append("harness 'setup' is missing required parameter: 'repository'")
    elif not _matches(REPOSITORY_PATTERN, params["repository"]):
        errors.append("harness 'setup' repository must be written owner/name")
    if "capabilities" in params and not _is_string_list(params["capabilities"]):
        errors.append("harness 'setup' capabilities must be an array of non-empty strings")
    if "config" in params:
        config = params["config"]
        if not isinstance(config, dict):
            errors.append("harness 'setup' config must be an object")
        else:
            for key in config:
                if not key.startswith("native."):
                    errors.append(f"harness 'setup' config allows only native.* keys, got {key!r}")
    return errors


def _validate_set_issue_params(params: dict[str, Any]) -> list[str]:
    errors = _unexpected_harness_params("set-issue", params, {"issue", "eligible", "title"})
    if "issue" not in params:
        errors.append("harness 'set-issue' is missing required parameter: 'issue'")
    elif type(params["issue"]) is not int or params["issue"] < 1:
        errors.append("harness 'set-issue' issue must be an integer >= 1")
    if "eligible" not in params:
        errors.append("harness 'set-issue' is missing required parameter: 'eligible'")
    elif type(params["eligible"]) is not bool:
        errors.append("harness 'set-issue' eligible must be a boolean")
    if "title" in params and not _is_non_empty_string(params["title"]):
        errors.append("harness 'set-issue' title must be a non-empty string")
    return errors


def _validate_hold_params(params: dict[str, Any]) -> list[str]:
    errors = _unexpected_harness_params("hold", params, {"point", "phase", "scope"})
    point = params.get("point")
    if "point" not in params:
        errors.append("harness 'hold' is missing required parameter: 'point'")
    elif point not in HOLD_POINTS:
        errors.append(f"harness 'hold' point {point!r} is not a known hold point")
    if point == "native-phase":
        if "phase" not in params:
            errors.append("harness 'hold' requires 'phase' at point 'native-phase'")
        elif not _is_non_empty_string(params["phase"]):
            errors.append("harness 'hold' phase must be a non-empty string")
    elif "phase" in params:
        errors.append("harness 'hold' takes 'phase' only at point 'native-phase'")
    if "scope" in params and params["scope"] not in ("next", "active"):
        errors.append("harness 'hold' scope must be 'next' or 'active'")
    return errors


def _validate_finish_attempt_params(params: dict[str, Any]) -> list[str]:
    errors = _unexpected_harness_params("finish-attempt", params, {"native_outcome", "reason"})
    if "native_outcome" not in params:
        errors.append("harness 'finish-attempt' is missing required parameter: 'native_outcome'")
    elif not _is_non_empty_string(params["native_outcome"]):
        errors.append("harness 'finish-attempt' native_outcome must be a non-empty string")
    if "reason" in params:
        reason = params["reason"]
        if reason is not None and not isinstance(reason, str):
            errors.append("harness 'finish-attempt' reason must be a string or null")
    return errors


def _validate_hold_lock_params(params: dict[str, Any]) -> list[str]:
    errors = _unexpected_harness_params("hold-lock", params, {"duration_ms"})
    if "duration_ms" not in params:
        errors.append("harness 'hold-lock' is missing required parameter: 'duration_ms'")
    elif type(params["duration_ms"]) is not int or not 1 <= params["duration_ms"] <= 60000:
        errors.append("harness 'hold-lock' duration_ms must be an integer between 1 and 60000")
    return errors


def _validate_crash_params(params: dict[str, Any]) -> list[str]:
    errors = _unexpected_harness_params("crash", params, {"point"})
    if "point" not in params:
        errors.append("harness 'crash' is missing required parameter: 'point'")
    elif params["point"] not in CRASH_POINTS:
        errors.append(f"harness 'crash' point {params['point']!r} is not a known crash point")
    return errors


def _validate_halt_params(params: dict[str, Any]) -> list[str]:
    errors = _unexpected_harness_params("halt", params, {"reason", "detail"})
    if "reason" not in params:
        errors.append("harness 'halt' is missing required parameter: 'reason'")
    elif not _is_non_empty_string(params["reason"]):
        errors.append("harness 'halt' reason must be a non-empty string")
    if "detail" in params and not isinstance(params["detail"], str):
        errors.append("harness 'halt' detail must be a string")
    return errors


def _validate_grow_archive_params(params: dict[str, Any]) -> list[str]:
    errors = _unexpected_harness_params("grow-archive", params, {"attempt_id", "append_bytes"})
    if "append_bytes" not in params:
        errors.append("harness 'grow-archive' is missing required parameter: 'append_bytes'")
    elif type(params["append_bytes"]) is not int or params["append_bytes"] < 1:
        errors.append("harness 'grow-archive' append_bytes must be an integer >= 1")
    if "attempt_id" in params:
        attempt_id = params["attempt_id"]
        if attempt_id is not None and not isinstance(attempt_id, str):
            errors.append("harness 'grow-archive' attempt_id must be a string or null")
    return errors


def validate_harness_reply(doc: object) -> list[str]:
    """Check a harness reply document; the empty list means valid (spec section 15.1)."""
    if not isinstance(doc, dict):
        return ["harness reply must be an object"]
    errors: list[str] = []
    if "harness" not in doc:
        errors.append("harness reply is missing required property: 'harness'")
    elif not _matches(CONTRACT_PATTERN, doc["harness"]):
        errors.append("harness must be a '<major>.<minor>' string")
    verb = doc.get("verb")
    if "verb" in doc and (not isinstance(verb, str) or verb not in HARNESS_VERBS):
        errors.append(f"harness reply verb {verb!r} is not a known 1.0 verb")
    if "ok" not in doc:
        errors.append("harness reply is missing required property: 'ok'")
        return errors
    ok = doc["ok"]
    if type(ok) is not bool:
        return [*errors, "harness reply ok must be a boolean"]
    if ok:
        if "result" not in doc:
            errors.append("harness reply ok:true requires 'result'")
        elif not isinstance(doc["result"], dict):
            errors.append("harness reply result must be an object")
        elif isinstance(verb, str) and verb in HARNESS_VERBS:
            errors.extend(_validate_harness_result(verb, doc["result"]))
        if "error" in doc:
            errors.append("harness reply ok:true must not contain 'error'")
    else:
        if "result" in doc:
            errors.append("harness reply ok:false must not contain 'result'")
        if "error" not in doc:
            errors.append("harness reply ok:false requires 'error'")
        elif not isinstance(doc["error"], dict):
            errors.append("harness reply error must be an object")
        else:
            errors.extend(_validate_harness_error(doc["error"]))
    return errors


def _validate_harness_error(doc: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    code = doc.get("code")
    if "code" not in doc:
        errors.append("harness error is missing required property: 'code'")
    elif not isinstance(code, str) or code not in HARNESS_ERROR_CODES:
        errors.append(f"harness error.code {code!r} is not a known 1.0 code")
    if "message" not in doc:
        errors.append("harness error is missing required property: 'message'")
    elif not isinstance(doc["message"], str):
        errors.append("harness error.message must be a string")
    if "details" in doc and not isinstance(doc["details"], dict):
        errors.append("harness error.details must be an object")
    if code == "unsupported":
        details = doc.get("details")
        if not isinstance(details, dict):
            errors.append("harness 'unsupported' requires error.details")
        else:
            if "what" not in details:
                errors.append("harness 'unsupported' requires details.what")
            elif details["what"] not in HARNESS_UNSUPPORTED_WHAT:
                errors.append(f"harness unsupported what {details['what']!r} is not known")
            if "name" not in details:
                errors.append("harness 'unsupported' requires details.name")
            elif not isinstance(details["name"], str):
                errors.append("harness unsupported details.name must be a string")
    return errors


def _validate_harness_result(verb: str, result: dict[str, Any]) -> list[str]:
    if verb == "setup":
        return _validate_setup_result(result)
    if verb == "teardown":
        if "removed" not in result:
            return ["harness 'teardown' result is missing required property: 'removed'"]
        if type(result["removed"]) is not bool:
            return ["harness 'teardown' result removed must be a boolean"]
        return []
    if verb == "set-issue":
        return _validate_set_issue_result(result)
    if verb == "hold":
        return _validate_hold_result(result)
    if verb == "release":
        if "released" not in result:
            return ["harness 'release' result is missing required property: 'released'"]
        if type(result["released"]) is not bool:
            return ["harness 'release' result released must be a boolean"]
        return []
    if verb == "finish-attempt":
        errors = []
        if "attempt_id" not in result:
            errors.append(
                "harness 'finish-attempt' result is missing required property: 'attempt_id'"
            )
        elif not _is_non_empty_string(result["attempt_id"]):
            errors.append("harness 'finish-attempt' result attempt_id must be a non-empty string")
        if "native_outcome" not in result:
            errors.append(
                "harness 'finish-attempt' result is missing required property: 'native_outcome'"
            )
        elif not _is_non_empty_string(result["native_outcome"]):
            errors.append(
                "harness 'finish-attempt' result native_outcome must be a non-empty string"
            )
        return errors
    if verb == "hold-lock":
        if "held_ms" not in result:
            return ["harness 'hold-lock' result is missing required property: 'held_ms'"]
        if type(result["held_ms"]) is not int or result["held_ms"] < 1:
            return ["harness 'hold-lock' result held_ms must be an integer >= 1"]
        return []
    if verb == "crash":
        if "point" not in result:
            return ["harness 'crash' result is missing required property: 'point'"]
        if result["point"] not in CRASH_POINTS:
            return [f"harness 'crash' result point {result['point']!r} is not a known crash point"]
        return []
    if verb == "terminate":
        if result.get("signal") != "SIGTERM":
            return ["harness 'terminate' result signal must be 'SIGTERM'"]
        return []
    if verb == "stop-container":
        if "running" not in result:
            return ["harness 'stop-container' result is missing required property: 'running'"]
        if type(result["running"]) is not bool or result["running"] is not False:
            return ["harness 'stop-container' result running must be false"]
        return []
    if verb == "start-container":
        if "running" not in result:
            return ["harness 'start-container' result is missing required property: 'running'"]
        if type(result["running"]) is not bool or result["running"] is not True:
            return ["harness 'start-container' result running must be true"]
        return []
    if verb == "halt":
        if "reason" not in result:
            return ["harness 'halt' result is missing required property: 'reason'"]
        if not _is_non_empty_string(result["reason"]):
            return ["harness 'halt' result reason must be a non-empty string"]
        return []
    if verb == "grow-archive":
        errors = []
        if "path" not in result:
            errors.append("harness 'grow-archive' result is missing required property: 'path'")
        elif not _is_non_empty_string(result["path"]):
            errors.append("harness 'grow-archive' result path must be a non-empty string")
        if "appended_bytes" not in result:
            errors.append(
                "harness 'grow-archive' result is missing required property: 'appended_bytes'"
            )
        elif type(result["appended_bytes"]) is not int or result["appended_bytes"] < 1:
            errors.append("harness 'grow-archive' result appended_bytes must be an integer >= 1")
        return errors
    if verb == "defer-publication":
        if result.get("armed") is not True:
            return ["harness 'defer-publication' result armed must be true"]
        return []
    raise AssertionError(f"unreachable harness verb: {verb!r}")


def _validate_setup_result(result: dict[str, Any]) -> list[str]:
    errors = []
    for key in (
        "container",
        "image",
        "volumes",
        "exec_user",
        "repository",
        "capabilities",
    ):
        if key not in result:
            errors.append(f"harness 'setup' result is missing required property: {key!r}")
    if "container" in result and not _is_non_empty_string(result["container"]):
        errors.append("harness 'setup' result container must be a non-empty string")
    if "image" in result and not _is_non_empty_string(result["image"]):
        errors.append("harness 'setup' result image must be a non-empty string")
    if "volumes" in result:
        errors.extend(_validate_setup_volumes(result["volumes"]))
    if "exec_user" in result:
        if not _is_non_empty_string(result["exec_user"]):
            errors.append("harness 'setup' result exec_user must be a non-empty string")
        elif result["exec_user"] == "root":
            errors.append("harness 'setup' result exec_user must not be 'root'")
    if "repository" in result and not _matches(REPOSITORY_PATTERN, result["repository"]):
        errors.append("harness 'setup' result repository must be written owner/name")
    if "capabilities" in result and not _is_string_list(result["capabilities"]):
        errors.append("harness 'setup' result capabilities must be an array of non-empty strings")
    return errors


def _validate_setup_volumes(value: object) -> list[str]:
    if not isinstance(value, list) or not value:
        return ["harness 'setup' result volumes must be a non-empty array"]
    errors: list[str] = []
    for entry in value:
        if not isinstance(entry, dict):
            errors.append("harness 'setup' result volumes entries must be objects")
            continue
        if "name" not in entry:
            errors.append("harness 'setup' result volumes entry is missing 'name'")
        elif not _is_non_empty_string(entry["name"]):
            errors.append("harness 'setup' result volumes entry name must be a non-empty string")
        if "path" not in entry:
            errors.append("harness 'setup' result volumes entry is missing 'path'")
        elif not isinstance(entry["path"], str) or not entry["path"].startswith("/"):
            errors.append("harness 'setup' result volumes entry path must be absolute")
    return errors


def _validate_set_issue_result(result: dict[str, Any]) -> list[str]:
    errors = []
    if "issue" not in result:
        errors.append("harness 'set-issue' result is missing required property: 'issue'")
    elif type(result["issue"]) is not int or result["issue"] < 1:
        errors.append("harness 'set-issue' result issue must be an integer >= 1")
    if "eligible" not in result:
        errors.append("harness 'set-issue' result is missing required property: 'eligible'")
    elif type(result["eligible"]) is not bool:
        errors.append("harness 'set-issue' result eligible must be a boolean")
    return errors


def _validate_hold_result(result: dict[str, Any]) -> list[str]:
    errors = []
    if "point" not in result:
        errors.append("harness 'hold' result is missing required property: 'point'")
    elif result["point"] not in HOLD_POINTS:
        errors.append(f"harness 'hold' result point {result['point']!r} is not a known hold point")
    if "attempt_id" not in result:
        errors.append("harness 'hold' result is missing required property: 'attempt_id'")
    elif result["attempt_id"] is not None and not _is_non_empty_string(result["attempt_id"]):
        errors.append("harness 'hold' result attempt_id must be a string or null")
    return errors


def validate_client_envelope(doc: object) -> list[str]:
    """Check an agentfleet client error document; the empty list means valid.

    A client failure is an envelope-shaped document with ``ok: false`` and an
    ``error.code`` in the ``client.`` namespace from ``client-table.json``.
    """
    if not isinstance(doc, dict):
        return ["client envelope must be an object"]
    errors: list[str] = []
    if "contract" not in doc:
        errors.append("missing required property: 'contract'")
    elif not _matches(CONTRACT_PATTERN, doc["contract"]):
        errors.append("contract must be a '<major>.<minor>' string")
    if "ok" not in doc:
        errors.append("missing required property: 'ok'")
    elif doc["ok"] is not False:
        errors.append("client envelope ok must be false")
    if "result" in doc:
        errors.append("client envelope must not contain 'result'")
    if "error" not in doc:
        errors.append("client envelope requires 'error'")
        return errors
    error = doc["error"]
    if not isinstance(error, dict):
        return [*errors, "error must be an object"]
    table = client_table()
    code = error.get("code")
    if "code" not in error:
        errors.append("error is missing required property: 'code'")
    elif not isinstance(code, str) or code not in table:
        errors.append(f"error.code {code!r} is not a known client code")
    if "message" not in error:
        errors.append("error is missing required property: 'message'")
    elif not isinstance(error["message"], str):
        errors.append("error.message must be a string")
    if "retryable" not in error:
        errors.append("error is missing required property: 'retryable'")
    elif type(error["retryable"]) is not bool:
        errors.append("error.retryable must be a boolean")
    if "details" in error and not isinstance(error["details"], dict):
        errors.append("error.details must be an object")
    return errors


def validate_ls(doc: object) -> list[str]:
    """Check an ``agentfleet ls --json`` document; the empty list means valid."""
    if not isinstance(doc, dict):
        return ["ls document must be an object"]
    errors: list[str] = []
    if doc.get("version") != 1:
        errors.append("ls document version must be 1")
    if "instances" not in doc:
        errors.append("ls document is missing required property: 'instances'")
        return errors
    instances = doc["instances"]
    if not isinstance(instances, list):
        return [*errors, "ls instances must be an array"]
    for index, entry in enumerate(instances):
        prefix = f"instances[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{prefix} must be an object")
            continue
        for key in ("name", "backend", "contract", "state", "duplicate"):
            if key not in entry:
                errors.append(f"{prefix} is missing required property: {key!r}")
        for key in entry:
            if key not in ("name", "backend", "contract", "state", "duplicate"):
                errors.append(f"{prefix} has an unexpected property: {key!r}")
        if "name" in entry and not _matches(INSTANCE_NAME_PATTERN, entry["name"]):
            errors.append(f"{prefix}.name must match the instance name pattern")
        if "backend" in entry and (not isinstance(entry["backend"], str) or not entry["backend"]):
            errors.append(f"{prefix}.backend must be a non-empty string")
        if "contract" in entry and not _matches(CONTRACT_MAJOR_PATTERN, entry["contract"]):
            errors.append(f"{prefix}.contract must be a contract major string")
        if "state" in entry and (not isinstance(entry["state"], str) or not entry["state"]):
            errors.append(f"{prefix}.state must be a non-empty string")
        if "duplicate" in entry and type(entry["duplicate"]) is not bool:
            errors.append(f"{prefix}.duplicate must be a boolean")
    return errors


def contract_major_of(contract: object) -> str:
    """Return the major of a dotted contract string, or ``""`` when absent."""
    return str(contract).split(".", 1)[0] if isinstance(contract, str) else ""


def classify_fixture(fixture: object) -> Classification:
    """Read one parsed fixture the way a consumer must (spec sections 4, 8.4, 15).

    Anything that is not a coherent producer answer — an unparsable stdout, an
    envelope that fails validation, or an exit code that contradicts the
    envelope — is ``malformed_stdout``: the consumer cannot trust it.
    """
    if not isinstance(fixture, dict):
        return "malformed_stdout"
    argv = fixture.get("argv")
    exit_code = fixture.get("exit_code")
    stdout = fixture.get("stdout")
    if (
        not isinstance(argv, list)
        or not argv
        or not all(isinstance(arg, str) for arg in argv)
        or type(exit_code) is not int
        or not isinstance(stdout, str)
    ):
        return "malformed_stdout"
    try:
        doc = json.loads(stdout)
    except ValueError:
        return "malformed_stdout"
    if not isinstance(doc, dict) or validate_envelope(doc) != []:
        return "malformed_stdout"
    contract = doc.get("contract")
    major = contract_major_of(contract)
    if major != "1":
        return "malformed_stdout"
    table = error_table()
    if doc["ok"] is True:
        if exit_code != 0:
            return "malformed_stdout"
        result = doc["result"]
        if not isinstance(result, dict):
            return "malformed_stdout"
        return "replay" if result.get("replayed") is True else "success"
    code = doc["error"]["code"]
    entry = table.get(code)
    if not isinstance(entry, dict) or exit_code != entry.get("exit_code"):
        return "malformed_stdout"
    if code == "unsupported":
        return "unsupported"
    if code == "contract_mismatch":
        return "contract_mismatch"
    if code == "not_found":
        return "not_found"
    if code == "agent_unreachable":
        return "ambiguous_outcome"
    if code == "internal":
        details = doc["error"].get("details")
        if isinstance(details, dict) and details.get("stored") is False:
            return "rejection"
        return "ambiguous_outcome"
    return "rejection"
