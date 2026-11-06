"""Validation and classification of the contract's documents.

Covers the reply envelope and ``Identity`` (spec sections 4 and 7.1) and the
consumer fixture format (spec section 15). Standard library only: this module
mirrors the JSON Schemas in ``schemas/`` rule for rule, so engines and
consumers without a JSON Schema engine reach the same verdicts. It is not a
general JSON Schema engine; agreement with the schemas is tested on the whole
fixture set (see ``tests/test_schemas.py``).

The ``error.code`` table lives once, as data, in ``error-table.json``: each
code with its fixed exit code and ``retryable`` flag (spec section 4). The
envelope schema's ``error.code`` enum mirrors these keys (tests enforce
equality); :func:`validate_envelope` and :func:`classify_fixture` read this
file at runtime. Pattern matching uses ``re.search``, exactly like the JSON
Schema ``pattern`` keyword, and every pattern constant below is pinned to the
matching schema pattern by tests.
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

SCHEMA_NAMES: tuple[str, ...] = ("envelope", "identity", "fixture", "instance-name")

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
