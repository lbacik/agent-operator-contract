"""Shared part of the library: argv grammar, envelopes and exit codes.

One definition of the contract argv grammar (spec section 6) that a producer
uses to parse argv and get the first failure in the section 4.1 check order
(``contract_mismatch`` -> ``usage`` -> ``unsupported`` -> ``invalid_argument``);
envelope rendering (one JSON document plus a newline, compact, sorted keys);
the ``error.code`` to exit code mapping, so a producer cannot pair a code with
the wrong exit; and a case-insensitive ``--expect-repo`` comparison helper.

Standard library only. Checking ``--expect-repo`` against the Target
Repository and everything after it in section 4.1 (idempotency, state,
commit) stays with the engine: only the engine knows its repository and its
commands.
"""

from __future__ import annotations

import json
import re
from collections.abc import Container, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from agent_operator_contract import CONTRACT_MAJOR, documents

CONTRACT_MINOR = 0
"""The contract minor this library speaks (spec section 5)."""

CONTRACT_VERSION = f"{CONTRACT_MAJOR}.{CONTRACT_MINOR}"
"""The ``contract`` field producers render into every envelope (spec section 4)."""

SUPPORTED_MAJORS = [1]
"""Contract majors this producer serves, echoed in ``contract_mismatch`` details."""

OPERATOR_CAPABILITIES: tuple[str, ...] = (
    "stop-after",
    "next-issue",
    "operator-handoff",
    "recovery-retry",
    "recovery-release",
    "errors-reset",
    "pool",
)
"""Every optional Operator Capability of spec section 5.1."""

MUTATIONS: frozenset[str] = frozenset(
    {
        "stop",
        "resume",
        "next-issue",
        "handoff",
        "recovery-retry",
        "recovery-release",
        "errors-reset",
    }
)
"""Operations that commit an Operator Command (spec section 6.2)."""

_REQUEST_ID_PATTERN = re.compile(documents.REQUEST_ID_PATTERN)
_REPOSITORY_PATTERN = re.compile(documents.REPOSITORY_PATTERN)

_KNOWN_FLAGS = frozenset(
    {
        "--contract",
        "--offline",
        "--request-id",
        "--expect-repo",
        "--after",
        "--limit",
        "--before",
        "--path",
        "--saved-at",
    }
)


@dataclass(frozen=True)
class ParsedCall:
    """A well-formed call: the operation plus its typed arguments."""

    operation: str
    offline: bool
    request_id: str | None
    expect_repo: str | None
    arguments: dict[str, Any]
    capability: str | None
    """The required optional capability, or None for the mandatory core."""


@dataclass(frozen=True)
class ArgvFailure:
    """The first failure in the section 4.1 check order."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    request_id: str | None = None


def exit_code_for(code: str) -> int:
    """Return the fixed process exit for an ``error.code`` (spec section 4)."""
    return int(documents.error_table()[code]["exit_code"])


def render_document(doc: dict[str, Any]) -> str:
    """Render one JSON document plus a newline, compact with sorted keys."""
    return json.dumps(doc, sort_keys=True, separators=(",", ":")) + "\n"


def success_document(result: dict[str, Any]) -> dict[str, Any]:
    """Build an ``ok:true`` envelope for an operation result."""
    return {"contract": CONTRACT_VERSION, "ok": True, "result": result}


def error_document(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
    request_id: str | None = None,
) -> dict[str, Any]:
    """Build an ``ok:false`` envelope; ``retryable`` comes from the error table."""
    error: dict[str, Any] = {
        "code": code,
        "message": message,
        "retryable": bool(documents.error_table()[code]["retryable"]),
    }
    if request_id is not None:
        error["request_id"] = request_id
    if details is not None:
        error["details"] = details
    return {"contract": CONTRACT_VERSION, "ok": False, "error": error}


def render_success(result: dict[str, Any]) -> str:
    """Render an ``ok:true`` envelope."""
    return render_document(success_document(result))


def render_error(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
    request_id: str | None = None,
) -> tuple[str, int]:
    """Render an ``ok:false`` envelope with its fixed exit code."""
    return render_document(error_document(code, message, details=details, request_id=request_id)), (
        exit_code_for(code)
    )


def repositories_match(expected: str, actual: str) -> bool:
    """Compare ``--expect-repo`` with the Target Repository, case-insensitively."""
    return expected.casefold() == actual.casefold()


class _Usage(Exception):
    """Raised while parsing when the argv shape itself is malformed."""


def parse_argv(
    argv: Sequence[str], *, capabilities: Container[str] = OPERATOR_CAPABILITIES
) -> ParsedCall | ArgvFailure:
    """Parse contract argv and return the first section 4.1 failure.

    ``argv`` is the full ``agentctl`` argv starting with ``--contract`` (no
    program name), as recorded in the consumer fixtures. ``capabilities`` is
    the instance's advertised set of optional capabilities.
    """
    if len(argv) < 2 or argv[0] != "--contract":
        return ArgvFailure("usage", "the first argument must be '--contract <major>'")
    try:
        major = int(argv[1], 10)
    except ValueError:
        return ArgvFailure("usage", f"invalid contract major: {argv[1]!r}")
    if major != CONTRACT_MAJOR:
        return ArgvFailure(
            "contract_mismatch",
            f"this instance speaks contract major {CONTRACT_MAJOR}",
            {"supported": list(SUPPORTED_MAJORS)},
        )
    tokens = list(argv[2:])
    offline = False
    if tokens[:1] == ["--offline"]:
        offline = True
        tokens = tokens[1:]
    try:
        operation, arguments, request_id, expect_repo = _parse_tokens(tokens)
    except _Usage as exc:
        return ArgvFailure("usage", str(exc), {}, _echo_request_id(tokens))
    capability = _required_capability(operation, arguments)
    if offline and operation in MUTATIONS:
        return ArgvFailure(
            "unsupported",
            f"operation {operation!r} is not available offline",
            {"mode": "offline"},
            _valid_or_none(request_id),
        )
    if capability is not None and capability not in capabilities:
        return ArgvFailure(
            "unsupported",
            f"this instance does not advertise {capability}",
            {"capability": capability},
            _valid_or_none(request_id),
        )
    invalid = _validate_values(operation, arguments, request_id, expect_repo)
    if invalid is not None:
        return invalid
    return ParsedCall(operation, offline, request_id, expect_repo, arguments, capability)


def _echo_request_id(tokens: list[str]) -> str | None:
    """Echo ``--request-id`` on a grammar failure, only when it is well-formed."""
    for index, token in enumerate(tokens):
        if token == "--request-id" and index + 1 < len(tokens):
            return _valid_or_none(tokens[index + 1])
    return None


def _valid_or_none(value: str | None) -> str | None:
    if value is not None and _REQUEST_ID_PATTERN.search(value) is not None:
        return value
    return None


def _parse_tokens(
    tokens: list[str],
) -> tuple[str, dict[str, Any], str | None, str | None]:
    """Parse everything after ``--contract <major> [--offline]``.

    Returns the canonical operation, its arguments, ``--request-id`` and
    ``--expect-repo``. Raises :class:`_Usage` for malformed argv.
    """
    if not tokens:
        raise _Usage("missing operation")
    head = tokens[0]
    if head == "identity":
        _require_no_tokens(tokens[1:], "identity")
        return "identity", {}, None, None
    if head == "status":
        _require_no_tokens(tokens[1:], "status")
        return "status", {}, None, None
    if head == "command":
        if len(tokens) < 2:
            raise _Usage("command requires a request ID")
        if len(tokens) > 2:
            raise _Usage(f"unexpected argument for command: {tokens[2]!r}")
        return "command", {"request_id": tokens[1]}, None, None
    if head == "attempts":
        return _parse_attempts(tokens[1:])
    if head == "stop":
        return _parse_mutation(tokens, "stop", positional=[], optional_flags={"after"})
    if head == "resume":
        return _parse_mutation(tokens, "resume", positional=[], optional_flags=set())
    if head == "next":
        return _parse_next(tokens)
    if head == "handoff":
        return _parse_handoff(tokens)
    if head == "recovery":
        return _parse_recovery(tokens)
    if head == "errors":
        return _parse_errors(tokens)
    raise _Usage(f"unknown operation: {head!r}")


def _require_no_tokens(tokens: list[str], operation: str) -> None:
    if tokens:
        raise _Usage(f"unexpected argument for {operation}: {tokens[0]!r}")


def _parse_mutation(
    tokens: list[str],
    operation: str,
    *,
    positional: list[str],
    optional_flags: set[str],
) -> tuple[str, dict[str, Any], str | None, str | None]:
    """Parse ``<operation> [positionals] [flags]`` for one mutation.

    ``positional`` names the required positional slots in order (each takes
    one token); ``optional_flags`` names the operation-specific flags that
    take a value (``--request-id`` and ``--expect-repo`` are always allowed).
    """
    arguments: dict[str, Any] = {}
    request_id: str | None = None
    expect_repo: str | None = None
    rest = tokens[1:]
    for name in positional:
        if not rest or rest[0].startswith("--"):
            raise _Usage(f"{operation} requires {name}")
        arguments[name] = rest[0]
        rest = rest[1:]
    seen: set[str] = set()
    allowed = {"request-id", "expect-repo"} | optional_flags
    while rest:
        token = rest[0]
        if not token.startswith("--"):
            raise _Usage(f"unexpected argument for {operation}: {token!r}")
        flag = token[2:]
        if flag not in allowed:
            raise _Usage(f"unexpected argument for {operation}: {token!r}")
        if flag in seen:
            raise _Usage(f"repeated argument for {operation}: {token!r}")
        seen.add(flag)
        if len(rest) < 2 or rest[1] in _KNOWN_FLAGS:
            raise _Usage(f"argument {token}: expected one argument")
        value = rest[1]
        if flag == "request-id":
            request_id = value
        elif flag == "expect-repo":
            expect_repo = value
        else:
            arguments[flag.replace("-", "_")] = value
        rest = rest[2:]
    if request_id is None:
        raise _Usage(f"{operation} requires --request-id")
    if expect_repo is None:
        raise _Usage(f"{operation} requires --expect-repo")
    return operation, arguments, request_id, expect_repo


def _parse_attempts(tokens: list[str]) -> tuple[str, dict[str, Any], str | None, str | None]:
    if not tokens:
        raise _Usage("attempts requires one of list, show or export")
    subcommand = tokens[0]
    if subcommand == "list":
        return "attempts-list", _parse_attempts_list(tokens[1:]), None, None
    if subcommand == "show":
        if len(tokens) < 2:
            raise _Usage("attempts show requires an attempt ID")
        if len(tokens) > 2:
            raise _Usage(f"unexpected argument for attempts show: {tokens[2]!r}")
        return "attempts-show", {"attempt_id": tokens[1]}, None, None
    if subcommand == "export":
        return "attempts-export", _parse_attempts_export(tokens[1:]), None, None
    raise _Usage(f"unknown attempts subcommand: {subcommand!r}")


def _parse_attempts_list(tokens: list[str]) -> dict[str, Any]:
    arguments: dict[str, Any] = {}
    seen: set[str] = set()
    rest = tokens
    while rest:
        token = rest[0]
        if token not in ("--limit", "--before"):
            raise _Usage(f"unexpected argument for attempts list: {token!r}")
        flag = token[2:]
        if flag in seen:
            raise _Usage(f"repeated argument for attempts list: {token!r}")
        seen.add(flag)
        if len(rest) < 2 or rest[1] in _KNOWN_FLAGS:
            raise _Usage(f"argument {token}: expected one argument")
        arguments[flag] = rest[1]
        rest = rest[2:]
    return arguments


def _parse_attempts_export(tokens: list[str]) -> dict[str, Any]:
    if not tokens or tokens[0].startswith("--"):
        raise _Usage("attempts export requires an attempt ID")
    arguments: dict[str, Any] = {"attempt_id": tokens[0], "paths": []}
    rest = tokens[1:]
    while rest:
        if rest[0] != "--path":
            raise _Usage(f"unexpected argument for attempts export: {rest[0]!r}")
        if len(rest) < 2 or rest[1] in _KNOWN_FLAGS:
            raise _Usage("argument --path: expected one argument")
        arguments["paths"].append(rest[1])
        rest = rest[2:]
    return arguments


def _parse_next(tokens: list[str]) -> tuple[str, dict[str, Any], str | None, str | None]:
    if len(tokens) < 3 or tokens[1] != "issue":
        raise _Usage("next requires 'issue <number>'")
    operation, arguments, request_id, expect_repo = _parse_mutation(
        [tokens[0], *tokens[3:]], "next-issue", positional=[], optional_flags=set()
    )
    return operation, {"issue": tokens[2], **arguments}, request_id, expect_repo


def _parse_handoff(tokens: list[str]) -> tuple[str, dict[str, Any], str | None, str | None]:
    if len(tokens) < 2 or tokens[1] != "now":
        raise _Usage("handoff requires 'now'")
    return _parse_mutation([tokens[0], *tokens[2:]], "handoff", positional=[], optional_flags=set())


def _parse_recovery(tokens: list[str]) -> tuple[str, dict[str, Any], str | None, str | None]:
    if not tokens[1:]:
        raise _Usage("recovery requires one of retry or release")
    if tokens[1] == "retry":
        return _parse_mutation(
            [tokens[0], *tokens[2:]],
            "recovery-retry",
            positional=["attempt_id"],
            optional_flags=set(),
        )
    if tokens[1] == "release":
        return _parse_mutation(
            [tokens[0], *tokens[2:]],
            "recovery-release",
            positional=["attempt_id"],
            optional_flags={"saved-at"},
        )
    raise _Usage(f"unknown recovery subcommand: {tokens[1]!r}")


def _parse_errors(tokens: list[str]) -> tuple[str, dict[str, Any], str | None, str | None]:
    if len(tokens) < 2 or tokens[1] != "reset":
        raise _Usage("errors requires 'reset'")
    return _parse_mutation(
        [tokens[0], *tokens[2:]], "errors-reset", positional=[], optional_flags=set()
    )


def _required_capability(operation: str, arguments: Mapping[str, Any]) -> str | None:
    if operation == "stop" and "after" in arguments:
        return "stop-after"
    return {
        "next-issue": "next-issue",
        "handoff": "operator-handoff",
        "recovery-retry": "recovery-retry",
        "recovery-release": "recovery-release",
        "errors-reset": "errors-reset",
    }.get(operation)


def _validate_values(
    operation: str, arguments: Mapping[str, Any], request_id: str | None, expect_repo: str | None
) -> ArgvFailure | None:
    if request_id is not None and _REQUEST_ID_PATTERN.search(request_id) is None:
        return ArgvFailure("invalid_argument", f"invalid request ID: {request_id!r}")
    if expect_repo is not None and _REPOSITORY_PATTERN.search(expect_repo) is None:
        return ArgvFailure(
            "invalid_argument",
            f"invalid repository: {expect_repo!r}, expected owner/name",
            {},
            _valid_or_none(request_id),
        )
    positional_id = arguments.get("request_id")
    if operation == "command" and (
        not isinstance(positional_id, str) or _REQUEST_ID_PATTERN.search(positional_id) is None
    ):
        return ArgvFailure("invalid_argument", f"invalid request ID: {positional_id!r}")
    for key, value in arguments.items():
        failure = _validate_argument(operation, key, value, request_id)
        if failure is not None:
            return failure
    return None


def _validate_argument(
    operation: str, key: str, value: Any, request_id: str | None
) -> ArgvFailure | None:
    echo = _valid_or_none(request_id)
    if key == "after":
        if not _is_int_in(value, 1, 999):
            return ArgvFailure(
                "invalid_argument",
                f"invalid --after {value!r}: expected an integer from 1 to 999",
                {},
                echo,
            )
    elif key == "limit":
        if not _is_int_in(value, 1, 100):
            return ArgvFailure(
                "invalid_argument",
                f"invalid --limit {value!r}: expected an integer from 1 to 100",
                {},
                echo,
            )
    elif key == "issue":
        if not _is_int_in(value, 1, None):
            return ArgvFailure(
                "invalid_argument",
                f"invalid issue {value!r}: expected an integer >= 1",
                {},
                echo,
            )
    elif key in ("attempt_id", "before", "saved_at"):
        if not isinstance(value, str) or not value:
            return ArgvFailure(
                "invalid_argument", f"invalid {key}: expected a non-empty string", {}, echo
            )
    elif key == "paths":
        if not isinstance(value, list) or any(
            not isinstance(path, str) or not path for path in value
        ):
            return ArgvFailure(
                "invalid_argument", "invalid --path: expected a non-empty string", {}, echo
            )
    elif key == "request_id":
        pass
    else:  # pragma: no cover - the parser only produces known argument names.
        raise AssertionError(f"unreachable argument: {operation}.{key}")
    return None


def _is_int_in(value: Any, low: int, high: int | None) -> bool:
    if not isinstance(value, str):
        return False
    try:
        number = int(value, 10)
    except ValueError:
        return False
    if str(number) != value:
        return False
    return number >= low and (high is None or number <= high)
