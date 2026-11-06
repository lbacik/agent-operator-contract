"""Unit tests for the shared producer helpers (spec sections 4.1, 4 and 6).

These tests go through the contract boundary's pure seam: argv in, the first
failure in the section 4.1 check order out. They are table-driven over the spec
tables, not over internals.
"""

import json
from typing import Any

import pytest

from agent_operator_contract import documents, shared

FULL_CAPABILITIES = frozenset(
    {
        "stop-after",
        "next-issue",
        "operator-handoff",
        "recovery-retry",
        "recovery-release",
        "errors-reset",
    }
)
CODING_CAPABILITIES = frozenset({"stop-after", "next-issue"})

VALID_MUTATION_TAIL = [
    "--request-id",
    "req-ABC123",
    "--expect-repo",
    "example-owner/example-repo",
]


def test_exit_codes_match_the_error_table() -> None:
    table = documents.error_table()
    for code in table:
        assert shared.exit_code_for(code) == table[code]["exit_code"], code


def test_exit_code_for_unknown_code_raises() -> None:
    with pytest.raises(KeyError):
        shared.exit_code_for("no-such-code")


def test_render_document_is_one_compact_sorted_document_and_a_newline() -> None:
    text = shared.render_document({"ok": True, "contract": "1.0", "result": {}})

    assert text.endswith("\n")
    assert text.count("\n") == 1
    assert text == '{"contract":"1.0","ok":true,"result":{}}\n'


def test_rendered_fixtures_keep_their_byte_form() -> None:
    for name in ("identity", "contract-mismatch", "usage"):
        fixture = documents.load_fixture(name)
        doc: Any = json.loads(fixture["stdout"])

        assert shared.render_document(doc) == fixture["stdout"], name


def test_render_success_and_error_pair_codes_with_the_right_exit() -> None:
    text = shared.render_success({"hello": "world"})

    assert text.endswith("\n")
    assert shared.exit_code_for("contract_mismatch") == 4

    error_text, exit_code = shared.render_error(
        "contract_mismatch", "speak 1", details={"supported": [1]}
    )

    assert exit_code == 4
    assert error_text.endswith("\n")


def test_error_document_marks_retryable_from_the_table() -> None:
    assert shared.error_document("usage", "bad")["error"]["retryable"] is False
    assert shared.error_document("agent_unreachable", "lost")["error"]["retryable"] is True


@pytest.mark.parametrize(
    ("expected", "actual", "matches"),
    [
        ("acme/widgets", "acme/widgets", True),
        ("acme/widgets", "ACME/WIDGETS", True),
        ("Acme/Widgets", "aCME/wIDGETS", True),
        ("acme/widgets", "acme/gadgets", False),
        ("acme/widgets", "acme/widgets2", False),
    ],
)
def test_repositories_match_is_case_insensitive(expected: str, actual: str, matches: bool) -> None:
    assert shared.repositories_match(expected, actual) is matches


def _failure(argv: list[str], capabilities: frozenset[str] = FULL_CAPABILITIES) -> Any:
    parsed = shared.parse_argv(argv, capabilities=capabilities)
    assert isinstance(parsed, shared.ArgvFailure)
    return parsed


def _success(argv: list[str], capabilities: frozenset[str] = FULL_CAPABILITIES) -> Any:
    parsed = shared.parse_argv(argv, capabilities=capabilities)
    assert isinstance(parsed, shared.ParsedCall)
    return parsed


@pytest.mark.parametrize(
    ("argv", "code"),
    [
        # --contract is missing or not first: malformed.
        ([], "usage"),
        (["identity"], "usage"),
        (["identity", "--contract", "1"], "usage"),
        (["--contract"], "usage"),
        (["--contract", "x", "identity"], "usage"),
        (["--contract", "1.0", "identity"], "usage"),
        (["--contract", "", "identity"], "usage"),
        # The contract check runs before everything else.
        (["--contract", "2", "frobnicate"], "contract_mismatch"),
        (["--contract", "2"], "contract_mismatch"),
        (["--contract", "2", "stop", "--after"], "contract_mismatch"),
        (["--contract", "0", "identity"], "contract_mismatch"),
        # Unknown operations and shapes are malformed.
        (["--contract", "1"], "usage"),
        (["--contract", "1", "frobnicate"], "usage"),
        (["--contract", "1", "attempts"], "usage"),
        (["--contract", "1", "attempts", "bogus", "x"], "usage"),
        (["--contract", "1", "stop", "--after"], "usage"),
        (["--contract", "1", "stop"], "usage"),
        (["--contract", "1", "identity", "--request-id", "req-ABC123"], "usage"),
        (["--contract", "1", "status", "--bogus"], "usage"),
        (["--contract", "1", "command"], "usage"),
        (["--contract", "1", "command", "a", "b"], "usage"),
        (["--contract", "1", "identity", "--offline"], "usage"),
        (["--contract", "1", "stop", "--request-id", "req-A", "--request-id", "req-B"], "usage"),
        (["--contract", "1", "stop", "--after", "3"], "usage"),
        (["--contract", "1", "next", "issue"], "usage"),
        (["--contract", "1", "handoff"], "usage"),
        (["--contract", "1", "recovery", "retry"], "usage"),
        (["--contract", "1", "recovery", "release", "a"], "usage"),
        (["--contract", "1", "errors"], "usage"),
        (["--contract", "1", "attempts", "list", "--limit"], "usage"),
    ],
)
def test_check_order_contract_then_usage(argv: list[str], code: str) -> None:
    assert _failure(argv).code == code


@pytest.mark.parametrize(
    "argv",
    [
        ["--contract", "1", "--offline", "stop", *VALID_MUTATION_TAIL],
        ["--contract", "1", "--offline", "resume", *VALID_MUTATION_TAIL],
        ["--contract", "1", "--offline", "errors", "reset", *VALID_MUTATION_TAIL],
    ],
)
def test_offline_mutations_are_unsupported(argv: list[str]) -> None:
    assert _failure(argv).code == "unsupported"


def test_capability_is_checked_before_argument_values() -> None:
    # A handoff with a bad request ID is still "unsupported" first when the
    # capability is missing, and "invalid_argument" when it is present.
    argv = [
        "--contract",
        "1",
        "handoff",
        "now",
        "--request-id",
        "not a valid id!",
        "--expect-repo",
        "example-owner/example-repo",
    ]

    assert _failure(argv, CODING_CAPABILITIES).code == "unsupported"
    assert _failure(argv, FULL_CAPABILITIES).code == "invalid_argument"


def test_missing_capability_beats_bad_values() -> None:
    argv = ["--contract", "1", "stop", "--after", "0", *VALID_MUTATION_TAIL]

    assert _failure(argv, frozenset({"next-issue"})).code == "unsupported"
    assert _failure(argv, FULL_CAPABILITIES).code == "invalid_argument"


@pytest.mark.parametrize(
    ("argv", "capabilities"),
    [
        (["--contract", "1", "handoff", "now", *VALID_MUTATION_TAIL], CODING_CAPABILITIES),
        (["--contract", "1", "next", "issue", "3", *VALID_MUTATION_TAIL], frozenset()),
        (
            ["--contract", "1", "stop", "--after", "3", *VALID_MUTATION_TAIL],
            frozenset({"next-issue"}),
        ),
        (
            [
                "--contract",
                "1",
                "recovery",
                "retry",
                "att-1",
                *VALID_MUTATION_TAIL,
            ],
            CODING_CAPABILITIES,
        ),
    ],
)
def test_unsupported_needs_the_missing_capability_in_details(
    argv: list[str], capabilities: frozenset[str]
) -> None:
    failure = _failure(argv, capabilities)

    assert failure.code == "unsupported"
    assert failure.details.get("capability") not in set(capabilities)


@pytest.mark.parametrize(
    "argv",
    [
        ["--contract", "1", "stop", "--after", "0", *VALID_MUTATION_TAIL],
        ["--contract", "1", "stop", "--after", "1000", *VALID_MUTATION_TAIL],
        ["--contract", "1", "stop", "--after", "many", *VALID_MUTATION_TAIL],
        [
            "--contract",
            "1",
            "stop",
            "--request-id",
            "not a valid id!",
            "--expect-repo",
            "example-owner/example-repo",
        ],
        ["--contract", "1", "stop", "--request-id", "req-A", "--expect-repo", "no-slash"],
        ["--contract", "1", "command", "not a valid id!"],
        ["--contract", "1", "attempts", "list", "--limit", "0"],
        ["--contract", "1", "attempts", "list", "--limit", "101"],
        ["--contract", "1", "attempts", "list", "--limit", "many"],
        ["--contract", "1", "attempts", "list", "--before", ""],
        ["--contract", "1", "next", "issue", "0", *VALID_MUTATION_TAIL],
        ["--contract", "1", "next", "issue", "many", *VALID_MUTATION_TAIL],
        ["--contract", "1", "attempts", "show", ""],
        [
            "--contract",
            "1",
            "recovery",
            "release",
            "att-1",
            "--saved-at",
            "",
            *VALID_MUTATION_TAIL,
        ],
    ],
)
def test_invalid_values_are_invalid_argument(argv: list[str]) -> None:
    assert _failure(argv, FULL_CAPABILITIES).code == "invalid_argument"


def test_contract_mismatch_carries_supported_and_consumes_no_request_id() -> None:
    failure = _failure(
        ["--contract", "2", "stop", "--request-id", "req-ABC123", "--expect-repo", "a/b"]
    )

    assert failure.code == "contract_mismatch"
    assert failure.details == {"supported": [1]}
    assert failure.request_id is None
    assert shared.exit_code_for(failure.code) == 4


def test_failures_echo_a_valid_request_id() -> None:
    failure = _failure(
        ["--contract", "1", "handoff", "now", *VALID_MUTATION_TAIL], CODING_CAPABILITIES
    )

    assert failure.code == "unsupported"
    assert failure.request_id == "req-ABC123"


def test_usage_without_a_request_id_echoes_none() -> None:
    failure = _failure(["--contract", "1", "stop", "--after"])

    assert failure.code == "usage"
    assert failure.request_id is None


@pytest.mark.parametrize(
    ("argv", "operation", "offline", "capability"),
    [
        (["--contract", "1", "identity"], "identity", False, None),
        (["--contract", "1", "--offline", "identity"], "identity", True, None),
        (["--contract", "1", "status"], "status", False, None),
        (["--contract", "1", "command", "req-ABC123"], "command", False, None),
        (["--contract", "1", "attempts", "list"], "attempts-list", False, None),
        (["--contract", "1", "attempts", "show", "#41/1"], "attempts-show", False, None),
        (
            ["--contract", "1", "attempts", "export", "#41/1", "--path", "events.jsonl"],
            "attempts-export",
            False,
            None,
        ),
        (["--contract", "1", "stop", *VALID_MUTATION_TAIL], "stop", False, None),
        (
            ["--contract", "1", "stop", "--after", "3", *VALID_MUTATION_TAIL],
            "stop",
            False,
            "stop-after",
        ),
        (["--contract", "1", "resume", *VALID_MUTATION_TAIL], "resume", False, None),
        (
            ["--contract", "1", "next", "issue", "41", *VALID_MUTATION_TAIL],
            "next-issue",
            False,
            "next-issue",
        ),
        (
            ["--contract", "1", "handoff", "now", *VALID_MUTATION_TAIL],
            "handoff",
            False,
            "operator-handoff",
        ),
        (
            ["--contract", "1", "errors", "reset", *VALID_MUTATION_TAIL],
            "errors-reset",
            False,
            "errors-reset",
        ),
    ],
)
def test_well_formed_argv_parses(
    argv: list[str], operation: str, offline: bool, capability: str | None
) -> None:
    parsed = _success(argv)

    assert parsed.operation == operation
    assert parsed.offline is offline
    assert parsed.capability == capability
