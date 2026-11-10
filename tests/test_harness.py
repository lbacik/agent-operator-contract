"""Unit tests for the conformance harness protocol (spec section 15.1).

The harness is the engine-supplied host executable the producer suite drives to
steer a Managed Instance through failure and race scenarios. These tests pin the
verb set, the hold and crash points, the harness error codes with their exit
codes, and the per-verb request and reply rules. Agreement of the validators
with the JSON Schemas over the whole example set is covered in
test_schemas.py.
"""

from typing import Any

from agent_operator_contract import documents


def test_harness_verbs_are_the_fifteen_spec_verbs() -> None:
    assert documents.HARNESS_VERBS == (
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
        "fail-finalization",
    )


def test_hold_points_are_the_four_spec_points() -> None:
    assert documents.HOLD_POINTS == (
        "before-claim",
        "after-claim",
        "native-phase",
        "after-outcome",
    )


def test_crash_points_cover_between_commit_and_reply_among_others() -> None:
    assert documents.CRASH_POINTS == (
        "between-commit-and-reply",
        "before-claim",
        "after-claim",
        "during-finalization",
        "now",
    )


def test_harness_error_codes_match_the_spec_table() -> None:
    assert documents.HARNESS_ERROR_CODES == ("usage", "unsupported", "failed")
    assert documents.HARNESS_EXIT_CODES == {"usage": 2, "unsupported": 3, "failed": 1}


def test_harness_version_is_1_1() -> None:
    assert documents.HARNESS_VERSION == "1.1"


def _request(verb: str, params: dict[str, Any]) -> dict[str, Any]:
    return {"verb": verb, "params": params}


def _reply(verb: str, result: dict[str, Any]) -> dict[str, Any]:
    return {"harness": "1.1", "verb": verb, "ok": True, "result": result}


def _error_reply(verb: str, code: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": "nope"}
    if details is not None:
        error["details"] = details
    return {"harness": "1.1", "verb": verb, "ok": False, "error": error}


def test_every_verb_has_a_request_example() -> None:
    names = set(documents.harness_example_names())
    for verb in documents.HARNESS_VERBS:
        assert f"{verb}.request" in names, verb


def test_every_verb_has_a_successful_reply_example() -> None:
    for verb in documents.HARNESS_VERBS:
        reply = documents.load_harness_example(f"{verb}.reply")
        assert reply["ok"] is True, verb
        assert reply["verb"] == verb, verb


def test_request_examples_validate() -> None:
    for verb in documents.HARNESS_VERBS:
        doc = documents.load_harness_example(f"{verb}.request")
        assert documents.validate_harness_request(doc) == [], verb


def test_reply_examples_validate() -> None:
    for name in documents.harness_example_names():
        if name.endswith(".reply"):
            doc = documents.load_harness_example(name)
            assert documents.validate_harness_reply(doc) == [], name


def test_validate_request_rejects_shape_mistakes() -> None:
    assert documents.validate_harness_request(None) != []
    assert documents.validate_harness_request([]) != []
    assert documents.validate_harness_request({}) != []
    assert documents.validate_harness_request({"verb": "setup"}) != []
    not_object: dict[str, Any] = {"verb": "setup", "params": []}
    assert documents.validate_harness_request(not_object) != []
    assert documents.validate_harness_request(_request("frobnicate", {})) != []
    assert documents.validate_harness_request(_request("setup", {"capabilities": []})) != []


def test_validate_request_rejects_unknown_params() -> None:
    assert documents.validate_harness_request(_request("teardown", {"now": True})) != []
    params = {"repository": "a/b", "bogus": 1}
    assert documents.validate_harness_request(_request("setup", params)) != []


def test_setup_config_accepts_only_native_keys() -> None:
    good = _request("setup", {"repository": "acme/widgets", "config": {"native.fake": 1}})
    assert documents.validate_harness_request(good) == []
    bad = _request("setup", {"repository": "acme/widgets", "config": {"provider": "x"}})
    assert documents.validate_harness_request(bad) != []


def test_validate_request_rejects_bad_repository() -> None:
    for repository in ("acme", "", "a/b/c", 7, None):
        doc = _request("setup", {"repository": repository})
        assert documents.validate_harness_request(doc) != [], repository


def test_hold_requires_phase_exactly_for_native_phase() -> None:
    assert documents.validate_harness_request(_request("hold", {"point": "native-phase"})) != []
    assert (
        documents.validate_harness_request(
            _request("hold", {"point": "native-phase", "phase": "model_running"})
        )
        == []
    )
    assert documents.validate_harness_request(_request("hold", {"point": "before-claim"})) == []
    assert (
        documents.validate_harness_request(
            _request("hold", {"point": "before-claim", "phase": "model_running"})
        )
        != []
    )


def test_hold_rejects_unknown_points_and_scopes() -> None:
    assert documents.validate_harness_request(_request("hold", {"point": "at-teatime"})) != []
    assert (
        documents.validate_harness_request(
            _request("hold", {"point": "after-claim", "scope": "eventually"})
        )
        != []
    )


def test_hold_lock_bounds_duration() -> None:
    assert documents.validate_harness_request(_request("hold-lock", {"duration_ms": 2500})) == []
    for duration in (0, -1, 60001, "2500", None):
        assert (
            documents.validate_harness_request(_request("hold-lock", {"duration_ms": duration}))
            != []
        ), duration


def test_crash_rejects_unknown_points() -> None:
    assert (
        documents.validate_harness_request(_request("crash", {"point": "between-commit-and-reply"}))
        == []
    )
    assert documents.validate_harness_request(_request("crash", {"point": "at-teatime"})) != []


def test_finish_attempt_requires_a_named_outcome() -> None:
    assert (
        documents.validate_harness_request(
            _request("finish-attempt", {"native_outcome": "complete"})
        )
        == []
    )
    for outcome in ("", None, 7):
        doc = _request("finish-attempt", {"native_outcome": outcome})
        assert documents.validate_harness_request(doc) != [], outcome


def test_validate_reply_rejects_shape_mistakes() -> None:
    assert documents.validate_harness_reply(None) != []
    assert documents.validate_harness_reply({}) != []
    assert documents.validate_harness_reply({"harness": "1.1"}) != []
    assert documents.validate_harness_reply(_reply("setup", {})) != []
    assert documents.validate_harness_reply(_error_reply("hold", "unsupported")) != []
    unknown = {"harness": "1.1", "verb": "hold", "ok": False, "error": {"code": "teapot"}}
    assert documents.validate_harness_reply(unknown) != []
    bad_version = {"harness": "1", "verb": "hold", "ok": True, "result": {}}
    assert documents.validate_harness_reply(bad_version) != []


def test_validate_reply_accepts_replies_without_a_verb_echo() -> None:
    doc: dict[str, Any] = {"harness": "1.1", "ok": True, "result": {}}
    assert documents.validate_harness_reply(doc) == []


def test_unsupported_requires_what_and_name() -> None:
    missing = _error_reply("hold", "unsupported", {"what": "hold-point"})
    assert documents.validate_harness_reply(missing) != []
    full = _error_reply("hold", "unsupported", {"what": "hold-point", "name": "after-outcome"})
    assert documents.validate_harness_reply(full) == []
    bad_what = _error_reply("hold", "unsupported", {"what": "teatime", "name": "x"})
    assert documents.validate_harness_reply(bad_what) != []


def test_setup_reply_rejects_root_and_bad_volumes() -> None:
    base: dict[str, Any] = {
        "container": "conformance-1",
        "image": "sha256:abc",
        "volumes": [{"name": "data", "path": "/data"}],
        "exec_user": "agent",
        "repository": "acme/widgets",
        "capabilities": ["next-issue"],
    }
    assert documents.validate_harness_reply(_reply("setup", base)) == []
    root = dict(base, exec_user="root")
    assert documents.validate_harness_reply(_reply("setup", root)) != []
    no_volumes = dict(base, volumes=[])
    assert documents.validate_harness_reply(_reply("setup", no_volumes)) != []
    relative = dict(base, volumes=[{"name": "data", "path": "data"}])
    assert documents.validate_harness_reply(_reply("setup", relative)) != []


def test_hold_reply_allows_a_null_attempt_id() -> None:
    armed = {"point": "before-claim", "attempt_id": None}
    assert documents.validate_harness_reply(_reply("hold", armed)) == []
    engaged = {"point": "native-phase", "attempt_id": "#12/3"}
    assert documents.validate_harness_reply(_reply("hold", engaged)) == []
    missing = {"point": "before-claim"}
    assert documents.validate_harness_reply(_reply("hold", missing)) != []


def test_fail_finalization_is_a_parameterless_one_shot() -> None:
    assert documents.validate_harness_request(_request("fail-finalization", {})) == []
    assert documents.validate_harness_request(_request("fail-finalization", {"now": True})) != []
    assert documents.validate_harness_reply(_reply("fail-finalization", {"armed": True})) == []
    assert documents.validate_harness_reply(_reply("fail-finalization", {"armed": False})) != []
    assert documents.validate_harness_reply(_reply("fail-finalization", {})) != []


def test_a_one_zero_harness_reply_is_still_accepted() -> None:
    """A minor never invalidates an older reply: the suite reads 1.0 harnesses too."""
    reply = {"harness": "1.0", "verb": "release", "ok": True, "result": {"released": True}}
    assert documents.validate_harness_reply(reply) == []
