"""Consumer and producer DTOs for the Status Snapshot (spec section 7.2).

Producer mode is strict: unknown fields and unknown enum values are rejected.
Consumer mode is lenient (spec section 5): unknown fields are ignored and
unknown enum values in optional positions are kept as the explicit string
``"unknown"`` so an operator never mistakes them for a real value.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from agent_operator_contract import documents

UNKNOWN = "unknown"
"""What lenient parsing keeps for an unknown enum value (spec section 5)."""


@dataclass(frozen=True)
class Figure:
    """One figure with its unit and basis (spec section 7.3)."""

    value: int | float | None
    unit: str
    basis: str
    reason: str | None = None


@dataclass(frozen=True)
class Halt:
    """A durably recorded engine halt (spec section 7.2)."""

    reason: str
    detail: str
    at: str


@dataclass(frozen=True)
class CommandRef:
    """A reference to a committed command (spec section 7.5)."""

    request_id: str
    sequence: int


@dataclass(frozen=True)
class StopPlan:
    """A ``stop --after N`` plan (spec section 8.3)."""

    request_id: str
    sequence: int | None = None
    remaining: int = 0
    counted: int = 0


@dataclass(frozen=True)
class Priority:
    """A one-shot issue priority (spec section 8.3)."""

    request_id: str
    issue: int


@dataclass(frozen=True)
class BudgetLimit:
    """One enforced ceiling with its used and limit figures."""

    name: str
    used: Figure
    limit: Figure


@dataclass(frozen=True)
class Budget:
    """The attempt budget (spec section 7.2)."""

    raw_tokens: Figure
    effective_tokens: Figure
    enforced_tokens: str
    limits: tuple[BudgetLimit, ...] = ()


@dataclass(frozen=True)
class CurrentAttempt:
    """The active attempt, or null when no attempt is active."""

    attempt_id: str
    issue_number: int
    issue_title: str | None
    native_phase: str
    started_at: str
    last_activity_at: str | None
    observed_at: str
    budget: Budget
    figures: dict[str, Figure] = field(default_factory=dict)


@dataclass(frozen=True)
class AttemptSummary:
    """A finished attempt (spec section 7.4)."""

    attempt_id: str
    issue_number: int
    issue_title: str | None
    started_at: str
    finished_at: str | None
    lifecycle_state: str
    outcome_native: str | None
    outcome_reason: str | None
    outcome_label: str | None
    attention_required: bool
    sealed: bool
    sealed_at: str | None


@dataclass(frozen=True)
class CommandRecord:
    """One committed operator command (spec section 7.5)."""

    request_id: str
    sequence: int
    kind: str
    arguments: dict[str, Any]
    repository: str
    acknowledgement: str
    detail: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ContractEvent:
    """One contract event (spec section 10)."""

    at: str
    level: str
    event: str
    text: str


@dataclass(frozen=True)
class Identity:
    """The nested identity of a snapshot."""

    backend: str
    agent_version: str
    contract_major: int
    contract_minor: int
    repository: str
    mode: str
    process_started_at: str | None
    capabilities: tuple[str, ...]


@dataclass(frozen=True)
class Instance:
    """The instance section of a snapshot."""

    state: str
    startup_check: str | None
    last_halt: Halt | None
    observed_at: str


@dataclass(frozen=True)
class Intake:
    """The intake section of a snapshot."""

    state: str
    pending_stop: CommandRef | None
    stop_plan: StopPlan | None
    priority: Priority | None
    observed_at: str


@dataclass(frozen=True)
class StatusSnapshot:
    """A parsed Status Snapshot."""

    generated_at: str
    identity: Identity
    instance: Instance
    intake: Intake
    current_attempt: CurrentAttempt | None
    last_finished: AttemptSummary | None
    recent_commands: tuple[CommandRecord, ...]
    recent_events: tuple[ContractEvent, ...]
    truncated: bool


class StatusError(ValueError):
    """A snapshot failed to parse in the requested mode."""


def _fail(prefix: str, message: str) -> StatusError:
    return StatusError(f"{prefix}: {message}")


def _require_object(doc: object, prefix: str) -> dict[str, Any]:
    if not isinstance(doc, dict):
        raise _fail(prefix, "must be an object")
    return doc


def _required(doc: dict[str, Any], key: str, prefix: str) -> Any:
    if key not in doc:
        raise _fail(prefix, f"is missing required property: {key!r}")
    return doc[key]


def _check_unknown(doc: dict[str, Any], allowed: frozenset[str], prefix: str, strict: bool) -> None:
    if strict:
        for key in doc:
            if key not in allowed:
                raise _fail(prefix, f"has an unexpected property: {key!r}")


def _parse_enum(
    value: object, allowed: tuple[str, ...], prefix: str, field_name: str, strict: bool
) -> str:
    if isinstance(value, str) and value in allowed:
        return value
    if strict:
        raise _fail(prefix, f"{field_name} {value!r} is not a known 1.0 value")
    return UNKNOWN


def _parse_limit_name(value: object, prefix: str, strict: bool) -> str:
    if isinstance(value, str) and (
        value in documents.LIMIT_NAMES
        or (value.startswith("native.") and len(value) > len("native."))
    ):
        return value
    if strict:
        raise _fail(prefix, f"name {value!r} is not a known 1.0 limit")
    return UNKNOWN


def _parse_timestamp(value: object, prefix: str, field_name: str) -> str:
    if not isinstance(value, str) or not documents.TIMESTAMP_PATTERN:
        raise _fail(prefix, f"{field_name} must be an RFC 3339 UTC timestamp")
    if re.search(documents.TIMESTAMP_PATTERN, value) is None:
        raise _fail(prefix, f"{field_name} must be an RFC 3339 UTC timestamp")
    return value


def _parse_nullable_timestamp(value: object, prefix: str, field_name: str) -> str | None:
    if value is None:
        return None
    return _parse_timestamp(value, prefix, field_name)


def parse_figure(doc: object, *, strict: bool, prefix: str = "figure") -> Figure:
    """Parse one Figure in either mode."""
    data = _require_object(doc, prefix)
    _check_unknown(data, frozenset({"value", "unit", "basis", "reason"}), prefix, strict)
    value = _required(data, "value", prefix)
    unit = _parse_enum(
        _required(data, "unit", prefix), documents.FIGURE_UNITS, prefix, "unit", strict
    )
    basis = _parse_enum(
        _required(data, "basis", prefix), documents.FIGURE_BASES, prefix, "basis", strict
    )
    reason = data.get("reason")
    if reason is not None and not isinstance(reason, str):
        raise _fail(prefix, "reason must be a string")
    if basis in documents.NULL_BASES:
        if value is not None:
            raise _fail(prefix, f"value must be null when basis is {basis!r}")
    elif basis == UNKNOWN:
        if value is not None and (not isinstance(value, (int, float)) or isinstance(value, bool)):
            raise _fail(prefix, "value must be a number or null")
    elif not isinstance(value, (int, float)) or isinstance(value, bool):
        raise _fail(prefix, f"value must be a number when basis is {basis!r}")
    return Figure(value=value, unit=unit, basis=basis, reason=reason)


def _parse_halt(doc: object, prefix: str, strict: bool) -> Halt | None:
    if doc is None:
        return None
    data = _require_object(doc, prefix)
    _check_unknown(data, frozenset({"reason", "detail", "at"}), prefix, strict)
    reason = _required(data, "reason", prefix)
    detail = _required(data, "detail", prefix)
    at = _required(data, "at", prefix)
    if not isinstance(reason, str) or not reason:
        raise _fail(prefix, "reason must be a non-empty string")
    if not isinstance(detail, str):
        raise _fail(prefix, "detail must be a string")
    return Halt(reason=reason, detail=detail, at=_parse_timestamp(at, prefix, "at"))


def _parse_budget(doc: object, prefix: str, strict: bool) -> Budget:
    data = _require_object(doc, prefix)
    _check_unknown(
        data,
        frozenset({"raw_tokens", "effective_tokens", "enforced_tokens", "limits"}),
        prefix,
        strict,
    )
    raw = parse_figure(
        _required(data, "raw_tokens", prefix), strict=strict, prefix=f"{prefix}.raw_tokens"
    )
    effective = parse_figure(
        _required(data, "effective_tokens", prefix),
        strict=strict,
        prefix=f"{prefix}.effective_tokens",
    )
    enforced = _parse_enum(
        _required(data, "enforced_tokens", prefix),
        documents.ENFORCED_TOKENS,
        prefix,
        "enforced_tokens",
        strict,
    )
    limits_raw = _required(data, "limits", prefix)
    if not isinstance(limits_raw, list):
        raise _fail(prefix, "limits must be an array")
    limits: list[BudgetLimit] = []
    for index, entry in enumerate(limits_raw):
        item_prefix = f"{prefix}.limits[{index}]"
        entry_data = _require_object(entry, item_prefix)
        _check_unknown(entry_data, frozenset({"name", "used", "limit"}), item_prefix, strict)
        name = _parse_limit_name(_required(entry_data, "name", item_prefix), item_prefix, strict)
        used = parse_figure(
            _required(entry_data, "used", item_prefix), strict=strict, prefix=f"{item_prefix}.used"
        )
        limit = parse_figure(
            _required(entry_data, "limit", item_prefix),
            strict=strict,
            prefix=f"{item_prefix}.limit",
        )
        limits.append(BudgetLimit(name=name, used=used, limit=limit))
    return Budget(
        raw_tokens=raw,
        effective_tokens=effective,
        enforced_tokens=enforced,
        limits=tuple(limits),
    )


def _parse_current_attempt(doc: object, prefix: str, strict: bool) -> CurrentAttempt | None:
    if doc is None:
        return None
    data = _require_object(doc, prefix)
    _check_unknown(
        data,
        frozenset(
            {
                "attempt_id",
                "issue",
                "native_phase",
                "started_at",
                "last_activity_at",
                "observed_at",
                "budget",
                "figures",
            }
        ),
        prefix,
        strict,
    )
    attempt_id = _required(data, "attempt_id", prefix)
    if not isinstance(attempt_id, str) or not attempt_id:
        raise _fail(prefix, "attempt_id must be a non-empty string")
    issue = _required(data, "issue", prefix)
    issue_data = _require_object(issue, f"{prefix}.issue")
    _check_unknown(issue_data, frozenset({"number", "title"}), f"{prefix}.issue", strict)
    number = _required(issue_data, "number", f"{prefix}.issue")
    if type(number) is not int or number < 1:
        raise _fail(prefix, "issue.number must be an integer >= 1")
    title = _required(issue_data, "title", f"{prefix}.issue")
    if title is not None and not isinstance(title, str):
        raise _fail(prefix, "issue.title must be a string or null")
    native_phase = _required(data, "native_phase", prefix)
    if not isinstance(native_phase, str) or not native_phase:
        raise _fail(prefix, "native_phase must be a non-empty string")
    started_at = _parse_timestamp(_required(data, "started_at", prefix), prefix, "started_at")
    last_activity_at = _parse_nullable_timestamp(
        _required(data, "last_activity_at", prefix), prefix, "last_activity_at"
    )
    observed_at = _parse_timestamp(_required(data, "observed_at", prefix), prefix, "observed_at")
    budget = _parse_budget(_required(data, "budget", prefix), f"{prefix}.budget", strict)
    figures_raw = _required(data, "figures", prefix)
    if not isinstance(figures_raw, dict):
        raise _fail(prefix, "figures must be an object")
    figures: dict[str, Figure] = {}
    for name, figure_doc in figures_raw.items():
        figures[name] = parse_figure(figure_doc, strict=strict, prefix=f"{prefix}.figures.{name}")
    return CurrentAttempt(
        attempt_id=attempt_id,
        issue_number=number,
        issue_title=title,
        native_phase=native_phase,
        started_at=started_at,
        last_activity_at=last_activity_at,
        observed_at=observed_at,
        budget=budget,
        figures=figures,
    )


def _parse_attempt_summary(doc: object, prefix: str, strict: bool) -> AttemptSummary | None:
    if doc is None:
        return None
    data = _require_object(doc, prefix)
    _check_unknown(
        data,
        frozenset(
            {
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
            }
        ),
        prefix,
        strict,
    )
    attempt_id = _required(data, "attempt_id", prefix)
    if not isinstance(attempt_id, str) or not attempt_id:
        raise _fail(prefix, "attempt_id must be a non-empty string")
    issue = _require_object(_required(data, "issue", prefix), f"{prefix}.issue")
    number = _required(issue, "number", f"{prefix}.issue")
    if type(number) is not int or number < 1:
        raise _fail(prefix, "issue.number must be an integer >= 1")
    title = _required(issue, "title", f"{prefix}.issue")
    if title is not None and not isinstance(title, str):
        raise _fail(prefix, "issue.title must be a string or null")
    started_at = _parse_timestamp(_required(data, "started_at", prefix), prefix, "started_at")
    finished_at = _parse_nullable_timestamp(
        _required(data, "finished_at", prefix), prefix, "finished_at"
    )
    lifecycle_state = _required(data, "lifecycle_state", prefix)
    if not isinstance(lifecycle_state, str) or not lifecycle_state:
        raise _fail(prefix, "lifecycle_state must be a non-empty string")
    outcome = _required(data, "outcome", prefix)
    outcome_native: str | None = None
    outcome_reason: str | None = None
    outcome_label: str | None = None
    if outcome is not None:
        outcome_data = _require_object(outcome, f"{prefix}.outcome")
        _check_unknown(
            outcome_data, frozenset({"native", "reason", "label"}), f"{prefix}.outcome", strict
        )
        native = _required(outcome_data, "native", f"{prefix}.outcome")
        if not isinstance(native, str) or not native:
            raise _fail(prefix, "outcome.native must be a non-empty string")
        reason = _required(outcome_data, "reason", f"{prefix}.outcome")
        if reason is not None and not isinstance(reason, str):
            raise _fail(prefix, "outcome.reason must be a string or null")
        label = _required(outcome_data, "label", f"{prefix}.outcome")
        if not isinstance(label, str) or not label:
            raise _fail(prefix, "outcome.label must be a non-empty string")
        outcome_native, outcome_reason, outcome_label = native, reason, label
    _required(data, "delivery", prefix)
    attention = _required(data, "attention_required", prefix)
    if type(attention) is not bool:
        raise _fail(prefix, "attention_required must be a boolean")
    sealed = _required(data, "sealed", prefix)
    if type(sealed) is not bool:
        raise _fail(prefix, "sealed must be a boolean")
    sealed_at = _parse_nullable_timestamp(_required(data, "sealed_at", prefix), prefix, "sealed_at")
    return AttemptSummary(
        attempt_id=attempt_id,
        issue_number=number,
        issue_title=title,
        started_at=started_at,
        finished_at=finished_at,
        lifecycle_state=lifecycle_state,
        outcome_native=outcome_native,
        outcome_reason=outcome_reason,
        outcome_label=outcome_label,
        attention_required=attention,
        sealed=sealed,
        sealed_at=sealed_at,
    )


def _parse_command_record(doc: object, prefix: str, strict: bool) -> CommandRecord:
    data = _require_object(doc, prefix)
    _check_unknown(
        data,
        frozenset(
            {
                "request_id",
                "sequence",
                "kind",
                "arguments",
                "repository",
                "acknowledgement",
                "detail",
                "created_at",
                "updated_at",
            }
        ),
        prefix,
        strict,
    )
    request_id = _required(data, "request_id", prefix)
    if (
        not isinstance(request_id, str)
        or re.search(documents.REQUEST_ID_PATTERN, request_id) is None
    ):
        raise _fail(prefix, "request_id must match the request ID pattern")
    sequence = _required(data, "sequence", prefix)
    if type(sequence) is not int or sequence < 0:
        raise _fail(prefix, "sequence must be an integer >= 0")
    kind = _parse_enum(
        _required(data, "kind", prefix), documents.COMMAND_KINDS, prefix, "kind", strict
    )
    arguments = _required(data, "arguments", prefix)
    if not isinstance(arguments, dict):
        raise _fail(prefix, "arguments must be an object")
    repository = _required(data, "repository", prefix)
    if (
        not isinstance(repository, str)
        or re.search(documents.REPOSITORY_PATTERN, repository) is None
    ):
        raise _fail(prefix, "repository must be written owner/name")
    acknowledgement = _parse_enum(
        _required(data, "acknowledgement", prefix),
        documents.ACKNOWLEDGEMENTS,
        prefix,
        "acknowledgement",
        strict,
    )
    detail = _required(data, "detail", prefix)
    if not isinstance(detail, str):
        raise _fail(prefix, "detail must be a string")
    created_at = _parse_timestamp(_required(data, "created_at", prefix), prefix, "created_at")
    updated_at = _parse_timestamp(_required(data, "updated_at", prefix), prefix, "updated_at")
    return CommandRecord(
        request_id=request_id,
        sequence=sequence,
        kind=kind,
        arguments=dict(arguments),
        repository=repository,
        acknowledgement=acknowledgement,
        detail=detail,
        created_at=created_at,
        updated_at=updated_at,
    )


def _parse_contract_event(doc: object, prefix: str, strict: bool) -> ContractEvent:
    data = _require_object(doc, prefix)
    if strict:
        for key in data:
            if key not in {
                "at",
                "level",
                "event",
                "text",
                "attempt_id",
                "phase",
                "issue",
                "request_id",
                "kind",
                "acknowledgement",
                "outcome",
                "reason",
                "check",
                "result",
                "agent_version",
                "signal",
            }:
                raise _fail(prefix, f"has an unexpected property: {key!r}")
    at = _parse_timestamp(_required(data, "at", prefix), prefix, "at")
    level = _parse_enum(
        _required(data, "level", prefix), documents.EVENT_LEVELS, prefix, "level", strict
    )
    event = _required(data, "event", prefix)
    if not isinstance(event, str) or not event:
        raise _fail(prefix, "event must be a non-empty string")
    text = _required(data, "text", prefix)
    if not isinstance(text, str):
        raise _fail(prefix, "text must be a string")
    if len(text) > 300:
        raise _fail(prefix, "text must be at most 300 characters")
    return ContractEvent(at=at, level=level, event=event, text=text)


def _parse_identity(doc: object, prefix: str, strict: bool) -> Identity:
    data = _require_object(doc, prefix)
    _check_unknown(
        data,
        frozenset(
            {
                "backend",
                "agent_version",
                "contract",
                "repository",
                "mode",
                "process_started_at",
                "capabilities",
            }
        ),
        prefix,
        strict,
    )
    backend = _required(data, "backend", prefix)
    agent_version = _required(data, "agent_version", prefix)
    if not isinstance(backend, str) or not backend:
        raise _fail(prefix, "backend must be a non-empty string")
    if not isinstance(agent_version, str) or not agent_version:
        raise _fail(prefix, "agent_version must be a non-empty string")
    contract = _require_object(_required(data, "contract", prefix), f"{prefix}.contract")
    major = _required(contract, "major", f"{prefix}.contract")
    minor = _required(contract, "minor", f"{prefix}.contract")
    if type(major) is not int or major < 1:
        raise _fail(prefix, "contract.major must be an integer >= 1")
    if type(minor) is not int or minor < 0:
        raise _fail(prefix, "contract.minor must be an integer >= 0")
    repository = _required(data, "repository", prefix)
    if (
        not isinstance(repository, str)
        or re.search(documents.REPOSITORY_PATTERN, repository) is None
    ):
        raise _fail(prefix, "repository must be written owner/name")
    mode = _parse_enum(
        _required(data, "mode", prefix), ("online", "offline"), prefix, "mode", strict
    )
    started_at = data.get("process_started_at", None if not strict else "missing")
    if strict and "process_started_at" not in data:
        raise _fail(prefix, "is missing required property: 'process_started_at'")
    if mode == "offline":
        if started_at is not None:
            raise _fail(prefix, "process_started_at must be null offline")
    elif mode == "online":
        if not isinstance(started_at, str):
            raise _fail(prefix, "process_started_at must be a string online")
        _parse_timestamp(started_at, prefix, "process_started_at")
    elif mode == UNKNOWN and started_at is not None and not isinstance(started_at, str):
        raise _fail(prefix, "process_started_at must be a string or null")
    capabilities_raw = _required(data, "capabilities", prefix)
    if not isinstance(capabilities_raw, list) or any(
        not isinstance(item, str) or not item for item in capabilities_raw
    ):
        raise _fail(prefix, "capabilities must be an array of non-empty strings")
    return Identity(
        backend=backend,
        agent_version=agent_version,
        contract_major=major,
        contract_minor=minor,
        repository=repository,
        mode=mode,
        process_started_at=started_at,
        capabilities=tuple(capabilities_raw),
    )


def parse_status(doc: object, *, strict: bool) -> StatusSnapshot:
    """Parse a Status Snapshot in producer (strict) or consumer (lenient) mode."""
    prefix = "status"
    data = _require_object(doc, prefix)
    _check_unknown(
        data,
        frozenset(
            {
                "generated_at",
                "identity",
                "instance",
                "intake",
                "current_attempt",
                "last_finished",
                "recent_commands",
                "recent_events",
                "truncated",
            }
        ),
        prefix,
        strict,
    )
    generated_at = _parse_timestamp(_required(data, "generated_at", prefix), prefix, "generated_at")
    identity = _parse_identity(_required(data, "identity", prefix), f"{prefix}.identity", strict)
    instance_raw = _require_object(_required(data, "instance", prefix), f"{prefix}.instance")
    _check_unknown(
        instance_raw,
        frozenset({"state", "startup_check", "last_halt", "observed_at"}),
        f"{prefix}.instance",
        strict,
    )
    state = _parse_enum(
        _required(instance_raw, "state", f"{prefix}.instance"),
        documents.STATUS_STATES,
        f"{prefix}.instance",
        "state",
        strict,
    )
    startup_check = _required(instance_raw, "startup_check", f"{prefix}.instance")
    if state == "starting":
        if not isinstance(startup_check, str) or not startup_check:
            raise StatusError(
                "status.instance: startup_check must be a non-empty string while starting"
            )
    elif startup_check is not None and not isinstance(startup_check, str):
        raise StatusError("status.instance: startup_check must be a string or null")
    last_halt = _parse_halt(
        _required(instance_raw, "last_halt", f"{prefix}.instance"),
        f"{prefix}.instance.last_halt",
        strict,
    )
    instance_observed = _parse_timestamp(
        _required(instance_raw, "observed_at", f"{prefix}.instance"),
        f"{prefix}.instance",
        "observed_at",
    )
    instance = Instance(
        state=state,
        startup_check=startup_check,
        last_halt=last_halt,
        observed_at=instance_observed,
    )
    intake_raw = _require_object(_required(data, "intake", prefix), f"{prefix}.intake")
    _check_unknown(
        intake_raw,
        frozenset({"state", "pending_stop", "stop_plan", "priority", "observed_at"}),
        f"{prefix}.intake",
        strict,
    )
    intake_state = _parse_enum(
        _required(intake_raw, "state", f"{prefix}.intake"),
        documents.INTAKE_STATES,
        f"{prefix}.intake",
        "state",
        strict,
    )
    pending_raw = _required(intake_raw, "pending_stop", f"{prefix}.intake")
    pending_stop: CommandRef | None = None
    if pending_raw is not None:
        pending_data = _require_object(pending_raw, f"{prefix}.intake.pending_stop")
        _check_unknown(
            pending_data,
            frozenset({"request_id", "sequence"}),
            f"{prefix}.intake.pending_stop",
            strict,
        )
        pending_stop = CommandRef(
            request_id=str(_required(pending_data, "request_id", f"{prefix}.intake.pending_stop")),
            sequence=int(_required(pending_data, "sequence", f"{prefix}.intake.pending_stop")),
        )
    plan_raw = _required(intake_raw, "stop_plan", f"{prefix}.intake")
    stop_plan: StopPlan | None = None
    if plan_raw is not None:
        plan_data = _require_object(plan_raw, f"{prefix}.intake.stop_plan")
        _check_unknown(
            plan_data,
            frozenset({"request_id", "remaining", "counted"}),
            f"{prefix}.intake.stop_plan",
            strict,
        )
        stop_plan = StopPlan(
            request_id=str(_required(plan_data, "request_id", f"{prefix}.intake.stop_plan")),
            remaining=int(_required(plan_data, "remaining", f"{prefix}.intake.stop_plan")),
            counted=int(_required(plan_data, "counted", f"{prefix}.intake.stop_plan")),
        )
    priority_raw = _required(intake_raw, "priority", f"{prefix}.intake")
    priority: Priority | None = None
    if priority_raw is not None:
        priority_data = _require_object(priority_raw, f"{prefix}.intake.priority")
        _check_unknown(
            priority_data,
            frozenset({"request_id", "issue"}),
            f"{prefix}.intake.priority",
            strict,
        )
        priority = Priority(
            request_id=str(_required(priority_data, "request_id", f"{prefix}.intake.priority")),
            issue=int(_required(priority_data, "issue", f"{prefix}.intake.priority")),
        )
    intake_observed = _parse_timestamp(
        _required(intake_raw, "observed_at", f"{prefix}.intake"),
        f"{prefix}.intake",
        "observed_at",
    )
    intake = Intake(
        state=intake_state,
        pending_stop=pending_stop,
        stop_plan=stop_plan,
        priority=priority,
        observed_at=intake_observed,
    )
    current_attempt = _parse_current_attempt(
        _required(data, "current_attempt", prefix), f"{prefix}.current_attempt", strict
    )
    last_finished = _parse_attempt_summary(
        _required(data, "last_finished", prefix), f"{prefix}.last_finished", strict
    )
    commands_raw = _required(data, "recent_commands", prefix)
    if not isinstance(commands_raw, list):
        raise _fail(prefix, "recent_commands must be an array")
    if len(commands_raw) > 20:
        raise _fail(prefix, "recent_commands must contain at most 20 entries")
    commands = tuple(
        _parse_command_record(entry, f"{prefix}.recent_commands[{index}]", strict)
        for index, entry in enumerate(commands_raw)
    )
    events_raw = _required(data, "recent_events", prefix)
    if not isinstance(events_raw, list):
        raise _fail(prefix, "recent_events must be an array")
    if len(events_raw) > 50:
        raise _fail(prefix, "recent_events must contain at most 50 entries")
    events = tuple(
        _parse_contract_event(entry, f"{prefix}.recent_events[{index}]", strict)
        for index, entry in enumerate(events_raw)
    )
    truncated = _required(data, "truncated", prefix)
    if type(truncated) is not bool:
        raise _fail(prefix, "truncated must be a boolean")
    return StatusSnapshot(
        generated_at=generated_at,
        identity=identity,
        instance=instance,
        intake=intake,
        current_attempt=current_attempt,
        last_finished=last_finished,
        recent_commands=commands,
        recent_events=events,
        truncated=truncated,
    )


def parse_status_strict(doc: object) -> StatusSnapshot:
    """Parse a snapshot in producer mode: unknown fields and enums fail."""
    return parse_status(doc, strict=True)


def parse_status_lenient(doc: object) -> StatusSnapshot:
    """Parse a snapshot in consumer mode: unknowns become ``\"unknown\"``."""
    return parse_status(doc, strict=False)
