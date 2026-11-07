"""``agentfleet``: the stateless host-side client over the library's client part.

``agentfleet <operation> <instance> [arguments]`` manages a Fleet of Managed
Instances through the operator contract. ``ls`` lists the Fleet from discovery
labels and container state alone (no ``exec``) and flags duplicate instance
names rather than hiding them. ``identity``, ``status`` and ``command`` relay
the producer reply: human output by default, ``--json`` prints the producer's
envelope verbatim. ``stop``, ``resume``, ``stop-after`` and ``next-issue``
submit mutations: the request ID is
generated before submission from a cryptographically secure source (or taken
from ``--request-id``), ``--expect-repo`` is always supplied by the caller and
never derived from the instance's identity, and an ``identity`` read precedes
every mutation to check the contract major and the required capability before
anything is sent. Reads of a stopped instance go through Offline Read
automatically (``docker run`` of the stopped container's image with every
volume and bind mount re-mounted read-only and no network, spec section 3.2);
mutations on a stopped instance are refused client-side and never sent. The
client's own failures (unknown instance, duplicate
instance, Docker unavailable, malformed producer stdout, usage, local contract
or capability refusal) print an envelope-shaped document with an
``error.code`` in the ``client.`` namespace and exit with the matching class
from spec section 4 (see ``client-table.json``).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from typing import Any, NoReturn

from agent_operator_contract import CONTRACT_MAJOR, __version__, client, shared

LS_VERSION = 1

MUTATION_TIMEOUT_ENV_VAR = "AGENTFLEET_MUTATION_TIMEOUT"
MAX_ATTEMPTS_ENV_VAR = "AGENTFLEET_MAX_ATTEMPTS"


def _env_number(name: str) -> float | None:
    """Read a number from the environment, or ``None`` when missing or unparsable."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    try:
        return float(raw.strip())
    except ValueError:
        return None


def _mutation_timeout(override: float | None) -> float | None:
    """Resolve the mutation timeout: CLI flag, env var, then the §3.3 default."""
    if override is not None:
        return override
    value = _env_number(MUTATION_TIMEOUT_ENV_VAR)
    if value is not None and value > 0:
        return value
    return None


def _mutation_max_attempts(override: int | None) -> int | None:
    """Resolve the bounded retry count: CLI flag, env var, then the default."""
    if override is not None:
        return override
    value = _env_number(MAX_ATTEMPTS_ENV_VAR)
    if value is not None and value >= 1 and value.is_integer():
        return int(value)
    return None


def _client_error_document(error: client.ClientError) -> dict[str, object]:
    return error.document()


def _print_client_error(
    error: client.ClientError, use_json: bool, details: dict[str, object] | None = None
) -> int:
    doc = _client_error_document(error)
    if details is not None:
        error_doc = doc["error"]
        assert isinstance(error_doc, dict)
        error_doc["details"] = details
    sys.stdout.write(json.dumps(doc, sort_keys=True) + "\n")
    sys.stdout.flush()
    if not use_json:
        sys.stderr.write(f"agentfleet: {error.message}\n")
    return error.exit_code


def _ls_document(fleet: client.Fleet) -> dict[str, object]:
    return {
        "version": LS_VERSION,
        "instances": [
            {
                "name": item.name,
                "backend": item.backend,
                "contract": item.contract,
                "state": item.state,
                "duplicate": item.name in fleet.duplicates,
            }
            for item in fleet.instances
        ],
    }


def _run_ls(docker: str, use_json: bool) -> int:
    try:
        fleet = client.discover(docker)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    doc = _ls_document(fleet)
    if use_json:
        sys.stdout.write(json.dumps(doc, sort_keys=True, indent=2) + "\n")
        return 0
    sys.stdout.write("NAME BACKEND CONTRACT STATE\n")
    for item in fleet.instances:
        marker = " DUPLICATE" if item.name in fleet.duplicates else ""
        sys.stdout.write(f"{item.name} {item.backend} {item.contract} {item.state}{marker}\n")
    return 0


def _human_identity(identity: object) -> str:
    if not isinstance(identity, dict):
        return "identity: <unreadable>\n"
    contract: Any = identity.get("contract")
    if isinstance(contract, dict):
        contract_text = f"{contract.get('major', '')}.{contract.get('minor', '')}"
    else:
        contract_text = ""
    capabilities: Any = identity.get("capabilities")
    if isinstance(capabilities, list) and all(isinstance(item, str) for item in capabilities):
        capabilities_text = ", ".join(capabilities)
    else:
        capabilities_text = ""
    lines = [
        f"backend: {identity.get('backend', '')}",
        f"agent_version: {identity.get('agent_version', '')}",
        f"contract: {contract_text}",
        f"repository: {identity.get('repository', '')}",
        f"mode: {identity.get('mode', '')}",
        f"process_started_at: {identity.get('process_started_at', '')}",
        f"capabilities: {capabilities_text}",
    ]
    return "\n".join(lines) + "\n"


def _run_read(
    docker: str,
    instance: client.Instance,
    agentctl_argv: list[str],
    timeout: float | None = None,
) -> client.ExecResult:
    """Run one read: online ``exec`` when running, Offline Read ``run`` when stopped.

    A stopped instance is re-inspected for its Offline Read target; when the
    container runs after all the read is refused. Raises
    :class:`client.ClientError` when the target cannot be built or the
    transport fails.
    """
    if instance.state == "running":
        if timeout is None:
            return client.invoke(docker, instance, agentctl_argv)
        return client.invoke(docker, instance, agentctl_argv, timeout=timeout)
    target = client.inspect_offline_target(docker, instance)
    if timeout is None:
        return client.invoke_offline(docker, target, agentctl_argv)
    return client.invoke_offline(docker, target, agentctl_argv, timeout=timeout)


def _run_identity(docker: str, name: str, use_json: bool) -> int:
    try:
        fleet = client.discover(docker)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        instance = client.resolve(fleet, name)
    except client.UnknownInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    except client.DuplicateInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    try:
        result = _run_read(docker, instance, ["identity"])
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        envelope = client.interpret(result)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    if use_json:
        sys.stdout.buffer.write(result.stdout)
        sys.stdout.buffer.flush()
        return result.exit_code
    if envelope["ok"] is True:
        result_doc = envelope["result"]
        assert isinstance(result_doc, dict)
        sys.stdout.write(_human_identity(result_doc))
        return 0
    error = envelope["error"]
    assert isinstance(error, dict)
    sys.stderr.write(f"agentfleet: {error.get('code')}: {error.get('message')}\n")
    return result.exit_code


_KNOWN_FIGURE_UNITS = ("tokens", "usd", "seconds", "count", "ratio")
_KNOWN_FIGURE_BASES = (
    "measured",
    "estimated",
    "configured",
    "assumed",
    "unavailable",
    "not_applicable",
)


def _display_enum(value: object, known: tuple[str, ...]) -> str:
    if isinstance(value, str) and value in known:
        return value
    return "unknown"


def _format_figure_value(figure: object) -> str:
    if not isinstance(figure, dict):
        return "unknown"
    value = figure.get("value")
    unit = _display_enum(figure.get("unit"), _KNOWN_FIGURE_UNITS)
    basis = _display_enum(figure.get("basis"), _KNOWN_FIGURE_BASES)
    if value is None or isinstance(value, bool):
        text = f"unknown {unit} ({basis})"
    else:
        text = f"{value} {unit} ({basis})"
    reason = figure.get("reason")
    if isinstance(reason, str) and reason:
        text += f" - {reason}"
    return text


def _human_status(snapshot: object) -> str:
    if not isinstance(snapshot, dict):
        return "status: <unreadable>\n"
    lines: list[str] = []
    lines.append(f"generated_at: {snapshot.get('generated_at', '')}")
    identity = snapshot.get("identity")
    if isinstance(identity, dict):
        lines.append(f"repository: {identity.get('repository', '')}")
        lines.append(f"mode: {identity.get('mode', '')}")
    instance = snapshot.get("instance")
    if isinstance(instance, dict):
        lines.append(
            f"instance: {instance.get('state', '')} (observed {instance.get('observed_at', '')})"
        )
        halt = instance.get("last_halt")
        if isinstance(halt, dict):
            lines.append(
                f"last_halt: {halt.get('reason', '')} "
                f"at {halt.get('at', '')} - {halt.get('detail', '')}"
            )
    intake = snapshot.get("intake")
    if isinstance(intake, dict):
        lines.append(
            f"intake: {intake.get('state', '')} (observed {intake.get('observed_at', '')})"
        )
    current = snapshot.get("current_attempt")
    if current is None:
        lines.append("current_attempt: none")
    elif isinstance(current, dict):
        issue = current.get("issue")
        issue_text = ""
        if isinstance(issue, dict):
            issue_text = f" issue {issue.get('number', '')}"
        lines.append(
            f"current_attempt: {current.get('attempt_id', '')}{issue_text} "
            f"{current.get('native_phase', '')} "
            f"(observed {current.get('observed_at', '')})"
        )
        budget = current.get("budget")
        if isinstance(budget, dict):
            raw = _format_figure_value(budget.get("raw_tokens"))
            effective = _format_figure_value(budget.get("effective_tokens"))
            enforced = budget.get("enforced_tokens", "")
            if enforced not in ("raw", "effective", "none"):
                enforced = "unknown"
            lines.append(f"budget: raw {raw}, effective {effective}, enforced: {enforced}")
            limits = budget.get("limits")
            if isinstance(limits, list) and limits:
                lines.append("limits:")
                for entry in limits:
                    if not isinstance(entry, dict):
                        continue
                    name = entry.get("name", "")
                    if not isinstance(name, str):
                        name = "unknown"
                    used = _format_figure_value(entry.get("used"))
                    limit = _format_figure_value(entry.get("limit"))
                    lines.append(f"  {name}: used {used} / limit {limit}")
        figures = current.get("figures")
        if isinstance(figures, dict) and figures:
            lines.append("figures:")
            for name in sorted(figures):
                lines.append(f"  {name}: {_format_figure_value(figures[name])}")
    last_finished = snapshot.get("last_finished")
    if isinstance(last_finished, dict):
        lines.append(
            f"last_finished: {last_finished.get('attempt_id', '')} "
            f"sealed={last_finished.get('sealed', '')}"
        )
    lines.append(f"truncated: {snapshot.get('truncated', '')}")
    return "\n".join(lines) + "\n"


def _run_status(docker: str, name: str, use_json: bool) -> int:
    try:
        fleet = client.discover(docker)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        instance = client.resolve(fleet, name)
    except client.UnknownInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    except client.DuplicateInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    try:
        result = _run_read(docker, instance, ["status"])
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        envelope = client.interpret(result)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    if use_json:
        sys.stdout.buffer.write(result.stdout)
        sys.stdout.buffer.flush()
        return result.exit_code
    if envelope["ok"] is True:
        result_doc = envelope["result"]
        assert isinstance(result_doc, dict)
        sys.stdout.write(_human_status(result_doc))
        return 0
    error = envelope["error"]
    assert isinstance(error, dict)
    sys.stderr.write(f"agentfleet: {error.get('code')}: {error.get('message')}\n")
    return result.exit_code


def _print_producer_failure(
    envelope: dict[str, object], exit_code: int, use_json: bool, stdout: bytes
) -> int:
    """Relay a producer ``ok:false`` envelope: verbatim as JSON, terse otherwise."""
    if use_json:
        sys.stdout.buffer.write(stdout)
        sys.stdout.buffer.flush()
        return exit_code
    error = envelope["error"]
    assert isinstance(error, dict)
    sys.stderr.write(f"agentfleet: {error.get('code')}: {error.get('message')}\n")
    return exit_code


def _print_ambiguous(error: client.AmbiguousMutationError, name: str, use_json: bool) -> int:
    """Report an ambiguous mutation that stayed ambiguous after the retries.

    The request ID is always printed so the operator can follow up with
    ``agentfleet command <instance> <request-id>`` instead of resubmitting.
    """
    if use_json and error.envelope is not None:
        sys.stdout.buffer.write(json.dumps(error.envelope, sort_keys=True).encode("utf-8"))
        sys.stdout.buffer.write(b"\n")
        sys.stdout.buffer.flush()
        sys.stderr.write(
            f"agentfleet: ambiguous outcome after {error.attempts} attempts; "
            f"request_id {error.request_id}; follow up with "
            f"'agentfleet command {name} {error.request_id}' instead of resubmitting\n"
        )
        return error.exit_code
    doc = client.ExecFailedError(str(error)).document(
        details={"request_id": error.request_id, "attempts": error.attempts}
    )
    sys.stdout.write(json.dumps(doc, sort_keys=True) + "\n")
    sys.stdout.write(f"request_id: {error.request_id}\n")
    sys.stdout.flush()
    sys.stderr.write(
        f"agentfleet: ambiguous outcome after {error.attempts} attempts; "
        f"request_id {error.request_id}; follow up with "
        f"'agentfleet command {name} {error.request_id}' instead of resubmitting\n"
    )
    return error.exit_code


def _run_mutation(
    docker: str,
    name: str,
    operation: str,
    expect_repo: str,
    request_id: str | None,
    use_json: bool,
    timeout: float | None = None,
    max_attempts: int | None = None,
    capability: str | None = None,
    argv_prefix: Sequence[str] | None = None,
) -> int:
    """Submit a mutation after the identity precheck (spec section 5).

    An ``identity`` read precedes every mutation: when it fails, or when the
    reported contract major or capabilities forbid the mutation, nothing is
    sent. ``--expect-repo`` always comes from the caller, never from the
    instance's own identity. Ambiguous outcomes (§8.4) are retried inside this
    call with the same request ID and payload; the request ID of every mutation
    is printed, so an ambiguous outcome can be followed up with
    ``command <request-id>``.

    ``operation`` names the mutation for display and error details;
    ``argv_prefix`` is the agentctl argv before ``--request-id`` (defaults to
    ``[operation]``); ``capability`` is the required optional capability, or
    ``None`` when the caller leaves the default derived from ``operation``.
    """

    if argv_prefix is None:
        prefix = [operation]
        effective_capability = (
            capability if capability is not None else shared.required_capability(operation, {})
        )
    else:
        prefix = list(argv_prefix)
        effective_capability = capability
    try:
        fleet = client.discover(docker)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        instance = client.resolve(fleet, name)
    except client.UnknownInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    except client.DuplicateInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    if instance.state != "running":
        return _print_client_error(
            client.InstanceStoppedError(name), use_json, {"instance": name}
        )
    try:
        identity_result = client.invoke(docker, instance, ["identity"])
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        identity_envelope = client.interpret(identity_result)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    if identity_envelope["ok"] is not True:
        return _print_producer_failure(
            identity_envelope, identity_result.exit_code, use_json, identity_result.stdout
        )
    identity_doc = identity_envelope["result"]
    assert isinstance(identity_doc, dict)
    try:
        client.ensure_mutation_allowed(identity_doc, effective_capability)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    sent_id = request_id if request_id is not None else client.new_request_id()
    argv = [*prefix, "--request-id", sent_id, "--expect-repo", expect_repo]
    effective_timeout = _mutation_timeout(timeout)
    effective_attempts = _mutation_max_attempts(max_attempts)
    try:
        mutation, envelope = client.invoke_mutation_with_retry(
            docker,
            instance,
            argv,
            sent_id,
            timeout=effective_timeout,
            max_attempts=effective_attempts,
        )
    except client.AmbiguousMutationError as exc:
        return _print_ambiguous(exc, name, use_json)
    except client.ClientError as exc:
        if not use_json:
            sys.stdout.write(f"request_id: {sent_id}\n")
            sys.stdout.flush()
        return _print_client_error(exc, use_json, {"request_id": sent_id, "operation": operation})
    if use_json:
        sys.stdout.buffer.write(mutation.stdout)
        sys.stdout.buffer.flush()
        return mutation.exit_code
    if envelope["ok"] is True:
        result_doc = envelope["result"]
        assert isinstance(result_doc, dict)
        command = result_doc.get("command")
        assert isinstance(command, dict)
        sys.stdout.write(f"request_id: {command.get('request_id', sent_id)}\n")
        sys.stdout.write(f"acknowledgement: {command.get('acknowledgement', '')}\n")
        sys.stdout.write(f"replayed: {result_doc.get('replayed', '')}\n")
        return 0
    error = envelope["error"]
    assert isinstance(error, dict)
    shown = error.get("request_id", sent_id)
    sys.stdout.write(f"request_id: {shown}\n")
    sys.stdout.flush()
    sys.stderr.write(f"agentfleet: {error.get('code')}: {error.get('message')}\n")
    return mutation.exit_code


def _human_command(command: object) -> str:
    if not isinstance(command, dict):
        return "command: <unreadable>\n"
    lines = [
        f"request_id: {command.get('request_id', '')}",
        f"sequence: {command.get('sequence', '')}",
        f"kind: {command.get('kind', '')}",
        f"acknowledgement: {command.get('acknowledgement', '')}",
        f"repository: {command.get('repository', '')}",
        f"detail: {command.get('detail', '')}",
        f"created_at: {command.get('created_at', '')}",
        f"updated_at: {command.get('updated_at', '')}",
    ]
    return "\n".join(lines) + "\n"


def _sealed_label(sealed: object) -> str:
    return "Sealed" if sealed is True else "not Sealed"


def _human_attempts_list(result: object) -> str:
    if not isinstance(result, dict):
        return "attempts: <unreadable>\n"
    lines: list[str] = []
    attempts = result.get("attempts")
    if isinstance(attempts, list):
        for entry in attempts:
            if not isinstance(entry, dict):
                continue
            attempt_id = entry.get("attempt_id", "")
            issue = entry.get("issue")
            issue_text = ""
            if isinstance(issue, dict):
                issue_text = f" issue #{issue.get('number', '')}"
            lines.append(
                f"{attempt_id}{issue_text} "
                f"{entry.get('lifecycle_state', '')} {_sealed_label(entry.get('sealed'))}"
            )
    next_cursor = result.get("next_cursor")
    if next_cursor is None:
        lines.append("next_cursor: none")
    elif isinstance(next_cursor, str):
        lines.append(f"next_cursor: {next_cursor}")
    return "\n".join(lines) + "\n" if lines else "attempts: none\n"


def _human_attempts_show(result: object) -> str:
    if not isinstance(result, dict):
        return "attempt: <unreadable>\n"
    attempt = result.get("attempt")
    lines: list[str] = []
    if isinstance(attempt, dict):
        lines.append(f"attempt_id: {attempt.get('attempt_id', '')}")
        issue = attempt.get("issue")
        if isinstance(issue, dict):
            lines.append(f"issue: #{issue.get('number', '')} {issue.get('title', '')}")
        lines.append(f"lifecycle_state: {attempt.get('lifecycle_state', '')}")
        sealed = attempt.get("sealed")
        lines.append(f"archive: {_sealed_label(sealed)}")
        lines.append(
            f"sealed: {sealed!s}".lower() if isinstance(sealed, bool) else f"sealed: {sealed}"
        )
        lines.append(f"sealed_at: {attempt.get('sealed_at', '')}")
        outcome = attempt.get("outcome")
        if outcome is None:
            lines.append("outcome: none")
        elif isinstance(outcome, dict):
            lines.append(f"outcome: {outcome.get('native', '')} ({outcome.get('label', '')})")
    manifest = result.get("manifest")
    if isinstance(manifest, list):
        lines.append(f"manifest: {len(manifest)} file(s)")
        for entry in manifest:
            if isinstance(entry, dict):
                lines.append(
                    f"  {entry.get('path', '')} {entry.get('kind', '')} "
                    f"{entry.get('size', '')} {entry.get('sha256', '')}"
                )
    return "\n".join(lines) + "\n"


def _run_attempts_export(
    docker: str,
    name: str,
    attempt_id: str,
    paths: Sequence[str] | None,
    output: str | None,
    timeout: float | None,
) -> int:
    """Stream one ``attempts export`` tar to a file or stdout (spec section 11.3).

    The verified tar bytes are written only after the client's size and
    ``sha256`` verification passes, so a torn read never leaves a partial
    file behind. A producer envelope answered before streaming starts (for
    example ``not_found``) is relayed with its own exit code; a torn export
    that stays torn after the bounded refetch exits 7.
    """
    try:
        fleet = client.discover(docker)
    except client.ClientError as exc:
        return _print_client_error(exc, False)
    try:
        instance = client.resolve(fleet, name)
    except client.UnknownInstanceError as exc:
        return _print_client_error(exc, False, {"instance": name})
    except client.DuplicateInstanceError as exc:
        return _print_client_error(exc, False, {"instance": name})
    delivered: list[bytes] = []
    try:
        if instance.state == "running":
            client.export_attempt(
                docker,
                instance,
                attempt_id,
                paths=paths,
                sink=delivered.append,
                timeout=timeout,
            )
        else:
            target = client.inspect_offline_target(docker, instance)
            client.export_attempt_offline(
                docker,
                target,
                attempt_id,
                paths=paths,
                sink=delivered.append,
                timeout=timeout,
            )
    except client.ExportProducerError as exc:
        return _print_producer_failure(exc.envelope, exc.exit_code, False, b"")
    except client.ClientError as exc:
        return _print_client_error(exc, False, {"attempt_id": attempt_id})
    assert len(delivered) == 1
    payload = delivered[0]
    if output is None:
        sys.stdout.flush()
        sys.stdout.buffer.write(payload)
        sys.stdout.buffer.flush()
        return 0
    try:
        with open(output, "wb") as handle:
            handle.write(payload)
    except OSError as exc:
        error = client.ClientError("client.exec_failed", f"cannot write {output!r}: {exc}")
        return _print_client_error(error, False, {"attempt_id": attempt_id})
    return 0


def _run_command(docker: str, name: str, request_id: str, use_json: bool) -> int:
    """Relay one ``command <request-id>`` read (spec section 6.1)."""
    try:
        fleet = client.discover(docker)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        instance = client.resolve(fleet, name)
    except client.UnknownInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    except client.DuplicateInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    try:
        result = _run_read(docker, instance, ["command", request_id])
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        envelope = client.interpret(result)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    if use_json:
        sys.stdout.buffer.write(result.stdout)
        sys.stdout.buffer.flush()
        return result.exit_code
    if envelope["ok"] is True:
        result_doc = envelope["result"]
        assert isinstance(result_doc, dict)
        sys.stdout.write(_human_command(result_doc.get("command")))
        return 0
    error = envelope["error"]
    assert isinstance(error, dict)
    sys.stderr.write(f"agentfleet: {error.get('code')}: {error.get('message')}\n")
    return result.exit_code


def _run_attempts_list(
    docker: str,
    name: str,
    use_json: bool,
    limit: str | None = None,
    before: str | None = None,
) -> int:
    """Relay ``attempts list`` (spec section 6.1)."""
    try:
        fleet = client.discover(docker)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        instance = client.resolve(fleet, name)
    except client.UnknownInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    except client.DuplicateInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    argv = ["attempts", "list"]
    if limit is not None:
        argv += ["--limit", limit]
    if before is not None:
        argv += ["--before", before]
    try:
        result = _run_read(
            docker, instance, argv, timeout=client.timeout_for("attempts-list")
        )
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        envelope = client.interpret(result)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    if use_json:
        sys.stdout.buffer.write(result.stdout)
        sys.stdout.buffer.flush()
        return result.exit_code
    if envelope["ok"] is True:
        result_doc = envelope["result"]
        assert isinstance(result_doc, dict)
        sys.stdout.write(_human_attempts_list(result_doc))
        return 0
    error = envelope["error"]
    assert isinstance(error, dict)
    sys.stderr.write(f"agentfleet: {error.get('code')}: {error.get('message')}\n")
    return result.exit_code


def _run_attempts_show(docker: str, name: str, attempt_id: str, use_json: bool) -> int:
    """Relay ``attempts show`` (spec section 6.1)."""
    try:
        fleet = client.discover(docker)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        instance = client.resolve(fleet, name)
    except client.UnknownInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    except client.DuplicateInstanceError as exc:
        return _print_client_error(exc, use_json, {"instance": name})
    try:
        result = _run_read(
            docker,
            instance,
            ["attempts", "show", attempt_id],
            timeout=client.timeout_for("attempts-show"),
        )
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        envelope = client.interpret(result)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    if use_json:
        sys.stdout.buffer.write(result.stdout)
        sys.stdout.buffer.flush()
        return result.exit_code
    if envelope["ok"] is True:
        result_doc = envelope["result"]
        assert isinstance(result_doc, dict)
        sys.stdout.write(_human_attempts_show(result_doc))
        return 0
    error = envelope["error"]
    assert isinstance(error, dict)
    sys.stderr.write(f"agentfleet: {error.get('code')}: {error.get('message')}\n")
    return result.exit_code


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        doc = client.ClientError("client.usage", message).document()
        sys.stdout.write(json.dumps(doc, sort_keys=True) + "\n")
        sys.stdout.flush()
        sys.stderr.write(f"agentfleet: {message}\n")
        self.exit(2)


def _build_parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="agentfleet",
        description="Manage a Fleet of Managed Instances through the operator contract.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__} (contract major {CONTRACT_MAJOR})",
    )
    parser.add_argument(
        "--json",
        dest="global_json",
        action="store_true",
        help=("print the producer envelope verbatim (identity, status) or the ls document as JSON"),
    )
    parser.add_argument(
        "--docker",
        dest="global_docker",
        default=None,
        help="docker executable (default: $AGENTFLEET_DOCKER or 'docker')",
    )
    sub = parser.add_subparsers(dest="operation", required=True, metavar="<operation>")
    ls_parser = sub.add_parser("ls", help="list Fleet members from labels and state")
    ls_parser.add_argument("--json", dest="json", action="store_true")
    ls_parser.add_argument("--docker", dest="docker", default=None)
    id_parser = sub.add_parser("identity", help="relay one instance's identity")
    id_parser.add_argument("instance", help="instance name from the discovery labels")
    id_parser.add_argument("--json", dest="json", action="store_true")
    id_parser.add_argument("--docker", dest="docker", default=None)
    status_parser = sub.add_parser("status", help="relay one instance's status snapshot")
    status_parser.add_argument("instance", help="instance name from the discovery labels")
    status_parser.add_argument("--json", dest="json", action="store_true")
    status_parser.add_argument("--docker", dest="docker", default=None)
    stop_parser = sub.add_parser("stop", help="close one instance's intake (drain)")
    stop_parser.add_argument("instance", help="instance name from the discovery labels")
    stop_parser.add_argument(
        "--expect-repo",
        required=True,
        help="Target Repository as owner/name; never taken from the instance itself",
    )
    stop_parser.add_argument(
        "--request-id",
        default=None,
        help="durable request ID (generated when omitted)",
    )
    stop_parser.add_argument("--json", dest="json", action="store_true")
    stop_parser.add_argument("--docker", dest="docker", default=None)
    stop_parser.add_argument(
        "--timeout",
        dest="timeout",
        type=float,
        default=None,
        help="consumer timeout in seconds (default: §3.3 class default, 15 s for mutations)",
    )
    stop_parser.add_argument(
        "--max-attempts",
        dest="max_attempts",
        type=int,
        default=None,
        help="bounded retries for an ambiguous mutation (default: 3 attempts)",
    )
    resume_parser = sub.add_parser("resume", help="reopen one instance's intake")
    resume_parser.add_argument("instance", help="instance name from the discovery labels")
    resume_parser.add_argument(
        "--expect-repo",
        required=True,
        help="Target Repository as owner/name; never taken from the instance itself",
    )
    resume_parser.add_argument(
        "--request-id",
        default=None,
        help="durable request ID (generated when omitted)",
    )
    resume_parser.add_argument("--json", dest="json", action="store_true")
    resume_parser.add_argument("--docker", dest="docker", default=None)
    resume_parser.add_argument(
        "--timeout",
        dest="timeout",
        type=float,
        default=None,
        help="consumer timeout in seconds (default: §3.3 class default, 15 s for mutations)",
    )
    resume_parser.add_argument(
        "--max-attempts",
        dest="max_attempts",
        type=int,
        default=None,
        help="bounded retries for an ambiguous mutation (default: 3 attempts)",
    )
    stop_after_parser = sub.add_parser(
        "stop-after", help="close one instance's intake after N finalized attempts"
    )
    stop_after_parser.add_argument("instance", help="instance name from the discovery labels")
    stop_after_parser.add_argument("after", help="finalized attempts before intake closes (1-999)")
    stop_after_parser.add_argument(
        "--expect-repo",
        required=True,
        help="Target Repository as owner/name; never taken from the instance itself",
    )
    stop_after_parser.add_argument(
        "--request-id",
        default=None,
        help="durable request ID (generated when omitted)",
    )
    stop_after_parser.add_argument("--json", dest="json", action="store_true")
    stop_after_parser.add_argument("--docker", dest="docker", default=None)
    stop_after_parser.add_argument(
        "--timeout",
        dest="timeout",
        type=float,
        default=None,
        help="consumer timeout in seconds (default: §3.3 class default, 15 s for mutations)",
    )
    stop_after_parser.add_argument(
        "--max-attempts",
        dest="max_attempts",
        type=int,
        default=None,
        help="bounded retries for an ambiguous mutation (default: 3 attempts)",
    )
    next_issue_parser = sub.add_parser(
        "next-issue", help="prioritize one issue number for the next claim"
    )
    next_issue_parser.add_argument("instance", help="instance name from the discovery labels")
    next_issue_parser.add_argument("issue", help="issue number prioritized for the next claim")
    next_issue_parser.add_argument(
        "--expect-repo",
        required=True,
        help="Target Repository as owner/name; never taken from the instance itself",
    )
    next_issue_parser.add_argument(
        "--request-id",
        default=None,
        help="durable request ID (generated when omitted)",
    )
    next_issue_parser.add_argument("--json", dest="json", action="store_true")
    next_issue_parser.add_argument("--docker", dest="docker", default=None)
    next_issue_parser.add_argument(
        "--timeout",
        dest="timeout",
        type=float,
        default=None,
        help="consumer timeout in seconds (default: §3.3 class default, 15 s for mutations)",
    )
    next_issue_parser.add_argument(
        "--max-attempts",
        dest="max_attempts",
        type=int,
        default=None,
        help="bounded retries for an ambiguous mutation (default: 3 attempts)",
    )
    command_parser = sub.add_parser(
        "command", help="read one committed Operator Command by request ID"
    )
    command_parser.add_argument("instance", help="instance name from the discovery labels")
    command_parser.add_argument("request_id", help="durable request ID of the command")
    command_parser.add_argument("--json", dest="json", action="store_true")
    command_parser.add_argument("--docker", dest="docker", default=None)
    attempts_parser = sub.add_parser(
        "attempts", help="browse Attempt Archives (list, show, export)"
    )
    attempts_sub = attempts_parser.add_subparsers(
        dest="attempts_operation", required=True, metavar="<attempts-operation>"
    )
    for alias in ("ls", "list"):
        list_parser = attempts_sub.add_parser(alias, help="list Attempt Archives, newest first")
        list_parser.add_argument("instance", help="instance name from the discovery labels")
        list_parser.add_argument("--limit", dest="limit", default=None)
        list_parser.add_argument("--before", dest="before", default=None)
        list_parser.add_argument("--json", dest="json", action="store_true")
        list_parser.add_argument("--docker", dest="docker", default=None)
    show_parser = attempts_sub.add_parser("show", help="show one Attempt Archive")
    show_parser.add_argument("instance", help="instance name from the discovery labels")
    show_parser.add_argument("attempt_id", help="attempt ID of the archive")
    show_parser.add_argument("--json", dest="json", action="store_true")
    show_parser.add_argument("--docker", dest="docker", default=None)
    export_parser = attempts_sub.add_parser(
        "export", help="stream one Attempt Archive as a tar (spec section 11.3)"
    )
    export_parser.add_argument("instance", help="instance name from the discovery labels")
    export_parser.add_argument("attempt_id", help="attempt ID of the archive")
    export_parser.add_argument(
        "--path",
        dest="paths",
        action="append",
        default=[],
        help="archive path to export (repeatable; defaults to the whole archive)",
    )
    export_parser.add_argument(
        "--output",
        dest="output",
        default=None,
        help="write the tar to FILE instead of stdout",
    )
    export_parser.add_argument(
        "--timeout",
        dest="timeout",
        type=float,
        default=None,
        help="progress timeout in seconds without bytes (default: 30 s)",
    )
    export_parser.add_argument("--docker", dest="docker", default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run ``agentfleet``; return the process exit code (spec section 4 class)."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    use_json = bool(args.global_json or getattr(args, "json", False))
    docker = client.docker_bin(args.global_docker or getattr(args, "docker", None))
    if args.operation == "ls":
        return _run_ls(docker, use_json)
    if args.operation == "identity":
        return _run_identity(docker, str(args.instance), use_json)
    if args.operation == "status":
        return _run_status(docker, str(args.instance), use_json)
    if args.operation in ("stop", "resume"):
        timeout = getattr(args, "timeout", None)
        max_attempts = getattr(args, "max_attempts", None)
        return _run_mutation(
            docker,
            str(args.instance),
            str(args.operation),
            str(args.expect_repo),
            args.request_id if isinstance(args.request_id, str) else None,
            use_json,
            timeout=float(timeout) if isinstance(timeout, (int, float)) else None,
            max_attempts=int(max_attempts) if isinstance(max_attempts, int) else None,
        )
    if args.operation == "stop-after":
        timeout = getattr(args, "timeout", None)
        max_attempts = getattr(args, "max_attempts", None)
        return _run_mutation(
            docker,
            str(args.instance),
            "stop-after",
            str(args.expect_repo),
            args.request_id if isinstance(args.request_id, str) else None,
            use_json,
            timeout=float(timeout) if isinstance(timeout, (int, float)) else None,
            max_attempts=int(max_attempts) if isinstance(max_attempts, int) else None,
            capability="stop-after",
            argv_prefix=["stop", "--after", str(args.after)],
        )
    if args.operation == "next-issue":
        timeout = getattr(args, "timeout", None)
        max_attempts = getattr(args, "max_attempts", None)
        return _run_mutation(
            docker,
            str(args.instance),
            "next-issue",
            str(args.expect_repo),
            args.request_id if isinstance(args.request_id, str) else None,
            use_json,
            timeout=float(timeout) if isinstance(timeout, (int, float)) else None,
            max_attempts=int(max_attempts) if isinstance(max_attempts, int) else None,
            capability="next-issue",
            argv_prefix=["next", "issue", str(args.issue)],
        )
    if args.operation == "command":
        return _run_command(docker, str(args.instance), str(args.request_id), use_json)
    if args.operation == "attempts":
        attempts_op = str(getattr(args, "attempts_operation", ""))
        if attempts_op in ("ls", "list"):
            limit = args.limit if isinstance(getattr(args, "limit", None), str) else None
            before = args.before if isinstance(getattr(args, "before", None), str) else None
            return _run_attempts_list(docker, str(args.instance), use_json, limit, before)
        if attempts_op == "show":
            return _run_attempts_show(docker, str(args.instance), str(args.attempt_id), use_json)
        if attempts_op == "export":
            paths = list(getattr(args, "paths", []) or [])
            output = getattr(args, "output", None)
            raw_timeout = getattr(args, "timeout", None)
            timeout = float(raw_timeout) if isinstance(raw_timeout, (int, float)) else None
            return _run_attempts_export(
                docker,
                str(args.instance),
                str(args.attempt_id),
                paths,
                str(output) if isinstance(output, str) else None,
                timeout,
            )
    doc = client.ClientError("client.usage", f"unknown operation {args.operation!r}").document()
    sys.stdout.write(json.dumps(doc, sort_keys=True) + "\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
