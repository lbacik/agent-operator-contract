"""``agentfleet``: the stateless host-side client over the library's client part.

``agentfleet <operation> <instance> [arguments]`` manages a Fleet of Managed
Instances through the operator contract. ``ls`` lists the Fleet from discovery
labels and container state alone (no ``exec``) and flags duplicate instance
names rather than hiding them. ``identity``, ``status`` and ``command`` relay
the producer reply: human output by default, ``--json`` prints the producer's
envelope verbatim. ``stop`` and ``resume`` submit mutations: the request ID is
generated before submission from a cryptographically secure source (or taken
from ``--request-id``), ``--expect-repo`` is always supplied by the caller and
never derived from the instance's identity, and an ``identity`` read precedes
every mutation to check the contract major and the required capability before
anything is sent. The client's own failures (unknown instance, duplicate
instance, Docker unavailable, malformed producer stdout, usage, local contract
or capability refusal) print an envelope-shaped document with an
``error.code`` in the ``client.`` namespace and exit with the matching class
from spec section 4 (see ``client-table.json``).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any, NoReturn

from agent_operator_contract import CONTRACT_MAJOR, __version__, client, shared

LS_VERSION = 1


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
        result = client.invoke(docker, instance, ["identity"])
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
        result = client.invoke(docker, instance, ["status"])
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


def _run_mutation(
    docker: str,
    name: str,
    operation: str,
    expect_repo: str,
    request_id: str | None,
    use_json: bool,
) -> int:
    """Submit ``stop`` or ``resume`` after the identity precheck (spec section 5).

    An ``identity`` read precedes every mutation: when it fails, or when the
    reported contract major or capabilities forbid the mutation, nothing is
    sent. ``--expect-repo`` always comes from the caller, never from the
    instance's own identity. The request ID of every mutation is printed, so an
    ambiguous outcome can be followed up with ``command <request-id>``.
    """
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
        client.ensure_mutation_allowed(identity_doc, shared.required_capability(operation, {}))
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    sent_id = request_id if request_id is not None else client.new_request_id()
    try:
        mutation = client.invoke(
            docker,
            instance,
            [operation, "--request-id", sent_id, "--expect-repo", expect_repo],
            timeout=client.MUTATION_TIMEOUT,
        )
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
    try:
        envelope = client.interpret(mutation)
    except client.ClientError as exc:
        return _print_client_error(exc, use_json)
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
        result = client.invoke(docker, instance, ["command", request_id])
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
    command_parser = sub.add_parser(
        "command", help="read one committed Operator Command by request ID"
    )
    command_parser.add_argument("instance", help="instance name from the discovery labels")
    command_parser.add_argument("request_id", help="durable request ID of the command")
    command_parser.add_argument("--json", dest="json", action="store_true")
    command_parser.add_argument("--docker", dest="docker", default=None)
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
        return _run_mutation(
            docker,
            str(args.instance),
            str(args.operation),
            str(args.expect_repo),
            args.request_id if isinstance(args.request_id, str) else None,
            use_json,
        )
    if args.operation == "command":
        return _run_command(docker, str(args.instance), str(args.request_id), use_json)
    doc = client.ClientError("client.usage", f"unknown operation {args.operation!r}").document()
    sys.stdout.write(json.dumps(doc, sort_keys=True) + "\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
