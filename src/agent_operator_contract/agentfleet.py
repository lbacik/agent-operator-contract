"""``agentfleet``: the stateless host-side client over the library's client part.

``agentfleet <operation> <instance> [arguments]`` manages a Fleet of Managed
Instances through the operator contract. ``ls`` lists the Fleet from discovery
labels and container state alone (no ``exec``) and flags duplicate instance
names rather than hiding them. ``identity`` relays the producer reply: human
output by default, ``--json`` prints the producer's envelope verbatim. The
client's own failures (unknown instance, duplicate instance, Docker
unavailable, malformed producer stdout, usage) print an envelope-shaped
document with an ``error.code`` in the ``client.`` namespace and exit with the
matching class from spec section 4 (see ``client-table.json``).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any, NoReturn

from agent_operator_contract import CONTRACT_MAJOR, __version__, client

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
        help="print the producer envelope verbatim (identity) or the ls document as JSON",
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
    doc = client.ClientError("client.usage", f"unknown operation {args.operation!r}").document()
    sys.stdout.write(json.dumps(doc, sort_keys=True) + "\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
