#!/usr/bin/env python3
"""Regenerate the error.code enumerations in the envelope schemas.

The spec section 4 table (src/agent_operator_contract/error-table.json) is the
single source of truth for producer error codes, and client-table.json is the
single source of truth for agentfleet client codes. A JSON Schema file is
static and cannot import from them, so this script copies each table's keys
into the matching schema's error.code enum. Run it after editing either
table; the test suite and CI fail on a drifted schema.

Usage: python scripts/generate_schemas.py [--check]
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
PACKAGE = REPO_ROOT / "src" / "agent_operator_contract"
ERROR_TABLE = PACKAGE / "error-table.json"
ENVELOPE_SCHEMA = PACKAGE / "schemas" / "envelope.schema.json"
CLIENT_TABLE = PACKAGE / "client-table.json"
CLIENT_ENVELOPE_SCHEMA = PACKAGE / "schemas" / "client-envelope.schema.json"


def render_envelope_schema(table: dict[str, object]) -> str:
    """Return the envelope schema with the error.code enum set to the table's keys."""
    schema = json.loads(ENVELOPE_SCHEMA.read_text(encoding="utf-8"))
    schema["properties"]["error"]["properties"]["code"]["enum"] = list(table)
    return json.dumps(schema, indent=2) + "\n"


def render_client_envelope_schema(table: dict[str, object]) -> str:
    """Return the client envelope schema with the error.code enum set to the table's keys."""
    schema = json.loads(CLIENT_ENVELOPE_SCHEMA.read_text(encoding="utf-8"))
    schema["properties"]["error"]["properties"]["code"]["enum"] = list(table)
    return json.dumps(schema, indent=2) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 with a diff when a schema is drifted instead of rewriting it",
    )
    args = parser.parse_args(argv)
    pairs = (
        (ERROR_TABLE, ENVELOPE_SCHEMA, render_envelope_schema),
        (CLIENT_TABLE, CLIENT_ENVELOPE_SCHEMA, render_client_envelope_schema),
    )
    failed = False
    for table_path, schema_path, render in pairs:
        table = json.loads(table_path.read_text(encoding="utf-8"))
        rendered = render(table)
        current = schema_path.read_text(encoding="utf-8")
        if rendered == current:
            continue
        failed = True
        if args.check:
            sys.stdout.writelines(
                difflib.unified_diff(
                    current.splitlines(keepends=True),
                    rendered.splitlines(keepends=True),
                    fromfile=str(schema_path),
                    tofile="generated",
                )
            )
        else:
            schema_path.write_text(rendered, encoding="utf-8")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
