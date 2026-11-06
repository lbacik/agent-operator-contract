#!/usr/bin/env python3
"""Regenerate the error.code enumeration in schemas/envelope.schema.json.

The spec section 4 table (src/agent_operator_contract/error-table.json) is the
single source of truth: each error.code with its fixed exit code and retryable
flag. A JSON Schema file is static and cannot import from it, so this script
copies the table's keys into the schema's error.code enum. Run it after editing
error-table.json; the test suite (test_envelope_schema_matches_generated_output)
and CI fail on a drifted schema.

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


def render_envelope_schema(table: dict[str, object]) -> str:
    """Return the envelope schema with the error.code enum set to the table's keys."""
    schema = json.loads(ENVELOPE_SCHEMA.read_text(encoding="utf-8"))
    schema["properties"]["error"]["properties"]["code"]["enum"] = list(table)
    return json.dumps(schema, indent=2) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 with a diff when the schema is drifted instead of rewriting it",
    )
    args = parser.parse_args(argv)
    table = json.loads(ERROR_TABLE.read_text(encoding="utf-8"))
    rendered = render_envelope_schema(table)
    current = ENVELOPE_SCHEMA.read_text(encoding="utf-8")
    if rendered == current:
        return 0
    if args.check:
        sys.stdout.writelines(
            difflib.unified_diff(
                current.splitlines(keepends=True),
                rendered.splitlines(keepends=True),
                fromfile=str(ENVELOPE_SCHEMA),
                tofile="generated",
            )
        )
        return 1
    ENVELOPE_SCHEMA.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
