"""``agentfleet``: the stateless host-side client over the library's client part.

Placeholder entry point; the operations listed in docs/adoption-plan.md
(step 1) are not implemented yet.
"""

import argparse
from collections.abc import Sequence

from agent_operator_contract import CONTRACT_MAJOR, __version__


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="agentfleet",
        description="Manage a Fleet of Managed Instances through the operator contract.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__} (contract major {CONTRACT_MAJOR})",
    )
    parser.parse_args(argv)
    parser.print_help()
    return 0
