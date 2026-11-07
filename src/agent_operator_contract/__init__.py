"""Operator contract, major version 1: shared library and client.

See docs/spec.md for the normative specification.
"""

from typing import Any

__all__ = ["CONTRACT_MAJOR", "__version__"]


def __getattr__(name: str) -> Any:
    """Resolve ``__version__`` lazily so importing this package stays cheap.

    ``importlib.metadata.version`` scans the installed distributions (about
    half a second on a slow filesystem), and every ``agentctl``/``agentfleet``
    spawn imports this package. Deferring the scan to the first actual use —
    the ``agentfleet --version`` action — keeps operator commands fast.
    """
    if name == "__version__":
        from importlib.metadata import version

        return version("agent-operator-contract")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


CONTRACT_MAJOR = 1
"""The contract major this library speaks (spec §5)."""
