"""Operator contract, major version 1: shared library and client.

See docs/spec.md for the normative specification.
"""

from importlib.metadata import version

__all__ = ["CONTRACT_MAJOR", "__version__"]

__version__ = version("agent-operator-contract")

CONTRACT_MAJOR = 1
"""The contract major this library speaks (spec §5)."""
