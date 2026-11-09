"""One version line: the distribution version is the contract version.

The tag ``vX.Y.Z`` is the contract version and ``X`` its major (README,
Versioning). The library, ``agentfleet`` and the suites ship in one
distribution, so its version must name the contract major and minor the
library renders into every envelope, and must be a final release.
"""

import re
import tomllib
from pathlib import Path

from agent_operator_contract import CONTRACT_MAJOR, __version__, shared

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _project_version() -> str:
    with PYPROJECT.open("rb") as handle:
        version: str = tomllib.load(handle)["project"]["version"]
    return version


def test_distribution_version_is_a_final_release() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", _project_version())


def test_installed_version_matches_pyproject() -> None:
    assert __version__ == _project_version()


def test_distribution_version_names_the_contract_version() -> None:
    major, minor, _ = _project_version().split(".")

    assert int(major) == CONTRACT_MAJOR
    assert f"{major}.{minor}" == shared.CONTRACT_VERSION
