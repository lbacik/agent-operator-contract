import pytest

from agent_operator_contract import CONTRACT_MAJOR, __version__
from agent_operator_contract.agentfleet import main


def test_version_names_the_contract_major(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])

    assert exit_info.value.code == 0
    assert capsys.readouterr().out.strip() == (
        f"agentfleet {__version__} (contract major {CONTRACT_MAJOR})"
    )
