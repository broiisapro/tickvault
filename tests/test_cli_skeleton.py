import re

import pytest
from typer.testing import CliRunner

from tickvault.cli import app

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return _ANSI_RE.sub("", text)

COMMANDS = ["ingest", "replay", "qa", "compact", "sql", "reconcile", "bench"]

STUB_INVOCATIONS = [
    ["qa", "--data", "data"],
    ["compact", "--data", "data", "--date", "2026-10-01"],
    ["sql", "--data", "data"],
    ["reconcile", "--tardis-date", "2026-10-01", "--venue", "coinbase", "--symbol", "BTC-USD"],
    ["bench", "--input", "data/raw"],
]

COMMAND_OPTIONS = {
    "ingest": ["--venues", "--symbols", "--duration", "--out", "--max-gb"],
    "replay": ["--input", "--out"],
    "qa": ["--data", "--date"],
    "compact": ["--data", "--date"],
    "sql": ["--data", "-c"],
    "reconcile": ["--tardis-date", "--venue", "--symbol", "--fixture", "--data"],
    "bench": ["--input"],
}


def test_help_lists_every_command() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    output = _plain(result.output)
    for command in COMMANDS:
        assert command in output


@pytest.mark.parametrize("command", COMMANDS)
def test_command_help_lists_its_options(command: str) -> None:
    result = runner.invoke(app, [command, "--help"])
    assert result.exit_code == 0
    output = _plain(result.output)
    for option in COMMAND_OPTIONS[command]:
        assert option in output, f"{command} help is missing {option}"


@pytest.mark.parametrize("args", STUB_INVOCATIONS, ids=[args[0] for args in STUB_INVOCATIONS])
def test_command_stubs_exit_2(args: list[str]) -> None:
    result = runner.invoke(app, args)
    assert result.exit_code == 2
    assert "not implemented yet" in result.stderr


@pytest.mark.parametrize("command", COMMANDS)
def test_missing_required_options_exit_2(command: str) -> None:
    result = runner.invoke(app, [command])
    if command in ("reconcile", "bench"):
        assert result.exit_code == 2
        assert "not implemented yet" in result.stderr
    else:
        assert result.exit_code == 2
