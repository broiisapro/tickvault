import re

import pytest
from typer.testing import CliRunner

from tickvault.cli import app

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return _ANSI_RE.sub("", text)


COMMANDS = ["ingest", "replay", "qa", "compact", "sql", "reconcile", "bench"]

COMMAND_OPTIONS = {
    "ingest": ["--venues", "--symbols", "--duration", "--out", "--max-gb"],
    "replay": ["--input", "--out"],
    "qa": ["--data", "--date", "--json", "--reports"],
    "compact": ["--data", "--date"],
    "sql": ["--data", "-c"],
    "reconcile": ["--tardis-date", "--venue", "--symbol", "--fixture", "--data", "--json"],
    "bench": ["--input", "--json"],
}

REQUIRED_OPTION_COMMANDS = ["ingest", "replay", "qa", "compact", "sql"]


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


@pytest.mark.parametrize("command", REQUIRED_OPTION_COMMANDS)
def test_missing_required_options_exit_2(command: str) -> None:
    result = runner.invoke(app, [command])
    assert result.exit_code == 2


def test_reconcile_without_mode_exits_2() -> None:
    result = runner.invoke(app, ["reconcile"])
    assert result.exit_code == 2
    assert "error:" in result.stderr
