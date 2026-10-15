"""CLI ``bench`` body (issue #54)."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from tickvault.bench import default_inputs
from tickvault.cli import app

runner = CliRunner()


def test_bench_synthetic_fixture_returns_numbers(synthetic_gap_path: Path) -> None:
    result = runner.invoke(app, ["bench", "--input", str(synthetic_gap_path)])

    assert result.exit_code == 0, result.output
    assert "messages: 105" in result.output
    assert "msg/s overall" in result.output
    assert "peak RSS" in result.output
    assert "PASS" in result.output or "FAIL" in result.output


def test_bench_json_output(synthetic_gap_path: Path) -> None:
    result = runner.invoke(app, ["bench", "--input", str(synthetic_gap_path), "--json"])

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["messages"] == 105
    assert document["seconds"] > 0
    assert document["messages_per_second"] > 0
    assert {venue["venue"] for venue in document["venues"]} == {"coinbase", "kraken"}
    assert document["max_rss_mb"] > 0
    assert document["target_messages_per_second"] == 2000.0
    assert document["target_max_rss_mb"] == 400.0
    assert isinstance(document["passed"], bool)


def test_bench_default_inputs_are_the_recorded_fixtures() -> None:
    names = [path.name for path in default_inputs()]
    assert names == ["coinbase_btc_5m.jsonl.zst", "kraken_btc_5m.jsonl.zst"]
    assert all(path.is_file() for path in default_inputs())


def test_bench_missing_input_exits_1(tmp_path: Path) -> None:
    result = runner.invoke(app, ["bench", "--input", str(tmp_path / "missing")])

    assert result.exit_code == 1
    assert "error:" in result.stderr
