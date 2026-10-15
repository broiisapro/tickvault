"""CLI ``reconcile`` body: offline fixture, download wiring and validation (issue #53)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from tickvault.cli import app
from tickvault.reconcile.tardis import ReconcileReport

runner = CliRunner()

_COUNT_KEYS = (
    "tardis_trades",
    "tickvault_trades",
    "matched",
    "missing_in_tickvault",
    "extra_in_tickvault",
    "price_mismatches",
    "size_mismatches",
    "side_mismatches",
)


def test_reconcile_offline_fixture_matches_expected_counts(
    fixtures_dir: Path, reconcile_expected: dict[str, Any]
) -> None:
    result = runner.invoke(
        app,
        [
            "reconcile",
            "--fixture",
            str(fixtures_dir / "tardis_coinbase_trades_sample.csv.gz"),
            "--data",
            str(fixtures_dir / "tv_coinbase_trades_sample"),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    for key in _COUNT_KEYS:
        assert document[key] == reconcile_expected[key], key
    assert document["missing_in_tickvault_ids"] == reconcile_expected["missing_in_tickvault_ids"]
    assert document["extra_in_tickvault_ids"] == reconcile_expected["extra_in_tickvault_ids"]
    assert document["price_mismatch_ids"] == reconcile_expected["price_mismatch_ids"]
    assert document["size_mismatch_ids"] == reconcile_expected["size_mismatch_ids"]
    assert document["side_mismatch_ids"] == reconcile_expected["side_mismatch_ids"]


def test_reconcile_offline_text_summary(
    fixtures_dir: Path, reconcile_expected: dict[str, Any]
) -> None:
    result = runner.invoke(
        app,
        [
            "reconcile",
            "--fixture",
            str(fixtures_dir / "tardis_coinbase_trades_sample.csv.gz"),
            "--data",
            str(fixtures_dir / "tv_coinbase_trades_sample"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert f"matched: {reconcile_expected['matched']}" in result.output
    assert f"tardis trades: {reconcile_expected['tardis_trades']}" in result.output
    assert f"tickvault trades: {reconcile_expected['tickvault_trades']}" in result.output


def test_reconcile_download_mode_wires_data_root_and_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: dict[str, object] = {}

    def fake_reconcile_live_day(
        root: str | Path,
        day: str | date,
        cache_dir: str | Path,
        *,
        venue: str = "coinbase",
        symbol: str = "BTC-USD",
        client: object = None,
    ) -> ReconcileReport:
        calls.update(
            root=Path(root), day=day, cache_dir=Path(cache_dir), venue=venue, symbol=symbol
        )
        return ReconcileReport(
            window=None,
            tardis_trades=1,
            tickvault_trades=1,
            matched=1,
            missing_in_tickvault_ids=(),
            extra_in_tickvault_ids=(),
            price_mismatch_ids=(),
            size_mismatch_ids=(),
            side_mismatch_ids=(),
        )

    monkeypatch.setattr("tickvault.cli.reconcile_live_day", fake_reconcile_live_day)
    root = tmp_path / "data"

    result = runner.invoke(
        app,
        [
            "reconcile",
            "--tardis-date",
            "2026-10-01",
            "--venue",
            "coinbase",
            "--symbol",
            "BTC-USD",
            "--data",
            str(root),
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == {
        "root": root,
        "day": date(2026, 10, 1),
        "cache_dir": root / "cache",
        "venue": "coinbase",
        "symbol": "BTC-USD",
    }
    assert "matched: 1" in result.output


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--tardis-date", "2026-10-02", "--data", "data"], "first day"),
        (["--tardis-date", "not-a-date", "--data", "data"], "error:"),
        (["--tardis-date", "2026-10-01"], "required"),
        (["--fixture", "reference.csv.gz"], "required"),
        (
            ["--fixture", "reference.csv.gz", "--tardis-date", "2026-10-01", "--data", "data"],
            "mutually exclusive",
        ),
        ([], "provide"),
    ],
)
def test_reconcile_usage_errors_exit_2(args: list[str], message: str) -> None:
    result = runner.invoke(app, ["reconcile", *args])

    assert result.exit_code == 2
    assert message in result.stderr
