"""CLI ``qa``, ``compact`` and ``sql`` bodies (issue #53)."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tickvault.cli import app
from tickvault.models import Trade
from tickvault.pipeline import run_replay
from tickvault.storage.compact import CompactionError
from tickvault.storage.parquet_writer import ParquetWriter

runner = CliRunner()

_BASE_NS = 1_793_491_200_000_000_000
_DATE = "2026-11-01"


def _trade(index: int, *, venue: str = "coinbase", symbol: str = "BTC-USD") -> Trade:
    ts_ns = _BASE_NS + index * 1_000_000
    return Trade(
        venue=venue,
        symbol=symbol,
        exchange_ts_ns=ts_ns,
        recv_ts_ns=ts_ns,
        connection_id=f"{venue}-conn",
        ingest_seq=index,
        trade_id=str(index),
        price=Decimal("100.50"),
        size=Decimal("0.010"),
        side="buy",
        sequence=index,
    )


def _write_trades(root: Path, records: list[Trade], *, max_rows: int = 1) -> None:
    writer = ParquetWriter(root, max_rows=max_rows)
    try:
        for record in records:
            writer.write("trades", [record])
    finally:
        writer.close()


def test_qa_text_table_reports_the_synthetic_session(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    out = tmp_path / "data"
    run_replay(synthetic_gap_path, out)

    result = runner.invoke(app, ["qa", "--data", str(out)])

    assert result.exit_code == 0, result.output
    assert "venue" in result.output
    assert "book_missing" in result.output
    assert "coinbase" in result.output
    assert "kraken" in result.output
    assert "TOTAL" in result.output


def test_qa_json_reports_dod_shape(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    out = tmp_path / "data"
    run_replay(synthetic_gap_path, out)

    result = runner.invoke(app, ["qa", "--data", str(out), "--json"])

    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    rows = {(row["venue"], row["symbol"]): row for row in document["rows"]}
    coinbase = rows[("coinbase", "BTC-USD")]
    kraken = rows[("kraken", "BTC-USD")]
    assert (coinbase["gaps"], coinbase["resyncs"]) == (1, 1)
    assert (kraken["checksum_failures"], kraken["resyncs"]) == (1, 1)
    assert coinbase["unexplained"] == 0
    assert kraken["unexplained"] == 0
    assert coinbase["book_missing"] is False
    assert kraken["book_missing"] is False


def test_qa_date_filter_and_reports(tmp_path: Path, synthetic_gap_path: Path) -> None:
    out = tmp_path / "data"
    run_replay(synthetic_gap_path, out)

    result = runner.invoke(
        app, ["qa", "--data", str(out), "--date", _DATE, "--reports"]
    )

    assert result.exit_code == 0, result.output
    json_path = out / "reports" / f"qa-{_DATE}.json"
    markdown_path = out / "reports" / f"qa-{_DATE}.md"
    assert json_path.is_file()
    assert markdown_path.is_file()
    document = json.loads(json_path.read_text(encoding="utf-8"))
    assert {row["date"] for row in document["rows"]} == {_DATE}

    all_dates = runner.invoke(app, ["qa", "--data", str(out), "--reports"])
    assert all_dates.exit_code == 0, all_dates.output
    assert json_path.is_file()


def test_qa_missing_book_exits_1(tmp_path: Path) -> None:
    out = tmp_path / "data"
    _write_trades(out, [_trade(0, symbol="ETH-USD")])

    result = runner.invoke(app, ["qa", "--data", str(out), "--json"])

    assert result.exit_code == 1, result.output
    document = json.loads(result.output)
    assert document["rows"][0]["book_missing"] is True


def test_qa_invalid_date_exits_2(tmp_path: Path) -> None:
    out = tmp_path / "data"
    (out / "norm").mkdir(parents=True)

    result = runner.invoke(app, ["qa", "--data", str(out), "--date", "2026-13-01"])

    assert result.exit_code == 2
    assert "error:" in result.stderr


def test_compact_merges_partitions_and_writes_manifest(tmp_path: Path) -> None:
    out = tmp_path / "data"
    _write_trades(out, [_trade(index) for index in range(3)], max_rows=1)

    result = runner.invoke(app, ["compact", "--data", str(out), "--date", _DATE])

    assert result.exit_code == 0, result.output
    assert "3 files merged into 1" in result.output
    assert "3 rows" in result.output
    manifest = out / "manifests" / f"date={_DATE}" / "manifest.json"
    assert manifest.is_file()
    document = json.loads(manifest.read_text(encoding="utf-8"))
    assert document["totals"]["files"] == 1
    assert document["totals"]["row_count"] == 3

    query = runner.invoke(
        app, ["sql", "--data", str(out), "-c", "select count(*) from trades"]
    )
    assert query.exit_code == 0, query.output
    assert query.output.split()[-1] == "3"


def test_compact_invalid_date_exits_2(tmp_path: Path) -> None:
    out = tmp_path / "data"
    (out / "norm").mkdir(parents=True)

    result = runner.invoke(app, ["compact", "--data", str(out), "--date", "2026-13-01"])

    assert result.exit_code == 2
    assert "error:" in result.stderr


def test_compact_verification_failure_exits_1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "data"
    _write_trades(out, [_trade(0)], max_rows=1)

    def boom(root: object, date: str) -> None:
        raise CompactionError("partition check failed")

    monkeypatch.setattr("tickvault.cli.compact_partitions", boom)
    result = runner.invoke(app, ["compact", "--data", str(out), "--date", _DATE])

    assert result.exit_code == 1
    assert "partition check failed" in result.stderr
    assert not (out / "manifests").exists()


def test_sql_prints_view_ddl(tmp_path: Path) -> None:
    out = tmp_path / "data"
    (out / "norm").mkdir(parents=True)

    result = runner.invoke(app, ["sql", "--data", str(out)])

    assert result.exit_code == 0, result.output
    assert "CREATE VIEW trades" in result.output


def test_sql_query_prints_two_venue_rows(tmp_path: Path) -> None:
    out = tmp_path / "data"
    _write_trades(
        out,
        [_trade(0), _trade(1), _trade(2, venue="kraken")],
        max_rows=1,
    )

    result = runner.invoke(
        app,
        [
            "sql",
            "--data",
            str(out),
            "-c",
            "select venue, count(*) from trades group by 1 order by 1",
        ],
    )

    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert lines[0].split() == ["venue", "count_star()"]
    assert len(lines) == 4
    assert lines[2].split() == ["coinbase", "2"]
    assert lines[3].split() == ["kraken", "1"]


def test_sql_error_exits_1(tmp_path: Path) -> None:
    out = tmp_path / "data"
    (out / "norm").mkdir(parents=True)

    result = runner.invoke(
        app, ["sql", "--data", str(out), "-c", "select * from missing_table"]
    )

    assert result.exit_code == 1
    assert "error:" in result.stderr
    assert "missing_table" in result.stderr
