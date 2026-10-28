"""Tests for the public sample builder (issue #127).

The input tree is the recorded five-minute Coinbase and Kraken BTC-USD
captures replayed through the real pipeline into a temporary data root, so the
sample is built from the same normalized tables the product writes. The
builder is exercised through :func:`scripts.build_public_sample.main` (CLI
semantics, exit codes) and, for the hour selection, over a small purpose-built
tree because one replayed capture covers two hours and cannot express a tie.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from scripts.build_public_sample import main
from tickvault.models import TABLES, Event, Trade
from tickvault.pipeline import run_replay
from tickvault.qa.report import exit_code as qa_exit_code
from tickvault.qa.report import run_qa, totals, write_reports
from tickvault.storage.manifest import verify_manifest
from tickvault.storage.parquet_writer import ParquetWriter
from tickvault.timeutil import parse_rfc3339_ns

ROOT = Path(__file__).resolve().parents[1]
DATE = "2026-10-15"
"""UTC day of both recorded five-minute fixtures."""
HOUR = "11"
"""The fixtures start at 11:56:18 UTC and end just after 12:00, so hour 11 has more rows."""
SYMBOL = "BTC-USD"


@pytest.fixture(scope="session")
def replayed_root(
    tmp_path_factory: pytest.TempPathFactory,
    coinbase_5m_path: Path,
    kraken_5m_path: Path,
) -> Path:
    """A data root replayed from both recorded fixtures (Coinbase and Kraken)."""
    root = tmp_path_factory.mktemp("replayed") / "data"
    for raw in (coinbase_5m_path, kraken_5m_path):
        run_replay(raw, root)
    return root


def _tree_digest(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _payload(out: Path) -> dict[str, object]:
    return json.loads((out / "sample.json").read_text(encoding="utf-8"))


def _sample_path(
    out: Path, table: str, venue: str, *, date: str = DATE, hour: str = HOUR
) -> Path:
    return (
        out
        / "norm"
        / table
        / f"venue={venue}"
        / f"date={date}"
        / f"hour={hour}"
        / f"sample-{date}-{hour}.parquet"
    )


def _matching_rows(path: Path, symbol: str) -> int:
    return pq.read_table(path, columns=["symbol"], filters=[("symbol", "=", symbol)]).num_rows


def _rows_in_partition(
    root: Path, table: str, venue: str, date: str, hour: str, symbol: str
) -> int:
    pattern = f"norm/{table}/venue={venue}/date={date}/hour={hour}/*.parquet"
    return sum(_matching_rows(path, symbol) for path in sorted(root.glob(pattern)))


def _source_pairs(root: Path, date: str, hour: str, symbol: str) -> set[tuple[str, str]]:
    """The (table, venue) pairs that hold at least one row of ``symbol``."""
    pairs: set[tuple[str, str]] = set()
    for table in TABLES:
        pattern = f"norm/{table}/venue=*/date={date}/hour={hour}/*.parquet"
        for path in sorted(root.glob(pattern)):
            venue = path.parent.parent.parent.name.removeprefix("venue=")
            if _rows_in_partition(root, table, venue, date, hour, symbol):
                pairs.add((table, venue))
    return pairs


def _build(root: Path, out: Path, *extra: str, date: str = DATE) -> int:
    return main(["--data", str(root), "--date", date, "--out", str(out), *extra])


def test_builder_copies_one_hour_of_the_symbol_for_every_venue(
    replayed_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "sample"
    assert _build(replayed_root, out, "--hour", HOUR) == 0
    assert "within --max-mb 50" in capsys.readouterr().out

    pairs = _source_pairs(replayed_root, DATE, HOUR, SYMBOL)
    assert {venue for _, venue in pairs} == {"coinbase", "kraken"}
    assert {table for table, _ in pairs} == {"book_deltas", "book_snapshots", "trades"}

    expected: set[str] = {
        "sample.json",
        "SHA256SUMS",
        f"manifests/date={DATE}/manifest.json",
    }
    expected |= {
        _sample_path(out, table, venue).relative_to(out).as_posix() for table, venue in pairs
    }
    written = {path.relative_to(out).as_posix() for path in out.rglob("*") if path.is_file()}
    assert written == expected
    assert {
        path.name.removeprefix("hour=") for path in out.glob("norm/*/venue=*/date=*/hour=*")
    } == {HOUR}
    assert {
        path.name.removeprefix("date=") for path in out.glob("norm/*/venue=*/date=*")
    } == {DATE}

    payload = _payload(out)
    assert set(payload) == {"date", "hour", "symbol", "venues", "rows", "source"}
    assert payload["date"] == DATE
    assert payload["hour"] == HOUR
    assert payload["symbol"] == SYMBOL
    assert payload["venues"] == ["coinbase", "kraken"]
    assert payload["source"] is None

    rows: dict[str, int] = {}
    for table, schema in TABLES.items():
        rows[table] = 0
        for venue in ("coinbase", "kraken"):
            path = _sample_path(out, table, venue)
            if (table, venue) not in pairs:
                assert not path.exists()
                continue
            table_rows = pq.read_table(path)
            assert table_rows.schema.equals(schema)
            assert table_rows.num_rows == _rows_in_partition(
                replayed_root, table, venue, DATE, HOUR, SYMBOL
            )
            assert set(table_rows.column("symbol").to_pylist()) == {SYMBOL}
            timestamps = table_rows.column("recv_ts_ns").to_pylist()
            assert timestamps == sorted(timestamps)
            rows[table] += table_rows.num_rows
    assert payload["rows"] == dict(sorted(rows.items()))
    assert rows["events"] == 0
    assert rows["trades"] > 0


def test_manifest_and_checksums_verify(replayed_root: Path, tmp_path: Path) -> None:
    out = tmp_path / "sample"
    assert _build(replayed_root, out, "--hour", HOUR) == 0

    manifest = out / "manifests" / f"date={DATE}" / "manifest.json"
    assert verify_manifest(manifest) == []
    document = json.loads(manifest.read_text(encoding="utf-8"))
    assert {entry["path"] for entry in document["files"]} == {
        _sample_path(out, table, venue).relative_to(out).as_posix()
        for table in TABLES
        for venue in ("coinbase", "kraken")
        if _sample_path(out, table, venue).is_file()
    }

    listed: dict[str, str] = {}
    for line in (out / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, relative = line.partition("  ")
        assert separator == "  ", line
        assert len(digest) == 64 and set(digest) <= set("0123456789abcdef")
        listed[relative] = digest
    assert list(listed) == sorted(listed)
    assert set(listed) == {
        path.relative_to(out).as_posix()
        for path in out.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    }
    for relative, digest in listed.items():
        assert hashlib.sha256((out / relative).read_bytes()).hexdigest() == digest


def test_hour_choice_uses_the_hour_with_most_rows(replayed_root: Path, tmp_path: Path) -> None:
    counts: dict[str, int] = {}
    for table in TABLES:
        pattern = f"norm/{table}/venue=*/date={DATE}/hour=*/*.parquet"
        for path in sorted(replayed_root.glob(pattern)):
            hour = path.parent.name.removeprefix("hour=")
            counts[hour] = counts.get(hour, 0) + _matching_rows(path, SYMBOL)
    assert len(counts) == 2
    (other,) = set(counts) - {HOUR}
    assert counts[HOUR] > counts[other]
    assert min(counts, key=lambda hour: (-counts[hour], hour)) == HOUR

    out = tmp_path / "sample"
    assert _build(replayed_root, out) == 0
    payload = _payload(out)
    assert payload["hour"] == HOUR
    assert _sample_path(out, "trades", "coinbase").is_file()


def _write_hour_choice_tree(root: Path, later_trades: int) -> None:
    """Trades and one symbol-bearing event in three hours of one day."""
    trades: list[Trade] = []
    events: list[Event] = []
    sequence = 0
    for hour, count in ((3, 3), (5, 7), (7, later_trades)):
        start = parse_rfc3339_ns(f"{DATE}T{hour:02d}:05:00Z")
        for index in range(count):
            trades.append(
                Trade(
                    venue="kraken",
                    symbol=SYMBOL,
                    exchange_ts_ns=start + index,
                    recv_ts_ns=start + index,
                    connection_id="kraken-conn",
                    ingest_seq=sequence,
                    trade_id=f"{hour}-{index}",
                    price=Decimal("100"),
                    size=Decimal("1"),
                    side="buy",
                )
            )
            sequence += 1
        events.append(
            Event(
                venue="kraken",
                recv_ts_ns=start,
                connection_id="kraken-conn",
                ingest_seq=sequence,
                event_type="resync",
                symbol=SYMBOL,
                exchange_ts_ns=start,
                detail=f'{{"hour":"{hour:02d}"}}',
            )
        )
        sequence += 1
    writer = ParquetWriter(root)
    writer.write("trades", trades)
    writer.write("events", events)
    writer.close()


def test_hour_choice_prefers_the_later_hour_with_more_rows(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write_hour_choice_tree(root, later_trades=9)

    out = tmp_path / "sample"
    assert _build(root, out) == 0

    assert _payload(out)["hour"] == "07"
    assert _sample_path(out, "trades", "kraken", hour="07").is_file()
    assert not (out / "norm" / "trades" / "venue=kraken" / f"date={DATE}" / "hour=05").exists()


def test_hour_choice_breaks_a_tie_with_the_earliest_hour(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write_hour_choice_tree(root, later_trades=7)

    out = tmp_path / "sample"
    assert _build(root, out) == 0

    payload = _payload(out)
    assert payload["hour"] == "05"
    assert payload["rows"] == {"book_deltas": 0, "book_snapshots": 0, "events": 1, "trades": 7}

    events = pq.read_table(_sample_path(out, "events", "kraken", hour="05"))
    assert events.schema.equals(TABLES["events"])
    assert events.num_rows == 1
    assert events.column("symbol").to_pylist() == [SYMBOL]
    assert events.column("detail").to_pylist() == ['{"hour":"05"}']
    assert not (out / "norm" / "trades" / "venue=kraken" / f"date={DATE}" / "hour=07").exists()


def test_builder_keeps_only_the_requested_symbol(tmp_path: Path) -> None:
    root = tmp_path / "data"
    start = parse_rfc3339_ns(f"{DATE}T05:05:00Z")
    trades = [
        Trade(
            venue="kraken",
            symbol=symbol,
            exchange_ts_ns=start + index,
            recv_ts_ns=start + index,
            connection_id="kraken-conn",
            ingest_seq=index,
            trade_id=f"{symbol}-{index}",
            price=Decimal("100"),
            size=Decimal("1"),
            side="buy",
        )
        for index, symbol in enumerate(["ETH-USD"] * 4 + [SYMBOL] * 5)
    ]
    writer = ParquetWriter(root)
    writer.write("trades", trades)
    writer.close()

    out = tmp_path / "sample"
    assert _build(root, out, "--hour", "05") == 0

    payload = _payload(out)
    assert payload["rows"] == {"book_deltas": 0, "book_snapshots": 0, "events": 0, "trades": 5}
    assert payload["symbol"] == SYMBOL
    copied = pq.read_table(_sample_path(out, "trades", "kraken", hour="05"))
    assert copied.num_rows == 5
    assert set(copied.column("symbol").to_pylist()) == {SYMBOL}
    assert copied.column("trade_id").to_pylist() == [f"{SYMBOL}-{index}" for index in range(4, 9)]


def test_sample_json_records_the_input_qa_summary(replayed_root: Path, tmp_path: Path) -> None:
    data = tmp_path / "data"
    shutil.copytree(replayed_root, data)
    report = run_qa(data, DATE)
    write_reports(report, data / "reports", DATE)
    summed = totals(report)

    out = tmp_path / "sample"
    assert _build(data, out, "--hour", HOUR) == 0

    assert _payload(out)["source"] == {
        "report": f"reports/qa-{DATE}.json",
        "date": DATE,
        "rows": len(report.rows),
        "unexplained": summed.unexplained,
        "book_missing": summed.book_missing,
        "exit_code": qa_exit_code(report),
    }


def test_two_runs_are_byte_identical_and_leave_the_input_alone(
    replayed_root: Path, tmp_path: Path
) -> None:
    before = _tree_digest(replayed_root)
    first = tmp_path / "first"
    second = tmp_path / "second"
    for out in (first, second):
        assert _build(replayed_root, out, "--hour", HOUR) == 0
    assert _tree_digest(first) == _tree_digest(second)
    assert _tree_digest(replayed_root) == before


def test_builder_refuses_a_non_empty_out(
    replayed_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "sample"
    out.mkdir()
    (out / "keep.txt").write_text("keep", encoding="utf-8")

    assert _build(replayed_root, out) == 1

    assert "exists and is not empty" in capsys.readouterr().err
    assert sorted(path.name for path in out.iterdir()) == ["keep.txt"]


def test_builder_accepts_an_existing_empty_out(replayed_root: Path, tmp_path: Path) -> None:
    out = tmp_path / "sample"
    out.mkdir()
    assert _build(replayed_root, out, "--hour", HOUR) == 0
    assert verify_manifest(out / "manifests" / f"date={DATE}" / "manifest.json") == []


def test_builder_refuses_a_day_without_the_symbol(
    replayed_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing_symbol = tmp_path / "eth"
    assert _build(replayed_root, missing_symbol, "--symbol", "ETH-USD") == 1
    assert "no ETH-USD rows" in capsys.readouterr().err
    assert not missing_symbol.exists()

    missing_day = tmp_path / "yesterday"
    assert _build(replayed_root, missing_day, date="2026-10-14") == 1
    assert "no BTC-USD rows" in capsys.readouterr().err
    assert not missing_day.exists()


def test_builder_refuses_an_empty_hour(
    replayed_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "sample"
    assert _build(replayed_root, out, "--hour", "03") == 1
    assert "no BTC-USD rows on" in capsys.readouterr().err
    assert not out.exists()
    assert not list(tmp_path.glob(".sample.*"))


def test_builder_stops_over_the_size_cap_before_publishing(
    replayed_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "sample"
    assert _build(replayed_root, out, "--hour", HOUR, "--max-mb", "0.001") == 1

    assert "over --max-mb" in capsys.readouterr().err
    assert not out.exists()
    assert not list(tmp_path.glob(".sample.*"))


def test_builder_rejects_an_out_inside_the_data_root(
    replayed_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    inside = replayed_root / "sample"
    assert _build(replayed_root, inside) == 2
    assert "--out must not be inside --data" in capsys.readouterr().err
    assert not inside.exists()


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (["--hour", "24"], "--hour"),
        (["--hour", "seven"], "--hour"),
        (["--max-mb", "0"], "--max-mb must be positive"),
        (["--symbol", "  "], "--symbol must not be empty"),
    ],
)
def test_builder_rejects_bad_options(
    replayed_root: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    extra: list[str],
    message: str,
) -> None:
    out = tmp_path / "sample"
    assert _build(replayed_root, out, *extra) == 2
    assert message in capsys.readouterr().err
    assert not out.exists()


@pytest.mark.parametrize(
    ("date", "message"),
    [("2026-02-30", "valid calendar date"), ("15-10-2026", "YYYY-MM-DD")],
)
def test_builder_rejects_bad_dates(
    replayed_root: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    date: str,
    message: str,
) -> None:
    out = tmp_path / "sample"
    assert _build(replayed_root, out, date=date) == 2
    assert message in capsys.readouterr().err
    assert not out.exists()


def test_builder_rejects_a_missing_data_root(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "sample"
    assert _build(tmp_path / "missing", out, "--hour", HOUR) == 2
    assert "data root not found" in capsys.readouterr().err
    assert not out.exists()


def test_builder_cli_exits_zero(tmp_path: Path, replayed_root: Path) -> None:
    script = ROOT / "scripts" / "build_public_sample.py"
    out = tmp_path / "cli"
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--data",
            str(replayed_root),
            "--date",
            DATE,
            "--out",
            str(out),
            "--hour",
            HOUR,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0
    assert "within --max-mb" in completed.stdout
    assert verify_manifest(out / "manifests" / f"date={DATE}" / "manifest.json") == []


def test_sample_files_keep_the_normalized_schemas(tmp_path: Path, replayed_root: Path) -> None:
    """Copied files keep the exact ``models.TABLES`` schema, nullable flags included."""
    out = tmp_path / "sample"
    assert _build(replayed_root, out, "--hour", HOUR) == 0
    copied = 0
    for table, expected in TABLES.items():
        for venue in ("coinbase", "kraken"):
            path = _sample_path(out, table, venue)
            if not path.is_file():
                continue
            schema = pq.ParquetFile(path).schema_arrow
            assert schema.equals(expected, check_metadata=True)
            assert [field.nullable for field in schema] == [
                field.nullable for field in expected
            ]
            copied += 1
    assert copied == len(_source_pairs(replayed_root, DATE, HOUR, SYMBOL))
