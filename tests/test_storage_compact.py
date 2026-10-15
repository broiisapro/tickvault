"""Tests for normalized-partition compaction (issue #27)."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tickvault.models import Event, RawMessage, Trade
from tickvault.storage import compact as compact_module
from tickvault.storage.compact import CompactionError, compact
from tickvault.storage.parquet_writer import ParquetWriter
from tickvault.storage.raw_writer import RawWriter
from tickvault.timeutil import parse_rfc3339_ns

_CONNECTION = "kraken-20261015T031901Z-3fa2c1d0"
_HOUR_10 = parse_rfc3339_ns("2026-10-15T10:00:00Z")
_DATE = "2026-10-15"


def _trade(
    index: int,
    *,
    recv_ts_ns: int = _HOUR_10,
    exchange_ts_ns: int = _HOUR_10,
    ingest_seq: int = 0,
    trade_id: str | None = None,
    venue: str = "kraken",
) -> Trade:
    return Trade(
        venue=venue,
        symbol="BTC-USD",
        exchange_ts_ns=exchange_ts_ns,
        recv_ts_ns=recv_ts_ns,
        connection_id=_CONNECTION,
        ingest_seq=ingest_seq,
        trade_id=trade_id if trade_id is not None else str(index),
        price=Decimal("1"),
        size=Decimal("2"),
        side="buy",
        sequence=index,
    )


def _hour_dir(root: Path, table: str = "trades", hour: str = "10") -> Path:
    return root / "norm" / table / "venue=kraken" / f"date={_DATE}" / f"hour={hour}"


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tree_hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): _hash(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _write_trades(root: Path, records: list[Trade], *, max_rows: int = 2) -> None:
    writer = ParquetWriter(root, max_rows=max_rows)
    writer.write("trades", records)
    writer.close()


def test_compacts_multiple_files_into_one_and_sorts_rows(tmp_path: Path) -> None:
    root = tmp_path / "data"
    records = [
        _trade(index, recv_ts_ns=_HOUR_10 + index, exchange_ts_ns=_HOUR_10 + index, ingest_seq=7)
        for index in range(5)
    ]
    _write_trades(root, records)
    partition = _hour_dir(root)
    assert len(list(partition.glob("part-*.parquet"))) == 3

    result = compact(root, _DATE)

    assert result.date == _DATE
    assert len(result.partitions) == 1
    merged = result.partitions[0]
    assert (merged.table, merged.venue, merged.hour, merged.rows, merged.inputs) == (
        "trades",
        "kraken",
        "10",
        5,
        3,
    )
    assert result.rows == 5
    assert result.files_removed == 3
    assert result.files_written == 1
    output = root / merged.output
    assert output == partition / f"compacted-{_DATE}-10.parquet"
    assert [path.name for path in partition.glob("*.parquet")] == [output.name]
    assert pq.read_table(output).column("trade_id").to_pylist() == ["0", "1", "2", "3", "4"]


def test_compaction_sorts_by_receive_connection_and_sequence(tmp_path: Path) -> None:
    root = tmp_path / "data"
    for ingest_seq, recv in ((5, 5), (3, 3)):
        _write_trades(
            root,
            [
                _trade(
                    recv,
                    recv_ts_ns=_HOUR_10 + recv,
                    exchange_ts_ns=_HOUR_10 + recv,
                    ingest_seq=ingest_seq,
                )
            ],
            max_rows=1,
        )
    for ingest_seq, recv in ((4, 4), (1, 1), (2, 2)):
        _write_trades(
            root,
            [
                _trade(
                    recv,
                    recv_ts_ns=_HOUR_10 + recv,
                    exchange_ts_ns=_HOUR_10 + recv,
                    ingest_seq=ingest_seq,
                )
            ],
            max_rows=1,
        )
    assert len(list(_hour_dir(root).glob("part-*.parquet"))) == 5

    compact(root, _DATE)

    output = _hour_dir(root) / f"compacted-{_DATE}-10.parquet"
    assert pq.read_table(output).column("trade_id").to_pylist() == ["1", "2", "3", "4", "5"]


def test_stable_sort_preserves_tie_order(tmp_path: Path) -> None:
    root = tmp_path / "data"
    for trade_id in ("first", "second"):
        writer = ParquetWriter(root, max_rows=1)
        writer.write(
            "trades",
            [_trade(0, ingest_seq=5, trade_id=trade_id)],
        )
        writer.close()
    partition = _hour_dir(root)
    inputs = sorted(partition.glob("part-*.parquet"))
    expected = [
        trade_id
        for path in inputs
        for trade_id in pq.read_table(path).column("trade_id").to_pylist()
    ]

    compact(root, _DATE)

    output = partition / f"compacted-{_DATE}-10.parquet"
    assert pq.read_table(output).column("trade_id").to_pylist() == expected


def test_single_file_directories_are_left_alone_and_reruns_are_noops(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write_trades(root, [_trade(index, recv_ts_ns=_HOUR_10 + index) for index in range(5)])
    writer = ParquetWriter(root, max_rows=10)
    writer.write(
        "trades",
        [
            _trade(
                99,
                recv_ts_ns=_HOUR_10 + 3_600_000_000_000,
                exchange_ts_ns=_HOUR_10 + 3_600_000_000_000,
            )
        ],
    )
    writer.close()

    result = compact(root, _DATE)
    assert [partition.hour for partition in result.partitions] == ["10"]
    hour_11 = _hour_dir(root, hour="11")
    assert [path.name for path in hour_11.glob("*.parquet")] == [
        f"part-{_CONNECTION}-000000000000.parquet"
    ]

    before = _tree_hashes(root)
    second = compact(root, _DATE)
    assert second.partitions == ()
    assert second.rows == 0
    assert _tree_hashes(root) == before


def test_compaction_never_touches_raw_and_handles_multiple_tables(tmp_path: Path) -> None:
    root = tmp_path / "data"
    with RawWriter(root) as writer:
        for index in range(3):
            writer.write(
                RawMessage(
                    venue="kraken",
                    connection_id=_CONNECTION,
                    ingest_seq=index,
                    recv_ts_ns=_HOUR_10 + index,
                    kind="msg",
                    payload=f'{{"n":{index}}}',
                )
            )
    raw_before = {
        path.relative_to(root).as_posix(): _hash(path)
        for path in sorted((root / "raw").rglob("*.jsonl.zst"))
    }
    trades = ParquetWriter(root, max_rows=2)
    trades.write("trades", [_trade(index, recv_ts_ns=_HOUR_10 + index) for index in range(3)])
    trades.close()
    events = ParquetWriter(root, max_rows=2)
    events.write(
        "events",
        [
            Event(
                venue="coinbase",
                recv_ts_ns=_HOUR_10 + index,
                connection_id="coinbase-20261015T031902Z-0000000b",
                ingest_seq=index,
                event_type="gap",
                symbol="BTC-USD",
                detail='{"reason":"gap"}',
            )
            for index in range(3)
        ],
    )
    events.close()

    result = compact(root, _DATE)

    assert sorted((partition.table, partition.hour) for partition in result.partitions) == [
        ("events", "10"),
        ("trades", "10"),
    ]
    raw_after = {
        path.relative_to(root).as_posix(): _hash(path)
        for path in sorted((root / "raw").rglob("*.jsonl.zst"))
    }
    assert raw_after == raw_before


def test_failed_verification_keeps_inputs_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "data"
    _write_trades(root, [_trade(index, recv_ts_ns=_HOUR_10 + index) for index in range(5)])
    partition = _hour_dir(root)
    before = {path.name: _hash(path) for path in partition.glob("part-*.parquet")}
    assert len(before) == 3

    def _write_missing_a_row(table: pa.Table, destination: Path) -> None:
        truncated = table.slice(0, max(table.num_rows - 1, 0))
        pq.write_table(truncated, destination, compression="zstd")

    monkeypatch.setattr(compact_module, "_write_compacted", _write_missing_a_row)
    with pytest.raises(CompactionError):
        compact(root, _DATE)

    after = {path.name: _hash(path) for path in partition.glob("part-*.parquet")}
    assert after == before
    assert not list(partition.glob("compacted-*.parquet"))
    assert not list(partition.glob("*.tmp"))


def test_failed_verification_keeps_a_previous_compacted_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "data"
    _write_trades(root, [_trade(index, recv_ts_ns=_HOUR_10 + index) for index in range(4)])
    compact(root, _DATE)
    writer = ParquetWriter(root, max_rows=1)
    writer.write(
        "trades",
        [_trade(9, recv_ts_ns=_HOUR_10 + 10_000_000_000, ingest_seq=9)],
    )
    writer.close()
    partition = _hour_dir(root)
    before = {path.name: _hash(path) for path in partition.glob("*.parquet")}
    assert len(before) == 2

    def _write_nothing(table: pa.Table, destination: Path) -> None:
        return None

    monkeypatch.setattr(compact_module, "_write_compacted", _write_nothing)
    with pytest.raises(CompactionError):
        compact(root, _DATE)

    assert {path.name: _hash(path) for path in partition.glob("*.parquet")} == before
    assert not list(partition.glob("*.tmp"))


def test_compact_reports_every_multi_file_partition(tmp_path: Path) -> None:
    root = tmp_path / "data"
    writer = ParquetWriter(root, max_rows=1)
    for index in range(2):
        writer.write("trades", [_trade(index, recv_ts_ns=_HOUR_10 + index, ingest_seq=index)])
        writer.write(
            "trades",
            [
                _trade(
                    index,
                    recv_ts_ns=_HOUR_10 + 3_600_000_000_000 + index,
                    exchange_ts_ns=_HOUR_10 + 3_600_000_000_000 + index,
                    ingest_seq=index,
                )
            ],
        )
    writer.close()

    result = compact(root, _DATE)

    assert [(partition.hour, partition.rows) for partition in result.partitions] == [
        ("10", 2),
        ("11", 2),
    ]


def test_empty_root_and_missing_date_are_noops(tmp_path: Path) -> None:
    assert compact(tmp_path / "nothing", _DATE).partitions == ()
    root = tmp_path / "data"
    _write_trades(root, [_trade(0)])
    assert compact(root, "2026-10-16").partitions == ()


def test_invalid_date_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        compact(tmp_path, "2026-1-1")
    with pytest.raises(ValueError):
        compact(tmp_path, "2026-13-01")
