"""Tests for the deterministic Parquet writer (issue #26)."""

from __future__ import annotations

import filecmp
import signal
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tickvault.models import BookSnapshotLevel, Event, Trade
from tickvault.storage.parquet_writer import ParquetWriter
from tickvault.timeutil import parse_rfc3339_ns

_CONNECTION = "kraken-20261015T031901Z-3fa2c1d0"
_HOUR_10 = parse_rfc3339_ns("2026-10-15T10:00:00Z")


def _trade(
    index: int,
    *,
    exchange_ts_ns: int = _HOUR_10,
    recv_ts_ns: int = _HOUR_10,
    connection_id: str = _CONNECTION,
    ingest_seq: int = 0,
    venue: str = "kraken",
    price: str = "1",
    size: str = "2",
) -> Trade:
    return Trade(
        venue=venue,
        symbol="BTC-USD",
        exchange_ts_ns=exchange_ts_ns,
        recv_ts_ns=recv_ts_ns,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        trade_id=str(index),
        price=Decimal(price),
        size=Decimal(size),
        side="buy",
        sequence=index,
    )


def _snapshot_level(level: int, *, exchange_ts_ns: int, recv_ts_ns: int) -> BookSnapshotLevel:
    return BookSnapshotLevel(
        venue="kraken",
        symbol="BTC-USD",
        exchange_ts_ns=exchange_ts_ns,
        recv_ts_ns=recv_ts_ns,
        connection_id=_CONNECTION,
        ingest_seq=0,
        snapshot_id="snapshot-0",
        source="sample",
        side="bid",
        level=level,
        price=Decimal("100"),
        size=Decimal("1"),
    )


def _partition(root: Path, table: str, hour: str = "10", date: str = "2026-10-15") -> Path:
    return root / "norm" / table / "venue=kraken" / f"date={date}" / f"hour={hour}"


def _files(partition: Path) -> list[Path]:
    return sorted(partition.glob("*.parquet"))


def _row_count(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def _assert_trees_identical(first: Path, second: Path) -> None:
    comparison = filecmp.dircmp(first, second)
    assert comparison.left_only == []
    assert comparison.right_only == []
    for name in comparison.common_files:
        assert filecmp.cmp(first / name, second / name, shallow=False), name
    for name in comparison.common_dirs:
        _assert_trees_identical(first / name, second / name)


def test_partition_keyed_by_exchange_hour_not_receive_hour(tmp_path: Path) -> None:
    root = tmp_path / "data"
    exchange_ts = parse_rfc3339_ns("2026-10-15T10:59:59.900000000Z")
    recv_ts = parse_rfc3339_ns("2026-10-15T11:00:00.100000000Z")
    writer = ParquetWriter(root)
    writer.write("trades", [_trade(0, exchange_ts_ns=exchange_ts, recv_ts_ns=recv_ts)])
    writer.close()
    assert [path.name for path in _files(_partition(root, "trades", hour="10"))] == [
        f"part-{_CONNECTION}-000000000000.parquet"
    ]
    assert not _partition(root, "trades", hour="11").exists()


def test_event_without_exchange_time_partitions_by_receive_time(tmp_path: Path) -> None:
    root = tmp_path / "data"
    recv_ts = parse_rfc3339_ns("2026-10-15T12:30:00Z")
    event = Event(
        venue="kraken",
        recv_ts_ns=recv_ts,
        connection_id=_CONNECTION,
        ingest_seq=4,
        event_type="gap",
        symbol="BTC-USD",
        detail='{"reason":"gap"}',
    )
    writer = ParquetWriter(root)
    writer.write("events", [event])
    writer.close()
    assert [path.name for path in _files(_partition(root, "events", hour="12"))] == [
        f"part-{_CONNECTION}-000000000004.parquet"
    ]


def test_file_name_uses_first_row_but_content_follows_write_order(tmp_path: Path) -> None:
    root = tmp_path / "data"
    writer = ParquetWriter(root)
    for index in range(3):
        writer.write("trades", [_trade(index, exchange_ts_ns=_HOUR_10 + index, ingest_seq=index)])
    writer.close()
    files = _files(_partition(root, "trades"))
    assert [path.name for path in files] == [f"part-{_CONNECTION}-000000000000.parquet"]
    assert pq.read_table(files[0]).column("trade_id").to_pylist() == ["0", "1", "2"]


def test_logical_clock_flushes_at_the_interval_boundary(tmp_path: Path) -> None:
    root = tmp_path / "data"
    writer = ParquetWriter(root, flush_interval_ns=60_000_000_000)
    writer.write("trades", [_trade(0, exchange_ts_ns=_HOUR_10, recv_ts_ns=_HOUR_10, ingest_seq=0)])
    writer.write(
        "trades",
        [
            _trade(
                1,
                exchange_ts_ns=_HOUR_10 + 1,
                recv_ts_ns=_HOUR_10 + 59_999_999_999,
                ingest_seq=1,
            )
        ],
    )
    partition = _partition(root, "trades")
    assert not partition.exists()
    writer.write(
        "trades",
        [
            _trade(
                2,
                exchange_ts_ns=_HOUR_10 + 2,
                recv_ts_ns=_HOUR_10 + 60_000_000_000,
                ingest_seq=2,
            )
        ],
    )
    assert [_row_count(path) for path in _files(partition)] == [3]
    writer.write(
        "trades",
        [
            _trade(
                3,
                exchange_ts_ns=_HOUR_10 + 3,
                recv_ts_ns=_HOUR_10 + 60_000_000_001,
                ingest_seq=3,
            )
        ],
    )
    writer.close()
    assert [_row_count(path) for path in _files(partition)] == [3, 1]


def test_max_rows_splits_a_message_and_keeps_names_unique(tmp_path: Path) -> None:
    root = tmp_path / "data"
    writer = ParquetWriter(root, max_rows=3)
    records = [
        _snapshot_level(index, exchange_ts_ns=_HOUR_10 + index, recv_ts_ns=_HOUR_10 + index)
        for index in range(7)
    ]
    writer.write("book_snapshots", records)
    partition = _partition(root, "book_snapshots")
    stem = f"part-{_CONNECTION}-000000000000"
    assert {path.name: _row_count(path) for path in _files(partition)} == {
        f"{stem}.parquet": 3,
        f"{stem}-1.parquet": 3,
    }
    writer.close()
    assert {path.name: _row_count(path) for path in _files(partition)} == {
        f"{stem}.parquet": 3,
        f"{stem}-1.parquet": 3,
        f"{stem}-2.parquet": 1,
    }


def test_flush_persists_without_closing(tmp_path: Path) -> None:
    root = tmp_path / "data"
    writer = ParquetWriter(root)
    writer.write("trades", [_trade(0, exchange_ts_ns=_HOUR_10, ingest_seq=0)])
    writer.flush()
    assert [_row_count(path) for path in _files(_partition(root, "trades"))] == [1]
    writer.write("trades", [_trade(1, exchange_ts_ns=_HOUR_10 + 1, ingest_seq=1)])
    writer.close()
    assert [_row_count(path) for path in _files(_partition(root, "trades"))] == [1, 1]


def test_replay_produces_byte_identical_trees(tmp_path: Path) -> None:
    def _write_session(root: Path) -> None:
        writer = ParquetWriter(root)
        for index in range(5):
            writer.write(
                "trades",
                [
                    _trade(
                        index,
                        exchange_ts_ns=_HOUR_10 + index * 1_000_000_000,
                        recv_ts_ns=_HOUR_10 + index * 1_000_000_000,
                        ingest_seq=index,
                    )
                ],
            )
        writer.write(
            "book_snapshots",
            [
                _snapshot_level(
                    index, exchange_ts_ns=_HOUR_10 + index, recv_ts_ns=_HOUR_10 + index
                )
                for index in range(4)
            ],
        )
        writer.write(
            "events",
            [
                Event(
                    venue="coinbase",
                    recv_ts_ns=_HOUR_10,
                    connection_id="coinbase-20261015T031902Z-0000000b",
                    ingest_seq=0,
                    event_type="resync",
                    symbol="BTC-USD",
                    detail='{"deltas_dropped":2,"reason":"gap"}',
                )
            ],
        )
        writer.close()

    first = tmp_path / "first"
    second = tmp_path / "second"
    _write_session(first)
    _write_session(second)
    _assert_trees_identical(first, second)


def test_decimals_are_exact_on_read_back(tmp_path: Path) -> None:
    root = tmp_path / "data"
    writer = ParquetWriter(root)
    writer.write(
        "trades",
        [_trade(0, exchange_ts_ns=_HOUR_10, price="0.00006120", size="12345.678901234567890123")],
    )
    writer.close()
    table = pq.read_table(_files(_partition(root, "trades"))[0])
    prices = table.column("price").to_pylist()
    sizes = table.column("size").to_pylist()
    assert prices == [Decimal("0.00006120")]
    assert str(prices[0]) == "0.000061200000000000"
    assert sizes == [Decimal("12345.678901234567890123")]


class _FakeClock:
    """Deterministic wall clock for the idle-flush tests."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_flush_idle_with_fake_clock(tmp_path: Path) -> None:
    root = tmp_path / "data"
    clock = _FakeClock()
    writer = ParquetWriter(root, idle_flush_s=10, clock=clock)
    writer.write("trades", [_trade(0, exchange_ts_ns=_HOUR_10)])
    clock.now = 9.999
    writer.flush_idle()
    assert not _partition(root, "trades").exists()
    clock.now = 10.0
    writer.flush_idle()
    assert [_row_count(path) for path in _files(_partition(root, "trades"))] == [1]
    writer.close()


def test_idle_flush_triggered_by_a_later_write(tmp_path: Path) -> None:
    root = tmp_path / "data"
    clock = _FakeClock()
    writer = ParquetWriter(root, idle_flush_s=10, clock=clock)
    writer.write("trades", [_trade(0, exchange_ts_ns=_HOUR_10)])
    clock.now = 11.0
    writer.write("trades", [_trade(1, exchange_ts_ns=_HOUR_10 + 3_600_000_000_000)])
    assert [_row_count(path) for path in _files(_partition(root, "trades", hour="10"))] == [1]
    assert not _partition(root, "trades", hour="11").exists()
    writer.close()


def test_write_after_close_raises_and_close_is_idempotent(tmp_path: Path) -> None:
    writer = ParquetWriter(tmp_path / "data")
    writer.write("trades", [_trade(0)])
    writer.close()
    writer.close()
    with pytest.raises(RuntimeError):
        writer.write("trades", [_trade(1)])


def test_writes_only_under_norm(tmp_path: Path) -> None:
    root = tmp_path / "data"
    writer = ParquetWriter(root)
    writer.write("trades", [_trade(index, exchange_ts_ns=_HOUR_10 + index) for index in range(3)])
    writer.close()
    files = [path for path in root.rglob("*") if path.is_file()]
    assert files
    assert all(path.is_relative_to(root / "norm") for path in files)
    assert not any(path.name.endswith(".tmp") for path in files)


_SIGNAL_CHILD = """\
import sys
import time
from decimal import Decimal
from pathlib import Path

from tickvault.models import Trade
from tickvault.storage.parquet_writer import ParquetWriter, install_signal_flush

root = Path(sys.argv[1])
writer = ParquetWriter(root)
for index in range(5):
    writer.write(
        "trades",
        [
            Trade(
                venue="kraken",
                symbol="BTC-USD",
                exchange_ts_ns=1_000 + index,
                recv_ts_ns=2_000 + index,
                connection_id="kraken-signal-test",
                ingest_seq=index,
                trade_id=str(index),
                price=Decimal("1"),
                size=Decimal("2"),
                side="buy",
            )
        ],
    )
install_signal_flush(writer)
print("ready", flush=True)
while True:
    time.sleep(0.05)
"""


def test_sigterm_closes_writer_before_exiting(tmp_path: Path) -> None:
    root = tmp_path / "data"
    process = subprocess.Popen(
        [sys.executable, "-c", _SIGNAL_CHILD, str(root)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    returncode = None
    try:
        assert process.stdout is not None
        if process.stdout.readline().strip() != "ready":
            stderr = process.stderr.read() if process.stderr is not None else ""
            pytest.fail(f"child did not start: {stderr}")
        process.send_signal(signal.SIGTERM)
        returncode = process.wait(timeout=30)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)
    assert returncode == -signal.SIGTERM
    partition = root / "norm" / "trades" / "venue=kraken" / "date=1970-01-01" / "hour=00"
    assert [_row_count(path) for path in _files(partition)] == [5]
