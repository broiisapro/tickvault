"""Tests for the append-only raw writer (issue #25)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import zstandard as zstd

from tickvault.models import RawMessage
from tickvault.paths import raw_file_path
from tickvault.rawio import RawReadStats, encode_line, iter_raw
from tickvault.storage.raw_writer import RawWriter
from tickvault.timeutil import parse_rfc3339_ns

_CONNECTION = "kraken-20261015T031901Z-3fa2c1d0"
_BASE_NS = parse_rfc3339_ns("2026-10-15T10:00:00Z")


def _raw(
    *,
    ingest_seq: int,
    recv_ts_ns: int,
    connection_id: str = _CONNECTION,
    venue: str = "kraken",
    payload: str | None = None,
) -> RawMessage:
    return RawMessage(
        venue=venue,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        recv_ts_ns=recv_ts_ns,
        kind="msg",
        payload=payload if payload is not None else f'{{"n":{ingest_seq}}}',
    )


def _decompress_single_frame(path: Path) -> bytes:
    with path.open("rb") as handle:
        return zstd.ZstdDecompressor().stream_reader(handle).read()


def test_write_roundtrip_byte_identical_and_single_frame(tmp_path: Path) -> None:
    messages = [
        _raw(ingest_seq=0, recv_ts_ns=_BASE_NS, payload='{"text":"héllo \\"q\\" 你好"}'),
        _raw(ingest_seq=1, recv_ts_ns=_BASE_NS + 1, payload="{}"),
        _raw(ingest_seq=2, recv_ts_ns=_BASE_NS + 2, payload="[]"),
    ]
    root = tmp_path / "data"
    with RawWriter(root) as writer:
        for message in messages:
            writer.write(message)
    assert list(iter_raw(root)) == messages
    path = raw_file_path(root, "kraken", _CONNECTION, _BASE_NS, 0)
    assert _decompress_single_frame(path) == b"".join(encode_line(m) for m in messages)


def test_hourly_rotation_at_midnight(tmp_path: Path) -> None:
    root = tmp_path / "data"
    before = parse_rfc3339_ns("2026-10-15T23:59:59.999000000Z")
    after = parse_rfc3339_ns("2026-10-16T00:00:00.000000000Z")
    messages = [
        _raw(ingest_seq=0, recv_ts_ns=before),
        _raw(ingest_seq=1, recv_ts_ns=before + 1_000),
        _raw(ingest_seq=2, recv_ts_ns=after),
        _raw(ingest_seq=3, recv_ts_ns=after + 1_000),
    ]
    with RawWriter(root) as writer:
        for message in messages:
            writer.write(message)
    first = (
        root
        / "raw"
        / "venue=kraken"
        / "date=2026-10-15"
        / "hour=23"
        / f"{_CONNECTION}-00000.jsonl.zst"
    )
    second = (
        root
        / "raw"
        / "venue=kraken"
        / "date=2026-10-16"
        / "hour=00"
        / f"{_CONNECTION}-00001.jsonl.zst"
    )
    assert first.is_file()
    assert second.is_file()
    assert list(iter_raw(first)) == messages[:2]
    assert list(iter_raw(second)) == messages[2:]
    assert list(iter_raw(root)) == messages


def test_hourly_rotation_within_a_day(tmp_path: Path) -> None:
    root = tmp_path / "data"
    before = parse_rfc3339_ns("2026-10-15T10:59:59Z")
    after = parse_rfc3339_ns("2026-10-15T11:00:00Z")
    with RawWriter(root) as writer:
        writer.write(_raw(ingest_seq=0, recv_ts_ns=before))
        writer.write(_raw(ingest_seq=1, recv_ts_ns=after))
    assert raw_file_path(root, "kraken", _CONNECTION, before, 0).is_file()
    assert raw_file_path(root, "kraken", _CONNECTION, after, 1).is_file()


def test_exclusive_create_raises_on_existing_path(tmp_path: Path) -> None:
    root = tmp_path / "data"
    path = raw_file_path(root, "kraken", _CONNECTION, _BASE_NS, 0)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"pre-existing bytes")
    with RawWriter(root) as writer, pytest.raises(FileExistsError):
        writer.write(_raw(ingest_seq=0, recv_ts_ns=_BASE_NS))
    assert path.read_bytes() == b"pre-existing bytes"


def test_auto_flush_once_per_second_of_recv_time(tmp_path: Path) -> None:
    root = tmp_path / "data"
    path = raw_file_path(root, "kraken", _CONNECTION, _BASE_NS, 0)
    writer = RawWriter(root)
    writer.write(_raw(ingest_seq=0, recv_ts_ns=_BASE_NS))
    writer.write(_raw(ingest_seq=1, recv_ts_ns=_BASE_NS + 999_000_000))
    writer.write(_raw(ingest_seq=2, recv_ts_ns=_BASE_NS + 1_001_000_000))
    writer.write(_raw(ingest_seq=3, recv_ts_ns=_BASE_NS + 1_002_000_000))
    mid_write_copy = tmp_path / "mid-write.jsonl.zst"
    mid_write_copy.write_bytes(path.read_bytes())
    stats = RawReadStats()
    recovered = list(iter_raw(mid_write_copy, stats=stats))
    # the message that crosses the one-second boundary is included in the
    # automatic flush; the two after it are still buffered in the compressor.
    assert [m.ingest_seq for m in recovered] == [0, 1, 2]
    assert stats.bad_lines == 0
    writer.close()


def test_crash_copy_is_readable_up_to_flush(tmp_path: Path) -> None:
    root = tmp_path / "data"
    path = raw_file_path(root, "kraken", _CONNECTION, _BASE_NS, 0)
    writer = RawWriter(root)
    flushed = [_raw(ingest_seq=index, recv_ts_ns=_BASE_NS + index) for index in range(5)]
    for message in flushed:
        writer.write(message)
    writer.flush()
    writer.write(
        _raw(
            ingest_seq=5,
            recv_ts_ns=_BASE_NS + 500_000_000,
            payload='{"pad":"'
            + "".join(hashlib.sha256(f"line-{i}".encode()).hexdigest() for i in range(4000))
            + '"}',
        )
    )
    crash_copy = tmp_path / "crash-tail.jsonl.zst"
    crash_copy.write_bytes(path.read_bytes())
    stats = RawReadStats()
    assert list(iter_raw(crash_copy, stats=stats)) == flushed
    assert stats.bad_lines == 0
    assert stats.truncated_files + stats.truncated_lines >= 1
    writer.close()


def test_hour_change_rotates_even_backwards(tmp_path: Path) -> None:
    root = tmp_path / "data"
    first_hour = parse_rfc3339_ns("2026-10-15T10:59:00Z")
    second_hour = parse_rfc3339_ns("2026-10-15T11:01:00Z")
    with RawWriter(root) as writer:
        writer.write(_raw(ingest_seq=0, recv_ts_ns=first_hour))
        writer.write(_raw(ingest_seq=1, recv_ts_ns=second_hour))
        writer.write(_raw(ingest_seq=2, recv_ts_ns=first_hour + 1))
    assert raw_file_path(root, "kraken", _CONNECTION, first_hour, 0).is_file()
    assert raw_file_path(root, "kraken", _CONNECTION, second_hour, 1).is_file()
    assert raw_file_path(root, "kraken", _CONNECTION, first_hour + 1, 2).is_file()


def test_multiple_connections_interleaved(tmp_path: Path) -> None:
    root = tmp_path / "data"
    connection_a = "kraken-20261015T031901Z-0000000a"
    connection_b = "coinbase-20261015T031902Z-0000000b"
    messages = [
        _raw(ingest_seq=0, recv_ts_ns=_BASE_NS + 3, connection_id=connection_a),
        _raw(ingest_seq=0, recv_ts_ns=_BASE_NS + 1, connection_id=connection_b, venue="coinbase"),
        _raw(ingest_seq=1, recv_ts_ns=_BASE_NS + 4, connection_id=connection_a),
        _raw(ingest_seq=1, recv_ts_ns=_BASE_NS + 2, connection_id=connection_b, venue="coinbase"),
    ]
    with RawWriter(root) as writer:
        for message in messages:
            writer.write(message)
    assert list(iter_raw(root)) == [
        messages[1],
        messages[3],
        messages[0],
        messages[2],
    ]
    assert len(list(root.rglob("*.jsonl.zst"))) == 2


def test_close_is_idempotent_and_write_after_close_raises(tmp_path: Path) -> None:
    writer = RawWriter(tmp_path / "data")
    writer.write(_raw(ingest_seq=0, recv_ts_ns=_BASE_NS))
    writer.close()
    writer.close()
    writer.flush()
    with pytest.raises(RuntimeError):
        writer.write(_raw(ingest_seq=1, recv_ts_ns=_BASE_NS + 1))
