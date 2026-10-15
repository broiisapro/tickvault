"""Streaming-reader equivalence: old materialize+sort vs the k-way merge (issue #51).

The reference reader below is a verbatim copy of the pre-#51 implementation:
read every file into a list, then stable-sort the concatenation by
``(recv_ts_ns, connection_id, ingest_seq)``. The streaming reader must produce
the same messages, the same order and the same :class:`RawReadStats` on every
committed fixture and on a multi-file tree with deliberate ties.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator, Sequence
from pathlib import Path

import pytest
import zstandard as zstd

from tickvault.models import RawMessage
from tickvault.rawio import RawReadStats, decode_line, encode_line, iter_raw

_READ_CHUNK = 1 << 20


def _reference_drain(buffer: bytearray, stats: RawReadStats) -> Iterator[RawMessage]:
    while True:
        newline = buffer.find(b"\n")
        if newline < 0:
            return
        line = bytes(buffer[:newline])
        del buffer[: newline + 1]
        try:
            yield decode_line(line)
        except ValueError:
            stats.bad_lines += 1


def _reference_iter_file(path: Path, stats: RawReadStats) -> Iterator[RawMessage]:
    buffer = bytearray()
    truncated_frame = False
    with path.open("rb") as handle:
        try:
            with contextlib.closing(zstd.ZstdDecompressor().stream_reader(handle)) as reader:
                while True:
                    try:
                        chunk = reader.read(_READ_CHUNK)
                    except zstd.ZstdError:
                        truncated_frame = True
                        break
                    if not chunk:
                        break
                    buffer.extend(chunk)
                    yield from _reference_drain(buffer, stats)
        except zstd.ZstdError:
            truncated_frame = True
    if truncated_frame:
        stats.truncated_files += 1
    if buffer:
        stats.truncated_lines += 1


def _reference_iter_raw(path_or_dir: str | Path) -> tuple[list[RawMessage], RawReadStats]:
    stats = RawReadStats()
    path = Path(path_or_dir)
    files = [path] if path.is_file() else sorted(path.rglob("*.jsonl.zst"))
    messages: list[RawMessage] = []
    for file_path in files:
        stats.files += 1
        messages.extend(_reference_iter_file(file_path, stats))
    messages.sort(
        key=lambda message: (message.recv_ts_ns, message.connection_id, message.ingest_seq)
    )
    stats.messages = len(messages)
    return messages, stats


def _raw(
    *,
    connection_id: str,
    ingest_seq: int,
    recv_ts_ns: int,
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


def _write_raw(path: Path, messages: Sequence[RawMessage], *, tail: bytes = b"") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle, zstd.ZstdCompressor().stream_writer(handle) as writer:
        for message in messages:
            writer.write(encode_line(message))
        if tail:
            writer.write(tail)


FIXTURE_NAMES = (
    "coinbase_btc_5m.jsonl.zst",
    "kraken_btc_5m.jsonl.zst",
    "synthetic_gap_session.jsonl.zst",
)


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_streaming_reader_matches_reference_on_fixtures(fixtures_dir: Path, name: str) -> None:
    path = fixtures_dir / name
    expected_messages, expected_stats = _reference_iter_raw(path)
    stats = RawReadStats()
    assert list(iter_raw(path, stats=stats)) == expected_messages
    assert stats == expected_stats


def test_streaming_reader_matches_reference_on_multi_file_tree(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    connection_a = "kraken-20261015T031901Z-0000000a"
    connection_b = "coinbase-20261015T031902Z-0000000b"
    connection_c = "kraken-20261015T041901Z-0000000c"
    files = {
        root / "venue=kraken" / "date=2026-10-15" / "hour=03" / "a-00000.jsonl.zst": [
            _raw(connection_id=connection_a, ingest_seq=2, recv_ts_ns=300),
            _raw(connection_id=connection_a, ingest_seq=0, recv_ts_ns=100, venue="kraken"),
            _raw(connection_id=connection_b, ingest_seq=5, recv_ts_ns=100, venue="coinbase"),
        ],
        root / "venue=coinbase" / "date=2026-10-15" / "hour=03" / "b-00000.jsonl.zst": [
            _raw(connection_id=connection_b, ingest_seq=1, recv_ts_ns=200, venue="coinbase"),
            _raw(connection_id=connection_b, ingest_seq=0, recv_ts_ns=100, venue="coinbase"),
            _raw(connection_id=connection_a, ingest_seq=1, recv_ts_ns=100),
            _raw(connection_id=connection_c, ingest_seq=7, recv_ts_ns=150),
        ],
        root / "venue=kraken" / "date=2026-10-15" / "hour=03" / "c-00000.jsonl.zst": [
            _raw(connection_id=connection_c, ingest_seq=1, recv_ts_ns=200),
            _raw(connection_id=connection_c, ingest_seq=0, recv_ts_ns=50),
            _raw(connection_id=connection_c, ingest_seq=7, recv_ts_ns=150),
            _raw(connection_id=connection_a, ingest_seq=3, recv_ts_ns=400),
        ],
    }
    for path, messages in files.items():
        _write_raw(path, messages)

    expected_messages, expected_stats = _reference_iter_raw(root)
    stats = RawReadStats()
    assert list(iter_raw(root, stats=stats)) == expected_messages
    assert stats == expected_stats
    # Ties keep the sorted file order, exactly like the old global stable sort
    # on the concatenation: the exact duplicate (c, 7, 150) appears in file
    # "b-00000" (read first) before file "c-00000".
    assert [(m.recv_ts_ns, m.connection_id, m.ingest_seq) for m in expected_messages] == [
        (50, connection_c, 0),
        (100, connection_b, 0),
        (100, connection_b, 5),
        (100, connection_a, 0),
        (100, connection_a, 1),
        (150, connection_c, 7),
        (150, connection_c, 7),
        (200, connection_b, 1),
        (200, connection_c, 1),
        (300, connection_a, 2),
        (400, connection_a, 3),
    ]


def test_streaming_reader_counts_truncated_tail_like_the_reference(tmp_path: Path) -> None:
    path = tmp_path / "part.jsonl.zst"
    messages = [
        _raw(connection_id="c-1", ingest_seq=index, recv_ts_ns=100 + index)
        for index in range(3)
    ]
    _write_raw(path, messages, tail=b'{"v":1,"venue":"kraken","connection')
    reference_messages, reference_stats = _reference_iter_raw(path)
    stats = RawReadStats()
    assert list(iter_raw(path, stats=stats)) == reference_messages
    assert stats == reference_stats
    assert stats.truncated_lines == 1
    assert stats.bad_lines == 0


def test_streaming_reader_missing_path_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        list(iter_raw(tmp_path / "does-not-exist"))
