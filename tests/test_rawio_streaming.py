"""Streaming-reader equivalence and bounded memory (issues #51, #77).

The reference reader below is a verbatim copy of the pre-#51 implementation:
read every file into a list, then stable-sort the concatenation by
``(recv_ts_ns, connection_id, ingest_seq)``. The streaming reader must produce
the same messages, the same order and the same :class:`RawReadStats` on every
committed fixture, on a multi-file tree with deliberate ties, and on shuffled
line orders that force the external-sort fallback. In-order files must stream
with memory that does not grow with the number of lines, and the fallback must
clean up its temporary run files even when the consumer stops early.
"""

from __future__ import annotations

import contextlib
import gc
import random
import tempfile
import tracemalloc
from collections.abc import Iterator, Sequence
from pathlib import Path

import pytest
import zstandard as zstd
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tickvault import rawio
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


def _canonical_key(message: RawMessage) -> tuple[int, str, int]:
    return (message.recv_ts_ns, message.connection_id, message.ingest_seq)


_MESSAGE_DATA = st.tuples(
    st.integers(min_value=0, max_value=8),
    st.sampled_from(["conn-a", "conn-b", "conn-c"]),
    st.integers(min_value=0, max_value=4),
)


def _messages_from_data(values: Sequence[tuple[int, str, int]]) -> list[RawMessage]:
    return [
        _raw(
            connection_id=connection_id,
            ingest_seq=ingest_seq,
            recv_ts_ns=recv_ts_ns,
            payload=f'{{"i":{index}}}',
        )
        for index, (recv_ts_ns, connection_id, ingest_seq) in enumerate(values)
    ]


# Each input overwrites the same two file paths, so reusing the pytest
# tmp_path across examples is intentional.
@settings(
    derandomize=True,
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    values=st.lists(_MESSAGE_DATA, min_size=1, max_size=25),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
)
def test_shuffled_line_orders_are_sorted_exactly(
    values: list[tuple[int, str, int]], seed: int, tmp_path: Path
) -> None:
    messages = _messages_from_data(values)
    shuffled = list(messages)
    random.Random(seed).shuffle(shuffled)
    path = tmp_path / "shuffled.jsonl.zst"
    _write_raw(path, shuffled)
    expected, expected_stats = _reference_iter_raw(path)
    stats = RawReadStats()
    assert list(iter_raw(path, stats=stats)) == expected
    assert stats == expected_stats

    # The exact reverse of the canonical order always contains an inversion
    # when more than one distinct key is drawn, so it always forces the
    # external-sort fallback.
    reversed_messages = sorted(messages, key=_canonical_key, reverse=True)
    reversed_path = tmp_path / "reversed.jsonl.zst"
    _write_raw(reversed_path, reversed_messages)
    expected_reversed, _ = _reference_iter_raw(reversed_path)
    assert list(iter_raw(reversed_path)) == expected_reversed


def _write_ordered(path: Path, count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle, zstd.ZstdCompressor().stream_writer(handle) as writer:
        for index in range(count):
            writer.write(
                encode_line(
                    _raw(
                        connection_id="kraken-memory",
                        ingest_seq=index,
                        recv_ts_ns=1_000_000_000 + index,
                    )
                )
            )


def _iteration_peak_bytes(path: Path) -> int:
    gc.collect()
    tracemalloc.start()
    try:
        for _ in iter_raw(path):
            pass
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return peak


def test_in_order_file_streams_with_memory_independent_of_size(tmp_path: Path) -> None:
    small = tmp_path / "small.jsonl.zst"
    large = tmp_path / "large.jsonl.zst"
    _write_ordered(small, 50_000)
    _write_ordered(large, 200_000)

    small_peak = _iteration_peak_bytes(small)
    large_peak = _iteration_peak_bytes(large)

    # A bounded-memory reader's peak does not grow with the line count; the
    # ratio catches per-file materialization while tolerating allocator noise.
    assert large_peak < small_peak * 1.5
    assert large_peak < 32 * 1024 * 1024


def test_fallback_spills_bounded_runs_and_removes_temporaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(rawio, "_SORT_RUN_LINES", 4)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_root))

    path = tmp_path / "shuffled.jsonl.zst"
    messages = [
        _raw(connection_id="kraken-run", ingest_seq=index, recv_ts_ns=value)
        for index, value in enumerate([5, 1, 4, 0, 3, 9, 2, 8, 6, 7, 10, 11, 12])
    ]
    _write_raw(path, messages)
    expected, expected_stats = _reference_iter_raw(path)

    stats = RawReadStats()
    assert list(iter_raw(path, stats=stats)) == expected
    assert stats == expected_stats
    assert list(temp_root.iterdir()) == []

    stream = iter_raw(path)
    assert next(stream) == expected[0]
    stream.close()
    assert list(temp_root.iterdir()) == []


def test_early_close_releases_fallback_temporaries_on_multi_file_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Cleanup must not depend on the garbage collector: on CPython 3.12.3 the
    # abandoned per-file generators are not finalized when the consumer closes
    # the outer iterator, so iter_raw has to close them itself.
    monkeypatch.setattr(rawio, "_SORT_RUN_LINES", 4)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_root))

    root = tmp_path / "raw"
    _write_raw(
        root / "a-ordered.jsonl.zst",
        [
            _raw(connection_id="kraken-a", ingest_seq=index, recv_ts_ns=100 + index)
            for index in range(6)
        ],
    )
    _write_raw(
        root / "b-unordered.jsonl.zst",
        [
            _raw(connection_id="kraken-b", ingest_seq=index, recv_ts_ns=value)
            for index, value in enumerate([5, 1, 4, 0, 3, 2])
        ],
    )

    stream = iter_raw(root)
    next(stream)
    # heapq.merge primes every per-file stream on the first next(), so the
    # unordered file has already spilled its runs into a temporary directory.
    assert list(temp_root.iterdir()) != []
    stream.close()
    assert list(temp_root.iterdir()) == []
