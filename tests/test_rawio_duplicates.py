"""Duplicate and conflicting raw lines (issue #124).

At-least-once buffered delivery commits offsets only after the raw writer has
flushed, so after a writer restart the raw layer can hold the same envelope
twice: in the same file (a redelivery wrote the line again) or in two files
(two writer runs). Reading drops a line when its canonical key
``(recv_ts_ns, connection_id, ingest_seq)`` and its stored bytes repeat the
previously yielded line, and keeps a same-key line whose bytes differ so the
conflict stays visible. Replaying a tree that contains exact duplicates must
therefore produce the same ``norm/`` tree as replaying the same messages once.

The three internal paths must agree: a file already in canonical order is
streamed straight out, a file with an inversion goes through the bounded
external-sort runs, and a tree is merged file by file.
"""

from __future__ import annotations

import gc
import json
import tracemalloc
from collections.abc import Iterator, Sequence
from pathlib import Path

import pytest
import zstandard as zstd

from tickvault import rawio
from tickvault.models import RawMessage
from tickvault.pipeline import run_replay
from tickvault.rawio import RawReadStats, encode_line, iter_raw

_CONNECTION_A = "kraken-20261015T031901Z-0000000a"
_CONNECTION_B = "kraken-20261015T031902Z-0000000b"


def _raw(
    *,
    connection_id: str = _CONNECTION_A,
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


def _write_lines(path: Path, lines: Sequence[bytes]) -> None:
    """Write raw envelope lines verbatim, compressed as one zstd stream."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle, zstd.ZstdCompressor().stream_writer(handle) as writer:
        for line in lines:
            writer.write(line)


def _key(message: RawMessage) -> tuple[int, str, int]:
    return (message.recv_ts_ns, message.connection_id, message.ingest_seq)


def _keys(messages: Sequence[RawMessage]) -> list[tuple[int, str, int]]:
    return [_key(message) for message in messages]


def _spy_on_runs(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Record every file that takes the external-sort path."""
    calls: list[Path] = []
    original = rawio._sorted_file_via_runs

    def _spy(path: Path, stats: RawReadStats) -> Iterator[rawio._RawLine]:
        calls.append(path)
        yield from original(path, stats)

    monkeypatch.setattr(rawio, "_sorted_file_via_runs", _spy)
    return calls


def _first() -> RawMessage:
    return _raw(connection_id=_CONNECTION_A, ingest_seq=0, recv_ts_ns=100)


def _repeat() -> RawMessage:
    return _raw(connection_id=_CONNECTION_A, ingest_seq=1, recv_ts_ns=200)


def _gap() -> RawMessage:
    return _raw(connection_id=_CONNECTION_A, ingest_seq=2, recv_ts_ns=300)


def _other() -> RawMessage:
    return _raw(connection_id=_CONNECTION_B, ingest_seq=0, recv_ts_ns=150)


def _other_next() -> RawMessage:
    return _raw(connection_id=_CONNECTION_B, ingest_seq=1, recv_ts_ns=250)


def _other_conflict() -> RawMessage:
    return _raw(connection_id=_CONNECTION_B, ingest_seq=1, recv_ts_ns=250, payload='{"n":1,"c":1}')


_EXPECTED_KEYS = [
    (100, _CONNECTION_A, 0),
    (150, _CONNECTION_B, 0),
    (200, _CONNECTION_A, 1),
    (250, _CONNECTION_B, 1),
    (250, _CONNECTION_B, 1),
    (300, _CONNECTION_A, 2),
]


def _scenario_lines() -> list[bytes]:
    """A file in canonical order with two exact repeats and one conflict."""
    return [
        encode_line(_first()),
        encode_line(_other()),
        encode_line(_other()),
        encode_line(_repeat()),
        encode_line(_repeat()),
        encode_line(_other_next()),
        encode_line(_other_conflict()),
        encode_line(_gap()),
    ]


def _assert_scenario(stats: RawReadStats, messages: Sequence[RawMessage]) -> None:
    assert _keys(messages) == _EXPECTED_KEYS
    assert stats.messages == 6
    assert stats.duplicates == 2
    assert stats.conflicts == 1


def test_ordered_fast_path_skips_same_file_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "ordered.jsonl.zst"
    _write_lines(path, _scenario_lines())
    runs = _spy_on_runs(monkeypatch)

    stats = RawReadStats()
    messages = list(iter_raw(path, stats=stats))

    _assert_scenario(stats, messages)
    assert stats.files == 1
    assert runs == []


def test_sorted_runs_path_skips_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(rawio, "_SORT_RUN_LINES", 3)
    path = tmp_path / "reversed.jsonl.zst"
    # The reverse of the canonical order always contains an inversion, so the
    # file always takes the external-sort path; three lines per run puts the
    # two copies of _other() in different runs.
    _write_lines(path, list(reversed(_scenario_lines())))
    runs = _spy_on_runs(monkeypatch)

    stats = RawReadStats()
    messages = list(iter_raw(path, stats=stats))

    _assert_scenario(stats, messages)
    assert runs == [path]
    # The conflict is yielded, both variants present, in run order.
    conflict_payloads = [
        message.payload
        for message in messages
        if _key(message) == (250, _CONNECTION_B, 1)
    ]
    assert conflict_payloads == ['{"n":1,"c":1}', '{"n":1}']


def test_k_way_merge_skips_cross_file_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "raw"
    first = root / "a-first.jsonl.zst"
    repeats = root / "b-repeats.jsonl.zst"
    rest = root / "c-rest.jsonl.zst"
    # Each file is in canonical order, so no file needs the sort fallback: the
    # duplicates only meet in the merge.
    _write_lines(first, [encode_line(_first()), encode_line(_repeat())])
    _write_lines(
        repeats,
        [
            encode_line(_other()),
            encode_line(_repeat()),
            encode_line(_other_next()),
            encode_line(_other_conflict()),
        ],
    )
    _write_lines(rest, [encode_line(_other()), encode_line(_gap())])
    runs = _spy_on_runs(monkeypatch)

    stats = RawReadStats()
    messages = list(iter_raw(root, stats=stats))

    _assert_scenario(stats, messages)
    assert stats.files == 3
    assert runs == []


def test_only_identical_bytes_are_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The same message can be stored in a different envelope formatting; it
    # decodes identically, but hiding it would hide a real conflict, so only
    # byte-identical lines are dropped.
    message = _raw(connection_id=_CONNECTION_A, ingest_seq=0, recv_ts_ns=100)
    canonical = encode_line(message)
    reformatted = (
        json.dumps(
            {
                "recv_ts_ns": message.recv_ts_ns,
                "ingest_seq": message.ingest_seq,
                "connection_id": message.connection_id,
                "venue": message.venue,
                "kind": message.kind,
                "payload": message.payload,
                "v": 1,
            }
        ).encode()
        + b"\n"
    )
    assert reformatted != canonical

    path = tmp_path / "formatted.jsonl.zst"
    _write_lines(path, [canonical, reformatted, canonical, canonical])
    runs = _spy_on_runs(monkeypatch)

    stats = RawReadStats()
    messages = list(iter_raw(path, stats=stats))

    # canonical, reformatted, then the two trailing canonical repeats are
    # dropped: one duplicate of the first line. The reformatted line is a
    # conflict against the first line, and the following canonical line is a
    # conflict against the reformatted one.
    assert len(messages) == 3
    assert stats.duplicates == 1
    assert stats.conflicts == 2
    assert stats.messages == 3
    assert runs == []


def test_interleaved_connections_never_collide(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = '{"n":0}'
    message_a = _raw(connection_id=_CONNECTION_A, ingest_seq=0, recv_ts_ns=100, payload=payload)
    message_b = _raw(connection_id=_CONNECTION_B, ingest_seq=0, recv_ts_ns=100, payload=payload)
    path = tmp_path / "interleaved.jsonl.zst"
    _write_lines(
        path,
        [
            encode_line(message_a),
            encode_line(message_a),
            encode_line(message_b),
            encode_line(message_b),
        ],
    )
    runs = _spy_on_runs(monkeypatch)

    stats = RawReadStats()
    messages = list(iter_raw(path, stats=stats))

    # The connection id is part of the key, so the two connections keep their
    # own line; only the identical repeat per connection is dropped.
    assert _keys(messages) == [(100, _CONNECTION_A, 0), (100, _CONNECTION_B, 0)]
    assert stats.messages == 2
    assert stats.duplicates == 2
    assert stats.conflicts == 0
    assert runs == []


def test_duplicates_in_first_and_later_files_are_both_handled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A restart writes the uncommitted tail of the first file again at the top
    # of the second file of the same connection; both copies sit in files that
    # are individually in order.
    root = tmp_path / "raw"
    first = root / "part-one.jsonl.zst"
    second = root / "part-two.jsonl.zst"
    messages = [
        _raw(connection_id=_CONNECTION_A, ingest_seq=index, recv_ts_ns=100 + index)
        for index in range(6)
    ]
    _write_lines(first, [encode_line(message) for message in messages[:4]])
    _write_lines(
        second,
        [encode_line(message) for message in [*messages[2:], messages[5]]],
    )
    runs = _spy_on_runs(monkeypatch)

    stats = RawReadStats()
    read = list(iter_raw(root, stats=stats))

    assert _keys(read) == [_key(message) for message in messages]
    assert stats.messages == len(messages)
    assert stats.duplicates == 3
    assert stats.conflicts == 0
    assert runs == []


def _tree_digest(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_replay_of_duplicated_tree_equals_replay_of_clean_tree(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    messages = list(iter_raw(synthetic_gap_path))
    assert messages

    clean_root = tmp_path / "clean" / "raw"
    duplicated_root = tmp_path / "duplicated" / "raw"
    clean_lines = [encode_line(message) for message in messages]
    _write_lines(clean_root / "part-00000.jsonl.zst", clean_lines)

    # Same messages, plus a same-file repeat of every fifth line and a second
    # file carrying repeats of every seventh line (the cross-file case).
    doubled: list[bytes] = []
    for index, line in enumerate(clean_lines):
        doubled.append(line)
        if index % 5 == 0:
            doubled.append(line)
    _write_lines(duplicated_root / "part-00000.jsonl.zst", doubled)
    _write_lines(
        duplicated_root / "part-00001.jsonl.zst",
        [line for index, line in enumerate(clean_lines) if index % 7 == 0],
    )
    expected_duplicates = len(range(0, len(clean_lines), 5)) + len(
        range(0, len(clean_lines), 7)
    )

    read_stats = RawReadStats()
    duplicated_messages = list(iter_raw(duplicated_root, stats=read_stats))
    assert duplicated_messages == messages
    assert read_stats.duplicates == expected_duplicates
    assert read_stats.conflicts == 0

    clean_stats = run_replay(clean_root, tmp_path / "out-clean")
    duplicated_stats = run_replay(duplicated_root, tmp_path / "out-duplicated")

    assert clean_stats.to_dict() == duplicated_stats.to_dict()
    clean_digest = _tree_digest(tmp_path / "out-clean" / "norm")
    assert clean_digest
    assert clean_digest == _tree_digest(tmp_path / "out-duplicated" / "norm")


def _write_repeated(path: Path, unique: int) -> None:
    """Write ``unique`` ordered messages, each stored twice in a row."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle, zstd.ZstdCompressor().stream_writer(handle) as writer:
        for index in range(unique):
            line = encode_line(
                _raw(connection_id="kraken-memory", ingest_seq=index, recv_ts_ns=1_000 + index)
            )
            writer.write(line)
            writer.write(line)


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


def test_duplicate_handling_keeps_memory_bounded(tmp_path: Path) -> None:
    small = tmp_path / "small.jsonl.zst"
    large = tmp_path / "large.jsonl.zst"
    _write_repeated(small, 20_000)
    _write_repeated(large, 80_000)

    small_peak = _iteration_peak_bytes(small)
    large_peak = _iteration_peak_bytes(large)

    # Only one line is retained for the byte comparison, so the peak does not
    # grow with the number of lines.
    assert large_peak < small_peak * 1.5
    assert large_peak < 32 * 1024 * 1024

    stats = RawReadStats()
    assert sum(1 for _ in iter_raw(large, stats=stats)) == 80_000
    assert stats.duplicates == 80_000
    assert stats.messages == 80_000
