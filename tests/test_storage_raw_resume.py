"""Raw writer resume: continue beside an earlier process's files.

A writer restarted in the same data root finds the previous process's raw
files for a connection and receive hour already on disk. Raw files are created
exclusively and never reopened, so the default writer refuses to write there;
``RawWriter(root, resume=True)`` steps past the rotations that already exist
and leaves those files untouched. The restarted writer's records land in the
next rotation, and reading the tree still returns every message in canonical
order.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest
import zstandard as zstd

from tickvault.models import RawMessage
from tickvault.paths import raw_file_path
from tickvault.rawio import RawReadStats, encode_line, iter_raw
from tickvault.storage.raw_writer import RawWriter

_CONNECTION = "kraken-20261015T031901Z-3fa2c1d0"
_BASE_NS = 1_793_491_200_000_000_000
"""2026-11-01T00:00:00Z, an hour boundary-friendly base."""


def _raw(*, ingest_seq: int, recv_ts_ns: int) -> RawMessage:
    return RawMessage(
        venue="kraken",
        connection_id=_CONNECTION,
        ingest_seq=ingest_seq,
        recv_ts_ns=recv_ts_ns,
        kind="msg",
        payload=f'{{"n":{ingest_seq}}}',
    )


def _messages(*, start: int, count: int, base_ns: int = _BASE_NS) -> list[RawMessage]:
    return [
        _raw(ingest_seq=start + index, recv_ts_ns=base_ns + (start + index) * 1_000)
        for index in range(count)
    ]


def _write(path: Path, messages: Sequence[RawMessage]) -> None:
    """Write ``messages`` as one closed raw file, like a finished writer run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle, zstd.ZstdCompressor().stream_writer(handle) as writer:
        for message in messages:
            writer.write(encode_line(message))


def _path(root: Path, recv_ts_ns: int, rotation: int) -> Path:
    return raw_file_path(root, "kraken", _CONNECTION, recv_ts_ns, rotation)


def test_default_writer_still_refuses_an_existing_file(tmp_path: Path) -> None:
    root = tmp_path / "data"
    with RawWriter(root) as writer:
        writer.write(_raw(ingest_seq=0, recv_ts_ns=_BASE_NS))

    with RawWriter(root) as writer, pytest.raises(FileExistsError):
        writer.write(_raw(ingest_seq=1, recv_ts_ns=_BASE_NS + 1_000))


def test_resume_continues_in_the_next_rotation(tmp_path: Path) -> None:
    root = tmp_path / "data"
    first = _messages(start=0, count=3)
    with RawWriter(root) as writer:
        for message in first:
            writer.write(message)
    original = _path(root, _BASE_NS, 0)
    original_bytes = original.read_bytes()

    resumed = _messages(start=3, count=3)
    with RawWriter(root, resume=True) as writer:
        for message in resumed:
            writer.write(message)

    # The earlier file is byte-for-byte what the earlier process left.
    assert original.read_bytes() == original_bytes
    second = _path(root, _BASE_NS, 1)
    assert second.exists()
    assert list(iter_raw(root)) == [*first, *resumed]


def test_resume_skips_every_used_rotation(tmp_path: Path) -> None:
    root = tmp_path / "data"
    for rotation in range(3):
        _write(_path(root, _BASE_NS, rotation), _messages(start=rotation, count=1))

    with RawWriter(root, resume=True) as writer:
        writer.write(_raw(ingest_seq=3, recv_ts_ns=_BASE_NS + 3_000))

    assert _path(root, _BASE_NS, 3).exists()
    read = list(iter_raw(root))
    assert [message.ingest_seq for message in read] == [0, 1, 2, 3]


def test_resume_on_a_fresh_root_starts_at_rotation_zero(tmp_path: Path) -> None:
    root = tmp_path / "data"

    with RawWriter(root, resume=True) as writer:
        writer.write(_raw(ingest_seq=0, recv_ts_ns=_BASE_NS))

    assert _path(root, _BASE_NS, 0).exists()
    assert not _path(root, _BASE_NS, 1).exists()


def test_resume_after_a_tail_left_by_a_crashed_process(tmp_path: Path) -> None:
    """A crashed process leaves a zstd frame without its end marker.

    The writer's streaming compressor emits block flushes, so everything it
    wrote is decodable; only the frame's end marker is missing. The resumed
    writer must leave that file exactly as it is and start the next rotation.
    """
    root = tmp_path / "data"
    path = _path(root, _BASE_NS, 0)
    path.parent.mkdir(parents=True, exist_ok=True)
    crashed = _messages(start=0, count=2)
    handle = path.open("xb")
    stream = zstd.ZstdCompressor().stream_writer(handle, closefd=False)
    for message in crashed:
        stream.write(encode_line(message))
        stream.flush(zstd.FLUSH_BLOCK)
    handle.close()
    crashed_bytes = path.read_bytes()

    resumed = _messages(start=2, count=1)
    with RawWriter(root, resume=True) as writer:
        writer.write(resumed[0])

    # The crashed file is untouched: no end marker was added, no byte rewritten.
    assert path.read_bytes() == crashed_bytes
    assert _path(root, _BASE_NS, 1).exists()
    stats = RawReadStats()
    assert list(iter_raw(root, stats=stats)) == [*crashed, *resumed]
    assert stats.duplicates == 0
    assert stats.bad_lines == 0


def test_resume_starts_a_new_hour_at_rotation_zero(tmp_path: Path) -> None:
    root = tmp_path / "data"
    next_hour_ns = _BASE_NS + 3_600_000_000_000
    _write(_path(root, _BASE_NS, 0), _messages(start=0, count=1))
    _write(_path(root, _BASE_NS, 1), _messages(start=1, count=1))

    with RawWriter(root, resume=True) as writer:
        writer.write(_raw(ingest_seq=2, recv_ts_ns=next_hour_ns))

    # Rotations live inside the receive-hour directory, so the new hour's
    # first file is rotation zero even though the earlier hour used two.
    assert _path(root, next_hour_ns, 0).exists()
    assert not _path(root, next_hour_ns, 1).exists()
    assert [message.ingest_seq for message in iter_raw(root)] == [0, 1, 2]


def test_resume_rotates_within_the_hour_like_a_fresh_writer(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write(_path(root, _BASE_NS, 0), _messages(start=0, count=1))
    next_hour_ns = _BASE_NS + 3_600_000_000_000
    messages = [
        _raw(ingest_seq=1, recv_ts_ns=_BASE_NS + 1_000),
        _raw(ingest_seq=2, recv_ts_ns=next_hour_ns),
    ]

    with RawWriter(root, resume=True) as writer:
        for message in messages:
            writer.write(message)

    assert _path(root, _BASE_NS, 1).exists()
    assert _path(root, next_hour_ns, 2).exists()
    assert [message.ingest_seq for message in iter_raw(root)] == [0, 1, 2]
