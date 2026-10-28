"""Read and write the raw envelope format.

One line per message, UTF-8 JSON, keys in exactly this order::

    {"v":1,"venue":...,"connection_id":...,"ingest_seq":...,"recv_ts_ns":...,
     "kind":...,"payload":...}

with compact separators and a trailing newline. ``payload`` is the websocket
text frame stored as a JSON string, so the venue bytes survive unchanged.
Files are zstd-compressed (one file, one or more frames); a truncated final
line or frame is skipped and counted, never raised.
"""

from __future__ import annotations

import contextlib
import heapq
import json
import tempfile
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from pathlib import Path

import zstandard as zstd

from tickvault.models import RawKind, RawMessage

_READ_CHUNK = 1 << 20

_SORT_RUN_LINES = 65_536
"""Messages held in memory per external-sort run before the run is spilled."""


@dataclass
class RawReadStats:
    """Counters collected while reading raw files.

    ``duplicates`` counts lines skipped because they repeat the previously
    yielded line byte for byte under the same canonical key; ``conflicts``
    counts lines that share that key but carry different bytes and are
    therefore yielded, not hidden.
    """

    files: int = 0
    messages: int = 0
    truncated_files: int = 0
    truncated_lines: int = 0
    bad_lines: int = 0
    duplicates: int = 0
    conflicts: int = 0


@dataclass(frozen=True, slots=True)
class _RawLine:
    """One envelope as stored, next to its decoded form.

    Duplicate detection compares the bytes actually on disk, so the decoded
    message alone is not enough: two lines can decode to the same message and
    still differ in whitespace or key order.
    """

    message: RawMessage
    raw: bytes


def encode_line(message: RawMessage) -> bytes:
    """Encode one raw envelope line, including the trailing newline.

    Key order, compact separators and non-ASCII preservation are part of the
    on-disk format and are covered by tests.
    """
    document = {
        "v": 1,
        "venue": message.venue,
        "connection_id": message.connection_id,
        "ingest_seq": message.ingest_seq,
        "recv_ts_ns": message.recv_ts_ns,
        "kind": message.kind,
        "payload": message.payload,
    }
    return json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"


def decode_line(line: bytes) -> RawMessage:
    """Decode one raw envelope line into a :class:`RawMessage`.

    Raises :class:`ValueError` on malformed or unsupported input.
    """
    try:
        document = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid raw envelope JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("raw envelope must be a JSON object")
    if document.get("v") != 1:
        raise ValueError(f"unsupported raw envelope version: {document.get('v')!r}")
    venue = document.get("venue")
    connection_id = document.get("connection_id")
    ingest_seq = document.get("ingest_seq")
    recv_ts_ns = document.get("recv_ts_ns")
    kind_raw = document.get("kind")
    payload = document.get("payload")
    if not isinstance(venue, str):
        raise ValueError("raw envelope 'venue' must be a string")
    if not isinstance(connection_id, str):
        raise ValueError("raw envelope 'connection_id' must be a string")
    if not isinstance(ingest_seq, int) or isinstance(ingest_seq, bool):
        raise ValueError("raw envelope 'ingest_seq' must be an integer")
    if not isinstance(recv_ts_ns, int) or isinstance(recv_ts_ns, bool):
        raise ValueError("raw envelope 'recv_ts_ns' must be an integer")
    if kind_raw not in ("msg", "control"):
        raise ValueError(f"raw envelope 'kind' must be 'msg' or 'control', got {kind_raw!r}")
    if not isinstance(payload, str):
        raise ValueError("raw envelope 'payload' must be a string")
    kind: RawKind = "msg" if kind_raw == "msg" else "control"
    return RawMessage(
        venue=venue,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        recv_ts_ns=recv_ts_ns,
        kind=kind,
        payload=payload,
    )


def _sort_key(message: RawMessage) -> tuple[int, str, int]:
    return (message.recv_ts_ns, message.connection_id, message.ingest_seq)


def _line_sort_key(line: _RawLine) -> tuple[int, str, int]:
    return _sort_key(line.message)


def iter_raw(
    path_or_dir: str | Path, *, stats: RawReadStats | None = None
) -> Iterator[RawMessage]:
    """Yield raw messages from one ``.jsonl.zst`` file or a raw tree.

    The reader is a streaming k-way merge: every file is decoded lazily by its
    own iterator, in the same sorted file order, and merged with
    :func:`heapq.merge`. Memory stays bounded by the number of files and one
    decode chunk, never by a file's size. A file whose lines arrive in
    canonical order ``(recv_ts_ns, connection_id, ingest_seq)`` (the normal
    case: one writer appends one connection's messages in order) is streamed
    straight out; a file containing an out-of-order line is sorted first with
    a bounded-memory external sort. Both per-file paths preserve the original
    order of equal keys, so the result is identical to materializing every
    message and stable-sorting the concatenation.

    At-least-once buffered delivery (decision A70) lets a writer restart
    repeat an envelope in the same file or in a later one. Because equal keys
    are adjacent in canonical order, a line is dropped as a duplicate when its
    canonical key and its stored bytes both equal the previously yielded
    line's; it is counted in ``stats.duplicates``. A line with the same key
    but different bytes is a conflict: it is yielded so the divergence stays
    visible and counted in ``stats.conflicts``. Only one line is kept for the
    comparison, so de-duplication does not materialize a file. A truncated
    final line or frame is skipped and counted in ``stats`` instead of raised.

    Pass a :class:`RawReadStats` instance to observe counters;
    ``stats.files`` counts files as they are opened and ``stats.messages``
    counts messages as they are yielded (duplicates are not yielded). A
    consumer that stops early closes the merge iterator and every per-file
    stream in a ``finally``, so file handles and the external-sort temporary
    directory are released immediately instead of when the garbage collector
    finalizes the abandoned generators.
    """
    stats = RawReadStats() if stats is None else stats
    path = Path(path_or_dir)
    if not path.exists():
        raise FileNotFoundError(path)
    files = [path] if path.is_file() else sorted(path.rglob("*.jsonl.zst"))
    streams = [_sorted_file(file_path, stats) for file_path in files]
    merged = heapq.merge(*streams, key=_line_sort_key)
    previous: _RawLine | None = None
    previous_key: tuple[int, str, int] | None = None
    try:
        for line in merged:
            key = _line_sort_key(line)
            if previous is not None and key == previous_key:
                if line.raw == previous.raw:
                    stats.duplicates += 1
                    continue
                stats.conflicts += 1
            previous = line
            previous_key = key
            stats.messages += 1
            yield line.message
    finally:
        merged.close()
        for stream in streams:
            stream.close()


def _sorted_file(path: Path, stats: RawReadStats) -> Generator[_RawLine, None, None]:
    """Yield one file's messages in canonical order with bounded memory.

    Whether the file is ordered has to be known before its first message is
    handed out — a message yielded early cannot be recalled when a later line
    turns out to be out of order — so the file is scanned first, keeping only
    the previous key. Ordered files are then streamed in a second pass that
    buffers nothing beyond the decode chunk; unordered files go through the
    bounded-memory external-sort fallback. ``stats`` is updated by the pass
    that yields messages, so every counter is incremented exactly once.
    """
    stats.files += 1
    if _file_is_ordered(path):
        yield from _iter_file(path, stats)
    else:
        yield from _sorted_file_via_runs(path, stats)


def _file_is_ordered(path: Path) -> bool:
    """True when every decodable line of ``path`` is in canonical key order.

    Stops at the first inversion. Malformed lines are ignored because they
    never reach the output, and a truncated tail is tolerated exactly like
    :func:`_iter_file`. The scan uses throwaway statistics so the real
    counters are only incremented by the pass that yields messages.
    """
    previous: tuple[int, str, int] | None = None
    for line in _iter_file(path, RawReadStats()):
        key = _line_sort_key(line)
        if previous is not None and key < previous:
            return False
        previous = key
    return True


def _sorted_file_via_runs(path: Path, stats: RawReadStats) -> Iterator[_RawLine]:
    """Sort ``path`` exactly: spill stable sorted runs, then merge them.

    The file is decoded once, in file order. Runs of at most
    ``_SORT_RUN_LINES`` messages are sorted stably and written to a temporary
    directory; :func:`heapq.merge` over the runs in spill order keeps equal
    keys in their original relative order, exactly like a single stable sort.
    The temporaries are removed when the iterator is exhausted or closed
    early. Decoding happens while the runs are built, so ``stats`` is
    incremented exactly once per file.
    """
    with tempfile.TemporaryDirectory(prefix="tickvault-rawio-") as directory:
        runs = _write_sorted_runs(path, stats, Path(directory))
        streams = [_iter_run(run_path) for run_path in runs]
        try:
            yield from heapq.merge(*streams, key=_line_sort_key)
        finally:
            for stream in streams:
                stream.close()


def _write_sorted_runs(path: Path, stats: RawReadStats, directory: Path) -> list[Path]:
    """Decode ``path`` once and spill its lines as stable sorted run files.

    The buffer keeps each line's canonical key and its stored bytes, not the
    decoded message, so a run holds at most ``_SORT_RUN_LINES`` lines of the
    file rather than two representations of them. Equal keys keep their
    original position through the stable sort.
    """
    runs: list[Path] = []
    buffer: list[tuple[tuple[int, str, int], bytes]] = []
    for line in _iter_file(path, stats):
        buffer.append((_line_sort_key(line), line.raw))
        if len(buffer) >= _SORT_RUN_LINES:
            runs.append(_spill_run(directory, len(runs), buffer))
            buffer = []
    if buffer:
        runs.append(_spill_run(directory, len(runs), buffer))
    return runs


def _entry_key(entry: tuple[tuple[int, str, int], bytes]) -> tuple[int, str, int]:
    return entry[0]


def _spill_run(
    directory: Path, index: int, entries: list[tuple[tuple[int, str, int], bytes]]
) -> Path:
    """Sort ``entries`` stably in place and write their stored lines as one run.

    The bytes are written verbatim — a complete line never contains a raw
    newline, because the reader splits on newlines — so a run file is an
    uncompressed copy of the file's lines in canonical order.
    """
    entries.sort(key=_entry_key)
    run_path = directory / f"run-{index:06d}.jsonl"
    with run_path.open("wb") as handle:
        for _, raw in entries:
            handle.write(raw)
            handle.write(b"\n")
    return run_path


def _iter_run(run_path: Path) -> Generator[_RawLine, None, None]:
    """Yield the lines of one spilled run file in its stored order."""
    with run_path.open("rb") as handle:
        for line in handle:
            raw = line.rstrip(b"\n")
            yield _RawLine(message=decode_line(raw), raw=raw)


def _iter_file(path: Path, stats: RawReadStats) -> Iterator[_RawLine]:
    buffer = bytearray()
    truncated_frame = False
    with path.open("rb") as handle:
        try:
            with contextlib.closing(
                zstd.ZstdDecompressor().stream_reader(handle)
            ) as reader:
                while True:
                    try:
                        chunk = reader.read(_READ_CHUNK)
                    except zstd.ZstdError:
                        truncated_frame = True
                        break
                    if not chunk:
                        break
                    buffer.extend(chunk)
                    yield from _drain_lines(buffer, stats)
        except zstd.ZstdError:
            truncated_frame = True
    if truncated_frame:
        stats.truncated_files += 1
    if buffer:
        stats.truncated_lines += 1


def _drain_lines(buffer: bytearray, stats: RawReadStats) -> Iterator[_RawLine]:
    while True:
        newline = buffer.find(b"\n")
        if newline < 0:
            return
        line = bytes(buffer[:newline])
        del buffer[: newline + 1]
        try:
            message = decode_line(line)
        except ValueError:
            stats.bad_lines += 1
            continue
        yield _RawLine(message=message, raw=line)
