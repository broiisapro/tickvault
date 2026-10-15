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
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import zstandard as zstd

from tickvault.models import RawKind, RawMessage

_READ_CHUNK = 1 << 20


@dataclass
class RawReadStats:
    """Counters collected while reading raw files."""

    files: int = 0
    messages: int = 0
    truncated_files: int = 0
    truncated_lines: int = 0
    bad_lines: int = 0


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


def iter_raw(
    path_or_dir: str | Path, *, stats: RawReadStats | None = None
) -> Iterator[RawMessage]:
    """Yield raw messages from one ``.jsonl.zst`` file or a raw tree.

    Files are read fully, then messages are yielded in canonical order
    ``(recv_ts_ns, connection_id, ingest_seq)`` across all of them. A
    truncated final line or frame is skipped and counted in ``stats`` instead
    of raised. Pass a :class:`RawReadStats` instance to observe counters.
    """
    stats = RawReadStats() if stats is None else stats
    path = Path(path_or_dir)
    if not path.exists():
        raise FileNotFoundError(path)
    files = [path] if path.is_file() else sorted(path.rglob("*.jsonl.zst"))
    messages: list[RawMessage] = []
    for file_path in files:
        stats.files += 1
        messages.extend(_iter_file(file_path, stats))
    messages.sort(key=_sort_key)
    for message in messages:
        stats.messages += 1
        yield message


def _iter_file(path: Path, stats: RawReadStats) -> Iterator[RawMessage]:
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


def _drain_lines(buffer: bytearray, stats: RawReadStats) -> Iterator[RawMessage]:
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
