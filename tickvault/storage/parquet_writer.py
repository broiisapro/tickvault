"""Deterministic Parquet writer for the normalized layer.

Records are buffered per ``(table, partition directory)``; the partition is
keyed by the exchange timestamp hour (:func:`tickvault.paths.partition_path`),
falling back to ``recv_ts_ns`` for events without an exchange time (decision
A5). A buffer is persisted when

- a newly written record's ``recv_ts_ns`` is at least ``flush_interval_ns``
  after the buffer's first record (the logical clock),
- the buffer reaches ``max_rows``,
- ``flush()`` or ``close()`` is called, or
- ``idle_flush_s`` is set (live only) and the buffer has been untouched for
  that many seconds of the injected wall clock.

Flushing never depends on wall-clock time unless ``idle_flush_s`` is set, so
replay and live ingestion of the same records produce identical files. Every
file is written to a temporary name in its destination directory and renamed
into place, so a crash cannot leave a half-written ``*.parquet``.

File names are ``part-<connection_id>-<first ingest_seq:012d>.parquet`` from
the buffer's first row. Should a second buffer of the same partition start on
the same row (only possible when a single message is split by ``max_rows``),
a ``-<n>`` suffix keeps the names unique; the suffix sequence is deterministic
because the write order is.
"""

from __future__ import annotations

import contextlib
import os
import signal
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType

import pyarrow.parquet as pq

from tickvault.models import NormalizedRecord, to_arrow
from tickvault.paths import partition_path

_ROW_GROUP_SIZE = 65_536
"""Fixed row-group size; part of the byte-reproducible on-disk format."""

type _BufferKey = tuple[str, Path]
"""``(table, partition directory)``."""


@dataclass(slots=True)
class _Buffer:
    """Records waiting to be written to one partition of one table."""

    key: _BufferKey
    first_connection_id: str
    first_ingest_seq: int
    first_recv_ts_ns: int
    last_touch: float
    records: list[NormalizedRecord] = field(default_factory=list)

    @property
    def partition(self) -> Path:
        return self.key[1]


class ParquetWriter:
    """Buffered Parquet writer implementing :class:`tickvault.storage.sink.Sink`.

    ``root`` is the data root; files are written under ``root/norm/`` only.
    ``clock`` is injected so tests can drive the idle flush; nothing else uses
    it. All other flush decisions are deterministic functions of the records.
    """

    def __init__(
        self,
        root: str | Path,
        flush_interval_ns: int = 60_000_000_000,
        max_rows: int = 200_000,
        idle_flush_s: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if flush_interval_ns < 0:
            raise ValueError("flush_interval_ns must be non-negative")
        if max_rows < 1:
            raise ValueError("max_rows must be at least 1")
        if idle_flush_s is not None and idle_flush_s <= 0:
            raise ValueError("idle_flush_s must be positive or None")
        self._root = Path(root)
        self._norm_root = (self._root / "norm").resolve()
        self._flush_interval_ns = flush_interval_ns
        self._max_rows = max_rows
        self._idle_flush_s = idle_flush_s
        self._clock = clock
        self._buffers: dict[_BufferKey, _Buffer] = {}
        self._closed = False

    def write(self, table: str, records: Sequence[NormalizedRecord]) -> None:
        """Buffer ``records`` for ``table``, flushing as the rules require.

        Raises :class:`RuntimeError` if the writer is closed and
        :class:`KeyError` if ``table`` is not a table of
        :data:`tickvault.models.TABLES`.
        """
        if self._closed:
            raise RuntimeError("ParquetWriter is closed")
        for record in records:
            buffer = self._buffer_for(table, record)
            buffer.records.append(record)
            if (
                len(buffer.records) >= self._max_rows
                or record.recv_ts_ns - buffer.first_recv_ts_ns >= self._flush_interval_ns
            ):
                self._flush(buffer)
        if self._idle_flush_s is not None:
            self.flush_idle()

    def flush(self) -> None:
        """Persist every buffered partition."""
        for buffer in list(self._buffers.values()):
            self._flush(buffer)

    def flush_idle(self) -> None:
        """Flush buffers untouched for ``idle_flush_s`` wall-clock seconds.

        No-op when ``idle_flush_s`` is ``None`` (replay); the live pipeline can
        call this from a timer when the feeds are quiet.
        """
        if self._idle_flush_s is None:
            return
        now = self._clock()
        for buffer in list(self._buffers.values()):
            if now - buffer.last_touch >= self._idle_flush_s:
                self._flush(buffer)

    def close(self) -> None:
        """Flush everything and mark the writer closed; idempotent."""
        if self._closed:
            return
        self.flush()
        self._closed = True

    def _buffer_for(self, table: str, record: NormalizedRecord) -> _Buffer:
        exchange_ts_ns = record.exchange_ts_ns
        ts_ns = record.recv_ts_ns if exchange_ts_ns is None else exchange_ts_ns
        partition = partition_path(self._root, table, record.venue, ts_ns)
        key = (table, partition)
        buffer = self._buffers.get(key)
        if buffer is None:
            buffer = _Buffer(
                key=key,
                first_connection_id=record.connection_id,
                first_ingest_seq=record.ingest_seq,
                first_recv_ts_ns=record.recv_ts_ns,
                last_touch=self._clock(),
            )
            self._buffers[key] = buffer
        else:
            buffer.last_touch = self._clock()
        return buffer

    def _flush(self, buffer: _Buffer) -> None:
        if not buffer.records:
            self._buffers.pop(buffer.key, None)
            return
        table = buffer.key[0]
        arrow_table = to_arrow(table, buffer.records)
        final = self._unique_path(buffer)
        if not final.resolve().is_relative_to(self._norm_root):
            raise RuntimeError(f"refusing to write outside {self._norm_root}: {final}")
        final.parent.mkdir(parents=True, exist_ok=True)
        temporary = final.parent / f"{final.name}.tmp"
        try:
            pq.write_table(
                arrow_table,
                temporary,
                compression="zstd",
                write_statistics=True,
                row_group_size=_ROW_GROUP_SIZE,
            )
            temporary.replace(final)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()
            raise
        self._buffers.pop(buffer.key, None)
        buffer.records = []

    def _unique_path(self, buffer: _Buffer) -> Path:
        stem = f"part-{buffer.first_connection_id}-{buffer.first_ingest_seq:012d}"
        candidate = buffer.partition / f"{stem}.parquet"
        suffix = 0
        while candidate.exists():
            suffix += 1
            candidate = buffer.partition / f"{stem}-{suffix}.parquet"
        return candidate


def install_signal_flush(writer: ParquetWriter) -> None:
    """Close ``writer`` on SIGTERM/SIGINT, then re-raise the default action.

    Must be called from the main thread. After the writer is closed the signal
    handler is restored to the OS default and the signal is re-sent, so the
    process exits with the conventional signal status instead of swallowing it.
    """

    def _handler(signum: int, frame: FrameType | None) -> None:
        writer.close()
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
