"""Append-only raw writer with hourly rotation.

Every message is persisted before anything else processes it. One open file
per ``connection_id``; the path is derived from the message's ``recv_ts_ns``
hour (:func:`tickvault.paths.raw_file_path`). When a message's receive hour
differs from the open file's hour the file is closed and the next one opened
with ``rotation + 1``. Files are created with exclusive-create mode (``"xb"``)
and never reopened, so no code path can overwrite raw data.

Each file is a single zstd frame built by a streaming compressor.
:meth:`RawWriter.flush` emits a block flush (``FLUSH_BLOCK``), so everything
written up to that point is decodable even if the process dies with the frame
open; the writer also flushes automatically at least once per second of
``recv_ts_ns`` time. The reader tolerates the resulting truncated tail.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

import zstandard as zstd

from tickvault.models import RawMessage
from tickvault.paths import raw_file_path
from tickvault.rawio import encode_line

_AUTO_FLUSH_NS = 1_000_000_000
"""Auto-flush when logical (``recv_ts_ns``) time advances this far."""


def _close_stream(stream: zstd.ZstdCompressionWriter) -> None:
    """End the zstd frame and release the compression context."""
    with contextlib.closing(stream):
        pass


@dataclass(slots=True)
class _OpenFile:
    """One open raw file: a zstd stream over an exclusively-created handle."""

    path: Path
    handle: BinaryIO
    stream: zstd.ZstdCompressionWriter
    rotation: int
    last_flush_ns: int


class RawWriter:
    """Append-only writer for the raw layer.

    ``root`` is the data root; files land under ``root/raw/``. ``write`` opens
    the first file of a connection lazily; ``flush`` makes everything written
    so far decodable; ``close`` ends every frame and is idempotent.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._open_files: dict[str, _OpenFile] = {}
        self._closed = False

    def write(self, message: RawMessage) -> None:
        """Append ``message`` to its connection's file, rotating by hour.

        Raises :class:`FileExistsError` if the target path already exists and
        :class:`RuntimeError` if the writer is closed.
        """
        if self._closed:
            raise RuntimeError("RawWriter is closed")
        open_file = self._open_files.get(message.connection_id)
        if open_file is not None:
            current = raw_file_path(
                self._root,
                message.venue,
                message.connection_id,
                message.recv_ts_ns,
                open_file.rotation,
            )
            if current != open_file.path:
                open_file = self._rotate(message, open_file)
        else:
            open_file = self._open(message)
        open_file.stream.write(encode_line(message))
        if message.recv_ts_ns - open_file.last_flush_ns >= _AUTO_FLUSH_NS:
            open_file.stream.flush(zstd.FLUSH_BLOCK)
            open_file.last_flush_ns = message.recv_ts_ns

    def flush(self) -> None:
        """Block-flush every open file so everything written so far is decodable."""
        for open_file in self._open_files.values():
            open_file.stream.flush(zstd.FLUSH_BLOCK)

    def close(self) -> None:
        """End every frame and close every file; idempotent."""
        if self._closed:
            return
        for open_file in self._open_files.values():
            _close_stream(open_file.stream)
            open_file.handle.close()
        self._open_files.clear()
        self._closed = True

    def __enter__(self) -> RawWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _open(self, message: RawMessage) -> _OpenFile:
        return self._open_rotated(message, rotation=0)

    def _rotate(self, message: RawMessage, previous: _OpenFile) -> _OpenFile:
        del self._open_files[message.connection_id]
        _close_stream(previous.stream)
        previous.handle.close()
        return self._open_rotated(message, rotation=previous.rotation + 1)

    def _open_rotated(self, message: RawMessage, *, rotation: int) -> _OpenFile:
        path = raw_file_path(
            self._root,
            message.venue,
            message.connection_id,
            message.recv_ts_ns,
            rotation,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("xb")
        stream = zstd.ZstdCompressor().stream_writer(handle, closefd=False)
        opened = _OpenFile(
            path=path,
            handle=handle,
            stream=stream,
            rotation=rotation,
            last_flush_ns=message.recv_ts_ns,
        )
        self._open_files[message.connection_id] = opened
        return opened
