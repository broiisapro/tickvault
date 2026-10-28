"""Buffer contract: the optional Kafka-compatible hop between ingest and writer.

With ``tickvault ingest --buffer <address>`` the ingester publishes every raw
envelope line (exactly the bytes :func:`tickvault.rawio.encode_line` produces,
nothing re-encoded) to :data:`RAW_TOPIC` and executes the resync requests it
reads from :data:`CONTROL_TOPIC`. ``tickvault consume --buffer <address>`` is
the writer: it reads raw lines back and runs them through the unchanged raw
writer → normalizer → book processor → Parquet path, publishing resync
requests to the control topic.

Delivery is at-least-once: a consumer commits an offset only after the raw
writer has flushed it, so a crashed writer replays the uncommitted tail. A new
consumer on the same broker resumes from the last committed offset, which is
how a restarted writer is simulated in tests.

This module ships two implementations: :class:`MemoryBuffer`, fully working
over an in-process :class:`MemoryBroker` (used by every test, since Docker is
not in CI), and, from wave 08 on, ``tickvault.buffer_kafka.KafkaBuffer`` for a
real broker on ``<host>:<port>``. :func:`open_buffer` chooses between them from
the address alone, so callers never import a concrete implementation.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Protocol, cast

from tickvault.book.processor import ResyncRequest

RAW_TOPIC = "tickvault.raw"
"""Topic carrying raw envelope lines, one partition, so order equals publish order."""

CONTROL_TOPIC = "tickvault.control"
"""Topic carrying resync requests from the writer back to the ingester."""

_COMMITTED_NONE = -1
"""Committed offset before anything has been committed (no record is handled yet)."""


@dataclass(frozen=True, slots=True)
class BufferRecord:
    """One raw envelope line read from the buffer."""

    offset: int
    """0-based offset on :data:`RAW_TOPIC`; contiguous across the topic."""

    value: bytes
    """Exactly the bytes :func:`tickvault.rawio.encode_line` returned."""


class Buffer(Protocol):
    """A Kafka-compatible raw/control buffer shared by ingester and writer."""

    async def start(self) -> None:
        """Connect to the broker (or take the shared in-process state)."""
        ...

    async def publish_raw(self, line: bytes) -> None:
        """Append one raw envelope line to :data:`RAW_TOPIC`."""
        ...

    async def publish_control(self, payload: bytes) -> None:
        """Append one encoded control request to :data:`CONTROL_TOPIC`."""
        ...

    def consume_raw(self) -> AsyncIterator[BufferRecord]:
        """Yield raw records from the last committed offset + 1, then new ones."""
        ...

    def consume_control(self) -> AsyncIterator[bytes]:
        """Yield control records published after :meth:`start`."""
        ...

    async def commit(self, offset: int) -> None:
        """Mark every raw record up to and including ``offset`` as handled."""
        ...

    async def close(self) -> None:
        """Release the connection and end this object's open iterators."""
        ...


class MemoryBroker:
    """Shared in-process state behind one or more :class:`MemoryBuffer` objects.

    Topics are append-only lists; the committed offset and the wait condition
    are broker-wide, so a second buffer on the same broker resumes from the
    last committed offset exactly like a restarted writer would.
    """

    def __init__(self) -> None:
        self.raw: list[BufferRecord] = []
        self.control: list[bytes] = []
        self.committed: int = _COMMITTED_NONE
        self.version = 0
        """Bumped on every append, so waiters cannot miss a publish."""
        self._waiters: list[asyncio.Event] = []

    def append_raw(self, line: bytes) -> int:
        """Append a raw line, return its offset and wake waiting consumers."""
        offset = len(self.raw)
        self.raw.append(BufferRecord(offset=offset, value=line))
        self._notify()
        return offset

    def append_control(self, payload: bytes) -> None:
        """Append a control record and wake waiting consumers."""
        self.control.append(payload)
        self._notify()

    def _notify(self) -> None:
        """Bump the version and wake every waiter (no event loop needed)."""
        self.version += 1
        waiters, self._waiters = self._waiters, []
        for event in waiters:
            event.set()

    async def wait_for_change(self, version: int) -> None:
        """Block until something is appended after ``version``.

        ``version`` is read before the caller's last look at the topics, so a
        publish that lands between that look and this call is never missed.
        """
        if self.version != version:
            return
        event = asyncio.Event()
        self._waiters.append(event)
        try:
            if self.version != version:
                return
            await event.wait()
        finally:
            if event in self._waiters:
                self._waiters.remove(event)


class MemoryBuffer:
    """A :class:`Buffer` over an in-process :class:`MemoryBroker` (all tests).

    A buffer created on the same broker after a ``commit`` resumes after the
    committed offset; uncommitted records are delivered again, which is how the
    tests simulate a crashed writer.
    """

    def __init__(self, broker: MemoryBroker) -> None:
        self.broker = broker
        self._control_start = 0
        self._closed = False

    async def start(self) -> None:
        """Start at the end of the control topic, like a fresh reader."""
        self._control_start = len(self.broker.control)

    async def publish_raw(self, line: bytes) -> None:
        """Append one raw envelope line to the in-process raw topic."""
        self.broker.append_raw(line)

    async def publish_control(self, payload: bytes) -> None:
        """Append one control record to the in-process control topic."""
        self.broker.append_control(payload)

    def consume_raw(self) -> AsyncIterator[BufferRecord]:
        """Yield raw records from the committed offset + 1 until closed."""
        return self._raw_stream()

    def consume_control(self) -> AsyncIterator[bytes]:
        """Yield control records published after :meth:`start` until closed."""
        return self._control_stream()

    async def commit(self, offset: int) -> None:
        """Record ``offset`` as durably handled (never moves backwards)."""
        self.broker.committed = max(self.broker.committed, offset)

    async def close(self) -> None:
        """End this buffer's iterators; the broker and other buffers stay usable."""
        self._closed = True
        self.broker._notify()

    async def _raw_stream(self) -> AsyncIterator[BufferRecord]:
        index = self.broker.committed + 1
        while True:
            version = self.broker.version
            while index < len(self.broker.raw):
                record = self.broker.raw[index]
                index += 1
                yield record
            if self._closed:
                return
            await self.broker.wait_for_change(version)

    async def _control_stream(self) -> AsyncIterator[bytes]:
        index = self._control_start
        while True:
            version = self.broker.version
            while index < len(self.broker.control):
                payload = self.broker.control[index]
                index += 1
                yield payload
            if self._closed:
                return
            await self.broker.wait_for_change(version)


_BROKERS: dict[str, MemoryBroker] = {}
"""Named in-process brokers, so ``memory://x`` twice is the same broker."""


def encode_control(request: ResyncRequest) -> bytes:
    """Encode one resync request as JSON with sorted keys, UTF-8."""
    document = {
        "venue": request.venue,
        "connection_id": request.connection_id,
        "symbol": request.symbol,
        "reason": request.reason,
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")


def decode_control(payload: bytes) -> ResyncRequest:
    """Decode a control payload written by :func:`encode_control`.

    Raises :class:`ValueError` when the payload is not a JSON object with the
    four string fields of a :class:`ResyncRequest`.
    """
    try:
        document = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid control payload: {exc}") from exc
    if not isinstance(document, Mapping):
        raise ValueError("control payload must be a JSON object")
    values: dict[str, str] = {}
    for name in ("venue", "connection_id", "symbol", "reason"):
        value = document.get(name)
        if not isinstance(value, str):
            raise ValueError(f"control payload field {name!r} must be a string")
        values[name] = value
    return ResyncRequest(
        venue=values["venue"],
        connection_id=values["connection_id"],
        symbol=values["symbol"],
        reason=values["reason"],
    )


def open_buffer(address: str) -> Buffer:
    """Open the buffer named by ``address``.

    ``memory://<name>`` returns a :class:`MemoryBuffer` on the process-wide
    broker of that name (two calls with the same name share one broker), and
    ``<host>:<port>`` returns a Kafka buffer from
    :mod:`tickvault.buffer_kafka`, which lands in wave 08.
    """
    if address.startswith("memory://"):
        name = address[len("memory://") :]
        if not name:
            raise ValueError("memory buffer address must be memory://<name>")
        broker = _BROKERS.get(name)
        if broker is None:
            broker = MemoryBroker()
            _BROKERS[name] = broker
        return MemoryBuffer(broker)
    if "://" in address:
        raise ValueError(f"unsupported buffer address: {address!r}")
    host, separator, port = address.rpartition(":")
    if not separator or not host or not port.isdigit():
        raise ValueError(f"buffer address must be HOST:PORT or memory://NAME: {address!r}")
    try:
        module = importlib.import_module("tickvault.buffer_kafka")
    except ModuleNotFoundError:
        raise RuntimeError("Kafka buffer not available yet") from None
    return cast(Buffer, module.KafkaBuffer(address))
