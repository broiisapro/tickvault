"""Buffer lag: end offsets and the writer's ``tickvault_buffer_lag_messages``.

The lag is ``end_offset - next_offset_to_consume``: how many raw records the
ingester has published that the writer has not handled yet. These tests drive
the arithmetic deterministically with a buffer whose end offset the test sets,
so an off-by-one (end offset is the *next* offset, not the last one) shows up
as a wrong exact value instead of a range.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path

import pytest

import tickvault.pipeline as pipeline_module
from tickvault.buffer import Buffer, BufferRecord, MemoryBroker, MemoryBuffer
from tickvault.metrics import PipelineMetrics
from tickvault.models import RawMessage
from tickvault.pipeline import run_writer
from tickvault.rawio import encode_line, iter_raw

_LAG_GAUGE = "tickvault_buffer_lag_messages"
_CONSUMED = "tickvault_buffer_consumed_total"


def _raw(ingest_seq: int) -> RawMessage:
    return RawMessage(
        venue="kraken",
        connection_id="kraken-lag",
        ingest_seq=ingest_seq,
        recv_ts_ns=1_000 + ingest_seq,
        kind="msg",
        payload=f'{{"n":{ingest_seq}}}',
    )


def _lines(count: int) -> list[bytes]:
    return [encode_line(_raw(index)) for index in range(count)]


class _ScriptedBuffer:
    """A buffer whose end offset the test sets and whose consumer never ends.

    Records are delivered once each, in order; after that the stream blocks
    until :meth:`close`, exactly like a live topic that has no new messages. So
    the writer stays running while the test changes ``end`` and watches the
    gauge, and every query is counted in ``end_calls`` so a test never has to
    sleep to wait for a tick.
    """

    def __init__(self, lines: Sequence[bytes], *, end: int | None = None) -> None:
        self.records = [
            BufferRecord(offset=index, value=line) for index, line in enumerate(lines)
        ]
        self.end: int | None = len(self.records) if end is None else end
        self.end_error: Exception | None = None
        self.end_calls = 0
        self.committed = -1
        self.closed = False
        self._release = asyncio.Event()

    async def start(self) -> None:
        return None

    async def publish_raw(self, line: bytes) -> None:
        raise AssertionError("the writer never publishes raw lines")

    async def publish_control(self, payload: bytes) -> None:
        return None

    def consume_raw(self) -> AsyncIterator[BufferRecord]:
        return self._stream()

    def consume_control(self) -> AsyncIterator[bytes]:
        return self._control_stream()

    async def commit(self, offset: int) -> None:
        self.committed = max(self.committed, offset)

    async def end_offset(self) -> int | None:
        self.end_calls += 1
        if self.end_error is not None:
            raise self.end_error
        return self.end

    async def close(self) -> None:
        self.closed = True
        self._release.set()

    async def _stream(self) -> AsyncIterator[BufferRecord]:
        for record in self.records:
            yield record
        await self._release.wait()

    async def _control_stream(self) -> AsyncIterator[bytes]:
        for _ in ():
            yield b""


class _CountingBuffer:
    """Wraps a real buffer and counts end-offset queries."""

    def __init__(self, inner: Buffer) -> None:
        self.inner = inner
        self.calls = 0

    async def start(self) -> None:
        await self.inner.start()

    async def publish_raw(self, line: bytes) -> None:
        await self.inner.publish_raw(line)

    async def publish_control(self, payload: bytes) -> None:
        await self.inner.publish_control(payload)

    def consume_raw(self) -> AsyncIterator[BufferRecord]:
        return self.inner.consume_raw()

    def consume_control(self) -> AsyncIterator[bytes]:
        return self.inner.consume_control()

    async def commit(self, offset: int) -> None:
        await self.inner.commit(offset)

    async def end_offset(self) -> int | None:
        self.calls += 1
        return await self.inner.end_offset()

    async def close(self) -> None:
        await self.inner.close()


def _gauge(metrics: PipelineMetrics, name: str) -> float | None:
    return metrics.registry.get_sample_value(name, {})


def _total(metrics: PipelineMetrics, name: str) -> float:
    return sum(
        sample.value
        for family in metrics.registry.collect()
        for sample in family.samples
        if sample.name == name
    )


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 20.0) -> None:
    """Poll a zero-argument predicate until it is true."""

    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout=timeout)


@pytest.fixture(autouse=True)
def fast_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the writer's one-second tick on a test-scale interval."""
    monkeypatch.setattr(pipeline_module, "_LIVE_TICK_S", 0.01)


async def test_memory_buffer_end_offset_is_the_next_offset() -> None:
    broker = MemoryBroker()
    buffer = MemoryBuffer(broker)
    await buffer.start()

    assert await buffer.end_offset() == 0
    for index in range(3):
        await buffer.publish_raw(bytes([index]))
    assert await buffer.end_offset() == 3

    # Committing moves the reader, never the end of the topic.
    await buffer.commit(1)
    assert await buffer.end_offset() == 3

    # A second buffer on the same broker sees the same end.
    restarted = MemoryBuffer(broker)
    await restarted.start()
    assert await restarted.end_offset() == 3

    await restarted.close()
    await buffer.close()


async def test_writer_sets_the_lag_gauge_from_the_end_offset(tmp_path: Path) -> None:
    buffer = _ScriptedBuffer(_lines(5), end=12)
    metrics = PipelineMetrics()
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        run_writer(buffer, tmp_path / "data", metrics=metrics, stop_event=stop_event)
    )
    try:
        await _wait_until(lambda: _total(metrics, _CONSUMED) >= 5)
        # Offsets 0..4 are handled, so the next one is 5 and 12 - 5 == 7.
        await _wait_until(lambda: _gauge(metrics, _LAG_GAUGE) == 7.0)

        # The ingester keeps publishing while the writer is caught up.
        buffer.end = 20
        await _wait_until(lambda: _gauge(metrics, _LAG_GAUGE) == 15.0)

        # An unknown end offset leaves the last known value in place.
        buffer.end = None
        queries = buffer.end_calls
        await _wait_until(lambda: buffer.end_calls >= queries + 5)
        assert _gauge(metrics, _LAG_GAUGE) == 15.0

        # A broker error is swallowed: the gauge keeps its value and the writer
        # keeps consuming.
        buffer.end_error = RuntimeError("broker went away")
        queries = buffer.end_calls
        await _wait_until(lambda: buffer.end_calls >= queries + 5)
        assert _gauge(metrics, _LAG_GAUGE) == 15.0
        assert not task.done()

        stop_event.set()
        stats = await asyncio.wait_for(task, timeout=20)
    finally:
        stop_event.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    assert stats.mode == "writer"
    assert stats.stopped_by == "stop_event"
    assert len(list(iter_raw(tmp_path / "data"))) == 5


async def test_lag_returns_to_zero_when_the_writer_is_caught_up(tmp_path: Path) -> None:
    lines = _lines(8)
    broker = MemoryBroker()
    publisher = MemoryBuffer(broker)
    await publisher.start()
    for line in lines:
        await publisher.publish_raw(line)
    buffer = _CountingBuffer(MemoryBuffer(broker))
    metrics = PipelineMetrics()
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        run_writer(buffer, tmp_path / "data", metrics=metrics, stop_event=stop_event)
    )
    try:
        await _wait_until(lambda: _total(metrics, _CONSUMED) >= len(lines))
        queries = buffer.calls
        await _wait_until(lambda: buffer.calls >= queries + 2)
        assert _gauge(metrics, _LAG_GAUGE) == 0.0

        stop_event.set()
        stats = await asyncio.wait_for(task, timeout=20)
    finally:
        stop_event.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await publisher.close()

    assert stats.stopped_by == "stop_event"
    assert [message.ingest_seq for message in iter_raw(tmp_path / "data")] == list(
        range(len(lines))
    )
