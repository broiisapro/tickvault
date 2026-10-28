"""Writer restart tests: at-least-once without holes, graceful without duplicates (issue #122)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path

import pytest

from tickvault.buffer import BufferRecord, MemoryBroker, MemoryBuffer, decode_control
from tickvault.connectors.base import Connector
from tickvault.metrics import PipelineMetrics
from tickvault.models import RawMessage
from tickvault.pipeline import PipelineStats, run_live, run_writer
from tickvault.rawio import encode_line, iter_raw
from tickvault.venues import VENUES

_SYMBOL = "BTC-USD"
_HOLD_AFTER = 3_000
"""Recording messages each connector publishes before the test crashes the writer."""


def _factory(connector: RecordedConnector) -> Callable[[Sequence[str]], Connector]:
    def build(symbols: Sequence[str]) -> Connector:
        connector.symbols = tuple(symbols)
        return connector

    return build


def _first_snapshot(venue: str, messages: Sequence[RawMessage]) -> RawMessage:
    """The recording's first frame that carries a full book snapshot."""
    normalize = VENUES[venue].normalize
    for message in messages:
        if message.kind != "msg":
            continue
        try:
            normalized = normalize(message)
        except Exception:
            continue
        if any(update.is_snapshot for update in normalized.book_updates):
            return message
    raise AssertionError(f"the {venue} fixture carries no book snapshot")


class RecordedConnector:
    """Replays a recorded fixture and answers resync requests with a snapshot.

    The recording is published up to ``hold_after`` messages and then paused
    until :meth:`release`, so the test can crash the writer mid-recording
    deterministically. The connection stays open after the recording ends (like
    the live connectors, which never drop just because a writer restarted).
    Each queued resync request is answered with a new connection: a ``connect``
    control frame that scopes it to the symbol, then a fresh snapshot taken
    from the recording — exactly what a venue resubscribe delivers.
    """

    def __init__(
        self, venue: str, messages: Sequence[RawMessage], *, hold_after: int
    ) -> None:
        self.venue = venue
        self.messages = list(messages)
        self.connection_id = self.messages[0].connection_id
        self.snapshot = _first_snapshot(venue, self.messages)
        self.snapshot_line = encode_line(self.snapshot)
        self.hold_after = hold_after
        self.symbols: tuple[str, ...] = ()
        self.closed = False
        self.resync_calls: list[tuple[str, str]] = []
        self.bundles = 0
        self.last_seq = max(message.ingest_seq for message in self.messages)
        self._last_ts_ns = max(message.recv_ts_ns for message in self.messages)
        self._released = asyncio.Event()
        self._finished = asyncio.Event()
        self._requests: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._generation = 0

    def release(self) -> None:
        """Let the recording continue to its end."""
        self._released.set()

    def finish(self) -> None:
        """Let the connection end once every pending resync request is served."""
        self._finished.set()

    async def subscribe(self, symbols: Sequence[str]) -> None:
        self.symbols = tuple(symbols)

    async def resync(self, symbol: str, reason: str) -> None:
        self.resync_calls.append((symbol, reason))
        self._requests.put_nowait((symbol, reason))

    async def close(self) -> None:
        self.closed = True
        self._finished.set()

    async def stream(self) -> AsyncIterator[RawMessage]:
        for index, message in enumerate(self.messages):
            if index == self.hold_after:
                await self._released.wait()
            if self.closed:
                return
            yield message
            # Let the other venue's producer run, so the two recordings are
            # published interleaved like two live connections, not one after
            # the other.
            await asyncio.sleep(0)
        while not self.closed:
            request = asyncio.ensure_future(self._requests.get())
            finished = asyncio.ensure_future(self._finished.wait())
            try:
                done, _ = await asyncio.wait(
                    {request, finished}, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                for task in (request, finished):
                    task.cancel()
                await asyncio.gather(request, finished, return_exceptions=True)
            if request in done:
                symbol, _reason = request.result()
                for message in self._reconnect_bundle(symbol):
                    yield message
                self.bundles += 1
                continue
            return

    def _reconnect_bundle(self, symbol: str) -> list[RawMessage]:
        """A new connection plus its snapshot, as a resubscribe would deliver."""
        self._generation += 1
        connection_id = f"{self.venue}-resync-{self._generation:04d}"
        self._last_ts_ns += 1_000
        connect = json.dumps(
            {
                "event": "connect",
                "url": "wss://example.invalid",
                "subscriptions": [],
                "symbols": [symbol],
            }
        )
        return [
            RawMessage(
                venue=self.venue,
                connection_id=connection_id,
                ingest_seq=0,
                recv_ts_ns=self._last_ts_ns,
                kind="control",
                payload=connect,
            ),
            RawMessage(
                venue=self.venue,
                connection_id=connection_id,
                ingest_seq=1,
                recv_ts_ns=self._last_ts_ns + 1,
                kind="msg",
                payload=self.snapshot.payload,
            ),
        ]


class _DuplicateFirstRecordBuffer:
    """A tiny in-memory buffer whose first record is delivered twice.

    At-least-once delivery can hand the same envelope over twice (a redelivered
    offset before a commit). The writer must write it once and still commit both
    offsets.
    """

    def __init__(self, lines: Sequence[bytes]) -> None:
        self.lines = list(lines)
        self.control: list[bytes] = []
        self.committed = -1
        self.closed = False
        self.records = [
            BufferRecord(offset=index, value=value)
            for index, value in enumerate(self.lines)
        ]
        self.records.append(
            BufferRecord(offset=len(self.lines), value=self.lines[0])
        )

    async def start(self) -> None:
        return None

    async def publish_raw(self, line: bytes) -> None:
        self.lines.append(line)

    async def publish_control(self, payload: bytes) -> None:
        self.control.append(payload)

    async def consume_raw(self) -> AsyncIterator[BufferRecord]:
        for record in self.records:
            yield record

    async def consume_control(self) -> AsyncIterator[bytes]:
        for payload in self.control:
            yield payload

    async def commit(self, offset: int) -> None:
        self.committed = max(self.committed, offset)

    async def close(self) -> None:
        self.closed = True


async def test_the_writer_writes_a_redelivered_record_once(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    messages = list(iter_raw(synthetic_gap_path))
    buffer = _DuplicateFirstRecordBuffer([encode_line(message) for message in messages])
    out = tmp_path / "data"

    stats = await run_writer(buffer, out)

    assert stats.mode == "writer"
    assert stats.stopped_by == "completed"
    assert buffer.closed
    assert buffer.committed == len(messages)
    written = list(iter_raw(out))
    assert [message.ingest_seq for message in written if message.venue == "coinbase"] == sorted(
        message.ingest_seq for message in messages if message.venue == "coinbase"
    )
    assert len(written) == len(messages)


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 60.0) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout=timeout)


def _total(metrics: PipelineMetrics, name: str) -> float:
    return sum(
        sample.value
        for family in metrics.registry.collect()
        for sample in family.samples
        if sample.name == name
    )


def _gauge(metrics: PipelineMetrics, name: str, **labels: str) -> float | None:
    return metrics.registry.get_sample_value(name, labels)


class _Fixture:
    """One ingester plus the recorded connectors feeding it through a broker."""

    def __init__(self, tmp_path: Path, paths: Sequence[Path]) -> None:
        self.out = tmp_path / "data"
        self.broker = MemoryBroker()
        self.ingest_buffer = MemoryBuffer(self.broker)
        self.connectors = {
            venue: RecordedConnector(
                venue, list(iter_raw(path)), hold_after=_HOLD_AFTER
            )
            for venue, path in zip(("coinbase", "kraken"), paths, strict=True)
        }
        self.ingester: asyncio.Task[PipelineStats] | None = None
        self.fixture_records = sum(
            len(connector.messages) for connector in self.connectors.values()
        )

    def start_ingester(self) -> None:
        self.ingester = asyncio.create_task(
            run_live(
                ["coinbase", "kraken"],
                [_SYMBOL],
                self.out,
                connector_factories={
                    venue: _factory(connector)
                    for venue, connector in self.connectors.items()
                },
                buffer=self.ingest_buffer,
                progress_interval=0,
            )
        )

    def release(self) -> None:
        for connector in self.connectors.values():
            connector.release()

    def finish(self) -> None:
        for connector in self.connectors.values():
            connector.finish()

    async def stop_ingester(self) -> None:
        assert self.ingester is not None
        self.finish()
        await asyncio.wait_for(self.ingester, timeout=60)

    def served_all(self) -> bool:
        return all(connector.bundles >= 1 for connector in self.connectors.values())

    def snapshot_offsets(self) -> dict[str, int]:
        """Published offset of each venue's first book snapshot."""
        lines = {
            connector.snapshot_line: venue for venue, connector in self.connectors.items()
        }
        found: dict[str, int] = {}
        for record in self.broker.raw:
            venue = lines.get(record.value)
            if venue is not None and venue not in found:
                found[venue] = record.offset
                if len(found) == len(lines):
                    break
        return found

    def committed_past_both_snapshots(self) -> bool:
        """The writer committed every venue's snapshot: both books were LIVE."""
        offsets = self.snapshot_offsets()
        return (
            len(offsets) == len(self.connectors)
            and self.broker.committed >= max(offsets.values())
            and self.broker.committed >= 1_000
        )

    def assert_complete_raw_layer(self) -> None:
        """Every connection's raw ``ingest_seq`` set is ``0..max`` with no hole."""
        by_connection: dict[str, set[int]] = {}
        for message in iter_raw(self.out):
            by_connection.setdefault(message.connection_id, set()).add(message.ingest_seq)
        assert by_connection
        for connection_id, sequences in by_connection.items():
            assert sorted(sequences) == list(range(max(sequences) + 1)), connection_id
        for connector in self.connectors.values():
            assert max(by_connection[connector.connection_id]) == connector.last_seq

    def assert_resync_round_trip(self, metrics: PipelineMetrics) -> None:
        controls = [decode_control(payload) for payload in self.broker.control]
        assert controls, "the restarted writer never asked for a snapshot"
        assert {request.reason for request in controls} >= {"writer_sync"}
        assert {request.symbol for request in controls} == {_SYMBOL}
        for connector in self.connectors.values():
            assert connector.resync_calls
            assert _gauge(
                metrics, "tickvault_book_live", venue=connector.venue, symbol=_SYMBOL
            ) == 1.0


async def test_writer_restart_loses_nothing(
    tmp_path: Path, coinbase_5m_path: Path, kraken_5m_path: Path
) -> None:
    fixture = _Fixture(tmp_path, (coinbase_5m_path, kraken_5m_path))
    fixture.start_ingester()

    first_metrics = PipelineMetrics()
    crashed = asyncio.create_task(
        run_writer(MemoryBuffer(fixture.broker), fixture.out, metrics=first_metrics)
    )
    await _wait_until(fixture.committed_past_both_snapshots)
    for connector in fixture.connectors.values():
        assert _gauge(
            first_metrics, "tickvault_book_live", venue=connector.venue, symbol=_SYMBOL
        ) == 1.0
    crashed.cancel()
    with pytest.raises(asyncio.CancelledError):
        await crashed
    committed_before_restart = fixture.broker.committed

    fixture.release()
    await _wait_until(lambda: len(fixture.broker.raw) >= fixture.fixture_records)

    metrics = PipelineMetrics()
    stop_event = asyncio.Event()
    restarted = asyncio.create_task(
        run_writer(
            MemoryBuffer(fixture.broker),
            fixture.out,
            metrics=metrics,
            stop_event=stop_event,
        )
    )
    await _wait_until(fixture.served_all)
    await fixture.stop_ingester()
    published = len(fixture.broker.raw)
    # The writer never loses a message the crashed process had not committed.
    await _wait_until(
        lambda: _total(metrics, "tickvault_buffer_consumed_total")
        >= published - committed_before_restart - 1
    )
    stop_event.set()
    stats = await asyncio.wait_for(restarted, timeout=30)

    assert stats.mode == "writer"
    assert stats.stopped_by == "stop_event"
    assert fixture.broker.committed == published - 1
    fixture.assert_complete_raw_layer()
    fixture.assert_resync_round_trip(metrics)
    assert stats.reconnects == len(fixture.connectors)
    assert stats.resyncs >= len(fixture.connectors)


async def test_graceful_stop_writes_no_duplicates(
    tmp_path: Path, coinbase_5m_path: Path, kraken_5m_path: Path
) -> None:
    fixture = _Fixture(tmp_path, (coinbase_5m_path, kraken_5m_path))
    fixture.start_ingester()

    first_stop = asyncio.Event()
    first = asyncio.create_task(
        run_writer(MemoryBuffer(fixture.broker), fixture.out, stop_event=first_stop)
    )
    await _wait_until(fixture.committed_past_both_snapshots)
    first_stop.set()
    first_stats = await asyncio.wait_for(first, timeout=30)
    assert first_stats.stopped_by == "stop_event"
    committed_before_restart = fixture.broker.committed

    fixture.release()
    await _wait_until(lambda: len(fixture.broker.raw) >= fixture.fixture_records)

    metrics = PipelineMetrics()
    second_stop = asyncio.Event()
    restarted = asyncio.create_task(
        run_writer(
            MemoryBuffer(fixture.broker),
            fixture.out,
            metrics=metrics,
            stop_event=second_stop,
        )
    )
    await _wait_until(fixture.served_all)
    await fixture.stop_ingester()
    published = len(fixture.broker.raw)
    await _wait_until(
        lambda: _total(metrics, "tickvault_buffer_consumed_total")
        >= published - committed_before_restart - 1
    )
    second_stop.set()
    stats = await asyncio.wait_for(restarted, timeout=30)

    assert stats.mode == "writer"
    seen: set[tuple[str, int]] = set()
    messages = list(iter_raw(fixture.out))
    for message in messages:
        key = (message.connection_id, message.ingest_seq)
        assert key not in seen, f"duplicate raw line for {key}"
        seen.add(key)
    assert len(messages) == len(seen)
    fixture.assert_complete_raw_layer()
    fixture.assert_resync_round_trip(metrics)
    assert stats.reconnects == len(fixture.connectors)
