"""Buffered ingest tests: exact bytes, control routing, stop and drain (issue #122)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path

import pytest

from tickvault.book.processor import ResyncRequest
from tickvault.buffer import MemoryBroker, MemoryBuffer, encode_control
from tickvault.connectors.base import Connector
from tickvault.models import RawMessage
from tickvault.pipeline import run_live
from tickvault.rawio import decode_line, encode_line, iter_raw

_BASE_NS = 1_793_491_200_000_000_000
_EXCHANGE_TS = "2026-11-01T00:00:00.000000000Z"


def _coinbase_message(sequence: int, recv_ts_ns: int, connection_id: str) -> RawMessage:
    """One valid Coinbase l2 message: a snapshot first, updates after."""
    if sequence == 1:
        event: dict[str, object] = {
            "type": "snapshot",
            "product_id": "BTC-USD",
            "updates": [
                {
                    "side": "bid",
                    "event_time": _EXCHANGE_TS,
                    "price_level": "81199.9",
                    "new_quantity": "0.5",
                },
                {
                    "side": "offer",
                    "event_time": _EXCHANGE_TS,
                    "price_level": "81200.1",
                    "new_quantity": "0.6",
                },
            ],
        }
    else:
        event = {
            "type": "update",
            "product_id": "BTC-USD",
            "updates": [
                {
                    "side": "bid",
                    "event_time": _EXCHANGE_TS,
                    "price_level": "81199.9",
                    "new_quantity": "0.55",
                }
            ],
        }
    payload = json.dumps(
        {
            "channel": "l2_data",
            "timestamp": _EXCHANGE_TS,
            "sequence_num": sequence,
            "events": [event],
        }
    )
    return RawMessage(
        venue="coinbase",
        connection_id=connection_id,
        ingest_seq=sequence - 1,
        recv_ts_ns=recv_ts_ns,
        kind="msg",
        payload=payload,
    )


class ScriptedConnector:
    """Yield fixed raw messages, then optionally hold the connection open."""

    def __init__(
        self, venue: str, messages: Sequence[RawMessage], *, hold: bool = False
    ) -> None:
        self.venue = venue
        self.messages = list(messages)
        self.hold = hold
        self.symbols: tuple[str, ...] = ()
        self.closed = False
        self.started = asyncio.Event()
        self.resync_calls: list[tuple[str, str]] = []
        self._stop = asyncio.Event()

    async def subscribe(self, symbols: Sequence[str]) -> None:
        self.symbols = tuple(symbols)

    async def resync(self, symbol: str, reason: str) -> None:
        self.resync_calls.append((symbol, reason))

    async def close(self) -> None:
        self.closed = True
        self._stop.set()

    async def stream(self) -> AsyncIterator[RawMessage]:
        self.started.set()
        for message in self.messages:
            if self.closed:
                return
            yield message
        if self.hold:
            await self._stop.wait()


class EndlessConnector:
    """Yield a valid Coinbase session as fast as the consumer takes it."""

    def __init__(self, venue: str = "coinbase") -> None:
        self.venue = venue
        self.symbols: tuple[str, ...] = ()
        self.closed = False
        self.yielded = 0
        self.yielded_at_close: int | None = None
        self.resync_calls: list[tuple[str, str]] = []

    async def subscribe(self, symbols: Sequence[str]) -> None:
        self.symbols = tuple(symbols)

    async def resync(self, symbol: str, reason: str) -> None:
        self.resync_calls.append((symbol, reason))

    async def close(self) -> None:
        self.closed = True
        if self.yielded_at_close is None:
            self.yielded_at_close = self.yielded

    async def stream(self) -> AsyncIterator[RawMessage]:
        sequence = 1
        recv_ts_ns = _BASE_NS
        while not self.closed:
            self.yielded += 1
            yield _coinbase_message(sequence, recv_ts_ns, "buffered-connector")
            sequence += 1
            recv_ts_ns += 1_000_000


def _factory(connector: Connector) -> Callable[[Sequence[str]], Connector]:
    def build(symbols: Sequence[str]) -> Connector:
        connector.symbols = tuple(symbols)
        return connector

    return build


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 10.0) -> None:
    """Poll ``predicate`` until it is true, failing the test instead of hanging."""

    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout=timeout)


async def test_buffered_ingest_publishes_exact_raw_bytes(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    messages = [message for message in iter_raw(synthetic_gap_path) if message.venue == "coinbase"]
    buffer = MemoryBuffer(MemoryBroker())
    connector = ScriptedConnector("coinbase", messages)
    out = tmp_path / "data"

    stats = await run_live(
        ["coinbase"],
        ["BTC-USD"],
        out,
        connector_factories={"coinbase": _factory(connector)},
        buffer=buffer,
        progress_interval=0,
    )

    assert [record.value for record in buffer.broker.raw] == [
        encode_line(message) for message in messages
    ]
    assert [decode_line(record.value) for record in buffer.broker.raw] == messages
    assert stats.mode == "live"
    assert stats.stopped_by == "completed"
    assert stats.raw_lines == len(messages)
    assert stats.messages == 0
    assert stats.trades == 0
    assert connector.closed
    assert not (out / "raw").exists()
    assert not (out / "norm").exists()


async def test_two_venues_publish_every_message(
    tmp_path: Path, synthetic_gap_path: Path
) -> None:
    messages = list(iter_raw(synthetic_gap_path))
    buffer = MemoryBuffer(MemoryBroker())
    coinbase = ScriptedConnector(
        "coinbase", [message for message in messages if message.venue == "coinbase"]
    )
    kraken = ScriptedConnector(
        "kraken", [message for message in messages if message.venue == "kraken"]
    )

    stats = await run_live(
        ["coinbase", "kraken"],
        ["BTC-USD"],
        tmp_path / "data",
        connector_factories={
            "coinbase": _factory(coinbase),
            "kraken": _factory(kraken),
        },
        buffer=buffer,
        progress_interval=0,
    )

    assert stats.raw_lines == len(messages)
    assert sorted(record.value for record in buffer.broker.raw) == sorted(
        encode_line(message) for message in messages
    )
    assert coinbase.closed and kraken.closed


async def test_control_requests_are_routed_to_the_matching_connector(
    tmp_path: Path,
) -> None:
    buffer = MemoryBuffer(MemoryBroker())
    connector = ScriptedConnector("coinbase", [], hold=True)
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            tmp_path / "data",
            connector_factories={"coinbase": _factory(connector)},
            buffer=buffer,
            stop_event=stop_event,
            progress_interval=0,
        )
    )
    await asyncio.wait_for(connector.started.wait(), timeout=10)

    await buffer.publish_control(b"not json")
    request = ResyncRequest(
        venue="coinbase",
        connection_id="coinbase-20261101T000000Z-0000c0de",
        symbol="BTC-USD",
        reason="gap",
    )
    await buffer.publish_control(encode_control(request))

    await _wait_until(lambda: connector.resync_calls == [("BTC-USD", "gap")])
    stop_event.set()
    stats = await asyncio.wait_for(task, timeout=20)

    assert stats.stopped_by == "stop_event"
    assert connector.closed


async def test_stop_event_drains_every_frame_into_the_buffer(tmp_path: Path) -> None:
    buffer = MemoryBuffer(MemoryBroker())
    connector = EndlessConnector()
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            tmp_path / "data",
            connector_factories={"coinbase": _factory(connector)},
            buffer=buffer,
            stop_event=stop_event,
            progress_interval=0,
        )
    )
    await _wait_until(lambda: len(buffer.broker.raw) >= 50)
    stop_event.set()
    stats = await asyncio.wait_for(task, timeout=20)

    assert stats.stopped_by == "stop_event"
    assert connector.closed
    assert connector.yielded_at_close is not None
    assert stats.raw_lines == connector.yielded_at_close
    assert len(buffer.broker.raw) == connector.yielded_at_close
    assert [decode_line(record.value).connection_id for record in buffer.broker.raw] == [
        "buffered-connector"
    ] * connector.yielded_at_close


async def test_duration_stops_a_buffered_ingest(tmp_path: Path) -> None:
    buffer = MemoryBuffer(MemoryBroker())
    connector = EndlessConnector()

    stats = await asyncio.wait_for(
        run_live(
            ["coinbase"],
            ["BTC-USD"],
            tmp_path / "data",
            duration=0.1,
            connector_factories={"coinbase": _factory(connector)},
            buffer=buffer,
            progress_interval=0,
        ),
        timeout=20,
    )

    assert stats.stopped_by == "duration"
    assert stats.raw_lines > 0
    assert len(buffer.broker.raw) == stats.raw_lines


async def test_the_ingester_starts_and_closes_the_buffer(tmp_path: Path) -> None:
    buffer = MemoryBuffer(MemoryBroker())
    connector = ScriptedConnector("coinbase", [])
    await run_live(
        ["coinbase"],
        ["BTC-USD"],
        tmp_path / "data",
        connector_factories={"coinbase": _factory(connector)},
        buffer=buffer,
        progress_interval=0,
    )

    async def drain() -> list[bytes]:
        return [payload async for payload in buffer.consume_control()]

    assert await asyncio.wait_for(drain(), timeout=5) == []


async def test_unknown_venue_is_rejected_before_the_buffer_starts(tmp_path: Path) -> None:
    buffer = MemoryBuffer(MemoryBroker())

    with pytest.raises(ValueError, match="unsupported venue"):
        await run_live(["binance"], ["BTC-USD"], tmp_path / "data", buffer=buffer)

    assert buffer.broker.raw == []
