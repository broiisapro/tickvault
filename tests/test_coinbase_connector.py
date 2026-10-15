"""Coinbase connector tests against a local fake websocket server.

The fake server binds 127.0.0.1 on a random port; no test touches the real
network. Connections, drops and resyncs are driven by queues and events, never
by wall-clock sleeps, and the connector's clock and sleep are injected.
"""

from __future__ import annotations

import asyncio
import itertools
import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import pytest
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from tickvault.connectors._ws import Backoff
from tickvault.connectors.coinbase import (
    CoinbaseConnector,
    subscribe_payload,
    subscribe_payloads,
    unsubscribe_payload,
)

START_NS = 1_793_491_200_000_000_000  # 2026-11-01T00:00:00Z


def compact(document: object) -> str:
    return json.dumps(document, separators=(",", ":"))


class FakeVenue:
    """Fake venue: records frames, optionally drops the first N connections."""

    def __init__(self, *, drop_connections: int = 0) -> None:
        self.received: list[str] = []
        self.connections: list[ServerConnection] = []
        self._received_event = asyncio.Event()
        self._drop_connections = drop_connections
        self._drops_done = 0

    async def handler(self, socket: ServerConnection) -> None:
        self.connections.append(socket)
        try:
            async for frame in socket:
                self.received.append(frame)
                self._received_event.set()
                if (
                    self._drops_done < self._drop_connections
                    and len(self.received) == 3 * (self._drops_done + 1)
                ):
                    self._drops_done += 1
                    await socket.close(1011, "drop")
        except ConnectionClosed:
            pass

    async def wait_for_received(self, count: int, *, timeout: float = 10.0) -> None:
        async with asyncio.timeout(timeout):
            while len(self.received) < count:
                self._received_event.clear()
                await self._received_event.wait()

    async def send(self, frame: str) -> None:
        await self.connections[-1].send(frame)


@asynccontextmanager
async def fake_venue(*, drop_connections: int = 0) -> AsyncIterator[tuple[FakeVenue, str]]:
    venue = FakeVenue(drop_connections=drop_connections)
    server = await serve(venue.handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield venue, f"ws://127.0.0.1:{port}"
    finally:
        server.close()
        await server.wait_closed()


class FakeClock:
    """A deterministic nanosecond clock; connection ids derive from its value."""

    def __init__(self, start: int = START_NS, step: int = 1_000_000) -> None:
        self._now = start
        self._step = step

    def __call__(self) -> int:
        value = self._now
        self._now += self._step
        return value


def token_factory() -> Callable[[int], str]:
    counter = itertools.count()
    return lambda _: f"{next(counter):08x}"


async def test_stream_control_lines_frame_integrity_and_resync() -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue() as (venue, url):
        connector = CoinbaseConnector(
            ["BTC-USD"],
            url=url,
            backoff_factory=lambda: Backoff(jitter=0.0),
            sleep=sleep,
            clock=FakeClock(),
            token_hex=token_factory(),
        )
        stream = connector.stream()

        connect_line = await anext(stream)
        assert connect_line.kind == "control"
        assert connect_line.ingest_seq == 0
        assert connect_line.connection_id == "coinbase-20261101T000000Z-00000000"
        connect = json.loads(connect_line.payload)
        assert connect == {
            "event": "connect",
            "url": url,
            "subscriptions": subscribe_payloads("BTC-USD"),
        }
        with pytest.raises(RuntimeError):
            await anext(connector.stream())

        await venue.wait_for_received(3)
        assert venue.received == [compact(payload) for payload in subscribe_payloads("BTC-USD")]

        ack = '{"channel":"subscriptions","sequence_num":0}'
        snapshot = '{"channel":"l2_data","sequence_num":1,"note":"h\u00e9llo"}'
        await venue.send(ack)
        await venue.send(snapshot)

        first = await anext(stream)
        second = await anext(stream)
        assert first.kind == "msg"
        assert first.ingest_seq == 1
        assert first.payload == ack
        assert second.kind == "msg"
        assert second.ingest_seq == 2
        assert second.payload == snapshot

        await connector.resync("BTC-USD", "gap")
        resync_line = await anext(stream)
        assert resync_line.kind == "control"
        assert resync_line.ingest_seq == 3
        assert json.loads(resync_line.payload) == {
            "event": "resync_request",
            "symbol": "BTC-USD",
            "reason": "gap",
        }
        await venue.wait_for_received(5)
        assert venue.received[3:5] == [
            compact(unsubscribe_payload("BTC-USD", "level2")),
            compact(subscribe_payload("BTC-USD", "level2")),
        ]

        await connector.close()
        disconnect = await anext(stream)
        assert disconnect.kind == "control"
        assert disconnect.ingest_seq == 4
        assert disconnect.connection_id == connect_line.connection_id
        assert json.loads(disconnect.payload) == {"event": "disconnect", "reason": "closed"}
        with pytest.raises(StopAsyncIteration):
            await anext(stream)
        assert slept == []


async def test_reconnect_gets_new_connection_id_and_grows_backoff() -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue(drop_connections=2) as (venue, url):
        connector = CoinbaseConnector(
            ["BTC-USD"],
            url=url,
            backoff_factory=lambda: Backoff(jitter=0.0),
            sleep=sleep,
            clock=FakeClock(),
            token_hex=token_factory(),
        )
        stream = connector.stream()

        first_connect = await anext(stream)
        first_disconnect = await anext(stream)
        second_connect = await anext(stream)
        second_disconnect = await anext(stream)
        third_connect = await anext(stream)

        assert first_connect.connection_id == "coinbase-20261101T000000Z-00000000"
        assert first_connect.ingest_seq == 0
        assert first_disconnect.ingest_seq == 1
        assert json.loads(first_disconnect.payload) == {
            "event": "disconnect",
            "reason": "connection_lost",
        }
        assert second_connect.connection_id == "coinbase-20261101T000000Z-00000001"
        assert second_connect.ingest_seq == 0
        assert second_disconnect.ingest_seq == 1
        assert third_connect.connection_id == "coinbase-20261101T000000Z-00000002"
        assert third_connect.ingest_seq == 0
        assert slept == [1.0, 2.0]

        await venue.wait_for_received(9)
        assert venue.received == [compact(payload) for payload in subscribe_payloads("BTC-USD")] * 3
        assert json.loads(second_connect.payload)["subscriptions"] == subscribe_payloads("BTC-USD")

        await connector.close()
        remaining = [message async for message in stream]
        assert remaining[-1].kind == "control"
        assert remaining[-1].connection_id == third_connect.connection_id
        assert json.loads(remaining[-1].payload) == {"event": "disconnect", "reason": "closed"}


async def test_subscribe_replaces_symbols_before_the_stream_starts() -> None:
    async with fake_venue() as (venue, url):
        connector = CoinbaseConnector(
            ["BTC-USD"], url=url, clock=FakeClock(), token_hex=token_factory()
        )
        await connector.subscribe(["ETH-USD"])
        stream = connector.stream()
        connect_line = await anext(stream)
        subscriptions = json.loads(connect_line.payload)["subscriptions"]
        assert subscriptions == subscribe_payloads("ETH-USD")
        with pytest.raises(RuntimeError):
            await connector.subscribe(["BTC-USD"])
        await connector.close()
        [message async for message in stream]
        assert venue.received == [compact(payload) for payload in subscribe_payloads("ETH-USD")]


async def test_resync_rejects_unknown_symbols() -> None:
    connector = CoinbaseConnector(["BTC-USD"])
    with pytest.raises(ValueError, match="unknown symbol"):
        await connector.resync("ETH-USD", "gap")


async def test_close_before_stream_finishes_immediately() -> None:
    connector = CoinbaseConnector(["BTC-USD"])
    await connector.close()
    assert [message async for message in connector.stream()] == []
