"""Kraken connector tests against a local fake websocket server.

The fake server binds 127.0.0.1 on a random port; no test touches the real
network. Connections, drops, rejected subscribes and resyncs are driven by
queues and events, never by wall-clock sleeps, and the connector's clock and
sleep are injected.
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
from tickvault.connectors.kraken import (
    KrakenConnector,
    subscribe_book_payload,
    subscribe_payloads,
    unsubscribe_book_payload,
)

START_NS = 1_793_491_200_000_000_000  # 2026-11-01T00:00:00Z
SUBSCRIBE_FRAMES = 2  # book + trade

REJECTED_ACK = (
    '{"method":"subscribe","result":{"channel":"book"},"success":false,'
    '"error":"Currency pair not supported"}'
)


def compact(document: object) -> str:
    return json.dumps(document, separators=(",", ":"))


class FakeVenue:
    """Fake Kraken: records frames, drops connections and rejects subscribes."""

    def __init__(
        self, *, drop_connections: int = 0, reject_first_connection: bool = False
    ) -> None:
        self.received: list[str] = []
        self.connections: list[ServerConnection] = []
        self.connection_count = 0
        self._received_event = asyncio.Event()
        self._drop_connections = drop_connections
        self._reject_first_connection = reject_first_connection
        self._drops_done = 0

    async def handler(self, socket: ServerConnection) -> None:
        self.connection_count += 1
        connection_number = self.connection_count
        self.connections.append(socket)
        frames_here = 0
        try:
            async for frame in socket:
                frames_here += 1
                self.received.append(frame)
                self._received_event.set()
                if (
                    connection_number == 1
                    and self._reject_first_connection
                    and frames_here == SUBSCRIBE_FRAMES
                ):
                    await socket.send(REJECTED_ACK)
                elif (
                    connection_number <= self._drop_connections
                    and frames_here == SUBSCRIBE_FRAMES
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
async def fake_venue(
    *, drop_connections: int = 0, reject_first_connection: bool = False
) -> AsyncIterator[tuple[FakeVenue, str]]:
    venue = FakeVenue(
        drop_connections=drop_connections, reject_first_connection=reject_first_connection
    )
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

    subscriptions = subscribe_payloads(["BTC/USD"], depth=25)
    async with fake_venue() as (venue, url):
        connector = KrakenConnector(
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
        assert connect_line.connection_id == "kraken-20261101T000000Z-00000000"
        assert json.loads(connect_line.payload) == {
            "event": "connect",
            "url": url,
            "symbols": ["BTC-USD"],
            "subscriptions": subscriptions,
        }
        with pytest.raises(RuntimeError):
            await anext(connector.stream())

        await venue.wait_for_received(2)
        assert venue.received == [compact(payload) for payload in subscriptions]

        status = '{"channel":"status","type":"update","data":[{"system":"online"}]}'
        book_update = (
            '{"channel":"book","type":"update","data":'
            '[{"symbol":"BTC/USD","bids":[],"asks":[],"checksum":12345}]}'
        )
        await venue.send(status)
        await venue.send(book_update)

        first = await anext(stream)
        second = await anext(stream)
        assert first.kind == "msg"
        assert first.ingest_seq == 1
        assert first.payload == status
        assert second.kind == "msg"
        assert second.ingest_seq == 2
        assert second.payload == book_update

        await connector.resync("BTC-USD", "checksum_failure")
        resync_line = await anext(stream)
        assert resync_line.kind == "control"
        assert resync_line.ingest_seq == 3
        assert json.loads(resync_line.payload) == {
            "event": "resync_request",
            "symbol": "BTC-USD",
            "reason": "checksum_failure",
        }
        await venue.wait_for_received(4)
        assert venue.received[2:4] == [
            compact(unsubscribe_book_payload("BTC/USD", 25)),
            compact(subscribe_book_payload("BTC/USD", 25)),
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

    subscriptions = subscribe_payloads(["BTC/USD", "ETH/USD"], depth=25)
    async with fake_venue(drop_connections=2) as (venue, url):
        connector = KrakenConnector(
            ["BTC-USD", "ETH-USD"],
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

        assert first_connect.connection_id == "kraken-20261101T000000Z-00000000"
        assert first_connect.ingest_seq == 0
        assert first_disconnect.ingest_seq == 1
        assert json.loads(first_disconnect.payload) == {
            "event": "disconnect",
            "reason": "connection_lost",
        }
        assert second_connect.connection_id == "kraken-20261101T000000Z-00000001"
        assert second_connect.ingest_seq == 0
        assert second_disconnect.ingest_seq == 1
        assert third_connect.connection_id == "kraken-20261101T000000Z-00000002"
        assert third_connect.ingest_seq == 0
        assert slept == [1.0, 2.0]

        await venue.wait_for_received(6)
        assert venue.received == [compact(payload) for payload in subscriptions] * 3
        assert json.loads(second_connect.payload)["subscriptions"] == subscriptions
        assert json.loads(third_connect.payload)["symbols"] == ["BTC-USD", "ETH-USD"]

        await connector.close()
        remaining = [message async for message in stream]
        assert remaining[-1].kind == "control"
        assert remaining[-1].connection_id == third_connect.connection_id
        assert json.loads(remaining[-1].payload) == {"event": "disconnect", "reason": "closed"}


async def test_rejected_subscribe_is_logged_and_reconnects(
    caplog: pytest.LogCaptureFixture,
) -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue(reject_first_connection=True) as (venue, url):
        connector = KrakenConnector(
            ["BTC-USD"],
            url=url,
            backoff_factory=lambda: Backoff(jitter=0.0),
            sleep=sleep,
            clock=FakeClock(),
            token_hex=token_factory(),
        )
        stream = connector.stream()

        first_connect = await anext(stream)
        await venue.wait_for_received(2)

        rejected = await anext(stream)
        assert rejected.kind == "msg"
        assert rejected.ingest_seq == 1
        assert json.loads(rejected.payload)["success"] is False

        first_disconnect = await anext(stream)
        assert json.loads(first_disconnect.payload) == {
            "event": "disconnect",
            "reason": "connection_lost",
        }

        second_connect = await anext(stream)
        assert second_connect.connection_id == "kraken-20261101T000000Z-00000001"
        assert second_connect.ingest_seq == 0
        assert json.loads(second_connect.payload)["symbols"] == ["BTC-USD"]
        assert slept == [1.0]
        assert "kraken rejected a subscribe request" in caplog.text

        await connector.close()
        remaining = [message async for message in stream]
        assert remaining[-1].kind == "control"
        assert remaining[-1].connection_id == second_connect.connection_id
        assert json.loads(remaining[-1].payload) == {"event": "disconnect", "reason": "closed"}
        assert first_connect.connection_id != second_connect.connection_id
        assert venue.connection_count == 2


async def test_subscribe_replaces_symbols_before_the_stream_starts() -> None:
    async with fake_venue() as (venue, url):
        connector = KrakenConnector(
            ["BTC-USD"], url=url, clock=FakeClock(), token_hex=token_factory()
        )
        await connector.subscribe(["ETH-USD"])
        stream = connector.stream()
        connect_line = await anext(stream)
        payload = json.loads(connect_line.payload)
        assert payload["symbols"] == ["ETH-USD"]
        assert payload["subscriptions"] == subscribe_payloads(["ETH/USD"], depth=25)
        with pytest.raises(RuntimeError):
            await connector.subscribe(["BTC-USD"])
        await connector.close()
        [message async for message in stream]
        assert venue.received == [compact(frame) for frame in payload["subscriptions"]]


async def test_resync_rejects_unknown_symbols() -> None:
    connector = KrakenConnector(["BTC-USD"])
    with pytest.raises(ValueError, match="unknown symbol"):
        await connector.resync("ETH-USD", "checksum_failure")


async def test_close_before_stream_finishes_immediately() -> None:
    connector = KrakenConnector(["BTC-USD"])
    await connector.close()
    assert [message async for message in connector.stream()] == []
