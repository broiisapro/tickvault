"""Bitfinex connector tests against a local fake websocket server.

The fake server binds 127.0.0.1 on a random port; no test touches the real
network. Connections, drops and resyncs are driven by queues and events, never
by wall-clock sleeps, and the connector's clock, sleep and resync timer are
injected. The last two tests read the committed five-minute recording and pin
the frame shapes the connector classifies against real venue traffic.
"""

from __future__ import annotations

import asyncio
import gc
import itertools
import json
import pathlib
from collections import Counter
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import pytest
import zstandard
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from tickvault.connectors._ws import Backoff
from tickvault.connectors.bitfinex import (
    BITFINEX_BACKOFF_BASE_S,
    BITFINEX_MIN_RECONNECT_S,
    BitfinexConnector,
    book_subscribe_payload,
    compact,
    is_book_snapshot,
    subscribe_payloads,
    trades_subscribe_payload,
    unsubscribe_payload,
    venue_symbol,
)

START_NS = 1_793_491_200_000_000_000  # 2026-11-01T00:00:00Z
SUBSCRIBE_FRAMES = 3  # conf + book + trades

SUBSCRIBED_TRADES = (
    '{"event":"subscribed","channel":"trades","chanId":258792,"symbol":"tBTCUSD","pair":"BTCUSD"}'
)
SUBSCRIBED_BOOK = (
    '{"event":"subscribed","channel":"book","chanId":396954,"symbol":"tBTCUSD",'
    '"prec":"P0","freq":"F0","len":"25","pair":"BTCUSD"}'
)
BOOK_SNAPSHOT = "[396954,[[81164,1,0.00022],[81163,1,0.0002822]],2,1793158779209]"
BOOK_UPDATE = "[396954,[81154,0,1],3,1793158779620]"
CHECKSUM = '[396954,"cs",-717955449,61,1793158781868]'
HEARTBEAT = '[396954,"hb",299,1793158793000]'
TRADES_SNAPSHOT = "[258792,[[2000903768,1793158765067,0.00016296,81173]],1,1793158778228]"

INFO_20051 = (
    '{"event":"info","code":20051,'
    '"msg":"Stop/Restart websocket server (please reconnect)","version":2}'
)
INFO_20060 = '{"event":"info","code":20060,"msg":"Entering maintenance window"}'
INFO_20061 = '{"event":"info","code":20061,"msg":"Exiting maintenance window"}'
ERROR_FRAME = '{"event":"error","code":10000,"msg":"Unknown symbol"}'


def jitterless_backoff() -> Backoff:
    """The production base with jitter off, so the schedule is exact."""
    return Backoff(base=BITFINEX_BACKOFF_BASE_S, jitter=0.0)


class FakeVenue:
    """Fake Bitfinex: records frames and drops connections after a frame count."""

    def __init__(self, *, drop_after: int | None = None) -> None:
        self.received: list[str] = []
        self.connections: list[ServerConnection] = []
        self.connection_count = 0
        self._received_event = asyncio.Event()
        self._drop_after = drop_after

    async def handler(self, socket: ServerConnection) -> None:
        self.connection_count += 1
        self.connections.append(socket)
        frames_here = 0
        try:
            async for frame in socket:
                frames_here += 1
                self.received.append(frame)
                self._received_event.set()
                if self._drop_after is not None and frames_here == self._drop_after:
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
async def fake_venue(*, drop_after: int | None = None) -> AsyncIterator[tuple[FakeVenue, str]]:
    venue = FakeVenue(drop_after=drop_after)
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


async def _no_wait(_seconds: float) -> None:
    """A resync deadline that fires immediately; used to test the timeout path."""


async def test_stream_control_lines_subscriptions_and_resync_round_trip() -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue() as (venue, url):
        connector = BitfinexConnector(
            ["BTC-USD"],
            url=url,
            backoff_factory=jitterless_backoff,
            sleep=sleep,
            clock=FakeClock(),
            token_hex=token_factory(),
        )
        stream = connector.stream()

        connect_line = await anext(stream)
        assert connect_line.kind == "control"
        assert connect_line.ingest_seq == 0
        assert connect_line.connection_id == "bitfinex-20261101T000000Z-00000000"
        assert json.loads(connect_line.payload) == {
            "event": "connect",
            "url": url,
            "symbols": ["BTC-USD"],
            "subscriptions": subscribe_payloads("BTC-USD"),
        }
        with pytest.raises(RuntimeError):
            await anext(connector.stream())

        await venue.wait_for_received(SUBSCRIBE_FRAMES)
        assert venue.received == [compact(payload) for payload in subscribe_payloads("BTC-USD")]

        await venue.send(SUBSCRIBED_TRADES)
        await venue.send(SUBSCRIBED_BOOK)
        trades_ack = await anext(stream)
        book_ack = await anext(stream)
        assert (trades_ack.kind, trades_ack.ingest_seq, trades_ack.payload) == (
            "msg",
            1,
            SUBSCRIBED_TRADES,
        )
        assert (book_ack.kind, book_ack.ingest_seq, book_ack.payload) == (
            "msg",
            2,
            SUBSCRIBED_BOOK,
        )

        await venue.send(BOOK_SNAPSHOT)
        snapshot = await anext(stream)
        assert (snapshot.ingest_seq, snapshot.payload) == (3, BOOK_SNAPSHOT)

        await connector.resync("BTC-USD", "checksum_failure")
        request = await anext(stream)
        assert request.kind == "control"
        assert request.ingest_seq == 4
        assert json.loads(request.payload) == {
            "event": "resync_request",
            "symbol": "BTC-USD",
            "reason": "checksum_failure",
        }
        await venue.wait_for_received(SUBSCRIBE_FRAMES + 2)
        assert venue.received[SUBSCRIBE_FRAMES:] == [
            compact(unsubscribe_payload(396954)),
            compact(book_subscribe_payload("BTC-USD")),
        ]

        # The venue reuses the channel id and answers the resubscribe with a
        # fresh snapshot; frames from the replaced subscription keep flowing.
        await venue.send(SUBSCRIBED_BOOK)
        same_channel = await anext(stream)
        assert same_channel.ingest_seq == 5
        assert same_channel.payload == SUBSCRIBED_BOOK
        await venue.send(BOOK_SNAPSHOT)
        fresh = await anext(stream)
        assert (fresh.ingest_seq, fresh.payload) == (6, BOOK_SNAPSHOT)

        await venue.send(BOOK_UPDATE)
        await venue.send(CHECKSUM)
        update = await anext(stream)
        checksum = await anext(stream)
        assert (update.ingest_seq, update.payload) == (7, BOOK_UPDATE)
        assert (checksum.ingest_seq, checksum.payload) == (8, CHECKSUM)

        await connector.close()
        disconnect = await anext(stream)
        assert disconnect.kind == "control"
        assert disconnect.ingest_seq == 9
        assert disconnect.connection_id == connect_line.connection_id
        assert json.loads(disconnect.payload) == {"event": "disconnect", "reason": "closed"}
        with pytest.raises(StopAsyncIteration):
            await anext(stream)
        assert slept == []
        assert venue.connection_count == 1


async def test_one_connection_per_symbol() -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue() as (venue, url):
        connector = BitfinexConnector(
            ["BTC-USD", "ETH-USD"],
            url=url,
            backoff_factory=jitterless_backoff,
            sleep=sleep,
            clock=FakeClock(),
            token_hex=token_factory(),
        )
        stream = connector.stream()
        connect_lines = []
        while len(connect_lines) < 2:
            message = await anext(stream)
            if message.kind == "control" and json.loads(message.payload)["event"] == "connect":
                connect_lines.append(message)

        payloads = {
            json.loads(line.payload)["symbols"][0]: json.loads(line.payload)
            for line in connect_lines
        }
        assert set(payloads) == {"BTC-USD", "ETH-USD"}
        for symbol, payload in payloads.items():
            assert payload["subscriptions"] == subscribe_payloads(symbol)
        assert {line.connection_id for line in connect_lines} == {
            "bitfinex-20261101T000000Z-00000000",
            "bitfinex-20261101T000000Z-00000001",
        }
        assert {line.ingest_seq for line in connect_lines} == {0}

        await venue.wait_for_received(2 * SUBSCRIBE_FRAMES)
        expected_blocks = sorted(
            compact(payload)
            for symbol in ("BTC-USD", "ETH-USD")
            for payload in subscribe_payloads(symbol)
        )
        assert sorted(venue.received) == expected_blocks
        # Each connection sends its own frames in order, not interleaved.
        blocks = [
            venue.received[start : start + SUBSCRIBE_FRAMES]
            for start in range(0, len(venue.received), SUBSCRIBE_FRAMES)
        ]
        expected_groups = [
            [compact(payload) for payload in subscribe_payloads(symbol)]
            for symbol in ("BTC-USD", "ETH-USD")
        ]
        assert sorted(blocks) == sorted(expected_groups)

        await connector.close()
        remaining = [message async for message in stream]
        disconnects = [message for message in remaining if message.kind == "control"]
        assert len(disconnects) == 2
        assert {message.connection_id for message in disconnects} == {
            line.connection_id for line in connect_lines
        }
        assert all(
            json.loads(message.payload) == {"event": "disconnect", "reason": "closed"}
            for message in disconnects
        )
        assert slept == []
        assert venue.connection_count == 2


async def test_reconnect_after_server_close_uses_rate_limited_backoff() -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue(drop_after=SUBSCRIBE_FRAMES) as (venue, url):
        connector = BitfinexConnector(
            ["BTC-USD"],
            url=url,
            backoff_factory=jitterless_backoff,
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

        assert first_connect.connection_id == "bitfinex-20261101T000000Z-00000000"
        assert first_connect.ingest_seq == 0
        assert first_disconnect.ingest_seq == 1
        assert json.loads(first_disconnect.payload) == {
            "event": "disconnect",
            "reason": "connection_lost",
        }
        assert second_connect.connection_id == "bitfinex-20261101T000000Z-00000001"
        assert second_connect.ingest_seq == 0
        assert json.loads(second_disconnect.payload) == {
            "event": "disconnect",
            "reason": "connection_lost",
        }
        assert third_connect.connection_id == "bitfinex-20261101T000000Z-00000002"
        assert slept == [BITFINEX_BACKOFF_BASE_S, 2 * BITFINEX_BACKOFF_BASE_S]
        # The schedule's jittered floor must respect the venue's ~20/min limit.
        assert BITFINEX_BACKOFF_BASE_S * 0.5 >= BITFINEX_MIN_RECONNECT_S

        await venue.wait_for_received(3 * SUBSCRIBE_FRAMES)
        assert venue.received == [compact(payload) for payload in subscribe_payloads("BTC-USD")] * 3
        assert json.loads(third_connect.payload)["subscriptions"] == subscribe_payloads("BTC-USD")

        await connector.close()
        remaining = [message async for message in stream]
        assert remaining[-1].kind == "control"
        assert remaining[-1].connection_id == third_connect.connection_id
        assert json.loads(remaining[-1].payload) == {"event": "disconnect", "reason": "closed"}


async def test_missing_resync_snapshot_reconnects() -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue() as (venue, url):
        connector = BitfinexConnector(
            ["BTC-USD"],
            url=url,
            timer=_no_wait,
            backoff_factory=jitterless_backoff,
            sleep=sleep,
            clock=FakeClock(),
            token_hex=token_factory(),
        )
        stream = connector.stream()

        first_connect = await anext(stream)
        await venue.wait_for_received(SUBSCRIBE_FRAMES)
        await venue.send(SUBSCRIBED_TRADES)
        await venue.send(SUBSCRIBED_BOOK)
        await anext(stream)
        await anext(stream)

        await connector.resync("BTC-USD", "checksum_failure")
        request = await anext(stream)
        assert json.loads(request.payload) == {
            "event": "resync_request",
            "symbol": "BTC-USD",
            "reason": "checksum_failure",
        }
        disconnect = await anext(stream)
        assert json.loads(disconnect.payload) == {
            "event": "disconnect",
            "reason": "connection_lost",
        }
        second_connect = await anext(stream)
        assert second_connect.connection_id != first_connect.connection_id
        assert second_connect.ingest_seq == 0
        assert json.loads(second_connect.payload)["symbols"] == ["BTC-USD"]
        assert slept == [BITFINEX_BACKOFF_BASE_S]

        await connector.close()
        remaining = [message async for message in stream]
        assert json.loads(remaining[-1].payload) == {"event": "disconnect", "reason": "closed"}


async def test_info_20051_reconnects() -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue() as (venue, url):
        connector = BitfinexConnector(
            ["BTC-USD"],
            url=url,
            backoff_factory=jitterless_backoff,
            sleep=sleep,
            clock=FakeClock(),
            token_hex=token_factory(),
        )
        stream = connector.stream()
        first_connect = await anext(stream)
        await venue.wait_for_received(SUBSCRIBE_FRAMES)

        await venue.send(INFO_20051)
        info = await anext(stream)
        assert (info.kind, info.ingest_seq, info.payload) == ("msg", 1, INFO_20051)
        disconnect = await anext(stream)
        assert json.loads(disconnect.payload) == {
            "event": "disconnect",
            "reason": "connection_lost",
        }
        second_connect = await anext(stream)
        assert second_connect.connection_id != first_connect.connection_id
        assert slept == [BITFINEX_BACKOFF_BASE_S]

        await connector.close()
        remaining = [message async for message in stream]
        assert json.loads(remaining[-1].payload) == {"event": "disconnect", "reason": "closed"}


async def test_maintenance_pauses_then_resubscribes_all(
    caplog: pytest.LogCaptureFixture,
) -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue() as (venue, url):
        connector = BitfinexConnector(
            ["BTC-USD"],
            url=url,
            backoff_factory=jitterless_backoff,
            sleep=sleep,
            clock=FakeClock(),
            token_hex=token_factory(),
        )
        stream = connector.stream()
        await anext(stream)
        await venue.wait_for_received(SUBSCRIBE_FRAMES)
        await venue.send(SUBSCRIBED_TRADES)
        await venue.send(SUBSCRIBED_BOOK)
        await anext(stream)
        await anext(stream)

        await venue.send(INFO_20060)
        paused = await anext(stream)
        assert paused.payload == INFO_20060
        assert "maintenance window started" in caplog.text

        # A resync during the maintenance window is recorded but not sent.
        await connector.resync("BTC-USD", "gap")
        request = await anext(stream)
        assert json.loads(request.payload)["event"] == "resync_request"
        await asyncio.sleep(0)
        assert len(venue.received) == SUBSCRIBE_FRAMES

        await venue.send(INFO_20061)
        resumed = await anext(stream)
        assert resumed.payload == INFO_20061
        await venue.wait_for_received(SUBSCRIBE_FRAMES + 4)
        assert venue.received[SUBSCRIBE_FRAMES:] == [
            compact(unsubscribe_payload(396954)),
            compact(unsubscribe_payload(258792)),
            compact(book_subscribe_payload("BTC-USD")),
            compact(trades_subscribe_payload("BTC-USD")),
        ]
        assert venue.connection_count == 1
        assert slept == []

        await connector.close()
        remaining = [message async for message in stream]
        assert json.loads(remaining[-1].payload) == {"event": "disconnect", "reason": "closed"}


async def test_error_event_reconnects(caplog: pytest.LogCaptureFixture) -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async with fake_venue() as (venue, url):
        connector = BitfinexConnector(
            ["BTC-USD"],
            url=url,
            backoff_factory=jitterless_backoff,
            sleep=sleep,
            clock=FakeClock(),
            token_hex=token_factory(),
        )
        stream = connector.stream()
        first_connect = await anext(stream)
        await venue.wait_for_received(SUBSCRIBE_FRAMES)

        await venue.send(ERROR_FRAME)
        error = await anext(stream)
        assert error.payload == ERROR_FRAME
        disconnect = await anext(stream)
        assert json.loads(disconnect.payload)["reason"] == "connection_lost"
        second_connect = await anext(stream)
        assert second_connect.connection_id != first_connect.connection_id
        assert slept == [BITFINEX_BACKOFF_BASE_S]
        assert "bitfinex sent an error frame" in caplog.text

        await connector.close()
        [message async for message in stream]


async def test_subscribe_replaces_symbols_before_the_stream_starts() -> None:
    async with fake_venue() as (venue, url):
        connector = BitfinexConnector(
            ["BTC-USD"], url=url, clock=FakeClock(), token_hex=token_factory()
        )
        await connector.subscribe(["ETH-USD"])
        stream = connector.stream()
        connect_line = await anext(stream)
        payload = json.loads(connect_line.payload)
        assert payload["symbols"] == ["ETH-USD"]
        assert payload["subscriptions"] == subscribe_payloads("ETH-USD")
        with pytest.raises(RuntimeError):
            await connector.subscribe(["BTC-USD"])
        await connector.close()
        [message async for message in stream]
        await venue.wait_for_received(SUBSCRIBE_FRAMES)
        assert venue.received == [compact(frame) for frame in payload["subscriptions"]]


async def test_resync_rejects_unknown_symbols() -> None:
    connector = BitfinexConnector(["BTC-USD"])
    with pytest.raises(ValueError, match="unknown symbol"):
        await connector.resync("ETH-USD", "checksum_failure")


async def test_close_before_stream_finishes_immediately() -> None:
    connector = BitfinexConnector(["BTC-USD"])
    await connector.close()
    assert [message async for message in connector.stream()] == []


async def test_close_while_waiting_for_the_resync_snapshot_leaks_nothing() -> None:
    loop = asyncio.get_running_loop()
    recorded: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()
    previous_debug = loop.get_debug()
    loop.set_exception_handler(lambda _loop, context: recorded.append(context))
    loop.set_debug(True)
    try:
        async with fake_venue() as (venue, url):
            connector = BitfinexConnector(
                ["BTC-USD"], url=url, clock=FakeClock(), token_hex=token_factory()
            )
            stream = connector.stream()
            await anext(stream)
            await venue.wait_for_received(SUBSCRIBE_FRAMES)
            await venue.send(SUBSCRIBED_TRADES)
            await venue.send(SUBSCRIBED_BOOK)
            await anext(stream)
            await anext(stream)

            await connector.resync("BTC-USD", "checksum_failure")
            await anext(stream)
            # Wait until the unsubscribe/resubscribe is on the wire, then give
            # the pump a tick to block on the snapshot with its deadline pending.
            await venue.wait_for_received(SUBSCRIBE_FRAMES + 2)
            await asyncio.sleep(0)
            await connector.close()
            remaining = [message async for message in stream]
            assert json.loads(remaining[-1].payload) == {
                "event": "disconnect",
                "reason": "closed",
            }
            await asyncio.sleep(0.05)

            pending = [
                pending_task
                for pending_task in asyncio.all_tasks()
                if pending_task is not asyncio.current_task() and not pending_task.done()
            ]
            assert pending == []
            gc.collect()
            assert recorded == []
    finally:
        loop.set_exception_handler(previous_handler)
        loop.set_debug(previous_debug)


async def _consume(connector: BitfinexConnector, connected: asyncio.Event) -> None:
    async for _message in connector.stream():
        connected.set()


async def test_cancel_mid_stream_leaves_nothing_pending_or_unretrieved() -> None:
    loop = asyncio.get_running_loop()
    recorded: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()
    previous_debug = loop.get_debug()
    loop.set_exception_handler(lambda _loop, context: recorded.append(context))
    loop.set_debug(True)
    try:
        async with fake_venue() as (venue, url):
            connector = BitfinexConnector(
                ["BTC-USD"], url=url, clock=FakeClock(), token_hex=token_factory()
            )
            connected = asyncio.Event()
            task = asyncio.create_task(_consume(connector, connected))
            await asyncio.wait_for(connected.wait(), timeout=10)
            # The pump creates its receiver/command tasks before the server
            # sees the subscriptions, so this pins the state we cancel from.
            await venue.wait_for_received(SUBSCRIBE_FRAMES)

            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await connector.close()
            await connector.close()
            await asyncio.sleep(0.05)

            pending = [
                pending_task
                for pending_task in asyncio.all_tasks()
                if pending_task is not asyncio.current_task() and not pending_task.done()
            ]
            assert pending == []
            gc.collect()
            assert recorded == []
    finally:
        loop.set_exception_handler(previous_handler)
        loop.set_debug(previous_debug)


def test_venue_symbol_mapping() -> None:
    assert venue_symbol("BTC-USD") == "tBTCUSD"
    assert venue_symbol("ETH-USD") == "tETHUSD"
    assert venue_symbol("USDT-USD") == "tUSDT:USD"
    assert venue_symbol("BTC-USDT") == "tBTC:USDT"
    with pytest.raises(ValueError, match="BASE-QUOTE"):
        venue_symbol("BTCUSD")
    with pytest.raises(ValueError, match="BASE-QUOTE"):
        venue_symbol("BTC-USD-EXTRA")


def _fixture_records() -> list[dict[str, object]]:
    path = pathlib.Path(__file__).parent / "fixtures" / "bitfinex_btc_5m.jsonl.zst"
    with path.open("rb") as stream:
        raw = zstandard.ZstdDecompressor().stream_reader(stream).read()
    return [json.loads(line) for line in raw.decode("utf-8").splitlines()]


def test_recorded_connect_payload_matches_the_subscription_frames() -> None:
    records = _fixture_records()
    connect = json.loads(records[0]["payload"])
    assert connect["event"] == "connect"
    assert connect["symbols"] == ["BTC-USD"]
    assert connect["subscriptions"] == subscribe_payloads("BTC-USD")
    assert [record["ingest_seq"] for record in records] == list(range(len(records)))


def test_recorded_frame_shapes_match_the_connector_classification() -> None:
    records = _fixture_records()
    counts: Counter[str] = Counter()
    book_chan_id: int | None = None
    for record in records[1:-1]:
        document = json.loads(record["payload"])
        if isinstance(document, dict):
            if document.get("event") == "subscribed":
                counts[f"ack_{document.get('channel')}"] += 1
                if document.get("channel") == "book":
                    book_chan_id = document["chanId"]
            continue
        payload = document[1]
        if payload == "cs":
            counts["cs"] += 1
        elif payload == "hb":
            counts["hb"] += 1
        elif payload == "te":
            counts["te"] += 1
        elif payload == "tu":
            counts["tu"] += 1
        elif is_book_snapshot(document, book_chan_id):
            counts["book_snapshot"] += 1
        elif isinstance(payload, list) and payload and isinstance(payload[0], list):
            counts["trades_snapshot"] += 1
        else:
            counts["book_update"] += 1
    assert counts == {
        "ack_trades": 1,
        "ack_book": 1,
        "trades_snapshot": 1,
        "book_snapshot": 1,
        "book_update": 7832,
        "cs": 91,
        "hb": 40,
        "te": 135,
        "tu": 135,
    }


def test_recorded_heartbeat_and_checksum_frames_are_not_snapshots() -> None:
    also_trades = json.loads(TRADES_SNAPSHOT)
    assert not is_book_snapshot(also_trades, 396954)
    assert not is_book_snapshot(json.loads(BOOK_UPDATE), 396954)
    assert not is_book_snapshot(json.loads(CHECKSUM), 396954)
    assert not is_book_snapshot(json.loads(HEARTBEAT), 396954)
    assert not is_book_snapshot(json.loads(SUBSCRIBED_BOOK), 396954)
    assert is_book_snapshot(json.loads(BOOK_SNAPSHOT), 396954)
    assert not is_book_snapshot(json.loads(BOOK_SNAPSHOT), None)
