"""Live Coinbase Advanced Trade websocket connector.

One connection per product, each carrying ``heartbeats``, ``level2`` and
``market_trades``, so a per-connection ``sequence_num`` gap always scopes to
one product. Every ``connect`` control payload declares the one normalized
symbol the connection serves in ``symbols``, so the book processor scopes the
connection per symbol instead of per venue. Connections come back with the
shared backoff schedule; every connection gets a fresh ``connection_id`` and
an ``ingest_seq`` that restarts at ``0``.

``resync`` re-synchronizes one product by unsubscribing and re-subscribing
``level2`` on the live connection. Verified live on 2026-10-15: Advanced Trade
answers a resubscribe with a fresh full-depth snapshot (39,792 levels for
BTC-USD) about half a second later, so no reconnect is needed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
import time
from collections.abc import AsyncIterator, Callable, Sequence

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI

from tickvault.connectors._ws import (
    Backoff,
    ClockFn,
    RawEmitter,
    SleepFn,
    control_payload,
    make_connection_id,
)
from tickvault.models import RawMessage

COINBASE_URL = "wss://advanced-trade-ws.coinbase.com"

COINBASE_CHANNELS = ("heartbeats", "level2", "market_trades")
"""Channels subscribed on every product connection, in this order."""

COINBASE_MAX_SIZE = 64 * 1024 * 1024
"""The level2 snapshot is a multi-MB text frame; the 1 MiB default is too small."""

# Failures that mean "the connection dropped or could not be established" and
# are retried with backoff. Anything else (a bug in this connector) propagates.
_CONNECTION_ERRORS = (ConnectionClosed, OSError, TimeoutError, InvalidHandshake, InvalidURI)


def subscribe_payload(product: str, channel: str) -> dict[str, object]:
    """One Advanced Trade subscribe payload for ``product`` and ``channel``."""
    return {"type": "subscribe", "product_ids": [product], "channel": channel}


def unsubscribe_payload(product: str, channel: str) -> dict[str, object]:
    """One Advanced Trade unsubscribe payload for ``product`` and ``channel``."""
    return {"type": "unsubscribe", "product_ids": [product], "channel": channel}


def subscribe_payloads(product: str) -> list[dict[str, object]]:
    """The subscribe payloads for one product connection, in channel order."""
    return [subscribe_payload(product, channel) for channel in COINBASE_CHANNELS]


class CoinbaseConnector:
    """Multiplexes one Advanced Trade websocket per product into one raw stream.

    ``stream`` yields the ``connect`` control line of each connection first,
    then every received frame unmodified, then its ``disconnect`` control line
    when the connection ends; ``resync`` calls insert a ``resync_request``
    control line on the affected connection. Calling :meth:`close` closes every
    socket and lets each connection task write its final ``disconnect`` line
    before the stream ends.
    """

    venue = "coinbase"

    def __init__(
        self,
        symbols: Sequence[str],
        url: str = COINBASE_URL,
        *,
        max_size: int = COINBASE_MAX_SIZE,
        open_timeout: float | None = 10.0,
        close_timeout: float | None = 2.0,
        backoff_factory: Callable[[], Backoff] = Backoff,
        sleep: SleepFn = asyncio.sleep,
        clock: ClockFn = time.time_ns,
        token_hex: Callable[[int], str] = secrets.token_hex,
    ) -> None:
        self._symbols = tuple(symbols)
        self._url = url
        self._max_size = max_size
        self._open_timeout = open_timeout
        self._close_timeout = close_timeout
        self._backoff_factory = backoff_factory
        self._sleep = sleep
        self._clock = clock
        self._token_hex = token_hex
        self._closed = False
        self._started = False
        self._stop = asyncio.Event()
        self._queue: asyncio.Queue[RawMessage | None] = asyncio.Queue()
        self._commands: dict[str, asyncio.Queue[str]] = {
            symbol: asyncio.Queue() for symbol in self._symbols
        }
        self._sockets: dict[str, ClientConnection] = {}
        self._tasks: list[asyncio.Task[None]] = []

    async def subscribe(self, symbols: Sequence[str]) -> None:
        """Set the products to stream; only valid before :meth:`stream` starts.

        This connector sends its subscriptions when each connection opens and
        serves one connection per product, so the symbol set is fixed at
        connect time rather than sent on an open socket.
        """
        if self._started:
            raise RuntimeError("subscriptions are fixed once the stream starts")
        self._symbols = tuple(symbols)
        self._commands = {symbol: asyncio.Queue() for symbol in self._symbols}

    async def resync(self, symbol: str, reason: str) -> None:
        """Re-synchronize ``symbol`` with an unsubscribe/resubscribe of ``level2``.

        The product's connection task emits the ``resync_request`` control line
        and sends the unsubscribe/subscribe frames in that order.
        """
        commands = self._commands.get(symbol)
        if commands is None:
            raise ValueError(f"unknown symbol: {symbol!r}")
        await commands.put(reason)

    async def close(self) -> None:
        """Close every live connection; the stream ends after the final disconnects."""
        self._closed = True
        self._stop.set()
        for socket in list(self._sockets.values()):
            with contextlib.suppress(Exception):
                await socket.close()

    async def stream(self) -> AsyncIterator[RawMessage]:
        """Yield raw messages from all product connections until closed."""
        if self._started:
            raise RuntimeError("stream() may only be called once")
        self._started = True
        if self._closed or not self._symbols:
            return
        self._tasks = [
            asyncio.create_task(self._run_product(symbol)) for symbol in self._symbols
        ]
        finished = 0
        try:
            while finished < len(self._tasks):
                message = await self._queue.get()
                if message is None:
                    finished += 1
                    continue
                yield message
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _run_product(self, symbol: str) -> None:
        """Keep one product connected until :meth:`close`."""
        backoff = self._backoff_factory()
        try:
            while not self._closed:
                await self._run_connection(symbol)
                if self._closed:
                    break
                await self._wait_backoff(backoff.next_delay())
        finally:
            await self._queue.put(None)

    async def _run_connection(self, symbol: str) -> None:
        subscriptions = subscribe_payloads(symbol)
        connection_id = make_connection_id(self.venue, self._clock(), token_hex=self._token_hex)
        emitter = RawEmitter(self.venue, connection_id, clock=self._clock)
        established = False
        try:
            async with connect(
                self._url,
                max_size=self._max_size,
                open_timeout=self._open_timeout,
                close_timeout=self._close_timeout,
            ) as socket:
                established = True
                self._sockets[symbol] = socket
                try:
                    await self._queue.put(
                        emitter.control(
                            control_payload(
                                "connect",
                                url=self._url,
                                symbols=[symbol],
                                subscriptions=subscriptions,
                            )
                        )
                    )
                    for payload in subscriptions:
                        await socket.send(json.dumps(payload, separators=(",", ":")))
                    await self._pump(symbol, socket, emitter)
                finally:
                    self._sockets.pop(symbol, None)
        except _CONNECTION_ERRORS:
            pass
        finally:
            if established:
                reason = "closed" if self._closed else "connection_lost"
                await self._queue.put(
                    emitter.control(control_payload("disconnect", reason=reason))
                )

    async def _pump(
        self, symbol: str, socket: ClientConnection, emitter: RawEmitter
    ) -> None:
        """Forward frames and handle resync commands until the socket ends."""
        commands = self._commands[symbol]
        while True:
            receiver = asyncio.create_task(socket.recv())
            command = asyncio.create_task(commands.get())
            done, pending = await asyncio.wait(
                {receiver, command}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if receiver in done:
                frame = receiver.result()
                if isinstance(frame, bytes):
                    frame = frame.decode("utf-8")
                await self._queue.put(emitter.message(frame))
            if command in done:
                reason = command.result()
                await self._queue.put(
                    emitter.control(
                        control_payload("resync_request", symbol=symbol, reason=reason)
                    )
                )
                for payload in (
                    unsubscribe_payload(symbol, "level2"),
                    subscribe_payload(symbol, "level2"),
                ):
                    await socket.send(json.dumps(payload, separators=(",", ":")))

    async def _wait_backoff(self, seconds: float) -> None:
        """Sleep for the backoff, waking early when :meth:`close` is called."""
        sleeper: asyncio.Task[None] = asyncio.create_task(self._sleep(seconds))
        stopper: asyncio.Task[bool] = asyncio.create_task(self._stop.wait())
        _, pending = await asyncio.wait(
            {sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
