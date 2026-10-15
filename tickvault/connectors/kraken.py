"""Live Kraken websocket v2 connector.

One connection carries every symbol: ``book`` at the configured depth and
``trade`` with ``snapshot: false``. Each ``connect`` control payload declares
all normalized symbols the connection serves in ``symbols``, so the book
processor scopes the connection per symbol. Symbols are translated from the
pipeline's ``BASE-QUOTE`` to Kraken's ``BASE/QUOTE`` only on the wire; control
records and the connector API use the normalized symbol.

``resync`` re-synchronizes one symbol by unsubscribing and re-subscribing
``book`` for it. Verified live on 2026-10-15: Kraken matches a subscription by
all of its params, so the unsubscribe must repeat ``depth`` (otherwise Kraken
rejects it with "Subscription with depth 10 not Found"); with the depth
repeated, the resubscribe answers with a fresh snapshot, so no reconnect is
needed. A subscribe ack with ``success: false`` is logged and treated as a
connection failure: the socket is dropped and re-established with the
reconnect backoff.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
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
    wait_first,
)
from tickvault.models import RawMessage

KRAKEN_URL = "wss://ws.kraken.com/v2"
KRAKEN_DEPTH = 25
KRAKEN_MAX_SIZE = 64 * 1024 * 1024

_LOGGER = logging.getLogger(__name__)

_CONNECTION_ERRORS = (ConnectionClosed, OSError, TimeoutError, InvalidHandshake, InvalidURI)


class SubscribeRejected(Exception):
    """A subscribe ack reported ``success: false``; the connection is rebuilt."""


def venue_symbol(symbol: str) -> str:
    """Translate a normalized ``BASE-QUOTE`` symbol to Kraken's ``BASE/QUOTE``."""
    return symbol.replace("-", "/")


def subscribe_payloads(
    venue_symbols: Sequence[str], depth: int = KRAKEN_DEPTH
) -> list[dict[str, object]]:
    """The two subscribe payloads for a Kraken v2 connection."""
    symbols = list(venue_symbols)
    return [
        {
            "method": "subscribe",
            "params": {"channel": "book", "symbol": symbols, "depth": depth},
        },
        {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": symbols, "snapshot": False},
        },
    ]


def unsubscribe_book_payload(symbol: str, depth: int = KRAKEN_DEPTH) -> dict[str, object]:
    """The unsubscribe payload for one venue symbol's ``book`` channel.

    Kraken matches a subscription by all of its params, so ``depth`` must be
    repeated exactly as subscribed or the request is rejected.
    """
    return {
        "method": "unsubscribe",
        "params": {"channel": "book", "symbol": [symbol], "depth": depth},
    }


def subscribe_book_payload(symbol: str, depth: int = KRAKEN_DEPTH) -> dict[str, object]:
    """The subscribe payload for one venue symbol's ``book`` channel."""
    return {
        "method": "subscribe",
        "params": {"channel": "book", "symbol": [symbol], "depth": depth},
    }


def _is_rejected_subscribe(frame: str) -> bool:
    """True when ``frame`` is a subscribe ack with ``success: false``."""
    try:
        document = json.loads(frame)
    except json.JSONDecodeError:
        return False
    return isinstance(document, dict) and document.get("success") is False


class KrakenConnector:
    """One Kraken v2 websocket for all symbols, merged into one raw stream.

    ``stream`` yields the connection's ``connect`` control line first, then
    every received frame unmodified (``status``, ``heartbeat`` and subscribe
    acks included), then its ``disconnect`` control line; ``resync`` calls
    insert a ``resync_request`` control line. :meth:`close` closes the socket
    and the stream ends after the final ``disconnect``.
    """

    venue = "kraken"

    def __init__(
        self,
        symbols: Sequence[str],
        url: str = KRAKEN_URL,
        *,
        depth: int = KRAKEN_DEPTH,
        max_size: int = KRAKEN_MAX_SIZE,
        open_timeout: float | None = 10.0,
        close_timeout: float | None = 1.0,
        backoff_factory: Callable[[], Backoff] = Backoff,
        sleep: SleepFn = asyncio.sleep,
        clock: ClockFn = time.time_ns,
        token_hex: Callable[[int], str] = secrets.token_hex,
    ) -> None:
        self._symbols = tuple(symbols)
        self._url = url
        self._depth = depth
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
        self._commands: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._socket: ClientConnection | None = None
        self._task: asyncio.Task[None] | None = None

    async def subscribe(self, symbols: Sequence[str]) -> None:
        """Set the symbols to stream; only valid before :meth:`stream` starts.

        This connector opens its single connection with the full symbol set, so
        the set is fixed at connect time rather than sent on an open socket.
        """
        if self._started:
            raise RuntimeError("subscriptions are fixed once the stream starts")
        self._symbols = tuple(symbols)

    async def resync(self, symbol: str, reason: str) -> None:
        """Re-synchronize ``symbol`` with an unsubscribe/resubscribe of ``book``.

        ``symbol`` is the normalized ``BASE-QUOTE`` symbol; the frames sent to
        Kraken use the venue's ``BASE/QUOTE`` form.
        """
        if symbol not in self._symbols:
            raise ValueError(f"unknown symbol: {symbol!r}")
        await self._commands.put((symbol, reason))

    async def close(self) -> None:
        """Close the connection; the stream ends after its ``disconnect`` line."""
        self._closed = True
        self._stop.set()
        if self._socket is not None:
            with contextlib.suppress(Exception):
                await self._socket.close()

    async def stream(self) -> AsyncIterator[RawMessage]:
        """Yield raw messages from the Kraken connection until closed."""
        if self._started:
            raise RuntimeError("stream() may only be called once")
        self._started = True
        if self._closed or not self._symbols:
            return
        self._task = asyncio.create_task(self._run())
        try:
            while True:
                message = await self._queue.get()
                if message is None:
                    break
                yield message
        finally:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        """Keep the connection alive until :meth:`close`."""
        backoff = self._backoff_factory()
        try:
            while not self._closed:
                await self._run_connection()
                if self._closed:
                    break
                await self._wait_backoff(backoff.next_delay())
        finally:
            await self._queue.put(None)

    async def _run_connection(self) -> None:
        subscriptions = subscribe_payloads(
            [venue_symbol(symbol) for symbol in self._symbols], self._depth
        )
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
                self._socket = socket
                try:
                    await self._queue.put(
                        emitter.control(
                            control_payload(
                                "connect",
                                url=self._url,
                                symbols=list(self._symbols),
                                subscriptions=subscriptions,
                            )
                        )
                    )
                    for payload in subscriptions:
                        await socket.send(json.dumps(payload, separators=(",", ":")))
                    await self._pump(socket, emitter)
                finally:
                    self._socket = None
        except _CONNECTION_ERRORS:
            pass
        except SubscribeRejected:
            pass
        finally:
            if established:
                reason = "closed" if self._closed else "connection_lost"
                await self._queue.put(
                    emitter.control(control_payload("disconnect", reason=reason))
                )

    async def _pump(self, socket: ClientConnection, emitter: RawEmitter) -> None:
        """Forward frames and handle resync commands until the socket ends."""
        while True:
            receiver = asyncio.create_task(socket.recv())
            command = asyncio.create_task(self._commands.get())
            done = await wait_first((receiver, command))
            if receiver in done:
                frame = receiver.result()
                if isinstance(frame, bytes):
                    frame = frame.decode("utf-8")
                await self._queue.put(emitter.message(frame))
                if _is_rejected_subscribe(frame):
                    _LOGGER.warning("kraken rejected a subscribe request: %s", frame)
                    raise SubscribeRejected(frame)
            if command in done:
                symbol, reason = command.result()
                await self._queue.put(
                    emitter.control(
                        control_payload("resync_request", symbol=symbol, reason=reason)
                    )
                )
                venue = venue_symbol(symbol)
                for payload in (
                    unsubscribe_book_payload(venue, self._depth),
                    subscribe_book_payload(venue, self._depth),
                ):
                    await socket.send(json.dumps(payload, separators=(",", ":")))

    async def _wait_backoff(self, seconds: float) -> None:
        """Sleep for the backoff, waking early when :meth:`close` is called."""
        sleeper: asyncio.Task[None] = asyncio.create_task(self._sleep(seconds))
        stopper: asyncio.Task[bool] = asyncio.create_task(self._stop.wait())
        await wait_first((sleeper, stopper))
