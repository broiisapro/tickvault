"""Live Bitfinex websocket v2 connector.

One websocket per symbol, each carrying ``book`` at ``prec P0`` / ``len 25``
and ``trades``, so a per-connection sequence gap always scopes to one symbol.
Every ``connect`` control payload declares the one normalized symbol the
connection serves, so the book processor scopes the connection per symbol.
Symbols are translated from the pipeline's ``BASE-QUOTE`` to the venue's
``t<BASE><QUOTE>`` (``t<BASE>:<QUOTE>`` when an asset is longer than three
letters) only on the wire; control records and the connector API use the
normalized symbol.

Before subscribing, each connection sends ``conf`` with flags ``229376``:
``SEQ_ALL`` (65536) plus ``OB_CHECKSUM`` (131072) plus ``TIMESTAMP`` (32768).
``SEQ_ALL`` puts a connection-wide sequence number on every frame after the
four unsequenced dict frames (``info``, the ``conf`` ack and the two
``subscribed`` acks), heartbeats and ``tu`` updates included; ``TIMESTAMP``
puts a venue millisecond timestamp on book frames, which otherwise carry none.
Every received frame is yielded unmodified, so the normalizer decides what to
do with it and the sequence tracker still sees heartbeats and ``tu`` frames.

``resync`` re-synchronizes one symbol without reconnecting: unsubscribing
``book`` by its channel id and resubscribing with the same params makes the
venue send a fresh snapshot on the same channel id. Frames of the old
subscription that are still in flight are yielded like any other frame; the
processor is in ``SYNCING`` and drops them. If no snapshot arrives within
:data:`BITFINEX_RESYNC_TIMEOUT_S`, the connection is rebuilt with the backoff
schedule, as it is for info code 20051 ("please reconnect").

Info code 20060 (maintenance start) pauses activity: a resync requested while
paused is recorded as a ``resync_request`` control line but the unsubscribe is
deferred, because the venue answers nothing during the window. Info code 20061
(maintenance end) unsubscribes the channels that were acknowledged and
resubscribes ``book`` and ``trades`` on the same connection, which brings a
fresh snapshot.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import secrets
import time
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from dataclasses import dataclass
from typing import Any, cast

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

BITFINEX_URL = "wss://api-pub.bitfinex.com/ws/2"

BITFINEX_PRECISION = "P0"
"""Book precision: exact prices, one update per level change."""

BITFINEX_BOOK_DEPTH = 25
"""Levels per side subscribed and hashed by the venue checksum."""

BITFINEX_CONF_FLAGS = 229376
"""SEQ_ALL (65536) + OB_CHECKSUM (131072) + TIMESTAMP (32768)."""

BITFINEX_MAX_SIZE = 64 * 1024 * 1024
"""Frame headroom well above the shared 32 MiB floor; snapshots are small but burst."""

BITFINEX_MIN_RECONNECT_S = 3.0
"""The venue allows roughly 20 new connections per minute, one every 3 s."""

BITFINEX_BACKOFF_BASE_S = 6.0
"""Reconnect schedule base; jitter scales it down to no less than the venue limit."""

BITFINEX_RESYNC_TIMEOUT_S = 10.0
"""A resubscribe is expected to answer with a snapshot well within this window."""

type TimerFn = Callable[[float], Coroutine[Any, Any, None]]
"""Sleep used for the resync snapshot deadline; injectable for deterministic tests."""

_LOGGER = logging.getLogger(__name__)

# Failures that mean "the connection dropped or could not be established" and
# are retried with backoff. Anything else (a bug in this connector) propagates.
_CONNECTION_ERRORS = (ConnectionClosed, OSError, TimeoutError, InvalidHandshake, InvalidURI)


class ReconnectRequired(Exception):
    """The venue asked for a reconnect (info code 20051) or a resync timed out."""


class SubscribeRejected(Exception):
    """The venue answered with an ``error`` event; the connection is rebuilt."""


def venue_symbol(symbol: str) -> str:
    """Translate a normalized ``BASE-QUOTE`` symbol to Bitfinex's ``t...`` form.

    Bitfinex concatenates the asset and funding currency behind a ``t`` prefix
    (``tBTCUSD``); when either side is longer than three letters the two are
    separated by a colon (``tUSDT:USD``).
    """
    base, separator, quote = symbol.partition("-")
    if not separator or not base or not quote or "-" in quote:
        raise ValueError(f"not a BASE-QUOTE symbol: {symbol!r}")
    if len(base) > 3 or len(quote) > 3:
        return f"t{base}:{quote}"
    return f"t{base}{quote}"


def conf_payload() -> dict[str, object]:
    """The per-connection configuration frame enabling sequence and checksums."""
    return {"event": "conf", "flags": BITFINEX_CONF_FLAGS}


def book_subscribe_payload(symbol: str) -> dict[str, object]:
    """The ``book`` subscribe payload for one normalized symbol.

    ``len`` is a string and ``prec`` is exact-price ``P0``; both are repeated
    verbatim on every resubscribe, because the venue matches a subscription on
    its params and a different ``len`` would change the checksum depth.
    """
    return {
        "event": "subscribe",
        "channel": "book",
        "symbol": venue_symbol(symbol),
        "prec": BITFINEX_PRECISION,
        "len": str(BITFINEX_BOOK_DEPTH),
    }


def trades_subscribe_payload(symbol: str) -> dict[str, object]:
    """The ``trades`` subscribe payload for one normalized symbol."""
    return {"event": "subscribe", "channel": "trades", "symbol": venue_symbol(symbol)}


def unsubscribe_payload(chan_id: int) -> dict[str, object]:
    """The unsubscribe payload for one acknowledged channel id."""
    return {"event": "unsubscribe", "chanId": chan_id}


def subscribe_payloads(symbol: str) -> list[dict[str, object]]:
    """The frames sent on a fresh connection, in wire order.

    ``conf`` must come first: the flags enable sequence numbers and checksums
    for every channel opened after it.
    """
    return [conf_payload(), book_subscribe_payload(symbol), trades_subscribe_payload(symbol)]


def compact(payload: dict[str, object]) -> str:
    """Encode a framework frame as compact JSON, the form the venue accepts."""
    return json.dumps(payload, separators=(",", ":"))


def _decode(frame: str) -> object | None:
    """Parse a venue frame, returning ``None`` when it is not JSON."""
    try:
        return cast(object, json.loads(frame))
    except json.JSONDecodeError:
        return None


def is_book_snapshot(document: object | None, book_chan_id: int | None) -> bool:
    """True when ``document`` is a ``book`` snapshot for ``book_chan_id``.

    A snapshot is ``[chanId, [[price, count, amount], ...], seq, ts]``; an
    update is ``[chanId, [price, count, amount], seq, ts]``, so the payload
    being a non-empty list of lists is what distinguishes them. Trades
    snapshots have the same shape on the other channel, hence the id check.
    """
    if book_chan_id is None or not isinstance(document, list) or len(document) < 2:
        return False
    if document[0] != book_chan_id:
        return False
    payload = document[1]
    return (
        isinstance(payload, list)
        and bool(payload)
        and all(isinstance(level, list) for level in payload)
    )


@dataclass(slots=True)
class _Channels:
    """What one connection has announced about its channels and venue state."""

    book: int | None = None
    trades: int | None = None
    paused: bool = False


class BitfinexConnector:
    """Multiplexes one Bitfinex websocket per symbol into one raw stream.

    ``stream`` yields the ``connect`` control line of each connection first,
    then every received frame unmodified, then its ``disconnect`` control line
    when the connection ends; ``resync`` calls insert a ``resync_request``
    control line on the affected connection. Calling :meth:`close` closes every
    socket and lets each connection task write its final ``disconnect`` line
    before the stream ends.
    """

    venue = "bitfinex"

    def __init__(
        self,
        symbols: Sequence[str],
        url: str = BITFINEX_URL,
        *,
        max_size: int = BITFINEX_MAX_SIZE,
        open_timeout: float | None = 10.0,
        close_timeout: float | None = 1.0,
        resync_timeout: float = BITFINEX_RESYNC_TIMEOUT_S,
        backoff_factory: Callable[[], Backoff] | None = None,
        sleep: SleepFn = asyncio.sleep,
        timer: TimerFn = asyncio.sleep,
        clock: ClockFn = time.time_ns,
        token_hex: Callable[[int], str] = secrets.token_hex,
    ) -> None:
        self._symbols = tuple(symbols)
        for symbol in self._symbols:
            venue_symbol(symbol)
        self._url = url
        self._max_size = max_size
        self._open_timeout = open_timeout
        self._close_timeout = close_timeout
        self._resync_timeout = resync_timeout
        self._backoff_factory = backoff_factory or (lambda: Backoff(base=BITFINEX_BACKOFF_BASE_S))
        self._sleep = sleep
        self._timer = timer
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
        """Set the symbols to stream; only valid before :meth:`stream` starts.

        This connector opens one connection per symbol and sends its frames
        when each connection opens, so the symbol set is fixed at connect time
        rather than sent on an open socket.
        """
        if self._started:
            raise RuntimeError("subscriptions are fixed once the stream starts")
        for symbol in symbols:
            venue_symbol(symbol)
        self._symbols = tuple(symbols)
        self._commands = {symbol: asyncio.Queue() for symbol in self._symbols}

    async def resync(self, symbol: str, reason: str) -> None:
        """Re-synchronize ``symbol`` with an unsubscribe/resubscribe of ``book``.

        The symbol's connection task emits the ``resync_request`` control line
        and replaces its book subscription, which brings a fresh snapshot.
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
        """Yield raw messages from all symbol connections until closed."""
        if self._started:
            raise RuntimeError("stream() may only be called once")
        self._started = True
        if self._closed or not self._symbols:
            return
        self._tasks = [asyncio.create_task(self._run_symbol(symbol)) for symbol in self._symbols]
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

    async def _run_symbol(self, symbol: str) -> None:
        """Keep one symbol connected until :meth:`close`."""
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
        channels = _Channels()
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
                        await socket.send(compact(payload))
                    await self._pump(symbol, socket, emitter, channels)
                finally:
                    self._sockets.pop(symbol, None)
        except _CONNECTION_ERRORS:
            pass
        except (ReconnectRequired, SubscribeRejected):
            pass
        finally:
            if established:
                reason = "closed" if self._closed else "connection_lost"
                await self._queue.put(emitter.control(control_payload("disconnect", reason=reason)))

    async def _pump(
        self,
        symbol: str,
        socket: ClientConnection,
        emitter: RawEmitter,
        channels: _Channels,
    ) -> None:
        """Forward frames and handle resync commands until the socket ends."""
        commands = self._commands[symbol]
        while True:
            receiver = asyncio.create_task(socket.recv())
            command = asyncio.create_task(commands.get())
            done = await wait_first((receiver, command))
            if receiver in done:
                frame = receiver.result()
                if isinstance(frame, bytes):
                    frame = frame.decode("utf-8")
                await self._queue.put(emitter.message(frame))
                await self._handle_frame(symbol, socket, channels, frame)
            if command in done:
                reason = command.result()
                await self._queue.put(
                    emitter.control(control_payload("resync_request", symbol=symbol, reason=reason))
                )
                await self._resync_book(symbol, socket, emitter, channels)

    async def _handle_frame(
        self, symbol: str, socket: ClientConnection, channels: _Channels, frame: str
    ) -> None:
        """Apply the bookkeeping a frame implies, raising when the socket must go.

        Only the ``subscribed`` acks (channel ids), the maintenance info codes
        and the ``error`` event matter to the connector; every frame is still
        yielded before this runs.
        """
        document = _decode(frame)
        if not isinstance(document, dict):
            return
        event = document.get("event")
        if event == "info":
            code = document.get("code")
            if code == 20051:
                _LOGGER.warning("bitfinex asked for a reconnect (info 20051)")
                raise ReconnectRequired("info 20051")
            if code == 20060:
                channels.paused = True
                _LOGGER.warning("bitfinex maintenance window started (info 20060)")
            elif code == 20061:
                channels.paused = False
                _LOGGER.warning("bitfinex maintenance window ended (info 20061)")
                await self._resubscribe_all(symbol, socket, channels)
        elif event == "subscribed":
            chan_id = document.get("chanId")
            channel = document.get("channel")
            if not isinstance(chan_id, int):
                return
            if channel == "book":
                channels.book = chan_id
            elif channel == "trades":
                channels.trades = chan_id
        elif event == "error":
            _LOGGER.warning("bitfinex sent an error frame: %s", frame)
            raise SubscribeRejected(frame)

    async def _resubscribe_all(
        self, symbol: str, socket: ClientConnection, channels: _Channels
    ) -> None:
        """Replace both subscriptions on the live connection after maintenance."""
        await self._unsubscribe_channels(socket, channels)
        for payload in subscribe_payloads(symbol)[1:]:
            await socket.send(compact(payload))

    async def _unsubscribe_channels(self, socket: ClientConnection, channels: _Channels) -> None:
        """Unsubscribe every channel the venue has acknowledged on this connection."""
        for chan_id in (channels.book, channels.trades):
            if chan_id is not None:
                await socket.send(compact(unsubscribe_payload(chan_id)))

    async def _resync_book(
        self,
        symbol: str,
        socket: ClientConnection,
        emitter: RawEmitter,
        channels: _Channels,
    ) -> None:
        """Replace the book subscription and wait for the fresh snapshot.

        Frames that arrive while waiting (the unsubscribe ack, the subscribe
        ack, and updates of the replaced subscription) are yielded unmodified,
        and the sequence order is preserved. A missing snapshot within the
        deadline rebuilds the connection.
        """
        if channels.paused:
            _LOGGER.warning("bitfinex maintenance is in progress; deferring the resync")
            return
        if channels.book is None:
            # No ack yet: the connection has not delivered the initial snapshot
            # either, so a resync on it cannot be done without a reconnect.
            raise ReconnectRequired("book channel id unknown")
        await socket.send(compact(unsubscribe_payload(channels.book)))
        await socket.send(compact(book_subscribe_payload(symbol)))
        deadline = asyncio.create_task(self._timer(self._resync_timeout))
        try:
            while True:
                receiver = asyncio.create_task(socket.recv())
                waiting: set[asyncio.Task[Any]] = {receiver, deadline}
                try:
                    done, _pending = await asyncio.wait(
                        waiting, return_when=asyncio.FIRST_COMPLETED
                    )
                except BaseException:
                    receiver.cancel()
                    await asyncio.gather(receiver, return_exceptions=True)
                    raise
                if deadline in done:
                    receiver.cancel()
                    await asyncio.gather(receiver, return_exceptions=True)
                    raise ReconnectRequired("no book snapshot after resubscribe")
                frame = receiver.result()
                if isinstance(frame, bytes):
                    frame = frame.decode("utf-8")
                await self._queue.put(emitter.message(frame))
                await self._handle_frame(symbol, socket, channels, frame)
                if is_book_snapshot(_decode(frame), channels.book):
                    return
        finally:
            deadline.cancel()
            await asyncio.gather(deadline, return_exceptions=True)

    async def _wait_backoff(self, seconds: float) -> None:
        """Sleep for the backoff, waking early when :meth:`close` is called."""
        sleeper: asyncio.Task[None] = asyncio.create_task(self._sleep(seconds))
        stopper: asyncio.Task[bool] = asyncio.create_task(self._stop.wait())
        await wait_first((sleeper, stopper))
