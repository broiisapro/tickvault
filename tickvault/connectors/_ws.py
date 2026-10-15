"""Shared helpers for the venue websocket connectors.

Both connectors build the same raw stream: one ``connect`` control line per
connection, every received frame as a ``msg`` line with ``recv_ts_ns`` stamped
at receipt and a contiguous per-connection ``ingest_seq``, and one
``disconnect`` control line when the connection ends. These helpers build the
connection ids, control payloads, raw envelopes, reconnect backoff and the
cancellation-safe task wait used by both, with the clock and sleep injectable
so tests never wait on wall time.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import random
import secrets
import time
from collections.abc import Callable, Collection, Coroutine
from typing import Any

from tickvault.models import RawKind, RawMessage

type SleepFn = Callable[[float], Coroutine[Any, Any, None]]
"""An asyncio sleep; connectors take one so tests can record delays."""

type ClockFn = Callable[[], int]
"""Returns the current time in nanoseconds since the Unix epoch."""

BACKOFF_BASE_S = 1.0
BACKOFF_CAP_S = 30.0


def utc_stamp(ns: int) -> str:
    """Format ``ns`` as ``%Y%m%dT%H%M%SZ`` UTC for a connection id."""
    moment = dt.datetime.fromtimestamp(ns // 1_000_000_000, tz=dt.UTC)
    return moment.strftime("%Y%m%dT%H%M%SZ")


def make_connection_id(
    venue: str,
    connect_ns: int,
    *,
    token_hex: Callable[[int], str] = secrets.token_hex,
) -> str:
    """Build ``<venue>-<UTC connect time %Y%m%dT%H%M%SZ>-<8 lowercase hex>``."""
    return f"{venue}-{utc_stamp(connect_ns)}-{token_hex(4)}"


def control_payload(event: str, **fields: object) -> str:
    """Encode a control record payload as compact JSON with ``event`` first."""
    document: dict[str, object] = {"event": event}
    document.update(fields)
    return json.dumps(document, separators=(",", ":"), ensure_ascii=False)


async def wait_first(tasks: Collection[asyncio.Task[Any]]) -> set[asyncio.Task[Any]]:
    """Wait until one task finishes, cancelling and awaiting the rest.

    The cancellation-safe replacement for
    ``asyncio.wait(tasks, return_when=FIRST_COMPLETED)``: the pending tasks
    are always cancelled and awaited, and when the wait itself is cancelled
    (a closing connector) every task is cancelled and awaited before
    ``CancelledError`` propagates. Done tasks keep their exceptions for the
    caller to read with ``.result()``; nothing is ever left unretrieved or
    pending afterwards.
    """
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    return done


class RawEmitter:
    """Builds raw envelopes for one connection with a contiguous ``ingest_seq``.

    The counter starts at ``0`` and advances over both ``msg`` and ``control``
    lines, so a connection's stream is gap-free by construction.
    """

    def __init__(
        self,
        venue: str,
        connection_id: str,
        *,
        clock: ClockFn = time.time_ns,
    ) -> None:
        self.venue = venue
        self.connection_id = connection_id
        self._clock = clock
        self._next_seq = 0

    def message(self, payload: str) -> RawMessage:
        """Stamp one received frame as a ``msg`` envelope."""
        return self._wrap("msg", payload)

    def control(self, payload: str) -> RawMessage:
        """Stamp one lifecycle record as a ``control`` envelope."""
        return self._wrap("control", payload)

    def _wrap(self, kind: RawKind, payload: str) -> RawMessage:
        message = RawMessage(
            venue=self.venue,
            connection_id=self.connection_id,
            ingest_seq=self._next_seq,
            recv_ts_ns=self._clock(),
            kind=kind,
            payload=payload,
        )
        self._next_seq += 1
        return message


class Backoff:
    """Exponential reconnect backoff: base, 2x, 4x ... capped.

    ``next_delay`` returns the schedule value scaled by ``1 - jitter * u`` with
    ``u`` uniform in ``[0, 1)``: the delay never exceeds the schedule, and
    jitter changes only wall-clock timing, never any recorded value. Tests use
    ``jitter=0`` (or a seeded ``random.Random``) so the schedule is exact.
    """

    def __init__(
        self,
        *,
        base: float = BACKOFF_BASE_S,
        cap: float = BACKOFF_CAP_S,
        jitter: float = 0.5,
        rng: random.Random | None = None,
    ) -> None:
        if base <= 0:
            raise ValueError("backoff base must be positive")
        if cap < base:
            raise ValueError("backoff cap must be at least the base")
        if not 0.0 <= jitter < 1.0:
            raise ValueError("backoff jitter must be in [0, 1)")
        self.base = base
        self.cap = cap
        self.jitter = jitter
        self._rng = rng if rng is not None else random.Random()
        self._attempt = 0

    def next_delay(self) -> float:
        """Return the next delay in seconds and advance the schedule."""
        delay = min(self.base * (2.0**self._attempt), self.cap)
        self._attempt += 1
        if self.jitter == 0.0:
            return delay
        return delay * (1.0 - self._rng.random() * self.jitter)
