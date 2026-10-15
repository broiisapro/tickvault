"""Connector protocol: the contract every venue websocket client implements."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Protocol

from tickvault.models import RawMessage


class Connector(Protocol):
    """A venue connection that streams raw messages and can resynchronize.

    Implementations are async generators: calling :meth:`stream` returns an
    async iterator of raw messages whose ``recv_ts_ns`` is stamped at receipt
    and whose ``ingest_seq`` is a contiguous per-connection counter starting
    at 0. A fresh connection writes its ``connect`` control line first and a
    ``disconnect`` control line before the iterator ends.
    """

    venue: str
    """Venue name used in paths and rows, e.g. ``coinbase`` or ``kraken``."""

    def stream(self) -> AsyncIterator[RawMessage]:
        """Yield raw messages until the connection is closed."""
        ...

    async def subscribe(self, symbols: Sequence[str]) -> None:
        """Subscribe to the venue channels for ``symbols``."""
        ...

    async def resync(self, symbol: str, reason: str) -> None:
        """Recover ``symbol`` after an anomaly (unsubscribe/resubscribe or reconnect)."""
        ...

    async def close(self) -> None:
        """Close the connection and release resources."""
        ...
