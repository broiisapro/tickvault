"""Live multi-symbol check across both venues; excluded from CI.

Runs the Coinbase and Kraken connectors concurrently for ``LIVE_SECONDS`` with
``BTC-USD,ETH-USD`` and feeds every frame through the real normalizer and
:class:`~tickvault.book.processor.BookProcessor` (no storage). Every
venue/symbol pair must receive at least one venue snapshot and book deltas,
and no anomaly may be left unexplained by a later resync.

Run once locally with:

    uv run pytest -q -s -m network tests/test_live_multisymbol.py
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from tickvault.book.processor import BookProcessor
from tickvault.connectors.coinbase import CoinbaseConnector
from tickvault.connectors.kraken import KrakenConnector
from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize import coinbase as coinbase_normalize
from tickvault.normalize import kraken as kraken_normalize

pytestmark = pytest.mark.network

LIVE_SECONDS = 45.0
SYMBOLS = ("BTC-USD", "ETH-USD")
PAIRS = tuple(
    (venue, symbol) for venue in ("coinbase", "kraken") for symbol in SYMBOLS
)
_ANOMALY_TYPES = ("gap", "checksum_failure", "invariant_violation")


class _Session:
    """Feed one processor from both venues and total the rows it accepts."""

    def __init__(self) -> None:
        self.processor = BookProcessor()
        self.snapshot_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
        self.deltas: dict[tuple[str, str], int] = defaultdict(int)
        self.unexplained: dict[tuple[str, str], int] = defaultdict(int)

    def feed(self, message: NormalizedMessage) -> None:
        output = self.processor.process(message)
        for row in output.book_snapshots:
            if row.source == "venue":
                self.snapshot_ids[(row.venue, row.symbol)].add(row.snapshot_id)
        for row in output.book_deltas:
            self.deltas[(row.venue, row.symbol)] += 1
        for event in output.events:
            if event.symbol is None:
                continue
            key = (event.venue, event.symbol)
            if event.event_type in _ANOMALY_TYPES:
                self.unexplained[key] += 1
            elif event.event_type == "resync":
                self.unexplained[key] = 0

    def summary(self) -> str:
        lines = []
        for venue, symbol in PAIRS:
            lines.append(
                f"{venue}/{symbol}: "
                f"venue_snapshots={len(self.snapshot_ids[(venue, symbol)])} "
                f"deltas={self.deltas[(venue, symbol)]} "
                f"unexplained={self.unexplained[(venue, symbol)]}"
            )
        return "\n".join(lines)


async def _drain(
    connector: CoinbaseConnector | KrakenConnector,
    normalizer: Callable[[RawMessage], NormalizedMessage],
    session: _Session,
) -> None:
    async for raw in connector.stream():
        session.feed(normalizer(raw))


async def test_live_multisymbol_books() -> None:
    coinbase = CoinbaseConnector(list(SYMBOLS))
    kraken = KrakenConnector(list(SYMBOLS))
    session = _Session()
    drains: list[Coroutine[Any, Any, None]] = [
        _drain(coinbase, coinbase_normalize.normalize, session),
        _drain(kraken, kraken_normalize.normalize, session),
    ]
    try:
        async with asyncio.timeout(LIVE_SECONDS):
            await asyncio.gather(*drains)
    except TimeoutError:
        pass
    finally:
        await asyncio.gather(coinbase.close(), kraken.close())

    print("\n" + session.summary())
    for venue, symbol in PAIRS:
        assert session.snapshot_ids[(venue, symbol)], f"{venue} {symbol} saw no venue snapshot"
        assert session.deltas[(venue, symbol)] > 0, f"{venue} {symbol} saw no book deltas"
        assert session.unexplained[(venue, symbol)] == 0, (
            f"{venue} {symbol} has unexplained anomalies"
        )
