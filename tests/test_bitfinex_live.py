"""Live Bitfinex check; excluded from CI via the network marker.

Run once locally with:

    uv run pytest -q -s -m network tests/test_bitfinex_live.py
"""

from __future__ import annotations

import asyncio
import json

import pytest

from tickvault.connectors.bitfinex import BitfinexConnector, is_book_snapshot

pytestmark = pytest.mark.network

LIVE_SECONDS = 30.0


class _Stats:
    """Counts the frame kinds we expect from a P0/25 book plus trades."""

    def __init__(self) -> None:
        self.frames = 0
        self.book_snapshots = 0
        self.book_updates = 0
        self.checksums = 0
        self.heartbeats = 0
        self.trades = 0
        self.book_chan_id: int | None = None

    def observe(self, payload: str) -> None:
        document = json.loads(payload)
        self.frames += 1
        if isinstance(document, dict):
            if document.get("event") == "subscribed" and document.get("channel") == "book":
                self.book_chan_id = document["chanId"]
            return
        kind = document[1]
        if kind == "cs":
            self.checksums += 1
        elif kind == "hb":
            self.heartbeats += 1
        elif kind == "te":
            self.trades += 1
        elif is_book_snapshot(document, self.book_chan_id):
            self.book_snapshots += 1
        elif document[0] == self.book_chan_id:
            self.book_updates += 1


async def test_live_bitfinex_btc_usd() -> None:
    connector = BitfinexConnector(["BTC-USD"])
    stats = _Stats()
    sequences: list[int] = []
    connection_ids: set[str] = set()
    try:
        async with asyncio.timeout(LIVE_SECONDS):
            async for raw in connector.stream():
                connection_ids.add(raw.connection_id)
                sequences.append(raw.ingest_seq)
                if raw.kind == "msg":
                    stats.observe(raw.payload)
    except TimeoutError:
        pass
    finally:
        await connector.close()

    print(
        f"bitfinex BTC-USD {LIVE_SECONDS:.0f}s: {stats.frames} frames, "
        f"{stats.book_snapshots} snapshots, {stats.book_updates} book updates, "
        f"{stats.checksums} checksums, {stats.trades} trades, "
        f"{stats.heartbeats} heartbeats, {len(connection_ids)} connection(s), "
        f"{len(sequences)} raw lines"
    )
    assert sequences == list(range(len(sequences)))
    assert stats.frames > 200
    assert stats.book_snapshots >= 1
    assert stats.book_updates > 0
    assert stats.checksums > 0
    assert stats.trades > 0
