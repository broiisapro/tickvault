"""Live Kraken check; excluded from CI via the network marker.

Run once locally with:

    uv run pytest -q -m network tests/test_kraken_live.py
"""

from __future__ import annotations

import asyncio
import json

import pytest

from tickvault.connectors.kraken import KrakenConnector

pytestmark = pytest.mark.network

LIVE_SECONDS = 30.0


class _Stats:
    def __init__(self) -> None:
        self.snapshots = 0
        self.updates = 0
        self.trades = 0
        self.status_messages = 0

    def observe(self, payload: str) -> None:
        message = json.loads(payload)
        channel = message.get("channel")
        message_type = message.get("type")
        if channel == "status":
            self.status_messages += 1
        elif channel == "book" and message_type == "snapshot":
            self.snapshots += 1
        elif channel == "book" and message_type == "update":
            self.updates += 1
        elif channel == "trade" and message_type == "update":
            self.trades += len(message.get("data", []))


async def test_live_kraken_btc_usd() -> None:
    connector = KrakenConnector(["BTC-USD"])
    stats = _Stats()
    try:
        async with asyncio.timeout(LIVE_SECONDS):
            async for raw in connector.stream():
                if raw.kind == "msg":
                    stats.observe(raw.payload)
    except TimeoutError:
        pass
    finally:
        await connector.close()

    assert stats.snapshots >= 1
    assert stats.updates > 0
    assert stats.trades > 0


async def test_live_kraken_resync_yields_a_fresh_snapshot() -> None:
    connector = KrakenConnector(["BTC-USD"])
    snapshots = 0
    try:
        async with asyncio.timeout(45.0):
            async for raw in connector.stream():
                if raw.kind != "msg":
                    continue
                message = json.loads(raw.payload)
                if message.get("channel") != "book" or message.get("type") != "snapshot":
                    continue
                snapshots += 1
                if snapshots == 1:
                    await connector.resync("BTC-USD", "manual")
                elif snapshots >= 2:
                    break
    except TimeoutError:
        pass
    finally:
        await connector.close()

    assert snapshots >= 2, "kraken did not send a fresh snapshot after resync"
