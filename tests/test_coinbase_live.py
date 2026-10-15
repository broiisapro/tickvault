"""Live Coinbase Advanced Trade check; excluded from CI via the network marker.

Run once locally with:

    uv run pytest -q -m network tests/test_coinbase_live.py
"""

from __future__ import annotations

import asyncio
import json

import pytest

from tickvault.connectors.coinbase import CoinbaseConnector

pytestmark = pytest.mark.network

LIVE_SECONDS = 30.0


class _Stats:
    def __init__(self) -> None:
        self.snapshots = 0
        self.updates = 0
        self.trades = 0
        self.sequences: list[int] = []

    def observe(self, payload: str) -> None:
        message = json.loads(payload)
        sequence = message.get("sequence_num")
        if isinstance(sequence, int):
            self.sequences.append(sequence)
        channel = message.get("channel")
        for event in message.get("events", []):
            if channel == "l2_data" and event.get("type") == "snapshot":
                self.snapshots += 1
            elif channel == "l2_data" and event.get("type") == "update":
                self.updates += 1
            elif channel == "market_trades" and event.get("type") == "update":
                self.trades += len(event.get("trades", []))


async def test_live_coinbase_btc_usd() -> None:
    connector = CoinbaseConnector(["BTC-USD"])
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
    assert stats.sequences[0] == 0
    assert stats.sequences == list(
        range(stats.sequences[0], stats.sequences[0] + len(stats.sequences))
    )
