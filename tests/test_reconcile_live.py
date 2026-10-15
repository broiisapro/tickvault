"""Live Tardis download check.

Excluded from the default run by ``-m 'not network'``; select it explicitly
with ``uv run pytest -m network tests/test_reconcile_live.py``. It downloads
the free 2026-10-01 Coinbase BTC-USD trades file (no API key) and validates
the cached file's header and row count.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from tickvault.reconcile.download import download_tardis_trades
from tickvault.reconcile.tardis import load_tardis_trades

HEADER = "exchange,symbol,timestamp,local_timestamp,id,side,price,amount"


@pytest.mark.network
def test_download_2026_10_01_coinbase_btc_usd(tmp_path: Path) -> None:
    path = download_tardis_trades("2026-10-01", tmp_path)
    assert path == tmp_path / "tardis/coinbase/trades/2026-10-01/BTC-USD.csv.gz"
    assert path.stat().st_size > 0
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        assert handle.readline().strip() == HEADER
    trades = load_tardis_trades(path)
    assert len(trades) > 1000
    assert trades[0].exchange == "coinbase"
    assert trades[0].symbol == "BTC-USD"
    assert trades[0].exchange_ts_ns > 0
