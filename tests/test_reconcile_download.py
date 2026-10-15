"""Tests for :mod:`tickvault.reconcile.download` (no network).

Every HTTP interaction goes through ``httpx.MockTransport``; the tests cover
URL formatting, cache reuse, first-of-month enforcement, HTTP errors, partial
streams, redirects and the ``reconcile_live_day`` convenience path.
"""

from __future__ import annotations

import gzip
from collections.abc import Callable, Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from tickvault.models import Trade, TradeSide, to_arrow
from tickvault.paths import partition_path
from tickvault.reconcile.download import (
    download_tardis_trades,
    reconcile_live_day,
    tardis_ids,
    tardis_url,
)

URL = "https://datasets.tardis.dev/v1/coinbase/trades/2026/10/01/BTC-USD.csv.gz"
HEADER = "exchange,symbol,timestamp,local_timestamp,id,side,price,amount"
PAYLOAD = (
    HEADER + "\n"
    "coinbase,BTC-USD,1790856000515155,1790856000555341,1,buy,83882.03,0.08764029\n"
    "coinbase,BTC-USD,1790856001515155,1790856001555341,2,sell,83882.0,0.5\n"
    "coinbase,BTC-USD,1790856002515155,1790856002555341,3,buy,83882.5,0.25\n"
)
PACKED = gzip.compress(PAYLOAD.encode())


TransportHandler = Callable[[httpx.Request], httpx.Response]


def _client(handler: TransportHandler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _target_dir(cache_dir: Path) -> Path:
    return cache_dir / "tardis" / "coinbase" / "trades" / "2026-10-01"


def _trade(
    trade_id: str,
    ts_ns: int,
    price: str,
    size: str,
    side: TradeSide,
) -> Trade:
    return Trade(
        venue="coinbase",
        symbol="BTC-USD",
        exchange_ts_ns=ts_ns,
        recv_ts_ns=ts_ns,
        connection_id="conn-a",
        ingest_seq=int(trade_id),
        trade_id=trade_id,
        price=Decimal(price),
        size=Decimal(size),
        side=side,
    )


def _write_trades(root: Path, trades: list[Trade]) -> None:
    grouped: dict[Path, list[Trade]] = {}
    for trade in trades:
        path = partition_path(root, "trades", trade.venue, trade.exchange_ts_ns)
        grouped.setdefault(path, []).append(trade)
    for path, group in grouped.items():
        path.mkdir(parents=True, exist_ok=True)
        pq.write_table(to_arrow("trades", group), path / "part-test.parquet")


def test_tardis_url_formats_date_and_symbol() -> None:
    assert tardis_url("coinbase", "BTC-USD", "2026-10-01") == URL
    assert tardis_url("coinbase", "BTC-USD", date(2026, 10, 1)) == URL


def test_tardis_ids_maps_coinbase_and_rejects_unknown_venue() -> None:
    assert tardis_ids("coinbase", "BTC-USD") == ("coinbase", "BTC-USD")
    with pytest.raises(ValueError, match="no Tardis dataset mapping"):
        tardis_ids("kraken", "BTC-USD")


def test_download_writes_cache_file_and_reuses_it(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=PACKED)

    with _client(handler) as client:
        path = download_tardis_trades("2026-10-01", tmp_path, client=client)
        assert path == _target_dir(tmp_path) / "BTC-USD.csv.gz"
        assert path.read_bytes() == PACKED
        assert len(requests) == 1
        assert str(requests[0].url) == URL

        again = download_tardis_trades("2026-10-01", tmp_path, client=client)
        assert again == path
        assert len(requests) == 1


def test_download_rejects_non_first_of_month(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="first day of each month"):
        download_tardis_trades("2026-10-02", tmp_path)
    with pytest.raises(ValueError, match="first day of each month"):
        download_tardis_trades(date(2026, 10, 15), tmp_path)
    assert not (tmp_path / "tardis").exists()


def test_download_http_error_leaves_no_file(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "not found"})

    with _client(handler) as client, pytest.raises(httpx.HTTPStatusError):
        download_tardis_trades("2026-10-01", tmp_path, client=client)
    assert _target_dir(tmp_path).exists()
    assert list(_target_dir(tmp_path).iterdir()) == []


def test_partial_download_leaves_no_file(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        def chunks() -> Iterator[bytes]:
            yield b"partial-bytes"
            raise httpx.ReadError("connection lost mid-stream")

        return httpx.Response(200, content=chunks())

    with _client(handler) as client, pytest.raises(httpx.ReadError):
        download_tardis_trades("2026-10-01", tmp_path, client=client)
    assert list(_target_dir(tmp_path).iterdir()) == []


def test_download_follows_redirects(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(302, headers={"Location": URL + "?cdn=1"})
        return httpx.Response(200, content=PACKED)

    with _client(handler) as client:
        path = download_tardis_trades("2026-10-01", tmp_path, client=client)
    assert len(requests) == 2
    assert requests[1].url.params["cdn"] == "1"
    assert path.read_bytes() == PACKED


def test_reconcile_live_day_end_to_end(tmp_path: Path) -> None:
    base_ns = 1_790_856_000_515_155_000

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=PACKED)

    root = tmp_path / "data"
    _write_trades(
        root,
        [
            _trade("1", base_ns, "83882.03", "0.08764029", "buy"),
            _trade("2", base_ns + 1_000_000_000, "99.0", "0.5", "sell"),
            _trade("4", base_ns + 2_000_000_000, "100.0", "1.0", "buy"),
        ],
    )
    with _client(handler) as client:
        report = reconcile_live_day(
            root, "2026-10-01", tmp_path / "cache", client=client
        )
    assert report.tardis_trades == 3
    assert report.tickvault_trades == 3
    assert report.matched == 2
    assert report.missing_in_tickvault_ids == ("3",)
    assert report.extra_in_tickvault_ids == ("4",)
    assert report.price_mismatch_ids == ("2",)
    assert report.size_mismatch_ids == ()
    assert report.side_mismatch_ids == ()
