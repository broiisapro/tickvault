"""Tardis.dev dataset download with an on-disk cache.

Tardis publishes free per-day trade files only for the first day of each
month; the URL pattern is
``https://datasets.tardis.dev/v1/<exchange>/trades/<YYYY>/<MM>/<DD>/<SYMBOL>.csv.gz``
and a plain ``GET`` works without an API key (a ``HEAD`` answers 404). Files
are cached under ``<cache_dir>/tardis/<exchange>/trades/<YYYY-MM-DD>/`` so a
reconciliation never downloads the same day twice.
"""

from __future__ import annotations

import os
import tempfile
from datetime import date as Date
from pathlib import Path
from typing import IO

import httpx

from tickvault.reconcile.tardis import (
    ReconcileReport,
    load_tardis_trades,
    load_tickvault_trades,
    reconcile,
)

_BASE_URL = "https://datasets.tardis.dev"
_TARDIS_EXCHANGE_IDS = {"coinbase": "coinbase"}


def _normalize_date(value: str | Date) -> Date:
    if isinstance(value, Date):
        return value
    return Date.fromisoformat(value)


def tardis_ids(venue: str, symbol: str) -> tuple[str, str]:
    """Map a TickVault venue/symbol pair to Tardis ``(exchange, symbol)`` ids.

    Phase 1 covers Coinbase, whose TickVault symbol text (``BTC-USD``) already
    matches the Tardis symbol id.
    """
    try:
        exchange = _TARDIS_EXCHANGE_IDS[venue]
    except KeyError:
        raise ValueError(f"no Tardis dataset mapping for venue {venue!r}") from None
    return exchange, symbol


def tardis_url(exchange: str, symbol: str, date: str | Date) -> str:
    """Return the Tardis trades CSV URL for one day.

    This only formats the URL; :func:`download_tardis_trades` enforces the
    free-tier constraint that the date is the first of a month.
    """
    day = _normalize_date(date)
    return f"{_BASE_URL}/v1/{exchange}/trades/{day:%Y}/{day:%m}/{day:%d}/{symbol}.csv.gz"


def _cache_path(cache_dir: str | Path, exchange: str, symbol: str, day: Date) -> Path:
    return (
        Path(cache_dir)
        / "tardis"
        / exchange
        / "trades"
        / day.isoformat()
        / f"{symbol}.csv.gz"
    )


def _stream_to_file(response: httpx.Response, handle: IO[bytes]) -> None:
    for chunk in response.iter_bytes():
        handle.write(chunk)


def download_tardis_trades(
    date: str | Date,
    cache_dir: str | Path,
    exchange: str = "coinbase",
    symbol: str = "BTC-USD",
    client: httpx.Client | None = None,
) -> Path:
    """Download one Tardis trades file into the cache and return its path.

    ``date`` must be the first day of a month (the only free files). The file
    is streamed to a temporary file next to its final location and renamed
    into place, so an interrupted or failed download leaves no ``.csv.gz``
    behind. An existing cache file is reused without any request. When
    ``client`` is given it is used as-is and not closed.
    """
    day = _normalize_date(date)
    if day.day != 1:
        raise ValueError(
            "Tardis free datasets exist only for the first day of each month: "
            f"{day.isoformat()} is not the 1st"
        )
    target = _cache_path(cache_dir, exchange, symbol, day)
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    url = tardis_url(exchange, symbol, day)
    http = client if client is not None else httpx.Client(timeout=60.0)
    temporary_path: Path | None = None
    try:
        with http.stream("GET", url, follow_redirects=True) as response:
            response.raise_for_status()
            with tempfile.NamedTemporaryFile(
                dir=target.parent, prefix=".download-", suffix=".part", delete=False
            ) as temporary:
                temporary_path = Path(temporary.name)
                _stream_to_file(response, temporary)
        assert temporary_path is not None
        os.replace(temporary_path, target)
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise
    finally:
        if client is None:
            http.close()
    return target


def reconcile_live_day(
    root: str | Path,
    date: str | Date,
    cache_dir: str | Path,
    *,
    venue: str = "coinbase",
    symbol: str = "BTC-USD",
    client: httpx.Client | None = None,
) -> ReconcileReport:
    """Download one Tardis day, then reconcile it against ``root``.

    Both datasets are clipped to their overlapping exchange-time window by
    :func:`tickvault.reconcile.tardis.reconcile`, so a partial day of live
    capture reconciles against the matching hours of the Tardis file.
    """
    exchange, tardis_symbol = tardis_ids(venue, symbol)
    path = download_tardis_trades(
        date, cache_dir, exchange=exchange, symbol=tardis_symbol, client=client
    )
    return reconcile(
        load_tardis_trades(path), load_tickvault_trades(root, venue, symbol)
    )
