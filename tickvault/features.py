# ruff: noqa: RUF002
"""Feature computation contract for the derived analytics table (decision A67).

This module is the specification the wave 08 implementation follows; today it
defines the paths and the two stubs. The definitions, verbatim:

Per venue/symbol/day at resolutions ``1s`` and ``1m``. A bar is the half-open
interval ``[bar_start_ns, bar_end_ns)``; a bar uses only rows with
``exchange_ts_ns < bar_end_ns`` (stricter than the PRD's "at or before the
close"). Columns: ``venue``, ``symbol``, ``resolution``, ``bar_start_ns``,
``bar_end_ns``; book state from the **last ``book_snapshots`` sample (A9) with
``exchange_ts_ns < bar_end_ns``**, only if that sample is at most 5 s older
than ``bar_end_ns`` (else null): ``best_bid``, ``best_ask``, ``mid``,
``spread_bps`` = (ask − bid) / mid × 10⁴, ``depth_imbalance_5`` = (Σ bid size
L0–4 − Σ ask size L0–4) / (sum of both), ``book_sample_ts_ns``; from trades
with ``bar_start_ns ≤ exchange_ts_ns < bar_end_ns``: ``trade_count``,
``volume``, ``signed_volume`` (+size taker buy, −size taker sell), ``vwap``
(null if no trades); ``realized_vol`` = sqrt(Σ r²) with r_k =
ln(m(T_k) / m(T_k − 1 s)) for every whole second T_k with
``bar_start_ns < T_k ≤ bar_end_ns``, where m(T) is the as-of mid at T (last
sample with ``exchange_ts_ns < T``, at most 5 s old); a ``1s`` bar therefore
has one return, a ``1m`` bar sixty; returns with a missing endpoint are
skipped and the value is null when none remain. Bars cover the span of that
day's data for the venue/symbol (first to last sample or trade), not 86,400
empty seconds. No statistic may use data at or after ``bar_end_ns``: no
centred windows, no whole-day normalization, no forward-fill past the 5 s
staleness limit. The classic-bug test is ``tests/test_features_lookahead.py``:
features for bars with ``bar_end_ns ≤ t`` are identical when every row with
``exchange_ts_ns ≥ t`` is deleted and features are recomputed (fixed cases
plus a Hypothesis property over ``t``).

Storage (decision A68):
``<out>/features/resolution=<1s|1m>/venue=<v>/date=<YYYY-MM-DD>/part-<symbol>.parquet``,
rows sorted by ``(symbol, bar_start_ns)``, deterministic bytes (two runs →
``diff -r`` clean). The schema is :data:`tickvault.models.FEATURES_SCHEMA`,
which is deliberately not one of the four normalized tables.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import pyarrow as pa

RESOLUTIONS: Mapping[str, int] = {"1s": 1_000_000_000, "1m": 60_000_000_000}
"""Supported resolution labels and their width in nanoseconds."""

MAX_SAMPLE_AGE_NS: int = 5_000_000_000
"""How old a book sample may be at ``bar_end_ns`` before it counts as missing."""


def features_path(
    out_root: str | Path, resolution: str, venue: str, date: str, symbol: str
) -> Path:
    """Return the one Parquet file holding ``symbol``'s bars for one day.

    The layout is
    ``<out_root>/features/resolution=<r>/venue=<v>/date=<YYYY-MM-DD>/part-<symbol>.parquet``;
    every bar of that resolution/venue/date/symbol pair lives in this file.
    """
    return (
        Path(out_root)
        / "features"
        / f"resolution={resolution}"
        / f"venue={venue}"
        / f"date={date}"
        / f"part-{symbol}.parquet"
    )


def compute_features(
    data_root: str | Path, date: str, resolutions: Sequence[str] = ("1s", "1m")
) -> dict[str, pa.Table]:
    """Compute the ``FEATURES_SCHEMA`` bars for ``date`` under ``data_root``.

    One table per resolution label in ``resolutions``, each holding every
    venue/symbol of that day, sorted by ``(symbol, bar_start_ns)``.
    """
    raise NotImplementedError("features: implemented in wave 08")


def write_features(
    tables: Mapping[str, pa.Table], out_root: str | Path, date: str
) -> list[Path]:
    """Write one table per resolution and return the files in resolution order.

    Files go through :func:`features_path` and hold deterministic bytes, so
    computing twice and comparing the trees with ``diff -r`` is clean.
    """
    raise NotImplementedError("features: implemented in wave 08")
