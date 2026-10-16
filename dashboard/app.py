"""Streamlit dashboard over a TickVault data root.

Run it against a live directory (``TICKVAULT_DATA_DIR``) or the bundled
sample::

    streamlit run dashboard/app.py

The header says what TickVault is, links the source and describes the current
data root in one caption: the bundled sample's venues, symbols and UTC span
(computed from the data, never hard-coded) or the live path when
``TICKVAULT_DATA_DIR`` is set. Every time window -- including the "messages/s
last 5 min" tile -- is anchored to the newest record in the root, so an old
sample renders exactly like a live root.

The app opens a fresh in-memory DuckDB connection per query (the views are
cheap) and caches every result with ``st.cache_data(ttl=10)``, so a live root
refreshes once every ten seconds. All aggregation and row capping happen in
SQL (:mod:`dashboard.queries`); the app never loads a whole table, never
rebuilds a book and caps every chart series at ``_SERIES_ROWS`` newest points.
Time-axis charts are Altair specs from :mod:`dashboard.charts` with an
explicit UTC scale, so a viewer in any time zone sees the same UTC ticks the
axis labels promise. With no data under the root it renders an explanatory
empty state instead of failing.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from dashboard import charts, queries  # noqa: E402
from tickvault.sql import connect  # noqa: E402

_ENV_DATA_ROOT = "TICKVAULT_DATA_DIR"
_SAMPLE_ROOT = Path(__file__).resolve().parent / "sample"
_SERIES_ROWS = 2000
_KPI_WINDOW_SECONDS = 300
_CHART_WINDOW_SECONDS = 3600
_CACHE_TTL_S = 10
_TITLE = "TickVault — crypto market-data quality"
_SOURCE_URL = "https://github.com/broiisapro/tickvault"
_INTRO = (
    "TickVault streams level-2 order books and trades from Coinbase and Kraken, "
    "stores every raw frame untouched and normalizes the feeds into hourly "
    "Parquet, validating them for sequence gaps, Kraken CRC32 checksums and "
    "book invariants. This page shows the resulting message throughput, "
    "quality events, top of book, spreads and per-day QA report for the "
    "current data root."
)


def _default_data_root() -> str:
    configured = os.environ.get(_ENV_DATA_ROOT)
    if configured:
        return configured
    if _SAMPLE_ROOT.is_dir():
        return str(_SAMPLE_ROOT)
    return "dashboard/sample"


def _has_normalized_data(root: Path) -> bool:
    norm = root / "norm"
    if not norm.is_dir():
        return False
    return next(norm.glob("*/venue=*/date=*/hour=*/*.parquet"), None) is not None


def _run(root: str, query: Callable[..., pd.DataFrame], /, **kwargs: Any) -> pd.DataFrame:
    con = connect(root)
    try:
        return query(con, **kwargs)
    finally:
        con.close()


@st.cache_data(ttl=_CACHE_TTL_S, show_spinner=False)
def _load_dimensions(root: str) -> pd.DataFrame:
    return _run(root, queries.dimensions)


@st.cache_data(ttl=_CACHE_TTL_S, show_spinner=False)
def _load_data_span(root: str) -> pd.DataFrame:
    return _run(root, queries.data_span)


@st.cache_data(ttl=_CACHE_TTL_S, show_spinner=False)
def _load_message_rate(root: str, window_seconds: int, venues: tuple[str, ...]) -> pd.DataFrame:
    return _run(
        root,
        queries.message_rate,
        window_seconds=window_seconds,
        venues=venues,
        limit=_SERIES_ROWS,
    )


@st.cache_data(ttl=_CACHE_TTL_S, show_spinner=False)
def _load_event_counts(root: str, venues: tuple[str, ...]) -> pd.DataFrame:
    return _run(root, queries.cumulative_event_counts, venues=venues)


@st.cache_data(ttl=_CACHE_TTL_S, show_spinner=False)
def _load_qa_history(root: str) -> pd.DataFrame:
    return queries.qa_history(root)


@st.cache_data(ttl=_CACHE_TTL_S, show_spinner=False)
def _load_top_of_book(
    root: str, venues: tuple[str, ...], symbols: tuple[str, ...]
) -> pd.DataFrame:
    return _run(
        root,
        queries.top_of_book,
        venues=venues,
        symbols=symbols,
        limit=_SERIES_ROWS,
    )


@st.cache_data(ttl=_CACHE_TTL_S, show_spinner=False)
def _load_spread_mid(root: str, venues: tuple[str, ...], symbols: tuple[str, ...]) -> pd.DataFrame:
    return _run(
        root,
        queries.spread_mid,
        venues=venues,
        symbols=symbols,
        limit=_SERIES_ROWS,
    )


def _total(frame: pd.DataFrame, column: str) -> int:
    return 0 if frame.empty else int(frame[column].sum())


def _ns_to_timestamp(nanoseconds: int) -> pd.Timestamp:
    return pd.to_datetime(nanoseconds, utc=True)


def _root_caption(root_text: str, root: Path | None) -> str:
    """One line describing the current data root, computed from the data."""
    if os.environ.get(_ENV_DATA_ROOT):
        return f"Live data root: `{root_text}`"
    if root is None or not _has_normalized_data(root):
        return f"Bundled sample: `{root_text}` (no normalized data yet)"
    dimensions = _load_dimensions(str(root))
    span = _load_data_span(str(root))
    if dimensions.empty or span.empty:
        return f"Bundled sample: `{root_text}` (no normalized data yet)"
    first_value = span["first_ts_ns"].iloc[0]
    last_value = span["last_ts_ns"].iloc[0]
    if pd.isna(first_value) or pd.isna(last_value):
        return f"Bundled sample: `{root_text}` (no normalized data yet)"
    venues = ", ".join(sorted(dimensions["venue"].unique().tolist()))
    symbols = ", ".join(sorted(dimensions["symbol"].unique().tolist()))
    first = _ns_to_timestamp(int(first_value))
    last = _ns_to_timestamp(int(last_value))
    minutes = max(1, round((last - first).total_seconds() / 60))
    if first.date() == last.date():
        span_text = f"{first:%Y-%m-%d %H:%M:%S}\u2013{last:%H:%M:%S} UTC"
    else:
        span_text = f"{first:%Y-%m-%d %H:%M:%S}\u2013{last:%Y-%m-%d %H:%M:%S} UTC"
    return f"Bundled sample: {venues} \u00b7 {symbols} \u00b7 {span_text} ({minutes} min)"


def main() -> None:
    st.set_page_config(page_title="TickVault", layout="wide")

    with st.sidebar:
        st.header("Data")
        root_text = st.text_input("Data root", value=_default_data_root())
        st.caption(
            f"Results refresh every {_CACHE_TTL_S} s; each chart series is capped at "
            f"{_SERIES_ROWS} newest points."
        )

    root_text = root_text.strip()
    root = Path(root_text).expanduser() if root_text else None

    st.title(_TITLE)
    st.markdown(_INTRO)
    st.markdown(f"[Source on GitHub]({_SOURCE_URL})")
    st.caption(_root_caption(root_text, root))

    if root is None:
        st.info("Enter a TickVault data root in the sidebar.")
        return
    if not _has_normalized_data(root):
        st.info(
            f"No normalized Parquet data under `{root}`. Replay a raw fixture with "
            "`tickvault replay`, or point `TICKVAULT_DATA_DIR` at a data root."
        )
        return

    dimensions = _load_dimensions(str(root))
    venues = sorted(dimensions["venue"].unique().tolist())
    symbols = sorted(dimensions["symbol"].unique().tolist())

    with st.sidebar:
        st.divider()
        selected_venues = st.multiselect("Venues", venues, default=venues)
        selected_symbols = st.multiselect("Symbols", symbols, default=symbols)

    if not selected_venues or not selected_symbols:
        st.info("Select at least one venue and one symbol in the sidebar.")
        return

    selected_venues_tuple = tuple(selected_venues)
    selected_symbols_tuple = tuple(selected_symbols)

    qa = _load_qa_history(str(root))
    qa_selected = qa[qa["venue"].isin(selected_venues)]
    recent = _load_message_rate(str(root), _KPI_WINDOW_SECONDS, selected_venues_tuple)
    recent_messages = _total(recent, "messages")

    tiles = st.columns(5)
    tiles[0].metric(
        "Messages/s (last 5 min)", f"{recent_messages / _KPI_WINDOW_SECONDS:,.1f}"
    )
    tiles[1].metric("Gaps", f"{_total(qa_selected, 'gaps'):,}")
    tiles[2].metric("Checksum failures", f"{_total(qa_selected, 'checksum_failures'):,}")
    tiles[3].metric("Reconnects", f"{_total(qa_selected, 'reconnects'):,}")
    tiles[4].metric("Unexplained", f"{_total(qa_selected, 'unexplained'):,}")

    st.subheader("Message throughput")
    rate = _load_message_rate(str(root), _CHART_WINDOW_SECONDS, selected_venues_tuple)
    if rate.empty:
        st.info("No trades or book deltas in the last hour of data.")
    else:
        st.altair_chart(charts.message_rate_chart(rate), use_container_width=True)

    st.subheader("Events by venue")
    events = _load_event_counts(str(root), selected_venues_tuple)
    if events.empty:
        st.info("No events recorded.")
    else:
        st.bar_chart(events, x="event_type", y="count", color="venue", height=280)

    st.subheader("Top of book")
    book = _load_top_of_book(str(root), selected_venues_tuple, selected_symbols_tuple)
    if book.empty:
        st.info("No sample snapshots for the selected venues and symbols.")
    else:
        st.altair_chart(charts.top_of_book_chart(book), use_container_width=True)

    st.subheader("Spread and mid")
    spread = _load_spread_mid(str(root), selected_venues_tuple, selected_symbols_tuple)
    if spread.empty:
        st.info("No sample snapshots for the selected venues and symbols.")
    else:
        left, right = st.columns(2)
        left.altair_chart(charts.mid_price_chart(spread), use_container_width=True)
        right.altair_chart(charts.spread_bps_chart(spread), use_container_width=True)

    st.subheader("QA history")
    if qa.empty:
        st.info("No QA rows yet.")
    else:
        st.dataframe(qa, hide_index=True)

    st.caption(
        "Every chart runs a single aggregated SQL query; series are capped at "
        f"{_SERIES_ROWS} newest points and QA history is cached for "
        f"{queries.QA_CACHE_TTL_S:.0f} s."
    )


main()
