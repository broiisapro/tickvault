"""Altair chart builders for the TickVault dashboard, pinned to UTC.

Every builder takes the DataFrame returned by the matching
:mod:`dashboard.queries` function and returns an :class:`altair.Chart` whose
temporal x encoding uses an explicit UTC scale (``alt.Scale(type="utc")``),
a 24-hour ``%H:%M`` tick format (Vega-Lite's default for UTC scales is the
ambiguously 12-hour ``%I:%M``) and a time tooltip preformatted in UTC with a
``UTC`` label. Vega-Lite otherwise formats temporal axes in the browser's
local time zone, which mislabelled the data on the hosted dashboard. Series,
colors, titles and the SQL row caps are unchanged from the Streamlit line
charts these builders replace.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

_UTC_SCALE = alt.Scale(type="utc")
_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
_TICK_FORMAT = "%H:%M"
_RECEIVE_TIME = "Receive time (UTC)"
_EXCHANGE_TIME = "Exchange time (UTC)"


def message_rate_chart(frame: pd.DataFrame) -> alt.Chart:
    """Messages per second per venue over time; x is ``bucket`` in UTC."""
    frame = _utc_time(frame, "bucket")
    return (
        alt.Chart(frame)
        .mark_line()
        .encode(
            x=alt.X(
                "bucket:T",
                scale=_UTC_SCALE,
                axis=alt.Axis(format=_TICK_FORMAT),
                title=_RECEIVE_TIME,
            ),
            y=alt.Y("msgs_per_second:Q", title="Messages/s"),
            color=alt.Color("venue:N", title=None),
            tooltip=[
                alt.Tooltip("bucket_utc:N", title=_RECEIVE_TIME),
                alt.Tooltip("venue:N", title="Venue"),
                alt.Tooltip("messages:Q", title="Messages"),
                alt.Tooltip("msgs_per_second:Q", title="Messages/s", format=".2f"),
            ],
        )
        .properties(height=320)
    )


def top_of_book_chart(frame: pd.DataFrame) -> alt.Chart:
    """Best bid and ask per venue/symbol over time; x is ``ts`` in UTC."""
    frame = _utc_time(frame, "ts")
    long = frame.melt(
        id_vars=["venue", "symbol", "ts", "ts_utc"],
        value_vars=["bid_price", "ask_price"],
        var_name="side",
        value_name="price",
    )
    long["series"] = _series_key(long) + " " + long["side"].str.removesuffix("_price")
    return (
        alt.Chart(long)
        .mark_line()
        .encode(
            x=alt.X(
                "ts:T",
                scale=_UTC_SCALE,
                axis=alt.Axis(format=_TICK_FORMAT),
                title=_EXCHANGE_TIME,
            ),
            y=alt.Y("price:Q", title="Price"),
            color=alt.Color("series:N", title=None),
            tooltip=[
                alt.Tooltip("ts_utc:N", title=_EXCHANGE_TIME),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("price:Q", title="Price", format=",.2f"),
            ],
        )
        .properties(height=360)
    )


def mid_price_chart(frame: pd.DataFrame) -> alt.Chart:
    """Mid price per venue/symbol over time; x is ``ts`` in UTC."""
    frame = _with_series(_utc_time(frame, "ts"))
    return (
        alt.Chart(frame)
        .mark_line()
        .encode(
            x=alt.X(
                "ts:T",
                scale=_UTC_SCALE,
                axis=alt.Axis(format=_TICK_FORMAT),
                title=_EXCHANGE_TIME,
            ),
            y=alt.Y("mid:Q", title="Mid price"),
            color=alt.Color("series:N", title=None),
            tooltip=[
                alt.Tooltip("ts_utc:N", title=_EXCHANGE_TIME),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("mid:Q", title="Mid price", format=",.2f"),
            ],
        )
        .properties(height=320)
    )


def spread_bps_chart(frame: pd.DataFrame) -> alt.Chart:
    """Bid-ask spread in basis points over time; x is ``ts`` in UTC."""
    frame = _with_series(_utc_time(frame, "ts"))
    return (
        alt.Chart(frame)
        .mark_line()
        .encode(
            x=alt.X(
                "ts:T",
                scale=_UTC_SCALE,
                axis=alt.Axis(format=_TICK_FORMAT),
                title=_EXCHANGE_TIME,
            ),
            y=alt.Y("spread_bps:Q", title="Spread (bps)"),
            color=alt.Color("series:N", title=None),
            tooltip=[
                alt.Tooltip("ts_utc:N", title=_EXCHANGE_TIME),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("spread_bps:Q", title="Spread (bps)", format=".2f"),
            ],
        )
        .properties(height=320)
    )


def _utc_time(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    """Return a copy of ``frame`` with ``column`` rendered as a UTC string.

    The timezone-aware column stays for the UTC-scaled temporal x encoding;
    the added ``<column>_utc`` string feeds the tooltip, so the hovered time
    reads in UTC in every browser like the axis does.
    """
    frame = frame.copy()
    frame[f"{column}_utc"] = frame[column].dt.strftime(_TIME_FORMAT)
    return frame


def _with_series(frame: pd.DataFrame) -> pd.DataFrame:
    frame["series"] = _series_key(frame)
    return frame


def _series_key(frame: pd.DataFrame) -> pd.Series:
    return frame["venue"] + " " + frame["symbol"]
