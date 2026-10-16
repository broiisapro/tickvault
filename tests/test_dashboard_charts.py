"""UTC encodings of the dashboard chart builders (issue #106).

Every time-axis chart on the dashboard must pin its temporal x encoding to a
UTC scale and format its time tooltip in UTC; these tests read the Altair
specs (no browser) and assert both, plus that the series, colors and titles
of the charts these builders replace are unchanged.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
import pytest

from dashboard import charts

_FIRST = pd.Timestamp("2026-10-15 17:08:00", tz="UTC")
_SECOND = pd.Timestamp("2026-10-15 17:09:00", tz="UTC")


def _message_rate_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "venue": ["coinbase", "coinbase", "kraken", "kraken"],
            "bucket": [_FIRST, _SECOND, _FIRST, _SECOND],
            "messages": [100, 120, 300, 330],
            "msgs_per_second": [1.67, 2.0, 5.0, 5.5],
        }
    )


def _top_of_book_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "venue": ["coinbase", "coinbase", "kraken", "kraken"],
            "symbol": ["BTC-USD"] * 4,
            "ts": [_FIRST, _SECOND, _FIRST, _SECOND],
            "bid_price": [81000.1, 81000.2, 81000.0, 81000.3],
            "bid_size": [0.5, 0.6, 0.4, 0.7],
            "ask_price": [81000.3, 81000.4, 81000.2, 81000.5],
            "ask_size": [0.5, 0.6, 0.4, 0.7],
        }
    )


def _spread_mid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "venue": ["coinbase", "coinbase", "kraken", "kraken"],
            "symbol": ["BTC-USD"] * 4,
            "ts": [_FIRST, _SECOND, _FIRST, _SECOND],
            "mid": [81000.2, 81000.3, 81000.1, 81000.4],
            "spread_bps": [0.25, 0.25, 0.24, 0.24],
        }
    )


_BUILDERS: list[tuple[Callable[[pd.DataFrame], Any], Callable[[], pd.DataFrame]]] = [
    (charts.message_rate_chart, _message_rate_frame),
    (charts.top_of_book_chart, _top_of_book_frame),
    (charts.mid_price_chart, _spread_mid_frame),
    (charts.spread_bps_chart, _spread_mid_frame),
]
_BUILDER_IDS = ["message-rate", "top-of-book", "mid-price", "spread-bps"]


def _spec(
    builder: Callable[[pd.DataFrame], Any], frame_factory: Callable[[], pd.DataFrame]
) -> dict:
    return builder(frame_factory()).to_dict()


def _rows(spec: dict) -> list[dict]:
    return spec["datasets"][spec["data"]["name"]]


@pytest.mark.parametrize(
    ("builder", "frame_factory"), _BUILDERS, ids=_BUILDER_IDS
)
def test_temporal_x_encoding_uses_a_utc_scale(
    builder: Callable[[pd.DataFrame], Any], frame_factory: Callable[[], pd.DataFrame]
) -> None:
    x = _spec(builder, frame_factory)["encoding"]["x"]
    assert x["type"] == "temporal"
    assert x["scale"]["type"] == "utc"
    assert x["axis"]["format"] == "%H:%M"
    assert x["title"].endswith("(UTC)")


@pytest.mark.parametrize(
    ("builder", "frame_factory"), _BUILDERS, ids=_BUILDER_IDS
)
def test_time_tooltip_is_preformatted_in_utc(
    builder: Callable[[pd.DataFrame], Any], frame_factory: Callable[[], pd.DataFrame]
) -> None:
    spec = _spec(builder, frame_factory)
    time_fields = [
        item for item in spec["encoding"]["tooltip"] if item["field"].endswith("_utc")
    ]
    assert len(time_fields) == 1
    assert time_fields[0]["title"].endswith("(UTC)")
    values = [row[time_fields[0]["field"]] for row in _rows(spec)]
    assert values
    assert all(isinstance(value, str) for value in values)
    assert all(" " in value and ":" in value for value in values)


@pytest.mark.parametrize(
    ("builder", "frame_factory"), _BUILDERS, ids=_BUILDER_IDS
)
def test_series_and_marks_are_the_same_line_charts_as_before(
    builder: Callable[[pd.DataFrame], Any], frame_factory: Callable[[], pd.DataFrame]
) -> None:
    spec = _spec(builder, frame_factory)
    assert spec["mark"]["type"] == "line"
    assert spec["encoding"]["color"]["field"] in {"venue", "series"}
    assert spec["height"] in {320, 360}


def test_top_of_book_series_match_the_previous_key_and_sides() -> None:
    spec = _spec(charts.top_of_book_chart, _top_of_book_frame)
    series = {row["series"] for row in _rows(spec)}
    assert series == {
        "coinbase BTC-USD bid",
        "coinbase BTC-USD ask",
        "kraken BTC-USD bid",
        "kraken BTC-USD ask",
    }


def test_mid_and_spread_share_the_venue_symbol_series() -> None:
    mid = _spec(charts.mid_price_chart, _spread_mid_frame)
    spread = _spec(charts.spread_bps_chart, _spread_mid_frame)
    for spec in (mid, spread):
        assert {row["series"] for row in _rows(spec)} == {
            "coinbase BTC-USD",
            "kraken BTC-USD",
        }
