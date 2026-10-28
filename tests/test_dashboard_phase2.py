"""Phase 2 dashboard tests (issue #117).

Three follow-ups from the Phase 1 review:

- the bundled sample is labelled by its repository-relative path
  (``dashboard/sample``) in the sidebar and the header caption, whether it is
  reached by default or through ``TICKVAULT_DATA_DIR``; any other root is
  shown as given. The hosted app used to print ``/mount/src/...``.
- the Streamlit calls use the pinned version's current width API
  (``width="stretch"``), not the deprecated ``use_container_width``.
- venues and symbols come from the data: a root holding a third venue
  (``bitfinex``) reaches every query and every chart.

The app is rendered with :class:`streamlit.testing.v1.AppTest`, so these tests
exercise the real page. The three-venue tree is small and written locally
(no network, no live capture).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pytest
from pyarrow import parquet as pq
from streamlit.testing.v1 import AppTest

from dashboard import queries
from tickvault.models import (
    TABLES,
    BookDelta,
    BookSnapshotLevel,
    Event,
    NormalizedRecord,
    Side,
    Trade,
    to_arrow,
)
from tickvault.sql import connect
from tickvault.storage.parquet_writer import ParquetWriter

_ROOT = Path(__file__).resolve().parents[1]
_APP_PATH = _ROOT / "dashboard" / "app.py"
_SAMPLE = _ROOT / "dashboard" / "sample"
_SAMPLE_LABEL = "dashboard/sample"
_VENUES = ("bitfinex", "coinbase", "kraken")
_SYMBOL = "BTC-USD"
_BASE_NS = int(pd.Timestamp("2026-10-15T17:00:00Z").value)
_NS_PER_SECOND = 1_000_000_000


def _records(venue: str, offset: int) -> dict[str, list[NormalizedRecord]]:
    """One trade, one delta, two sample snapshots and one event for ``venue``."""
    connection = f"conn-{venue}"
    ts_ns = _BASE_NS + offset * 60 * _NS_PER_SECOND
    trades: list[NormalizedRecord] = [
        Trade(
            venue=venue,
            symbol=_SYMBOL,
            exchange_ts_ns=ts_ns,
            recv_ts_ns=ts_ns,
            connection_id=connection,
            ingest_seq=1,
            trade_id=f"{venue}-trade-1",
            price=Decimal("100.50"),
            size=Decimal("0.25"),
            side="buy",
        )
    ]
    deltas: list[NormalizedRecord] = [
        BookDelta(
            venue=venue,
            symbol=_SYMBOL,
            exchange_ts_ns=ts_ns,
            recv_ts_ns=ts_ns,
            connection_id=connection,
            ingest_seq=2,
            side="bid",
            price=Decimal("100.40"),
            size=Decimal("1.5"),
        )
    ]
    snapshots: list[NormalizedRecord] = []
    for step in (0, 1):
        snapshot_ts = ts_ns + step * _NS_PER_SECOND
        sides: tuple[tuple[Side, str], ...] = (("bid", "100.40"), ("ask", "100.60"))
        for side, price in sides:
            snapshots.append(
                BookSnapshotLevel(
                    venue=venue,
                    symbol=_SYMBOL,
                    exchange_ts_ns=snapshot_ts,
                    recv_ts_ns=snapshot_ts,
                    connection_id=connection,
                    ingest_seq=3 + step,
                    snapshot_id=f"{venue}-sample-{step}",
                    source="sample",
                    side=side,
                    level=0,
                    price=Decimal(price),
                    size=Decimal("1.0"),
                )
            )
    events: list[NormalizedRecord] = [
        Event(
            venue=venue,
            recv_ts_ns=ts_ns,
            connection_id=connection,
            ingest_seq=5,
            event_type="reconnect",
            symbol=_SYMBOL,
            exchange_ts_ns=ts_ns,
            detail="{}",
        )
    ]
    return {
        "trades": trades,
        "book_deltas": deltas,
        "book_snapshots": snapshots,
        "events": events,
    }


def _write_three_venue_root(root: Path) -> None:
    """Write one small partition per table and venue through the storage path.

    ``to_arrow`` fixes the on-disk schema (the writer serializes through it);
    the check below makes that link explicit, and ``ParquetWriter`` owns the
    partitioning and file naming.
    """
    writer = ParquetWriter(root)
    try:
        for offset, venue in enumerate(_VENUES):
            for table, records in _records(venue, offset).items():
                expected = to_arrow(table, records)
                assert expected.column_names == [field.name for field in TABLES[table]]
                writer.write(table, records)
    finally:
        writer.close()
    for venue in _VENUES:
        files = sorted((root / "norm").rglob(f"venue={venue}/**/*.parquet"))
        assert files
        assert pq.read_table(files[0]).num_rows > 0


@pytest.fixture(scope="module")
def three_venue_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("dashboard-three-venues")
    _write_three_venue_root(root)
    return root


@contextmanager
def _connection(root: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    con = connect(root)
    try:
        yield con
    finally:
        con.close()


def _chart_venues(chart: Any) -> set[str]:
    """Venues present in the Arrow dataset Streamlit ships with a chart."""
    venues: set[str] = set()
    for dataset in chart.proto.datasets:
        payload = bytes(dataset.data.data)
        if not payload:
            continue
        table = pa.ipc.open_stream(payload).read_all()
        if "venue" in table.column_names:
            venues.update(table.column("venue").to_pylist())
    return venues


def _render(monkeypatch: pytest.MonkeyPatch, root: Path | None) -> AppTest:
    if root is None:
        monkeypatch.delenv("TICKVAULT_DATA_DIR", raising=False)
    else:
        monkeypatch.setenv("TICKVAULT_DATA_DIR", str(root))
    app = AppTest.from_file(str(_APP_PATH), default_timeout=60)
    app.run()
    return app


def _captions(app: AppTest) -> list[str]:
    return [caption.value for caption in app.caption]


def test_default_root_is_labelled_as_the_bundled_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _render(monkeypatch, None)
    assert app.exception == []
    assert app.text_input[0].value == _SAMPLE_LABEL
    sample_captions = [c for c in _captions(app) if c.startswith("Bundled sample:")]
    assert len(sample_captions) == 1
    caption = sample_captions[0]
    assert _SAMPLE_LABEL in caption
    assert str(_SAMPLE) not in caption
    assert any("bundled sample" in c.lower() for c in _captions(app))


def test_env_root_pointing_at_the_sample_is_labelled_the_same_way(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _render(monkeypatch, _SAMPLE)
    assert app.exception == []
    assert app.text_input[0].value == _SAMPLE_LABEL
    sample_captions = [c for c in _captions(app) if c.startswith("Bundled sample:")]
    assert len(sample_captions) == 1
    assert _SAMPLE_LABEL in sample_captions[0]
    for text in [*_captions(app), *(block.value for block in app.markdown)]:
        assert str(_SAMPLE) not in text
    assert any("bundled sample" in c.lower() for c in _captions(app))


def test_any_other_root_is_shown_as_given(
    three_venue_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _render(monkeypatch, three_venue_root)
    assert app.exception == []
    assert app.text_input[0].value == str(three_venue_root)
    live = [c for c in _captions(app) if c.startswith("Live data root:")]
    assert len(live) == 1
    assert str(three_venue_root) in live[0]
    assert not any("bundled sample" in c.lower() for c in _captions(app))


def test_no_deprecated_use_container_width_remains() -> None:
    offenders = [
        path.as_posix()
        for path in (_ROOT / "dashboard").rglob("*.py")
        if "use_container_width" in path.read_text(encoding="utf-8")
    ]
    assert offenders == []


def test_rendering_logs_no_deprecated_width_warning(
    three_venue_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        app = _render(monkeypatch, three_venue_root)
    assert app.exception == []
    assert [
        record for record in caplog.records if "use_container_width" in record.getMessage()
    ] == []


def test_every_query_sees_the_third_venue(three_venue_root: Path) -> None:
    with _connection(three_venue_root) as con:
        dimensions = queries.dimensions(con)
        rate = queries.message_rate(con)
        events = queries.cumulative_event_counts(con)
        book = queries.top_of_book(con)
        spread = queries.spread_mid(con)
        span = queries.data_span(con)
    frames = {
        "dimensions": dimensions,
        "message_rate": rate,
        "cumulative_event_counts": events,
        "top_of_book": book,
        "spread_mid": spread,
    }
    for name, frame in frames.items():
        assert set(frame["venue"]) == set(_VENUES), name
    assert set(dimensions["symbol"]) == {_SYMBOL}
    # Two distinct messages per venue: the trade and the delta.
    assert dict(rate.groupby("venue")["messages"].sum()) == dict.fromkeys(_VENUES, 2)
    assert set(book["symbol"]) == {_SYMBOL}
    assert bool(pd.notna(span["first_ts_ns"].iloc[0]))
    qa = queries.qa_history(three_venue_root)
    assert set(qa["venue"]) == set(_VENUES)
    assert set(qa["symbol"]) == {_SYMBOL}


def test_every_chart_sees_the_third_venue(
    three_venue_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _render(monkeypatch, three_venue_root)
    assert app.exception == []
    venue_options = app.multiselect[0].options
    assert list(venue_options) == list(_VENUES)
    assert app.multiselect[0].value == list(_VENUES)
    header = [c for c in _captions(app) if c.startswith("Live data root:")]
    assert len(header) == 1

    charts = app.get("vega_lite_chart")
    assert len(charts) == 5
    for chart in charts:
        assert _chart_venues(chart) == set(_VENUES)
