"""Classic-bug test: no bar may use data at or after its own end (issue #118).

The rule under test is decision A67's: features for bars with
``bar_end_ns ≤ t`` are identical when every normalized row with
``exchange_ts_ns ≥ t`` is deleted and the features are recomputed. The data
root is a five-minute Kraken replay of ``tests/fixtures/kraken_btc_5m.jsonl.zst``
built once per session; every case works on a filtered copy of it, so the
original tree is never modified.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tickvault.features import compute_features
from tickvault.models import TABLES
from tickvault.pipeline import run_replay

_DAY = "2026-10-15"
_SECOND = 1_000_000_000
_TABLES = tuple(TABLES)
_ROW_GROUP_SIZE = 65_536


@pytest.fixture(scope="session")
def session_root(tmp_path_factory: pytest.TempPathFactory, kraken_5m_path: Path) -> Path:
    """Replay the five-minute Kraken recording into a normalized tree once."""
    root = tmp_path_factory.mktemp("features-lookahead-replay")
    run_replay(kraken_5m_path, root)
    return root


@pytest.fixture(scope="session")
def full_features(session_root: Path) -> dict[str, pa.Table]:
    """Features over the whole session, used as the reference."""
    return compute_features(session_root, _DAY)


def _truncated_root(source: Path, cutoff_ns: int, destination: Path) -> Path:
    """Copy ``source`` without every row whose ``exchange_ts_ns ≥ cutoff_ns``.

    All four normalized tables are filtered. Rows with a null
    ``exchange_ts_ns`` carry no exchange time at all (events), so they are
    kept: ``null ≥ t`` is not true. Empty files are skipped.
    """
    for table in _TABLES:
        source_dir = source / "norm" / table
        for path in sorted(source_dir.rglob("*.parquet")):
            data = pq.read_table(path)
            column = data.column("exchange_ts_ns")
            # null means "no exchange time" (events), which is never >= t.
            keep = pc.if_else(
                pc.is_null(column),
                True,
                pc.less(column, pa.scalar(cutoff_ns, type=pa.int64())),
            )
            kept = data.filter(keep)
            if kept.num_rows == 0:
                continue
            target = destination / "norm" / table / path.relative_to(source_dir)
            target.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                kept,
                target,
                compression="zstd",
                write_statistics=True,
                row_group_size=_ROW_GROUP_SIZE,
            )
    return destination


def _bars_before(table: pa.Table, cutoff_ns: int) -> list[dict[str, object]]:
    """The bars of ``table`` that close at or before ``cutoff_ns``."""
    return [
        row
        for row in table.to_pylist()
        if isinstance(row["bar_end_ns"], int) and row["bar_end_ns"] <= cutoff_ns
    ]


def _assert_prefix_is_unchanged(
    source: Path,
    full: dict[str, pa.Table],
    cutoff_ns: int,
    destination: Path,
) -> None:
    truncated = _truncated_root(source, cutoff_ns, destination)
    recomputed = compute_features(truncated, _DAY)
    assert set(recomputed) == set(full)
    compared = 0
    for resolution, reference in full.items():
        expected = _bars_before(reference, cutoff_ns)
        actual = _bars_before(recomputed[resolution], cutoff_ns)
        assert actual == expected, f"{resolution}: bars changed before t={cutoff_ns}"
        compared += len(expected)
    # A cutoff inside the session must leave something to compare, otherwise
    # the case would pass vacuously.
    if cutoff_ns >= _span_start(full) + 2 * _SECOND:
        assert compared > 0


def _span_start(full: dict[str, pa.Table]) -> int:
    return int(str(full["1s"].column("bar_start_ns")[0]))


def _span_end(full: dict[str, pa.Table]) -> int:
    return int(str(full["1s"].column("bar_end_ns")[-1]))


def test_reference_has_bars_at_both_resolutions(full_features: dict[str, pa.Table]) -> None:
    assert set(full_features) == {"1s", "1m"}
    assert full_features["1s"].num_rows > 200
    assert full_features["1m"].num_rows > 2
    assert _span_start(full_features) < _span_end(full_features)


@pytest.mark.parametrize(
    "offset_ns",
    [
        pytest.param(0, id="at-the-first-bar-start"),
        pytest.param(60 * _SECOND, id="one-minute-in"),
        pytest.param(61 * _SECOND + 500_000_000, id="mid-bar-off-boundary"),
        pytest.param(120 * _SECOND, id="two-minutes-in"),
        pytest.param(240 * _SECOND, id="four-minutes-in"),
    ],
)
def test_fixed_cutoffs_leave_earlier_bars_untouched(
    session_root: Path,
    full_features: dict[str, pa.Table],
    tmp_path_factory: pytest.TempPathFactory,
    offset_ns: int,
) -> None:
    cutoff_ns = _span_start(full_features) + offset_ns
    destination = tmp_path_factory.mktemp(f"prefix-{offset_ns}")
    _assert_prefix_is_unchanged(session_root, full_features, cutoff_ns, destination)


def _max_exchange_ts(root: Path) -> int:
    """Largest non-null ``exchange_ts_ns`` anywhere in the normalized tree."""
    largest = 0
    for table in _TABLES:
        for path in sorted((root / "norm" / table).rglob("*.parquet")):
            column = pq.read_table(path, columns=["exchange_ts_ns"]).column("exchange_ts_ns")
            value = pc.max(column).as_py()
            if value is not None:
                largest = max(largest, int(str(value)))
    return largest


def _row_count(root: Path, table: str) -> int:
    return sum(
        pq.read_metadata(path).num_rows for path in (root / "norm" / table).rglob("*.parquet")
    )


def test_cutoff_after_the_last_row_changes_nothing(
    session_root: Path,
    full_features: dict[str, pa.Table],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    # One nanosecond past the latest row, so the filter keeps everything.
    cutoff_ns = _max_exchange_ts(session_root) + 1
    destination = tmp_path_factory.mktemp("prefix-end")
    _assert_prefix_is_unchanged(session_root, full_features, cutoff_ns, destination)
    for table in _TABLES:
        assert _row_count(destination, table) == _row_count(session_root, table), table


@settings(
    derandomize=True,
    max_examples=6,
    deadline=None,
)
@given(
    offset_seconds=st.integers(min_value=0, max_value=300),
)
def test_prefix_property_over_cutoffs(
    session_root: Path,
    full_features: dict[str, pa.Table],
    tmp_path_factory: pytest.TempPathFactory,
    offset_seconds: int,
) -> None:
    cutoff_ns = _span_start(full_features) + offset_seconds * _SECOND
    destination = tmp_path_factory.mktemp(f"prefix-property-{offset_seconds}")
    _assert_prefix_is_unchanged(session_root, full_features, cutoff_ns, destination)
