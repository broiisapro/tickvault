from hypothesis import given, settings
from hypothesis import strategies as st

from tickvault.book.sequence import FIRST, GAP, OK, STALE, SequenceTracker

_KEY = "coinbase-20261015T031901Z-00000001"


def test_single_missing_sequence_is_flagged() -> None:
    tracker = SequenceTracker()
    assert tracker.observe(_KEY, 1) == FIRST()
    assert tracker.observe(_KEY, 2) == OK()
    assert tracker.observe(_KEY, 4) == GAP(expected=3, received=4)
    assert tracker.last(_KEY) == 4


def test_duplicate_is_stale_not_gap() -> None:
    tracker = SequenceTracker()
    assert tracker.observe(_KEY, 1) == FIRST()
    assert tracker.observe(_KEY, 2) == OK()
    assert tracker.observe(_KEY, 2) == STALE(received=2)
    assert tracker.last(_KEY) == 2
    assert tracker.observe(_KEY, 1) == STALE(received=1)
    assert tracker.last(_KEY) == 2
    assert tracker.observe(_KEY, 3) == OK()


def test_reset_starts_fresh() -> None:
    tracker = SequenceTracker()
    assert tracker.observe(_KEY, 5) == FIRST()
    tracker.reset(_KEY)
    assert tracker.last(_KEY) is None
    assert tracker.observe(_KEY, 105) == FIRST()


def test_keys_are_tracked_independently() -> None:
    tracker = SequenceTracker()
    assert tracker.observe("connection-a", 0) == FIRST()
    assert tracker.observe("connection-b", 0) == FIRST()
    assert tracker.observe("connection-a", 1) == OK()
    assert tracker.observe("connection-b", 7) == GAP(expected=1, received=7)


@settings(derandomize=True, max_examples=100)
@given(
    st.integers(min_value=0, max_value=10**15),
    st.integers(min_value=1, max_value=500),
)
def test_in_order_stream_produces_no_gap(start: int, length: int) -> None:
    tracker = SequenceTracker()
    results = [tracker.observe(_KEY, start + offset) for offset in range(length)]
    assert results[0] == FIRST()
    assert all(result == OK() for result in results[1:])


@settings(derandomize=True, max_examples=100)
@given(
    st.integers(min_value=0, max_value=10**12),
    st.sets(st.integers(min_value=0, max_value=59), min_size=1, max_size=50),
)
def test_removed_numbers_produce_exact_gaps(start: int, removed: set[int]) -> None:
    present = [start + offset for offset in range(60) if offset not in removed]
    tracker = SequenceTracker()
    gaps: list[GAP] = []
    for index, seq in enumerate(present):
        result = tracker.observe(_KEY, seq)
        if index == 0:
            assert result == FIRST()
        elif isinstance(result, GAP):
            gaps.append(result)
        else:
            assert result == OK()
    expected = [
        (present[index - 1] + 1, present[index])
        for index in range(1, len(present))
        if present[index] != present[index - 1] + 1
    ]
    assert [(gap.expected, gap.received) for gap in gaps] == expected
