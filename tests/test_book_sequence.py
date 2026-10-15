import json
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from tickvault.book.processor import BookProcessor
from tickvault.book.sequence import FIRST, GAP, OK, STALE, SequenceTracker
from tickvault.models import BookUpdate, NormalizedMessage, PriceLevel, RawMessage

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


_SYNC_CONNECTION = "coinbase-20261101T000000Z-00000001"
_SYNC_TS = 1_793_491_200_000_000_000


def _cb_level(price: str, size: str) -> PriceLevel:
    return (Decimal(price), Decimal(size))


def _cb_msg(
    sequence: int,
    *,
    snapshot: bool,
    ingest_seq: int,
    bids: tuple[PriceLevel, ...] = (),
    asks: tuple[PriceLevel, ...] = (),
) -> NormalizedMessage:
    raw = RawMessage(
        venue="coinbase",
        connection_id=_SYNC_CONNECTION,
        ingest_seq=ingest_seq,
        recv_ts_ns=_SYNC_TS,
        kind="msg",
        payload="{}",
    )
    update = BookUpdate(
        symbol="BTC-USD",
        is_snapshot=snapshot,
        bids=bids,
        asks=asks,
        exchange_ts_ns=_SYNC_TS,
    )
    return NormalizedMessage(raw=raw, channel="l2_data", sequence=sequence, book_updates=(update,))


def test_single_missing_sequence_is_flagged_and_resynced() -> None:
    processor = BookProcessor()
    processor.process(
        _cb_msg(
            1,
            snapshot=True,
            ingest_seq=0,
            bids=(_cb_level("100", "1"),),
            asks=(_cb_level("101", "1"),),
        )
    )
    processor.process(_cb_msg(2, snapshot=False, ingest_seq=1, bids=(_cb_level("100", "2"),)))

    gap_output = processor.process(
        _cb_msg(4, snapshot=False, ingest_seq=2, bids=(_cb_level("100", "3"),))
    )
    gaps = [event for event in gap_output.events if event.event_type == "gap"]
    assert len(gaps) == 1
    assert (gaps[0].expected_seq, gaps[0].received_seq) == (3, 4)
    assert gaps[0].symbol == "BTC-USD"
    assert gaps[0].channel == "l2_data"
    assert gap_output.book_deltas == []
    assert len(gap_output.resync_requests) == 1
    assert gap_output.resync_requests[0].reason == "gap"

    dropped = processor.process(
        _cb_msg(5, snapshot=False, ingest_seq=3, bids=(_cb_level("100", "4"),))
    )
    assert dropped.book_deltas == []

    recovered = processor.process(
        _cb_msg(
            6,
            snapshot=True,
            ingest_seq=4,
            bids=(_cb_level("100", "9"),),
            asks=(_cb_level("101", "9"),),
        )
    )
    resyncs = [event for event in recovered.events if event.event_type == "resync"]
    assert len(resyncs) == 1
    assert json.loads(resyncs[0].detail) == {"deltas_dropped": 1, "reason": "gap"}

    subsequent = processor.process(
        _cb_msg(7, snapshot=False, ingest_seq=5, bids=(_cb_level("100", "5"),))
    )
    assert [row.size for row in subsequent.book_deltas] == [Decimal("5")]
