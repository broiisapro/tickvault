#!/usr/bin/env python3
"""Build the deterministic synthetic gap/checksum session fixture.

The fixture is a pure function of this module: no network, no randomness and
no wall clock. Two connections are interleaved in one raw layer file by
receive time (Kraken is offset +50 ms so no receive timestamps tie):

* Coinbase Advanced Trade ``BTC-USD``: one sequence gap. The message that
  exposes the gap carries sequence number 23 where 22 was expected; two
  updates then arrive while the book is SYNCING and are dropped, and a fresh
  snapshot recovers the book.
* Kraken WS v2 ``BTC/USD``: one checksum failure. The bad update carries the
  correct checksum plus one; two updates then arrive while the book is
  SYNCING and are dropped, and a fresh snapshot recovers the book.

``tests/fixtures/synthetic_gap_session.expected.json`` records the exact
counts and event order that a ``BookProcessor`` is expected to produce from
the session. ``--check`` regenerates both outputs in memory, self-tests the
checksum implementation against Kraken's documented vector and the live
vector captured on 2026-10-15, and exits non-zero unless the committed files
are byte-identical to the regenerated ones.
"""

import argparse
import datetime as dt
import json
import sys
import zlib
from decimal import Decimal
from itertools import pairwise
from pathlib import Path

import zstandard

ROOT = Path(__file__).resolve().parent.parent
FIXTURES_DIR = ROOT / "tests" / "fixtures"
SESSION_PATH = FIXTURES_DIR / "synthetic_gap_session.jsonl.zst"
EXPECTED_PATH = FIXTURES_DIR / "synthetic_gap_session.expected.json"

COINBASE_URL = "wss://advanced-trade-ws.coinbase.com"
KRAKEN_URL = "wss://ws.kraken.com/v2"
COINBASE_CONNECTION_ID = "coinbase-20261101T000000Z-0000c0de"
KRAKEN_CONNECTION_ID = "kraken-20261101T000000Z-0000beef"

BASE_NS = int(dt.datetime(2026, 11, 1, tzinfo=dt.UTC).timestamp()) * 1_000_000_000
SLOT_NS = 100_000_000  # messages are 100 ms apart in exchange time
RECV_OFFSET_NS = 5_000_000  # recv_ts_ns = exchange_ts_ns + 5 ms
KRAKEN_OFFSET_NS = 50_000_000  # Kraken starts 50 ms later so receive times never tie

ZSTD_LEVEL = 10

COINBASE_SUBSCRIPTIONS = [
    {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "heartbeats"},
    {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "level2"},
    {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "market_trades"},
]
KRAKEN_SUBSCRIPTIONS = [
    {"method": "subscribe", "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 25}},
    {
        "method": "subscribe",
        "params": {"channel": "trade", "symbol": ["BTC/USD"], "snapshot": False},
    },
]

# Counts a BookProcessor must produce, keyed by the expected.json layout.
COINBASE_COUNTS = {
    "gaps": 1,
    "checksum_failures": 0,
    "resyncs": 1,
    "reconnects": 0,
    "invariant_violations": 0,
    "trades": 6,
    "book_deltas_applied": 27,
    "book_deltas_dropped": 2,
    "venue_snapshots": 2,
    "unexplained": 0,
}
KRAKEN_COUNTS = {
    "gaps": 0,
    "checksum_failures": 1,
    "resyncs": 1,
    "reconnects": 0,
    "invariant_violations": 0,
    "trades": 3,
    "book_deltas_applied": 50,
    "book_deltas_dropped": 2,
    "venue_snapshots": 2,
    "unexplained": 0,
}

EXPECTED_NOTES = [
    "book_deltas_applied counts price levels committed to the live book; "
    "book_deltas_dropped counts levels in updates discarded while SYNCING.",
    "the Coinbase update that exposes the sequence gap and the Kraken update whose "
    "checksum fails are validated and discarded, so their levels are counted as "
    "neither applied nor dropped.",
    "trades counts emitted trade rows from market_trades/trade update messages; "
    "trade snapshot backfills are not emitted.",
    "expected_seq/received_seq are null except for gap events.",
    "unexplained is 0: every anomaly is followed by a resync for the same (venue, symbol).",
]

# Kraken checksum self-test vectors.
KRAKEN_DOCUMENTED_VECTOR = (
    "45285210000045286415457195345286615457110945289615456091145290215890660452918154553491"
    "45294744547494529613538000045297599455424529951877282745283510000000452834154582015"
    "45282110000000452810100000004528031545925864527907990000452776331010345277530000000"
    "45277315460273745276615445238"
)
KRAKEN_DOCUMENTED_EXPECTED = 3_310_070_434
LIVE_VECTOR_ASKS = [  # ascending
    ("81207.2", "0.84869087"),
    ("81207.7", "0.18471158"),
    ("81209.7", "0.09431125"),
    ("81209.8", "1.57483831"),
    ("81209.9", "3.07844585"),
    ("81211.0", "0.00006120"),
    ("81212.1", "3.07836042"),
    ("81213.0", "0.01900000"),
    ("81213.2", "0.00019000"),
    ("81213.8", "0.00010000"),
]
LIVE_VECTOR_BIDS = [  # descending
    ("81207.1", "0.00056179"),
    ("81203.0", "0.00006120"),
    ("81199.8", "0.01000000"),
    ("81199.1", "0.18473102"),
    ("81199.0", "0.00129514"),
    ("81198.9", "0.05405688"),
    ("81196.9", "0.00020000"),
    ("81195.9", "0.22736237"),
    ("81195.8", "3.07897538"),
    ("81194.9", "0.00006120"),
]
LIVE_VECTOR_EXPECTED = 1_264_451_060
FLOAT_BUG_EXPECTED = 3_408_421_861  # parsing the numbers as floats produces this wrong value


def canon_number(value: str) -> str:
    """Canonical digits for one Kraken number: drop the point, strip leading zeros.

    Trailing zeros are significant (``0.07250000`` -> ``7250000``), which is
    exactly what parsing the value as a float would lose.
    """
    return value.replace(".", "").lstrip("0") or "0"


def kraken_checksum(asks: list[tuple[str, str]], bids: list[tuple[str, str]]) -> int:
    """CRC32 over the top 10 levels per side as unsigned 32-bit.

    ``asks`` must be ascending and ``bids`` descending; price and qty are the
    number texts exactly as sent by the venue.
    """
    payload = "".join(canon_number(price) + canon_number(qty) for price, qty in asks)
    payload += "".join(canon_number(price) + canon_number(qty) for price, qty in bids)
    return zlib.crc32(payload.encode("ascii")) & 0xFFFFFFFF


def selftest() -> None:
    assert BASE_NS == 1_793_491_200_000_000_000, "base time is not 2026-11-01T00:00:00Z"
    assert zlib.crc32(KRAKEN_DOCUMENTED_VECTOR.encode("ascii")) & 0xFFFFFFFF == (
        KRAKEN_DOCUMENTED_EXPECTED
    ), "documented Kraken checksum vector mismatch"
    assert kraken_checksum(LIVE_VECTOR_ASKS, LIVE_VECTOR_BIDS) == LIVE_VECTOR_EXPECTED, (
        "live Kraken checksum vector mismatch; the text canonicalization is wrong"
    )
    float_bytes = "".join(
        canon_number(str(float(price))) + canon_number(str(float(qty)))
        for price, qty in LIVE_VECTOR_ASKS + LIVE_VECTOR_BIDS
    )
    assert zlib.crc32(float_bytes.encode("ascii")) & 0xFFFFFFFF == FLOAT_BUG_EXPECTED, (
        "float parsing of the live vector no longer reproduces the classic checksum bug"
    )


def rfc3339(ns: int, digits: int) -> str:
    """Format a Unix timestamp in ns with exactly ``digits`` fractional digits."""
    seconds, remainder = divmod(ns, 1_000_000_000)
    stamp = dt.datetime.fromtimestamp(seconds, tz=dt.UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{stamp}.{remainder // 10 ** (9 - digits):0{digits}d}Z"


def dec(units: int, places: int) -> str:
    """Exact decimal text for ``units`` scaled by ``10 ** -places``."""
    text = str(units).rjust(places + 1, "0")
    return f"{text[:-places]}.{text[-places:]}"


class KrakenBook:
    """Venue-side depth-25 book, kept at the subscribed depth after each message.

    Quantity and price texts are stored exactly as sent; the checksum depends
    on the digits, including trailing zeros.
    """

    DEPTH = 25

    def __init__(self) -> None:
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}

    def reset(self, bids: list[tuple[str, str]], asks: list[tuple[str, str]]) -> None:
        self.bids = dict(bids)
        self.asks = dict(asks)
        self._truncate()

    def apply(self, side: str, price: str, qty: str) -> None:
        book = self.bids if side == "bid" else self.asks
        if qty == "0.0":
            book.pop(price, None)
        else:
            book[price] = qty
        self._truncate()

    def _truncate(self) -> None:
        self.bids = dict(self._top(self.bids, reverse=True, limit=self.DEPTH))
        self.asks = dict(self._top(self.asks, reverse=False, limit=self.DEPTH))

    @staticmethod
    def _top(
        book: dict[str, str], *, reverse: bool, limit: int
    ) -> list[tuple[str, str]]:
        return sorted(book.items(), key=lambda item: Decimal(item[0]), reverse=reverse)[:limit]

    def top10(self) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        asks = self._top(self.asks, reverse=False, limit=10)
        bids = self._top(self.bids, reverse=True, limit=10)
        return asks, bids

    def checksum(self) -> int:
        asks, bids = self.top10()
        return kraken_checksum(asks, bids)


# ---------------------------------------------------------------------------
# Coinbase Advanced Trade message text
# ---------------------------------------------------------------------------

COINBASE_SNAPSHOT_LEVELS = [
    ("bid", "81199.90", "0.50000000"),
    ("bid", "81199.80", "0.75000000"),
    ("bid", "81199.70", "0.25000000"),
    ("bid", "81199.60", "0.12500000"),
    ("bid", "81199.50", "1.00000000"),
    ("offer", "81200.10", "0.60000000"),
    ("offer", "81200.20", "0.30000000"),
    ("offer", "81200.30", "0.90000000"),
    ("offer", "81200.40", "0.40000000"),
    ("offer", "81200.50", "0.20000000"),
]
COINBASE_RESYNC_SNAPSHOT_LEVELS = [
    ("bid", "81199.90", "0.55000000"),
    ("bid", "81199.80", "0.70000000"),
    ("bid", "81199.70", "0.30000000"),
    ("bid", "81199.60", "0.15000000"),
    ("bid", "81199.50", "0.90000000"),
    ("offer", "81200.10", "0.52000000"),
    ("offer", "81200.20", "0.44000000"),
    ("offer", "81200.30", "0.92000000"),
    ("offer", "81200.40", "0.36000000"),
    ("offer", "81200.50", "0.22000000"),
]
# 17 updates scaled so that the block of sequence numbers 2-21 also contains
# three market_trades updates (3 + 17 = 20 messages); then sequence 22 is the
# one missing number and the next message jumps to 23.
COINBASE_PRE_GAP_UPDATES = [
    ("bid", "81199.90", "0.60000000"),
    ("offer", "81200.10", "0.50000000"),
    ("bid", "81199.80", "0"),  # level removal
    ("bid", "81199.70", "0.30000000"),
    ("offer", "81200.20", "0.45000000"),
    ("bid", "81199.60", "0.20000000"),
    ("offer", "81200.30", "1.10000000"),
    ("bid", "81199.50", "0.80000000"),
    ("offer", "81200.40", "0.35000000"),
    ("bid", "81199.90", "0.55000000"),
    ("offer", "81200.10", "0.95000000"),
    ("bid", "81199.70", "0.28000000"),
    ("offer", "81200.20", "0.65000000"),
    ("bid", "81199.60", "0.15000000"),
    ("offer", "81200.30", "0.05000000"),
    ("bid", "81199.80", "0.42000000"),
    ("offer", "81200.40", "0.25000000"),
]
COINBASE_GAP_UPDATE = ("bid", "81199.50", "0.70000000")
COINBASE_GAP_DROPPED_UPDATES = [
    ("offer", "81200.50", "0.18000000"),
    ("bid", "81199.90", "0.58000000"),
]
COINBASE_POST_RESYNC_UPDATES = [
    ("bid", "81199.90", "0.61000000"),
    ("offer", "81200.10", "0.52000000"),
    ("bid", "81199.70", "0.31000000"),
    ("offer", "81200.20", "0.44000000"),
    ("bid", "81199.60", "0.21000000"),
    ("offer", "81200.30", "0.92000000"),
    ("bid", "81199.50", "0.81000000"),
    ("offer", "81200.40", "0.36000000"),
    ("bid", "81199.80", "0.43000000"),
    ("offer", "81200.50", "0.22000000"),
]
# (trade_id, price, size, side) per message; the first message is newest-first
# on the wire to exercise the ascending-trade_id emission rule.
COINBASE_BLOCK_TRADES = [
    [
        ("9100002", "81200.20", "0.01000000", "BUY"),
        ("9100001", "81200.10", "0.02000000", "SELL"),
    ],
    [("9100003", "81200.30", "0.00500000", "BUY")],
    [("9100004", "81200.15", "0.01500000", "SELL")],
]
COINBASE_POST_RESYNC_TRADES = [
    [("9100005", "81200.10", "0.00750000", "BUY")],
    [("9100006", "81200.20", "0.01250000", "SELL")],
]
COINBASE_TRADE_POSITIONS = (2, 7, 12)


def cb_level(side: str, event_time: str, price: str, qty: str) -> str:
    return (
        '{"side":"' + side + '","event_time":"' + event_time + '",'
        '"price_level":"' + price + '","new_quantity":"' + qty + '"}'
    )


def cb_l2_message(
    ts_ns: int, seq: int, event_type: str, updates: list[tuple[str, str, str]]
) -> str:
    levels = ",".join(
        cb_level(side, rfc3339(ts_ns, 6), price, qty) for side, price, qty in updates
    )
    return (
        '{"channel":"l2_data","timestamp":"' + rfc3339(ts_ns, 9) + '",'
        '"sequence_num":' + str(seq) + ',"events":[{"type":"' + event_type + '",'
        '"product_id":"BTC-USD","updates":[' + levels + "]}]}"
    )


def cb_subscriptions_message(ts_ns: int) -> str:
    events = [
        '{"subscriptions":{"heartbeats":["heartbeats"]}}',
        '{"subscriptions":{"level2":["BTC-USD"]}}',
        '{"subscriptions":{"market_trades":["BTC-USD"]}}',
    ]
    return (
        '{"channel":"subscriptions","timestamp":"' + rfc3339(ts_ns, 9) + '",'
        '"sequence_num":0,"events":[' + ",".join(events) + "]}"
    )


def cb_trades_message(ts_ns: int, seq: int, trades: list[tuple[str, str, str, str]]) -> str:
    items = []
    for index, (trade_id, price, size, side) in enumerate(trades):
        trade_ns = ts_ns - (index + 1) * 1_000_000
        items.append(
            '{"product_id":"BTC-USD","trade_id":"' + trade_id + '","price":"' + price + '",'
            '"size":"' + size + '","time":"' + rfc3339(trade_ns, 6) + '","side":"' + side + '"}'
        )
    return (
        '{"channel":"market_trades","timestamp":"' + rfc3339(ts_ns, 9) + '",'
        '"sequence_num":' + str(seq) + ',"events":[{"type":"update","trades":['
        + ",".join(items)
        + "]}]}"
    )


# ---------------------------------------------------------------------------
# Kraken WS v2 message text
# ---------------------------------------------------------------------------

KRAKEN_STATUS = (
    '{"channel":"status","type":"update","data":[{"version":"2.0.10","system":"online",'
    '"api_version":"v2","connection_id":12063774461018186484,'
    '"upcoming_maintenance":[],"emergency":[]}]}'
)


def kraken_snapshot_levels(
    bid_base: int, ask_base: int
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    bids = [(dec(811999 - 10 * i, 1), dec(bid_base + i * 1_234_567, 8)) for i in range(25)]
    asks = [(dec(812001 + 10 * i, 1), dec(ask_base + i * 1_234_567, 8)) for i in range(25)]
    return bids, asks


def kraken_ok_ops() -> list[tuple[str, int, int]]:
    """The 30 updates before the bad one, including the required edge cases."""
    ops = [
        ("bid", 811999, 4_500_000),  # top-of-book update
        ("ask", 812001, 3_200_000),
        ("bid", 811984, 2_500_000),  # insertion inside depth 25: pushes the worst bid out
        ("bid", 811849, 0),  # level removal (qty 0)
        ("ask", 812011, 1_750_000),
        ("bid", 811879, 6_660_000),  # update outside the top 10
    ]
    for k in range(6, 30):
        side = "bid" if k % 2 == 0 else "ask"
        price = (811999 - 10 * (k % 5)) if side == "bid" else (812001 + 10 * (k % 5))
        qty = 10_000_000 + (k * 123_457) % 90_000_000
        ops.append((side, price, qty))
    return ops


def kraken_post_resync_ops() -> list[tuple[str, int, int]]:
    ops = []
    for k in range(20):
        side = "ask" if k % 2 == 0 else "bid"
        price = (812001 + 10 * (k % 5)) if side == "ask" else (811999 - 10 * (k % 5))
        qty = 20_000_000 + (k * 234_567) % 50_000_000
        ops.append((side, price, qty))
    return ops


def kraken_level(price: str, qty: str) -> str:
    return '{"price":' + price + ',"qty":' + qty + "}"


def kraken_book_message(
    ts_ns: int,
    msg_type: str,
    bids: list[tuple[str, str]],
    asks: list[tuple[str, str]],
    checksum: int,
) -> str:
    bids_json = ",".join(kraken_level(price, qty) for price, qty in bids)
    asks_json = ",".join(kraken_level(price, qty) for price, qty in asks)
    return (
        '{"channel":"book","type":"' + msg_type + '","data":[{"symbol":"BTC/USD",'
        '"bids":[' + bids_json + '],"asks":[' + asks_json + "],"
        '"checksum":' + str(checksum) + ',"timestamp":"' + rfc3339(ts_ns, 6) + '"}]}'
    )


def kraken_trade_message(ts_ns: int, trades: list[tuple[int, str, str, str]]) -> str:
    items = []
    for index, (trade_id, side, price, qty) in enumerate(trades):
        trade_ns = ts_ns - (index + 1) * 1_000_000
        items.append(
            '{"symbol":"BTC/USD","side":"' + side + '","price":' + price + ","
            '"qty":' + qty + ',"ord_type":"limit","trade_id":' + str(trade_id) + ","
            '"timestamp":"' + rfc3339(trade_ns, 6) + '"}'
        )
    return '{"channel":"trade","type":"update","data":[' + ",".join(items) + "]}"


def kraken_subscribe_ack(ts_ns: int, result_text: str) -> str:
    return (
        '{"method":"subscribe","result":{' + result_text + ',"symbol":"BTC/USD"},'
        '"success":true,"time_in":"' + rfc3339(ts_ns, 6) + '",'
        '"time_out":"' + rfc3339(ts_ns + 46_000, 6) + '"}'
    )


# ---------------------------------------------------------------------------
# Session assembly
# ---------------------------------------------------------------------------

Line = tuple[int, str, str]  # recv_ts_ns, kind, payload


def cb_exchange(slot: int) -> int:
    return BASE_NS + slot * SLOT_NS


def kr_exchange(slot: int) -> int:
    return BASE_NS + KRAKEN_OFFSET_NS + slot * SLOT_NS


def control_payload(**fields: str | list[dict[str, object]]) -> str:
    return json.dumps(fields, separators=(",", ":"), ensure_ascii=False)


def build_coinbase() -> tuple[list[Line], dict[str, int]]:
    lines: list[Line] = []
    counters = {"applied": 0, "dropped": 0, "trades": 0, "snapshots": 0}

    def add(slot: int, kind: str, payload: str) -> None:
        lines.append((cb_exchange(slot) + RECV_OFFSET_NS, kind, payload))

    add(
        0,
        "control",
        control_payload(
            event="connect", url=COINBASE_URL, subscriptions=COINBASE_SUBSCRIPTIONS
        ),
    )
    add(1, "msg", cb_subscriptions_message(cb_exchange(1)))

    seq = 1
    add(2, "msg", cb_l2_message(cb_exchange(2), seq, "snapshot", COINBASE_SNAPSHOT_LEVELS))
    counters["snapshots"] += 1

    seq = 2
    l2_updates = iter(COINBASE_PRE_GAP_UPDATES)
    trade_messages = iter(COINBASE_BLOCK_TRADES)
    for position in range(20):
        slot = 3 + position
        if position in COINBASE_TRADE_POSITIONS:
            trades = next(trade_messages)
            add(slot, "msg", cb_trades_message(cb_exchange(slot), seq, trades))
            counters["trades"] += len(trades)
        else:
            add(
                slot,
                "msg",
                cb_l2_message(cb_exchange(slot), seq, "update", [next(l2_updates)]),
            )
            counters["applied"] += 1
        seq += 1

    # One missing sequence number (22): the next message jumps from 21 to 23.
    gap_index = len(lines)
    add(23, "msg", cb_l2_message(cb_exchange(23), 23, "update", [COINBASE_GAP_UPDATE]))

    add(
        24,
        "control",
        control_payload(event="resync_request", symbol="BTC-USD", reason="gap"),
    )
    for slot, update in zip((25, 26), COINBASE_GAP_DROPPED_UPDATES, strict=True):
        add(slot, "msg", cb_l2_message(cb_exchange(slot), slot - 1, "update", [update]))
        counters["dropped"] += 1

    resync_index = len(lines)
    add(
        27,
        "msg",
        cb_l2_message(cb_exchange(27), 26, "snapshot", COINBASE_RESYNC_SNAPSHOT_LEVELS),
    )
    counters["snapshots"] += 1

    for index, update in enumerate(COINBASE_POST_RESYNC_UPDATES):
        slot = 28 + index
        add(slot, "msg", cb_l2_message(cb_exchange(slot), 27 + index, "update", [update]))
        counters["applied"] += 1
    for index, trades in enumerate(COINBASE_POST_RESYNC_TRADES):
        slot = 38 + index
        add(slot, "msg", cb_trades_message(cb_exchange(slot), 37 + index, trades))
        counters["trades"] += len(trades)

    add(
        40,
        "control",
        control_payload(event="disconnect", reason="session_complete"),
    )
    assert len(lines) == 41
    assert gap_index == 23 and resync_index == 27
    return lines, counters


def build_kraken() -> tuple[list[Line], dict[str, int]]:
    lines: list[Line] = []
    counters = {"applied": 0, "dropped": 0, "trades": 0, "snapshots": 0}

    def add(slot: int, kind: str, payload: str) -> None:
        lines.append((kr_exchange(slot) + RECV_OFFSET_NS, kind, payload))

    book = KrakenBook()
    add(
        0,
        "control",
        control_payload(event="connect", url=KRAKEN_URL, subscriptions=KRAKEN_SUBSCRIPTIONS),
    )
    add(1, "msg", KRAKEN_STATUS)
    add(
        2,
        "msg",
        kraken_subscribe_ack(kr_exchange(2), '"channel":"book","depth":25,"snapshot":true'),
    )
    add(3, "msg", kraken_subscribe_ack(kr_exchange(3), '"channel":"trade","snapshot":false'))

    bids, asks = kraken_snapshot_levels(2_000_000, 3_000_000)
    book.reset(bids, asks)
    add(4, "msg", kraken_book_message(kr_exchange(4), "snapshot", bids, asks, book.checksum()))
    counters["snapshots"] += 1

    seq_bad_index = 5 + len(kraken_ok_ops())
    for index, (side, price, qty) in enumerate(kraken_ok_ops()):
        slot = 5 + index
        price_text = dec(price, 1)
        qty_text = "0.0" if qty == 0 else dec(qty, 8)
        book.apply(side, price_text, qty_text)
        bids_line = [(price_text, qty_text)] if side == "bid" else []
        asks_line = [(price_text, qty_text)] if side == "ask" else []
        add(
            slot,
            "msg",
            kraken_book_message(kr_exchange(slot), "update", bids_line, asks_line, book.checksum()),
        )
        counters["applied"] += 1

    # One update carries the correct checksum plus one.
    bad_side, bad_price, bad_qty = "bid", 811_999, 5_500_000
    bad_price_text, bad_qty_text = dec(bad_price, 1), dec(bad_qty, 8)
    book.apply(bad_side, bad_price_text, bad_qty_text)
    correct = book.checksum()
    wrong = (correct + 1) % 2**32
    add(
        seq_bad_index,
        "msg",
        kraken_book_message(
            kr_exchange(seq_bad_index),
            "update",
            [(bad_price_text, bad_qty_text)],
            [],
            wrong,
        ),
    )

    add(
        seq_bad_index + 1,
        "control",
        control_payload(
            event="resync_request", symbol="BTC/USD", reason="checksum_failure"
        ),
    )
    for offset, (side, price, qty) in enumerate(
        [("ask", 812_001, 4_100_000), ("bid", 811_989, 3_700_000)]
    ):
        slot = seq_bad_index + 2 + offset
        price_text, qty_text = dec(price, 1), dec(qty, 8)
        book.apply(side, price_text, qty_text)
        bids_line = [(price_text, qty_text)] if side == "bid" else []
        asks_line = [(price_text, qty_text)] if side == "ask" else []
        add(
            slot,
            "msg",
            kraken_book_message(kr_exchange(slot), "update", bids_line, asks_line, book.checksum()),
        )
        counters["dropped"] += 1

    resync_index = seq_bad_index + 4
    bids, asks = kraken_snapshot_levels(4_000_000, 6_000_000)
    book.reset(bids, asks)
    add(
        resync_index,
        "msg",
        kraken_book_message(kr_exchange(resync_index), "snapshot", bids, asks, book.checksum()),
    )
    counters["snapshots"] += 1

    for index, (side, price, qty) in enumerate(kraken_post_resync_ops()):
        slot = resync_index + 1 + index
        price_text, qty_text = dec(price, 1), dec(qty, 8)
        book.apply(side, price_text, qty_text)
        bids_line = [(price_text, qty_text)] if side == "bid" else []
        asks_line = [(price_text, qty_text)] if side == "ask" else []
        add(
            slot,
            "msg",
            kraken_book_message(kr_exchange(slot), "update", bids_line, asks_line, book.checksum()),
        )
        counters["applied"] += 1

    trade_base = resync_index + 1 + len(kraken_post_resync_ops())
    post_trades = [
        (211_000_001, "buy", "81200.1", "0.00100000"),
        (211_000_002, "buy", "81200.2", "0.00250000"),
        (211_000_003, "sell", "81200.0", "0.00175000"),
    ]
    for offset, trade in enumerate(post_trades):
        slot = trade_base + offset
        add(slot, "msg", kraken_trade_message(kr_exchange(slot), [trade]))
        counters["trades"] += 1

    add(
        trade_base + len(post_trades),
        "control",
        control_payload(event="disconnect", reason="session_complete"),
    )
    assert len(lines) == 64
    assert seq_bad_index == 35 and resync_index == 39
    return lines, counters


def encode_lines(venue: str, connection_id: str, lines: list[Line]) -> list[tuple[int, str]]:
    encoded = []
    for ingest_seq, (recv_ts_ns, kind, payload) in enumerate(lines):
        record = {
            "v": 1,
            "venue": venue,
            "connection_id": connection_id,
            "ingest_seq": ingest_seq,
            "recv_ts_ns": recv_ts_ns,
            "kind": kind,
            "payload": payload,
        }
        encoded.append(
            (recv_ts_ns, json.dumps(record, separators=(",", ":"), ensure_ascii=False))
        )
    return encoded


def check_coinbase_sequence_numbers(lines: list[Line]) -> None:
    sequences = []
    for _, kind, payload in lines:
        if kind != "msg":
            continue
        message = json.loads(payload)
        if "sequence_num" in message:
            sequences.append(message["sequence_num"])
    expected = [0, *range(1, 22), 23, 24, 25, 26, *range(27, 39)]
    assert sequences == expected, f"sequence numbering is wrong: {sequences}"
    missing = sorted(set(range(39)) - set(sequences))
    assert missing == [22], f"exactly one sequence number must be missing, got {missing}"


def build_session() -> bytes:
    cb_lines, cb_counters = build_coinbase()
    kr_lines, kr_counters = build_kraken()
    check_coinbase_sequence_numbers(cb_lines)

    assert cb_counters["applied"] == COINBASE_COUNTS["book_deltas_applied"], cb_counters
    assert cb_counters["dropped"] == COINBASE_COUNTS["book_deltas_dropped"], cb_counters
    assert cb_counters["trades"] == COINBASE_COUNTS["trades"], cb_counters
    assert cb_counters["snapshots"] == COINBASE_COUNTS["venue_snapshots"], cb_counters
    assert kr_counters["applied"] == KRAKEN_COUNTS["book_deltas_applied"], kr_counters
    assert kr_counters["dropped"] == KRAKEN_COUNTS["book_deltas_dropped"], kr_counters
    assert kr_counters["trades"] == KRAKEN_COUNTS["trades"], kr_counters
    assert kr_counters["snapshots"] == KRAKEN_COUNTS["venue_snapshots"], kr_counters

    merged = encode_lines("coinbase", COINBASE_CONNECTION_ID, cb_lines)
    merged += encode_lines("kraken", KRAKEN_CONNECTION_ID, kr_lines)
    merged.sort(key=lambda item: item[0])

    recv_values = [recv for recv, _ in merged]
    assert all(left < right for left, right in pairwise(recv_values)), (
        "receive timestamps must be strictly increasing across the file"
    )
    body = "\n".join(text for _, text in merged) + "\n"
    return zstandard.ZstdCompressor(level=ZSTD_LEVEL, threads=0).compress(body.encode("utf-8"))


def build_expected() -> bytes:
    expected = {
        "fixture": SESSION_PATH.name,
        "base_time": "2026-11-01T00:00:00Z",
        "counts": {
            "coinbase": {"BTC-USD": COINBASE_COUNTS},
            "kraken": {"BTC/USD": KRAKEN_COUNTS},
        },
        "events": [
            {
                "event_type": "gap",
                "venue": "coinbase",
                "symbol": "BTC-USD",
                "connection_id": COINBASE_CONNECTION_ID,
                "ingest_seq": 23,
                "expected_seq": 22,
                "received_seq": 23,
            },
            {
                "event_type": "resync",
                "venue": "coinbase",
                "symbol": "BTC-USD",
                "connection_id": COINBASE_CONNECTION_ID,
                "ingest_seq": 27,
                "expected_seq": None,
                "received_seq": None,
            },
            {
                "event_type": "checksum_failure",
                "venue": "kraken",
                "symbol": "BTC/USD",
                "connection_id": KRAKEN_CONNECTION_ID,
                "ingest_seq": 35,
                "expected_seq": None,
                "received_seq": None,
            },
            {
                "event_type": "resync",
                "venue": "kraken",
                "symbol": "BTC/USD",
                "connection_id": KRAKEN_CONNECTION_ID,
                "ingest_seq": 39,
                "expected_seq": None,
                "received_seq": None,
            },
        ],
        "notes": EXPECTED_NOTES,
    }
    return (json.dumps(expected, indent=2) + "\n").encode("utf-8")


def compare(path: Path, regenerated: bytes) -> bool:
    if not path.exists():
        print(f"{path}: missing")
        return False
    committed = path.read_bytes()
    if committed == regenerated:
        print(f"{path.relative_to(ROOT)}: OK")
        return True
    print(
        f"{path.relative_to(ROOT)}: does not match the freshly built output "
        f"({len(committed)} vs {len(regenerated)} bytes)"
    )
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="regenerate in memory and fail unless the committed files match",
    )
    args = parser.parse_args(argv)

    selftest()
    session = build_session()
    expected = build_expected()

    if args.check:
        ok = compare(SESSION_PATH, session)
        ok = compare(EXPECTED_PATH, expected) and ok
        return 0 if ok else 1

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    SESSION_PATH.write_bytes(session)
    EXPECTED_PATH.write_bytes(expected)
    print(f"wrote {SESSION_PATH.relative_to(ROOT)} ({len(session) / 1024:.1f} KiB)")
    print(f"wrote {EXPECTED_PATH.relative_to(ROOT)} ({len(expected)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
