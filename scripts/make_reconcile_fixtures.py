#!/usr/bin/env python3
"""Build the Tardis reconciliation fixtures.

Downloads the free first-of-month Tardis file for Coinbase BTC-USD trades on
2026-10-01, keeps the rows whose exchange timestamp falls in
[2026-10-01T12:00:00Z, 2026-10-01T12:10:00Z) with their original text, and
builds a TickVault data root from the same rows with deterministic, deliberate
discrepancies:

* 5 Tardis trades missing from the TickVault side (fixed row indices);
* 2 rows with a different price, 1 with a different size, 1 with a different
  side (fixed row indices);
* 3 TickVault-only trades whose ids are not in Tardis.

``tests/fixtures/reconcile_expected.json`` records the exact counts and id
lists. The outputs are committed, so tests never touch the network. The
script is re-runnable: columns of the CSV sample keep their original text and
the gzip header carries no timestamp, so a rerun against the same download
produces byte-identical outputs. ``--tardis-file`` uses a local copy instead
of downloading.

Usage:
    scripts/make_reconcile_fixtures.py
    scripts/make_reconcile_fixtures.py --tardis-file /tmp/BTC-USD.csv.gz
"""

import argparse
import datetime as dt
import gzip
import json
import sys
from decimal import Decimal
from pathlib import Path

import httpx
import pyarrow.parquet as pq

from tickvault.models import TRADES_SCHEMA, Trade, TradeSide, to_arrow
from tickvault.paths import partition_path

ROOT = Path(__file__).resolve().parent.parent
FIXTURES_DIR = ROOT / "tests" / "fixtures"
CSV_PATH = FIXTURES_DIR / "tardis_coinbase_trades_sample.csv.gz"
DATA_ROOT = FIXTURES_DIR / "tv_coinbase_trades_sample"
EXPECTED_PATH = FIXTURES_DIR / "reconcile_expected.json"

TARDIS_URL = "https://datasets.tardis.dev/v1/coinbase/trades/2026/10/01/BTC-USD.csv.gz"
TARDIS_HEADER = "exchange,symbol,timestamp,local_timestamp,id,side,price,amount"

WINDOW_START = dt.datetime(2026, 10, 1, 12, 0, 0, tzinfo=dt.UTC)
WINDOW_END = dt.datetime(2026, 10, 1, 12, 10, 0, tzinfo=dt.UTC)
WINDOW_START_NS = int(WINDOW_START.timestamp()) * 1_000_000_000

CONNECTION_ID = "coinbase-20261001T115900Z-00000001"

OMIT_INDICES = (7, 101, 512, 1023, 1500)
PRICE_MISMATCH_DELTAS = {11: Decimal("0.01"), 623: Decimal("-0.02")}
SIZE_MISMATCH_DELTAS = {37: Decimal("0.001")}
SIDE_MISMATCH_INDICES = (250,)

# (trade_id, side, price, size) for rows that exist only in TickVault.
EXTRA_TRADES: tuple[tuple[str, TradeSide, Decimal, Decimal], ...] = (
    ("9000000001", "buy", Decimal("84000.01"), Decimal("0.01000000")),
    ("9000000002", "sell", Decimal("84001.02"), Decimal("0.00150000")),
    ("9000000003", "buy", Decimal("84002.03"), Decimal("0.00025000")),
)

NOTES = [
    "matched counts trades whose id is present on both sides (2089 - 5 = 2084); "
    "the price/size/side mismatch lists are subsets of matched.",
    "missing_in_tickvault and extra_in_tickvault are the unmatched id lists.",
    "TickVault rows use exchange_ts_ns = timestamp * 1000 and "
    "recv_ts_ns = local_timestamp * 1000; ingest_seq is the final row index.",
    "the deliberate discrepancies are chosen by fixed row index and are stable "
    "for the 2026-10-01 Tardis file.",
]


def download(url: str) -> bytes:
    response = httpx.get(url, follow_redirects=True, timeout=120.0)
    response.raise_for_status()
    return response.content


def load_rows(content: bytes) -> tuple[str, list[str], list[list[str]]]:
    """Return the header line, the window's raw text lines and their fields."""
    text = gzip.decompress(content).decode("utf-8")
    lines = text.splitlines(keepends=True)
    if not lines:
        raise SystemExit("Tardis file is empty")
    header_line = lines[0]
    if header_line.rstrip("\n") != TARDIS_HEADER:
        raise SystemExit(f"unexpected Tardis header: {header_line!r}")
    start_us = int(WINDOW_START.timestamp()) * 1_000_000
    end_us = int(WINDOW_END.timestamp()) * 1_000_000
    raw_lines: list[str] = []
    rows: list[list[str]] = []
    for line in lines[1:]:
        fields = line.rstrip("\n").split(",")
        if start_us <= int(fields[2]) < end_us:
            raw_lines.append(line)
            rows.append(fields)
    if not rows:
        raise SystemExit("no Tardis rows fall inside the reconciliation window")
    max_index = max(
        [*OMIT_INDICES, *PRICE_MISMATCH_DELTAS, *SIZE_MISMATCH_DELTAS, *SIDE_MISMATCH_INDICES]
    )
    if len(rows) <= max_index:
        raise SystemExit(
            f"window has {len(rows)} rows, need more than {max_index} for the fixed indices"
        )
    if len({fields[4] for fields in rows}) != len(rows):
        raise SystemExit("trade ids are not unique inside the window")
    return header_line, raw_lines, rows


def build_tickvault(rows: list[list[str]]) -> tuple[list[Trade], dict[str, list[str]]]:
    records: list[Trade] = []
    ids: dict[str, list[str]] = {
        "missing_in_tickvault": [],
        "price_mismatches": [],
        "size_mismatches": [],
        "side_mismatches": [],
    }
    for index, fields in enumerate(rows):
        _exchange, symbol, timestamp, local_timestamp, trade_id, side, price, size = fields
        if index in OMIT_INDICES:
            ids["missing_in_tickvault"].append(trade_id)
            continue
        record_price = Decimal(price)
        record_size = Decimal(size)
        if side not in ("buy", "sell"):
            raise SystemExit(f"unexpected Tardis side {side!r} at row {index}")
        record_side: TradeSide = "buy" if side == "buy" else "sell"
        if index in PRICE_MISMATCH_DELTAS:
            record_price += PRICE_MISMATCH_DELTAS[index]
            ids["price_mismatches"].append(trade_id)
        if index in SIZE_MISMATCH_DELTAS:
            record_size += SIZE_MISMATCH_DELTAS[index]
            ids["size_mismatches"].append(trade_id)
        if index in SIDE_MISMATCH_INDICES:
            record_side = "sell" if record_side == "buy" else "buy"
            ids["side_mismatches"].append(trade_id)
        records.append(
            Trade(
                venue="coinbase",
                symbol=symbol,
                exchange_ts_ns=int(timestamp) * 1000,
                recv_ts_ns=int(local_timestamp) * 1000,
                connection_id=CONNECTION_ID,
                ingest_seq=len(records),
                trade_id=trade_id,
                price=record_price,
                size=record_size,
                side=record_side,
                sequence=None,
            )
        )
    extra_ids = []
    for offset, (trade_id, side, price, size) in enumerate(EXTRA_TRADES):
        exchange_ns = WINDOW_START_NS + (300 + offset) * 1_000_000_000
        extra_ids.append(trade_id)
        records.append(
            Trade(
                venue="coinbase",
                symbol="BTC-USD",
                exchange_ts_ns=exchange_ns,
                recv_ts_ns=exchange_ns + 1_000_000,
                connection_id=CONNECTION_ID,
                ingest_seq=len(records),
                trade_id=trade_id,
                price=price,
                size=size,
                side=side,
                sequence=None,
            )
        )
    ids["extra_in_tickvault"] = extra_ids
    return records, ids


def expected_document(
    rows: list[list[str]], records: list[Trade], ids: dict[str, list[str]]
) -> dict[str, object]:
    tardis = {fields[4]: (Decimal(fields[6]), Decimal(fields[7]), fields[5]) for fields in rows}
    tickvault = {record.trade_id: (record.price, record.size, record.side) for record in records}
    matched_ids = sorted(set(tardis) & set(tickvault))
    price_mismatch_ids = sorted(
        trade_id for trade_id in matched_ids if tardis[trade_id][0] != tickvault[trade_id][0]
    )
    size_mismatch_ids = sorted(
        trade_id for trade_id in matched_ids if tardis[trade_id][1] != tickvault[trade_id][1]
    )
    side_mismatch_ids = sorted(
        trade_id for trade_id in matched_ids if tardis[trade_id][2] != tickvault[trade_id][2]
    )
    assert ids["missing_in_tickvault"] == sorted(set(tardis) - set(tickvault))
    assert sorted(ids["extra_in_tickvault"]) == sorted(set(tickvault) - set(tardis))
    assert ids["price_mismatches"] == price_mismatch_ids
    assert ids["size_mismatches"] == size_mismatch_ids
    assert ids["side_mismatches"] == side_mismatch_ids
    assert len(ids["missing_in_tickvault"]) == 5
    assert len(ids["extra_in_tickvault"]) == 3
    assert len(price_mismatch_ids) == 2
    assert len(size_mismatch_ids) == 1
    assert len(side_mismatch_ids) == 1
    return {
        "window": {
            "start": WINDOW_START.isoformat().replace("+00:00", "Z"),
            "end": WINDOW_END.isoformat().replace("+00:00", "Z"),
        },
        "url": TARDIS_URL,
        "tardis_trades": len(rows),
        "tickvault_trades": len(records),
        "matched": len(matched_ids),
        "missing_in_tickvault": len(ids["missing_in_tickvault"]),
        "extra_in_tickvault": len(ids["extra_in_tickvault"]),
        "price_mismatches": len(price_mismatch_ids),
        "size_mismatches": len(size_mismatch_ids),
        "side_mismatches": len(side_mismatch_ids),
        "missing_in_tickvault_ids": ids["missing_in_tickvault"],
        "extra_in_tickvault_ids": ids["extra_in_tickvault"],
        "price_mismatch_ids": price_mismatch_ids,
        "size_mismatch_ids": size_mismatch_ids,
        "side_mismatch_ids": side_mismatch_ids,
        "notes": NOTES,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--tardis-file",
        type=Path,
        default=None,
        help="local copy of the Tardis .csv.gz instead of downloading it",
    )
    args = parser.parse_args(argv)

    content = args.tardis_file.read_bytes() if args.tardis_file else download(TARDIS_URL)
    header_line, raw_lines, rows = load_rows(content)
    records, ids = build_tickvault(rows)
    expected = expected_document(rows, records, ids)

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    sample = header_line + "".join(raw_lines)
    CSV_PATH.write_bytes(gzip.compress(sample.encode("utf-8"), mtime=0))

    for stale in DATA_ROOT.rglob("*.parquet"):
        stale.unlink()
    partition = partition_path(DATA_ROOT, "trades", "coinbase", WINDOW_START_NS + 1)
    partition.mkdir(parents=True, exist_ok=True)
    parquet_path = partition / f"part-{CONNECTION_ID}-{0:012d}.parquet"
    table = to_arrow(TRADES_SCHEMA, records)
    pq.write_table(table, parquet_path, compression="zstd", write_statistics=True)

    EXPECTED_PATH.write_text(json.dumps(expected, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {CSV_PATH.relative_to(ROOT)}: {len(rows)} tardis rows")
    print(f"wrote {parquet_path.relative_to(ROOT)}: {len(records)} tickvault rows")
    print(
        f"wrote {EXPECTED_PATH.relative_to(ROOT)}: matched {expected['matched']}, "
        f"missing {expected['missing_in_tickvault']}, extra {expected['extra_in_tickvault']}, "
        f"price/size/side mismatches {expected['price_mismatches']}/"
        f"{expected['size_mismatches']}/{expected['side_mismatches']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
