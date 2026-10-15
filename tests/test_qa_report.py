"""Tests for :mod:`tickvault.qa.report`.

The golden tests build one small tree (two venues, one explained gap and one
unexplained checksum failure) and pin the exact JSON, Markdown and terminal
outputs, so renderer changes cannot slip through silently.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from tickvault.models import BookDelta, Event, EventType, NormalizedRecord, Trade, to_arrow
from tickvault.paths import partition_path
from tickvault.qa.report import (
    QAReport,
    QATotals,
    exit_code,
    run_qa,
    to_json,
    to_markdown,
    to_text_table,
    totals,
    write_reports,
)

SECOND_NS = 1_000_000_000
MILLISECOND_NS = 1_000_000
DAY = "2026-10-01"
DAY2 = "2026-10-02"

GOLDEN_JSON = (
    "{\n"
    "  \"date\": \"2026-10-01\",\n"
    "  \"generated_from\": \"data-root\",\n"
    "  \"rows\": [\n"
    "    {\n"
    "      \"book_deltas\": 1,\n"
    "      \"book_missing\": true,\n"
    "      \"checksum_failures\": 0,\n"
    "      \"coverage\": 1.0,\n"
    "      \"date\": \"2026-10-01\",\n"
    "      \"gaps\": 1,\n"
    "      \"invariant_violations\": 0,\n"
    "      \"lag_p50_ms\": 2.0,\n"
    "      \"lag_p99_ms\": 2.98,\n"
    "      \"messages\": 5,\n"
    "      \"reconnects\": 0,\n"
    "      \"resyncs\": 1,\n"
    "      \"symbol\": \"BTC-USD\",\n"
    "      \"trades\": 2,\n"
    "      \"unexplained\": 0,\n"
    "      \"venue\": \"coinbase\",\n"
    "      \"venue_snapshots\": 0\n"
    "    },\n"
    "    {\n"
    "      \"book_deltas\": 0,\n"
    "      \"book_missing\": true,\n"
    "      \"checksum_failures\": 1,\n"
    "      \"coverage\": 1.0,\n"
    "      \"date\": \"2026-10-01\",\n"
    "      \"gaps\": 0,\n"
    "      \"invariant_violations\": 0,\n"
    "      \"lag_p50_ms\": 1.0,\n"
    "      \"lag_p99_ms\": 1.0,\n"
    "      \"messages\": 2,\n"
    "      \"reconnects\": 0,\n"
    "      \"resyncs\": 0,\n"
    "      \"symbol\": \"BTC-USD\",\n"
    "      \"trades\": 1,\n"
    "      \"unexplained\": 1,\n"
    "      \"venue\": \"kraken\",\n"
    "      \"venue_snapshots\": 0\n"
    "    }\n"
    "  ]\n"
    "}\n"
)

GOLDEN_MARKDOWN = (
    "| venue | symbol | date | messages | trades | book_deltas | venue_snapshots |"
    " gaps | resyncs | checksum_failures | reconnects | invariant_violations |"
    " lag_p50_ms | lag_p99_ms | coverage | unexplained | book_missing |\n"
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    " --- | --- | --- | --- |\n"
    "| coinbase | BTC-USD | 2026-10-01 | 5 | 2 | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 2.0 |"
    " 2.98 | 1.0 | 0 | True |\n"
    "| kraken | BTC-USD | 2026-10-01 | 2 | 1 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 1.0 | 1.0"
    " | 1.0 | 1 | True |\n"
    "| TOTAL | - | - | 7 | 3 | 1 | 0 | 1 | 1 | 1 | 0 | 0 | - | - | - | 1 | 2 |\n"
)

GOLDEN_TEXT = (
    "venue     symbol   date        messages  trades  book_deltas  venue_snapshots "
    " gaps  resyncs  checksum_failures  reconnects  invariant_violations  lag_p50_ms"
    "  lag_p99_ms  coverage  unexplained  book_missing\n"
    "--------  -------  ----------  --------  ------  -----------  --------------- "
    " ----  -------  -----------------  ----------  --------------------  ----------"
    "  ----------  --------  -----------  ------------\n"
    "coinbase  BTC-USD  2026-10-01         5       2            1                0  "
    "   1        1                  0           0                     0         2.0 "
    "       2.98       1.0            0          True\n"
    "  kraken  BTC-USD  2026-10-01         2       1            0                0  "
    "   0        0                  1           0                     0         1.0 "
    "        1.0       1.0            1          True\n"
    "   TOTAL        -           -         7       3            1                0  "
    "   1        1                  1           0                     0           - "
    "          -         -            1             2\n"
)


def _ts(second: int, hour: int = 12, day: int = 1) -> int:
    return int(datetime(2026, 10, day, hour, tzinfo=UTC).timestamp()) * SECOND_NS + (
        second * SECOND_NS
    )


def _write(root: Path, table: str, records: Sequence[NormalizedRecord]) -> None:
    grouped: dict[Path, list[NormalizedRecord]] = {}
    for record in records:
        ts_ns = record.recv_ts_ns if record.exchange_ts_ns is None else record.exchange_ts_ns
        path = partition_path(root, table, record.venue, ts_ns)
        grouped.setdefault(path, []).append(record)
    for path, group in grouped.items():
        path.mkdir(parents=True, exist_ok=True)
        pq.write_table(to_arrow(table, group), path / "part-test.parquet")


def _trade(
    venue: str, ts_ns: int, lag_ms: int, ingest_seq: int, connection_id: str = "conn-a"
) -> Trade:
    return Trade(
        venue=venue,
        symbol="BTC-USD",
        exchange_ts_ns=ts_ns,
        recv_ts_ns=ts_ns + lag_ms * MILLISECOND_NS,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        trade_id=f"{venue}-{ingest_seq}",
        price=Decimal("100.5"),
        size=Decimal("0.25"),
        side="buy",
    )


def _delta(venue: str, ts_ns: int, lag_ms: int, ingest_seq: int) -> BookDelta:
    return BookDelta(
        venue=venue,
        symbol="BTC-USD",
        exchange_ts_ns=ts_ns,
        recv_ts_ns=ts_ns + lag_ms * MILLISECOND_NS,
        connection_id="conn-a",
        ingest_seq=ingest_seq,
        side="bid",
        price=Decimal("100.0"),
        size=Decimal("1.5"),
    )


def _event(
    event_type: EventType, venue: str, ts_ns: int, ingest_seq: int, connection_id: str = "conn-a"
) -> Event:
    return Event(
        venue=venue,
        recv_ts_ns=ts_ns,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        event_type=event_type,
        symbol="BTC-USD",
        exchange_ts_ns=ts_ns,
    )


def _build_tree(root: Path) -> None:
    _write(
        root,
        "trades",
        [
            _trade("coinbase", _ts(1), lag_ms=1, ingest_seq=0),
            _trade("coinbase", _ts(2), lag_ms=3, ingest_seq=1),
            _trade("kraken", _ts(1), lag_ms=1, ingest_seq=0, connection_id="conn-k"),
        ],
    )
    _write(root, "book_deltas", [_delta("coinbase", _ts(2), lag_ms=2, ingest_seq=2)])
    _write(
        root,
        "events",
        [
            _event("gap", "coinbase", _ts(3), ingest_seq=3),
            _event("resync", "coinbase", _ts(4), ingest_seq=4),
            _event("checksum_failure", "kraken", _ts(2), ingest_seq=1, connection_id="conn-k"),
        ],
    )


def _golden_report(root: Path) -> QAReport:
    _build_tree(root)
    return QAReport(rows=tuple(run_qa(root).rows), generated_from="data-root", date=DAY)


def test_totals_sum_every_count_column(tmp_path: Path) -> None:
    report = _golden_report(tmp_path)
    assert totals(report) == QATotals(
        rows=2,
        messages=7,
        trades=3,
        book_deltas=1,
        venue_snapshots=0,
        gaps=1,
        resyncs=1,
        checksum_failures=1,
        reconnects=0,
        invariant_violations=0,
        unexplained=1,
        book_missing=2,
    )


def test_exit_code_is_one_when_any_row_is_unexplained(tmp_path: Path) -> None:
    report = _golden_report(tmp_path)
    assert exit_code(report) == 1
    explained = QAReport(
        rows=tuple(replace(row, unexplained=0, book_missing=False) for row in report.rows),
        generated_from=report.generated_from,
        date=report.date,
    )
    assert exit_code(explained) == 0


def test_exit_code_is_one_when_a_book_is_missing(tmp_path: Path) -> None:
    report = _golden_report(tmp_path)
    explained = QAReport(
        rows=tuple(replace(row, unexplained=0) for row in report.rows),
        generated_from=report.generated_from,
        date=report.date,
    )
    assert exit_code(explained) == 1
    recovered = QAReport(
        rows=tuple(replace(row, unexplained=0, book_missing=False) for row in report.rows),
        generated_from=report.generated_from,
        date=report.date,
    )
    assert exit_code(recovered) == 0


def test_exit_code_zero_for_empty_report() -> None:
    assert exit_code(QAReport(rows=(), generated_from="empty", date=None)) == 0


def test_run_qa_records_source_and_filtered_date(tmp_path: Path) -> None:
    _build_tree(tmp_path)
    full = run_qa(tmp_path)
    assert full.generated_from == str(tmp_path)
    assert full.date is None
    assert [row.venue for row in full.rows] == ["coinbase", "kraken"]

    filtered = run_qa(tmp_path, date(2026, 10, 2))
    assert filtered.date == DAY2
    assert filtered.rows == ()


def test_reports_are_deterministic(tmp_path: Path) -> None:
    _build_tree(tmp_path)
    assert run_qa(tmp_path) == run_qa(tmp_path)
    assert to_json(run_qa(tmp_path)) == to_json(run_qa(tmp_path))
    assert to_markdown(run_qa(tmp_path)) == to_markdown(run_qa(tmp_path))
    assert to_text_table(run_qa(tmp_path)) == to_text_table(run_qa(tmp_path))


def test_run_qa_accepts_string_date(tmp_path: Path) -> None:
    report = run_qa(tmp_path, DAY2)
    assert report.date == DAY2


def test_json_keys_are_sorted_and_carry_no_timestamp(tmp_path: Path) -> None:
    payload = json.loads(to_json(_golden_report(tmp_path)))
    assert list(payload) == ["date", "generated_from", "rows"]
    assert "generated_at" not in payload
    assert [list(row) for row in payload["rows"]] == [
        sorted(payload["rows"][0]),
        sorted(payload["rows"][1]),
    ]


def test_write_reports_creates_both_files(tmp_path: Path) -> None:
    report = _golden_report(tmp_path)
    reports_dir = tmp_path / "reports"
    json_path, markdown_path = write_reports(report, reports_dir, DAY)
    assert json_path == reports_dir / f"qa-{DAY}.json"
    assert markdown_path == reports_dir / f"qa-{DAY}.md"
    assert json_path.read_text(encoding="utf-8") == to_json(report)
    assert markdown_path.read_text(encoding="utf-8") == to_markdown(report)


def test_golden_json(tmp_path: Path) -> None:
    assert to_json(_golden_report(tmp_path)) == GOLDEN_JSON


def test_golden_markdown(tmp_path: Path) -> None:
    assert to_markdown(_golden_report(tmp_path)) == GOLDEN_MARKDOWN


def test_golden_text_table(tmp_path: Path) -> None:
    assert to_text_table(_golden_report(tmp_path)) == GOLDEN_TEXT
