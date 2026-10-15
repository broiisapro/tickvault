"""Result object and renderers for the data-quality report.

:func:`run_qa` wraps :func:`tickvault.qa.metrics.compute_qa` in a
:class:`QAReport` that also records the data root and the optional date filter.
The renderers are pure and never embed the current time, so the same tree
produces byte-identical JSON, Markdown and terminal output across runs.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from datetime import date as Date
from pathlib import Path

from tickvault.qa.metrics import QARow, compute_qa

_COUNT_FIELDS = (
    "messages",
    "trades",
    "book_deltas",
    "venue_snapshots",
    "gaps",
    "resyncs",
    "checksum_failures",
    "reconnects",
    "invariant_violations",
    "unexplained",
    "book_missing",
)
_COLUMNS = tuple(field.name for field in fields(QARow))


@dataclass(frozen=True, slots=True)
class QAReport:
    """A complete quality report: rows plus where they came from.

    ``rows`` are sorted the way :func:`compute_qa` returns them; ``date`` is
    the normalized ``YYYY-MM-DD`` filter or ``None`` for the whole tree.
    """

    rows: tuple[QARow, ...]
    generated_from: str
    date: str | None = None


@dataclass(frozen=True, slots=True)
class QATotals:
    """Sums of the integer count columns across every row of a report.

    ``book_missing`` is the number of rows with a missing book, not a sum of
    integers; it is reported alongside the counts so the totals line shows it.
    """

    rows: int
    messages: int
    trades: int
    book_deltas: int
    venue_snapshots: int
    gaps: int
    resyncs: int
    checksum_failures: int
    reconnects: int
    invariant_violations: int
    unexplained: int
    book_missing: int


def _iso_date(value: str | Date) -> str:
    if isinstance(value, Date):
        return value.isoformat()
    return Date.fromisoformat(value).isoformat()


def run_qa(root: str | Path, date: str | Date | None = None) -> QAReport:
    """Compute the quality rows for ``root`` and wrap them in a report."""
    return QAReport(
        rows=tuple(compute_qa(root, date)),
        generated_from=str(root),
        date=None if date is None else _iso_date(date),
    )


def totals(report: QAReport) -> QATotals:
    """Sum the count columns across every row of ``report``."""
    return QATotals(
        rows=len(report.rows),
        messages=sum(row.messages for row in report.rows),
        trades=sum(row.trades for row in report.rows),
        book_deltas=sum(row.book_deltas for row in report.rows),
        venue_snapshots=sum(row.venue_snapshots for row in report.rows),
        gaps=sum(row.gaps for row in report.rows),
        resyncs=sum(row.resyncs for row in report.rows),
        checksum_failures=sum(row.checksum_failures for row in report.rows),
        reconnects=sum(row.reconnects for row in report.rows),
        invariant_violations=sum(row.invariant_violations for row in report.rows),
        unexplained=sum(row.unexplained for row in report.rows),
        book_missing=sum(1 for row in report.rows if row.book_missing),
    )


def exit_code(report: QAReport) -> int:
    """Return ``1`` when any row is unexplained or missing its book, else ``0``."""
    return 1 if any(row.unexplained > 0 or row.book_missing for row in report.rows) else 0


def to_json(report: QAReport) -> str:
    """Render ``report`` as deterministic JSON with sorted keys."""
    payload = {
        "date": report.date,
        "generated_from": report.generated_from,
        "rows": [asdict(row) for row in report.rows],
    }
    return json.dumps(payload, sort_keys=True, indent=2) + "\n"


def _total_cells() -> dict[str, str]:
    return {column: "-" for column in _COLUMNS}


def to_markdown(report: QAReport) -> str:
    """Render ``report`` as one Markdown table plus a totals line."""
    lines = [
        "| " + " | ".join(_COLUMNS) + " |",
        "| " + " | ".join("---" for _ in _COLUMNS) + " |",
    ]
    for row in report.rows:
        values = [str(getattr(row, column)) for column in _COLUMNS]
        lines.append("| " + " | ".join(values) + " |")
    cells = _total_cells()
    cells["venue"] = "TOTAL"
    summed = totals(report)
    for column in _COUNT_FIELDS:
        cells[column] = str(getattr(summed, column))
    lines.append("| " + " | ".join(cells[column] for column in _COLUMNS) + " |")
    return "\n".join(lines) + "\n"


def to_text_table(report: QAReport) -> str:
    """Render ``report`` as a fixed-width table for the terminal."""
    data_rows = [[str(getattr(row, column)) for column in _COLUMNS] for row in report.rows]
    cells = _total_cells()
    cells["venue"] = "TOTAL"
    summed = totals(report)
    for column in _COUNT_FIELDS:
        cells[column] = str(getattr(summed, column))
    data_rows.append([cells[column] for column in _COLUMNS])
    widths = [
        max(len(label), max(len(row[index]) for row in data_rows))
        for index, label in enumerate(_COLUMNS)
    ]
    header = "  ".join(label.ljust(width) for label, width in zip(_COLUMNS, widths, strict=True))
    separator = "  ".join("-" * width for width in widths)
    body = [
        "  ".join(cell.rjust(width) for cell, width in zip(row, widths, strict=True))
        for row in data_rows
    ]
    return "\n".join([header, separator, *body]) + "\n"


def write_reports(
    report: QAReport, reports_dir: str | Path, date: str | Date
) -> tuple[Path, Path]:
    """Write ``qa-<date>.json`` and ``qa-<date>.md`` into ``reports_dir``."""
    iso_date = _iso_date(date)
    directory = Path(reports_dir)
    directory.mkdir(parents=True, exist_ok=True)
    json_path = directory / f"qa-{iso_date}.json"
    markdown_path = directory / f"qa-{iso_date}.md"
    json_path.write_text(to_json(report), encoding="utf-8")
    markdown_path.write_text(to_markdown(report), encoding="utf-8")
    return json_path, markdown_path
