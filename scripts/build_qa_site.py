"""Build the static QA site published on GitHub Pages.

Usage::

    uv run python scripts/build_qa_site.py --data dashboard/sample --out site

For every UTC date with normalized Parquet partitions under ``<data>/norm``,
the script computes the same per-day QA report as ``tickvault qa --date`` and
writes ``qa-<date>.json``, ``qa-<date>.md`` and ``qa-<date>.html``.
``index.html`` shows the latest report and links every date; ``latest.json``
is the compact machine-readable summary that Phase 3's always-on site keeps
(``date``, ``source``, ``rows``, ``unexplained``, ``book_missing``,
``exit_code``). An empty ``.nojekyll`` marks the directory as plain static
files.

The output is deterministic: no wall-clock timestamps are embedded, dates are
sorted, and two builds of the same tree are byte-identical. Only the standard
library and the ``tickvault`` package are used.
"""

from __future__ import annotations

import argparse
import html
import json
from collections.abc import Sequence
from dataclasses import fields
from datetime import date as Date
from pathlib import Path

from tickvault.qa.metrics import QARow
from tickvault.qa.report import QAReport, exit_code, run_qa, to_json, to_markdown, totals

REPOSITORY_URL = "https://github.com/broiisapro/tickvault"
"""Where the published site links back to."""

_EXPLANATION = (
    "Gaps are sequence numbers missing from a venue feed; resyncs are "
    "recovery snapshots requested after an anomaly; checksum failures are "
    "Kraken book updates whose CRC32 checksum did not match; invariant "
    "violations are books that broke cross-side rules; and unexplained counts "
    "anomalies with no later resync for the same venue and symbol. A row is "
    "highlighted when it is unexplained or has messages but no venue snapshot."
)

_COLUMNS = tuple(field.name for field in fields(QARow))

_TOTAL_COLUMNS = (
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

_STYLE = """\
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body { margin: 2rem auto; max-width: 76rem; padding: 0 1rem;
       font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
h1 { font-size: 1.4rem; margin-bottom: .25rem; }
h2 { font-size: 1.1rem; margin-top: 2rem; }
a { color: inherit; }
code { font-size: .9em; }
.muted { color: #767676; }
.table-wrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; margin: 1rem 0;
        font-variant-numeric: tabular-nums; }
th, td { border: 1px solid #8886; padding: .25rem .5rem; text-align: right;
         white-space: nowrap; }
th:nth-child(-n+3), td:nth-child(-n+3) { text-align: left; }
tr.bad td { background: #d3332a22; }
tr.totals td { font-weight: 600; }
"""


def _partition_dates(root: Path) -> list[str]:
    """Sorted ``YYYY-MM-DD`` partition dates present under ``root/norm``."""
    norm = root / "norm"
    if not norm.is_dir():
        return []
    dates: set[str] = set()
    for table_dir in norm.iterdir():
        if not table_dir.is_dir():
            continue
        for venue_dir in table_dir.glob("venue=*"):
            if not venue_dir.is_dir():
                continue
            for date_dir in venue_dir.glob("date=*"):
                day = date_dir.name.removeprefix("date=")
                try:
                    dates.add(Date.fromisoformat(day).isoformat())
                except ValueError:
                    continue
    return sorted(dates)


def _cell(value: object) -> str:
    return html.escape(str(value))


def _table_html(report: QAReport) -> str:
    """The report as one HTML table with a totals row."""
    head = "".join(f"<th>{_cell(column)}</th>" for column in _COLUMNS)
    lines = ['<div class="table-wrap">', "<table>", f"<thead><tr>{head}</tr></thead>", "<tbody>"]
    for row in report.rows:
        classes = ' class="bad"' if row.unexplained > 0 or row.book_missing else ""
        cells = "".join(f"<td>{_cell(getattr(row, column))}</td>" for column in _COLUMNS)
        lines.append(f"<tr{classes}>{cells}</tr>")
    summed = totals(report)
    total_cells = []
    for column in _COLUMNS:
        if column == "venue":
            value: object = "TOTAL"
        elif column in _TOTAL_COLUMNS:
            value = getattr(summed, column)
        else:
            value = "-"
        total_cells.append(f"<td>{_cell(value)}</td>")
    lines.append(f'<tr class="totals">{"".join(total_cells)}</tr>')
    lines.extend(["</tbody>", "</table>", "</div>"])
    return "\n".join(lines)


def _page(title: str, body: str) -> str:
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{_cell(title)}</title>\n"
        f"<style>\n{_STYLE}</style>\n"
        "</head>\n"
        "<body>\n"
        f"{body}\n"
        "</body>\n"
        "</html>\n"
    )


def _source_line(source: str) -> str:
    return (
        f'<p class="muted">Source: <code>{_cell(source)}</code> &middot; '
        f'<a href="{REPOSITORY_URL}">Repository</a></p>'
    )


def _date_body(report: QAReport, day: str, source: str) -> str:
    return "\n".join(
        [
            f"<h1>TickVault data quality &mdash; {_cell(day)}</h1>",
            _source_line(source),
            '<p><a href="index.html">All reports</a></p>',
            _table_html(report),
        ]
    )


def _index_body(reports: dict[str, QAReport], source: str) -> str:
    lines = [
        "<h1>TickVault data quality</h1>",
        _source_line(source),
        f"<p>{_EXPLANATION}</p>",
    ]
    if reports:
        latest = max(reports)
        lines.append(f"<h2>Latest report &mdash; {_cell(latest)}</h2>")
        lines.append(
            f'<p><a href="qa-{_cell(latest)}.html">Open the {_cell(latest)} page</a> '
            f"&middot; exit code {exit_code(reports[latest])}</p>"
        )
        lines.append(_table_html(reports[latest]))
        lines.append("<h2>History</h2>")
        lines.append("<ul>")
        for day in sorted(reports, reverse=True):
            lines.append(f'<li><a href="qa-{_cell(day)}.html">{_cell(day)}</a></li>')
        lines.append("</ul>")
    else:
        lines.append("<p>No QA reports yet.</p>")
    return "\n".join(lines)


def _latest_payload(report: QAReport | None, source: str) -> dict[str, object]:
    if report is None:
        return {
            "date": None,
            "source": source,
            "rows": 0,
            "unexplained": 0,
            "book_missing": 0,
            "exit_code": 0,
        }
    summed = totals(report)
    return {
        "date": report.date,
        "source": source,
        "rows": len(report.rows),
        "unexplained": summed.unexplained,
        "book_missing": summed.book_missing,
        "exit_code": exit_code(report),
    }


def build_site(data: str | Path, out: str | Path) -> list[Path]:
    """Write the QA site for ``data`` into ``out``; return the written files."""
    root = Path(data)
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    source = str(root)
    reports = {day: run_qa(root, day) for day in _partition_dates(root)}
    written: list[Path] = []
    for day, report in reports.items():
        paths = {
            out_dir / f"qa-{day}.json": to_json(report),
            out_dir / f"qa-{day}.md": to_markdown(report),
            out_dir / f"qa-{day}.html": _page(
                f"TickVault QA {day}", _date_body(report, day, source)
            ),
        }
        for path, text in paths.items():
            path.write_text(text, encoding="utf-8")
            written.append(path)
    latest = reports[max(reports)] if reports else None
    latest_json = out_dir / "latest.json"
    latest_json.write_text(
        json.dumps(_latest_payload(latest, source), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    written.append(latest_json)
    index = out_dir / "index.html"
    index.write_text(_page("TickVault data quality", _index_body(reports, source)), "utf-8")
    written.append(index)
    nojekyll = out_dir / ".nojekyll"
    nojekyll.write_text("", encoding="utf-8")
    written.append(nojekyll)
    return written


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("dashboard/sample"),
        help="Data root with a norm/ tree (default: dashboard/sample).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("site"),
        help="Directory to write the static site into (default: site).",
    )
    args = parser.parse_args(argv)
    written = build_site(args.data, args.out)
    date_pages = [
        path for path in written if path.name.startswith("qa-") and path.suffix == ".html"
    ]
    print(f"wrote {len(written)} files ({len(date_pages)} date page(s)) into {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
