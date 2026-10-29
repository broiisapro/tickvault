"""Typer command-line interface.

Every command is a thin layer over the pipeline or an Epic 1 function, so the
Dagster assets and the CLI share the same code paths. ``ingest`` configures
stderr logging once (``--log-level``, default INFO) and reports progress every
``--progress-interval`` seconds, so live runs are observable in
``docker compose logs``.

Exit codes: ``0`` on success, ``1`` on an unexpected error, ``2`` on invalid
arguments, ``3`` when ``ingest --max-gb`` stopped because the data root grew
past the limit.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
from collections.abc import Sequence
from datetime import date as Date
from pathlib import Path
from typing import Annotated, NoReturn

import duckdb
import typer

from tickvault.bench import run_benchmark
from tickvault.buffer import open_buffer
from tickvault.features import RESOLUTIONS, compute_features, write_features
from tickvault.metrics import MetricsServer, PipelineMetrics
from tickvault.pipeline import PipelineStats, run_live, run_replay, run_writer
from tickvault.qa.report import exit_code as qa_exit_code
from tickvault.qa.report import run_qa, to_json, to_text_table, write_reports
from tickvault.reconcile.download import reconcile_live_day
from tickvault.reconcile.tardis import (
    ReconcileReport,
    load_tardis_trades,
    load_tickvault_trades,
    report_to_json,
    report_to_text,
)
from tickvault.reconcile.tardis import reconcile as reconcile_trades
from tickvault.sql import run_query, view_ddl
from tickvault.storage.compact import CompactionError
from tickvault.storage.compact import compact as compact_partitions
from tickvault.storage.manifest import write_manifest
from tickvault.venues import venue_names
from tickvault.verify import VerifyReport, verify_remote

app = typer.Typer(
    help="TickVault: live crypto market-data pipeline.",
    no_args_is_help=True,
)

_SUPPORTED_VENUES = venue_names()
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")


def _usage_error(message: str) -> NoReturn:
    print(f"error: {message}", file=sys.stderr)
    raise typer.Exit(code=2)


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _configure_logging(level: str) -> None:
    """Configure stderr logging once, from the CLI only.

    Library modules log through their own loggers and never configure
    handlers; this is the single place that does, so ``ingest`` is observable
    in container logs. The level is applied to the root logger even when
    handlers already exist (for example under pytest), which keeps
    ``--log-level`` effective inside the test harness.
    """
    resolved = logging.getLevelNamesMapping().get(level.upper())
    if resolved is None:
        _usage_error(f"unknown log level: {level!r}")
    logging.basicConfig(
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger().setLevel(resolved)


def _validate_venues(venues: str) -> list[str]:
    parsed = _split_csv(venues)
    if not parsed:
        _usage_error("--venues must list at least one venue")
    unknown = [venue for venue in parsed if venue not in _SUPPORTED_VENUES]
    if unknown:
        supported = ", ".join(_SUPPORTED_VENUES)
        _usage_error(f"unsupported venue(s): {', '.join(unknown)} (expected one of: {supported})")
    return parsed


def _validate_symbols(symbols: str) -> list[str]:
    parsed = _split_csv(symbols)
    if not parsed:
        _usage_error("--symbols must list at least one symbol")
    invalid = [symbol for symbol in parsed if _SYMBOL_PATTERN.fullmatch(symbol) is None]
    if invalid:
        _usage_error(
            f"invalid symbol(s): {', '.join(invalid)} (expected BASE-QUOTE, e.g. BTC-USD)"
        )
    return parsed


def _print_stats(stats: PipelineStats) -> None:
    """Print the one-screen pipeline summary shared by ingest and replay."""
    print(f"{stats.mode} stopped: {stats.stopped_by}")
    print(f"raw messages: {stats.raw_lines}")
    print(f"normalized messages: {stats.messages} (normalize errors: {stats.normalize_errors})")
    print(
        f"rows: trades={stats.trades} book_deltas={stats.book_deltas} "
        f"book_snapshots={stats.book_snapshots} events={stats.events}"
    )
    print(
        "events by type: "
        f"gap={stats.gaps} resync={stats.resyncs} "
        f"checksum_failure={stats.checksum_failures} "
        f"invariant_violation={stats.invariant_violations} "
        f"reconnect={stats.reconnects} disconnect={stats.disconnects}"
    )
    print(f"resync requests: {stats.resync_requests}")


def _format_table(columns: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """Render query results as an aligned text table (header, rule, rows)."""
    if not columns:
        return "(no columns)\n"
    cells = [["NULL" if value is None else str(value) for value in row] for row in rows]
    widths = [len(column) for column in columns]
    for row in cells:
        widths = [max(width, len(cell)) for width, cell in zip(widths, row, strict=True)]
    lines = ["  ".join(column.ljust(width) for column, width in zip(columns, widths, strict=True))]
    lines.append("  ".join("-" * width for width in widths))
    lines.extend(
        "  ".join(cell.rjust(width) for cell, width in zip(row, widths, strict=True))
        for row in cells
    )
    return "\n".join(lines) + "\n"


def _print_reconcile_report(report: ReconcileReport, *, as_json: bool) -> None:
    text = report_to_json(report) if as_json else report_to_text(report)
    print(text, end="")


def _buffer_address(value: str | None) -> str | None:
    """Resolve a buffer option: ``None`` and the empty string both mean off."""
    if value is None:
        return None
    address = value.strip()
    return address or None


def _start_metrics(port: int | None) -> tuple[PipelineMetrics | None, MetricsServer | None]:
    """Build the metrics registry and HTTP endpoint for ``--metrics-port``.

    ``None`` and ``0`` both mean "off". The port is validated here so the CLI
    exit code stays 2 for bad arguments instead of 1 for a failed bind.
    """
    if port is not None and not 0 <= port <= 65535:
        _usage_error("--metrics-port must be between 0 and 65535")
    if not port:
        return None, None
    metrics = PipelineMetrics()
    return metrics, metrics.serve(port)


def _print_verify_report(report: VerifyReport) -> None:
    print(f"verify {report.manifest}: {report.ok}/{report.files} files ok")
    for problem in report.problems:
        print(problem)


@app.command()
def ingest(
    venues: Annotated[
        str, typer.Option("--venues", help="Comma-separated venues, e.g. coinbase,kraken,bitfinex.")
    ],
    symbols: Annotated[
        str, typer.Option("--symbols", help="Comma-separated BASE-QUOTE symbols.")
    ],
    out: Annotated[Path, typer.Option("--out", help="Data root for raw and normalized output.")],
    duration: Annotated[
        float | None, typer.Option("--duration", help="Stop after this many seconds.")
    ] = None,
    max_gb: Annotated[
        float | None,
        typer.Option("--max-gb", help="Stop ingesting when the data root grows past this size."),
    ] = None,
    progress_interval: Annotated[
        float,
        typer.Option(
            "--progress-interval",
            help="Seconds between ingest progress log lines; 0 disables them.",
        ),
    ] = 10.0,
    log_level: Annotated[
        str,
        typer.Option("--log-level", help="Stderr logging level, e.g. INFO or DEBUG."),
    ] = "INFO",
    buffer: Annotated[
        str | None,
        typer.Option(
            "--buffer",
            envvar="TICKVAULT_BUFFER",
            help="Publish raw lines to a buffer instead of writing them: "
            "HOST:PORT or memory://NAME; empty = off.",
        ),
    ] = None,
    metrics_port: Annotated[
        int | None,
        typer.Option(
            "--metrics-port",
            envvar="TICKVAULT_METRICS_PORT",
            help="Serve Prometheus metrics on this port; 0 or unset = off.",
        ),
    ] = None,
) -> None:
    """Stream live venue data into the raw and normalized layers."""
    _configure_logging(log_level)
    venue_list = _validate_venues(venues)
    symbol_list = _validate_symbols(symbols)
    address = _buffer_address(buffer)
    metrics, server = _start_metrics(metrics_port)
    try:
        stats = asyncio.run(
            run_live(
                venue_list,
                symbol_list,
                out,
                duration=duration,
                max_gb=max_gb,
                progress_interval=progress_interval,
                buffer=None if address is None else open_buffer(address),
                metrics=metrics,
            )
        )
    except KeyboardInterrupt:
        print("error: interrupted before the run could start", file=sys.stderr)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None
    finally:
        if server is not None:
            server.close()
    _print_stats(stats)
    if stats.stopped_by == "max_gb":
        raise typer.Exit(code=3)


@app.command()
def replay(
    input: Annotated[Path, typer.Option("--input", help="Raw file or raw tree to replay.")],
    out: Annotated[Path, typer.Option("--out", help="Data root for normalized output.")],
) -> None:
    """Replay raw files deterministically through the live pipeline."""
    try:
        stats = run_replay(input, out)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None
    _print_stats(stats)


@app.command()
def qa(
    data: Annotated[Path, typer.Option("--data", help="Data root holding the normalized layer.")],
    date: Annotated[
        str | None,
        typer.Option("--date", help="UTC date to report on (YYYY-MM-DD); all days when omitted."),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print the JSON report instead of the text table.")
    ] = False,
    reports: Annotated[
        bool,
        typer.Option("--reports", help="Also write <data>/reports/qa-<date>.json and .md."),
    ] = False,
) -> None:
    """Print the per-venue/symbol data-quality report."""
    try:
        report = run_qa(data, date)
    except ValueError as exc:
        _usage_error(str(exc))
    if json_output:
        print(to_json(report), end="")
    else:
        print(to_text_table(report), end="")
    if reports:
        days = (
            [report.date]
            if report.date is not None
            else sorted({row.date for row in report.rows})
        )
        for day in days:
            json_path, markdown_path = write_reports(run_qa(data, day), data / "reports", day)
            print(f"wrote {json_path} and {markdown_path}")
    raise typer.Exit(code=qa_exit_code(report))


@app.command()
def compact(
    data: Annotated[Path, typer.Option("--data", help="Data root holding the normalized layer.")],
    date: Annotated[str, typer.Option("--date", help="UTC date to compact (YYYY-MM-DD).")],
) -> None:
    """Merge small hourly Parquet files into one per table/venue/hour."""
    try:
        result = compact_partitions(data, date)
    except CompactionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None
    except ValueError as exc:
        _usage_error(str(exc))
    manifest_path = write_manifest(data, date)
    print(
        f"compacted {result.files_written} partition(s): "
        f"{result.files_removed} files merged into {result.files_written}, {result.rows} rows"
    )
    for partition in result.partitions:
        print(f"  {partition.output}: {partition.inputs} files -> 1, {partition.rows} rows")
    print(f"manifest: {manifest_path}")


@app.command()
def sql(
    data: Annotated[Path, typer.Option("--data", help="Data root holding the normalized layer.")],
    command: Annotated[
        str | None,
        typer.Option("-c", "--command", help="SQL to run; prints the view DDL when omitted."),
    ] = None,
) -> None:
    """Open DuckDB views over the Parquet tree, or run one query."""
    if command is None:
        print(view_ddl(data), end="")
        return
    try:
        columns, rows = run_query(data, command)
    except duckdb.Error as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None
    print(_format_table(columns, rows), end="")


@app.command()
def reconcile(
    tardis_date: Annotated[
        str | None,
        typer.Option("--tardis-date", help="First-of-month Tardis date to download (YYYY-MM-DD)."),
    ] = None,
    venue: Annotated[
        str | None, typer.Option("--venue", help="Venue to reconcile, e.g. coinbase.")
    ] = None,
    symbol: Annotated[
        str | None, typer.Option("--symbol", help="Symbol to reconcile, e.g. BTC-USD.")
    ] = None,
    fixture: Annotated[
        Path | None,
        typer.Option("--fixture", help="Local Tardis-format CSV.gz for offline reconciliation."),
    ] = None,
    data: Annotated[
        Path | None,
        typer.Option("--data", help="TickVault data root or trades sample to reconcile."),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print the JSON report instead of the text summary.")
    ] = False,
) -> None:
    """Compare captured trades against Tardis.dev reference data."""
    resolved_venue = venue if venue is not None else "coinbase"
    resolved_symbol = symbol if symbol is not None else "BTC-USD"
    if fixture is not None and tardis_date is not None:
        _usage_error("--fixture and --tardis-date are mutually exclusive")
    if fixture is not None:
        if data is None:
            _usage_error("--data is required with --fixture")
        try:
            report = reconcile_trades(
                load_tardis_trades(fixture),
                load_tickvault_trades(data, resolved_venue, resolved_symbol),
            )
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            raise typer.Exit(code=1) from None
    elif tardis_date is not None:
        if data is None:
            _usage_error("--data is required with --tardis-date")
        try:
            day = Date.fromisoformat(tardis_date)
        except ValueError as exc:
            _usage_error(str(exc))
        if day.day != 1:
            _usage_error(
                "Tardis free datasets exist only for the first day of each month: "
                f"{tardis_date} is not the 1st"
            )
        try:
            report = reconcile_live_day(
                data,
                day,
                data / "cache",
                venue=resolved_venue,
                symbol=resolved_symbol,
            )
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            raise typer.Exit(code=1) from None
    else:
        _usage_error(
            "provide --fixture <csv.gz> --data <root> or "
            "--tardis-date <YYYY-MM-01> --data <root>"
        )
    _print_reconcile_report(report, as_json=json_output)


@app.command()
def bench(
    input: Annotated[
        Path | None, typer.Option("--input", help="Raw file or tree to benchmark.")
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print the benchmark as JSON.")
    ] = False,
) -> None:
    """Measure replay throughput for the benchmark reported in the README."""
    try:
        report = run_benchmark(None if input is None else [input])
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None
    if json_output:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(report.to_text(), end="")


@app.command()
def consume(
    buffer: Annotated[
        str,
        typer.Option("--buffer", help="Buffer address to read: HOST:PORT or memory://NAME."),
    ],
    out: Annotated[Path, typer.Option("--out", help="Data root for raw and normalized output.")],
    metrics_port: Annotated[
        int | None,
        typer.Option(
            "--metrics-port",
            help="Serve Prometheus metrics on this port; 0 or unset = off.",
        ),
    ] = None,
    duration: Annotated[
        float | None, typer.Option("--duration", help="Stop after this many seconds.")
    ] = None,
    log_level: Annotated[
        str,
        typer.Option("--log-level", help="Stderr logging level, e.g. INFO or DEBUG."),
    ] = "INFO",
) -> None:
    """Consume a raw buffer into the raw and normalized layers."""
    _configure_logging(log_level)
    address = _buffer_address(buffer)
    if address is None:
        _usage_error("--buffer must not be empty")
    metrics, server = _start_metrics(metrics_port)
    try:
        stats = asyncio.run(
            run_writer(
                open_buffer(address),
                out,
                duration=duration,
                metrics=metrics,
            )
        )
    except KeyboardInterrupt:
        print("error: interrupted before the run could start", file=sys.stderr)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None
    finally:
        if server is not None:
            server.close()
    _print_stats(stats)


@app.command()
def features(
    data: Annotated[Path, typer.Option("--data", help="Data root holding the normalized layer.")],
    date: Annotated[str, typer.Option("--date", help="UTC date to compute (YYYY-MM-DD).")],
    out: Annotated[Path, typer.Option("--out", help="Data root to write the features tree under.")],
    resolution: Annotated[
        str,
        typer.Option("--resolution", help="Comma-separated resolutions, e.g. 1s,1m."),
    ] = "1s,1m",
) -> None:
    """Compute the derived features table for one UTC date."""
    try:
        parsed_day = Date.fromisoformat(date)
    except ValueError:
        _usage_error(f"invalid date: {date!r} (expected YYYY-MM-DD)")
    if parsed_day.isoformat() != date:
        _usage_error(f"invalid date: {date!r} (expected YYYY-MM-DD)")
    resolutions = _split_csv(resolution)
    if not resolutions:
        _usage_error("--resolution must list at least one resolution")
    unknown = [item for item in resolutions if item not in RESOLUTIONS]
    if unknown:
        _usage_error(
            f"unsupported resolution(s): {', '.join(unknown)} "
            f"(expected one of: {', '.join(RESOLUTIONS)})"
        )
    try:
        tables = compute_features(data, date, resolutions)
        paths = write_features(tables, out, date)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None
    for resolution_name in resolutions:
        table = tables.get(resolution_name)
        rows = 0 if table is None else table.num_rows
        files = sum(1 for path in paths if f"resolution={resolution_name}" in path.parts)
        print(f"features {resolution_name}: {rows} rows, {files} files")


@app.command()
def verify(
    manifest: Annotated[
        str, typer.Option("--manifest", help="Manifest path or http(s) URL to verify.")
    ],
) -> None:
    """Verify a published manifest against the files it describes."""
    try:
        report = verify_remote(manifest)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None
    _print_verify_report(report)
    if report.problems:
        raise typer.Exit(code=1)
