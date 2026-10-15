"""Typer command-line interface.

The command signatures and options are the contract for Epic 2. ``ingest`` and
``replay`` call the pipeline entry points; the remaining bodies are stubs that
print ``not implemented yet`` to stderr and exit with code 2.

Exit codes: ``0`` on success, ``1`` on an unexpected error, ``2`` on invalid
arguments, ``3`` when ``ingest --max-gb`` stopped because the data root grew
past the limit.
"""

from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path
from typing import Annotated, NoReturn

import typer

from tickvault.pipeline import PipelineStats, run_live, run_replay

app = typer.Typer(
    help="TickVault: live crypto market-data pipeline.",
    no_args_is_help=True,
)

_SUPPORTED_VENUES = ("coinbase", "kraken")
_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")


def _not_implemented() -> None:
    print("not implemented yet", file=sys.stderr)
    raise typer.Exit(code=2)


def _usage_error(message: str) -> NoReturn:
    print(f"error: {message}", file=sys.stderr)
    raise typer.Exit(code=2)


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


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


@app.command()
def ingest(
    venues: Annotated[
        str, typer.Option("--venues", help="Comma-separated venues, e.g. coinbase,kraken.")
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
) -> None:
    """Stream live venue data into the raw and normalized layers."""
    venue_list = _validate_venues(venues)
    symbol_list = _validate_symbols(symbols)
    try:
        stats = asyncio.run(
            run_live(venue_list, symbol_list, out, duration=duration, max_gb=max_gb)
        )
    except KeyboardInterrupt:
        print("error: interrupted before the run could start", file=sys.stderr)
        raise typer.Exit(code=1) from None
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(code=1) from None
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
) -> None:
    """Print the per-venue/symbol data-quality report."""
    _not_implemented()


@app.command()
def compact(
    data: Annotated[Path, typer.Option("--data", help="Data root holding the normalized layer.")],
    date: Annotated[str, typer.Option("--date", help="UTC date to compact (YYYY-MM-DD).")],
) -> None:
    """Merge small hourly Parquet files into one per table/venue/hour."""
    _not_implemented()


@app.command()
def sql(
    data: Annotated[Path, typer.Option("--data", help="Data root holding the normalized layer.")],
    command: Annotated[
        str | None,
        typer.Option("-c", "--command", help="SQL to run; prints the view DDL when omitted."),
    ] = None,
) -> None:
    """Open DuckDB views over the Parquet tree, or run one query."""
    _not_implemented()


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
        typer.Option("--data", help="TickVault trades sample for offline reconciliation."),
    ] = None,
) -> None:
    """Compare captured trades against Tardis.dev reference data."""
    _not_implemented()


@app.command()
def bench(
    input: Annotated[
        Path | None, typer.Option("--input", help="Raw file or tree to benchmark.")
    ] = None,
) -> None:
    """Measure replay throughput for the benchmark reported in the README."""
    _not_implemented()
