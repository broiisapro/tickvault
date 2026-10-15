"""Typer command-line interface.

The command signatures and options are the contract for Epic 2; the bodies are
stubs that print ``not implemented yet`` to stderr and exit with code 2.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(
    help="TickVault: live crypto market-data pipeline.",
    no_args_is_help=True,
)


def _not_implemented() -> None:
    print("not implemented yet", file=sys.stderr)
    raise typer.Exit(code=2)


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
    _not_implemented()


@app.command()
def replay(
    input: Annotated[Path, typer.Option("--input", help="Raw file or raw tree to replay.")],
    out: Annotated[Path, typer.Option("--out", help="Data root for normalized output.")],
) -> None:
    """Replay raw files deterministically through the live pipeline."""
    _not_implemented()


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
