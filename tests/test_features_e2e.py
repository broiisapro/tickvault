"""End-to-end features check through Dagster, SQL and the CLI (issue #142).

The data root is built the way live ingestion builds one: two committed
five-minute recordings are replayed through ``tickvault.pipeline.run_replay``
into a temp root that also holds the raw files as a ``raw/`` tree. Then:

- the Dagster daily job is materialized in-process for that day, so the
  ``features`` asset writes the derived tree next to ``norm/``;
- ``tickvault sql`` reads the resulting ``features`` view and must return the
  row counts the job reported;
- ``tickvault features`` over the same root must produce byte-identical files;
- the input ``norm/`` and ``raw/`` trees must hash the same before and after.

Dagster gets a throw-away ``DAGSTER_HOME`` under the test's temp directory, so
nothing is written into the worktree.
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dagster as dg
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from tickvault.cli import app
from tickvault.features import features_path
from tickvault.pipeline import run_replay
from tickvault.storage.compact import compact

_PARTITION = "2026-10-15"
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_VENUES = ("coinbase", "kraken")
_SYMBOL = "BTC-USD"


@dataclass(frozen=True, slots=True)
class _Replayed:
    """A replayed data root, its pre-job hashes and the daily job result."""

    root: Path
    before: dict[str, str]
    after: dict[str, str]
    metadata: dict[str, dict[str, object]]


def _plain(text: str) -> str:
    return _ANSI_RE.sub("", text)


def _definitions() -> Any:
    # pytest does not put the repo root on sys.path and ``orchestration`` is a
    # top-level package next to it, not an installed one.
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))
    from orchestration import definitions

    return definitions


def _tree_hashes(root: Path) -> dict[str, str]:
    """SHA-256 of every file under ``root``, keyed by its relative path."""
    hashes: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            hashes[path.relative_to(root).as_posix()] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    return hashes


def _input_hashes(root: Path) -> dict[str, str]:
    """Hashes of the two input layers of a data root, raw before norm."""
    hashes: dict[str, str] = {}
    for layer in ("raw", "norm"):
        hashes.update(
            {
                f"{layer}/{path}": digest
                for path, digest in _tree_hashes(root / layer).items()
            }
        )
    return hashes


def _materialization_metadata(
    result: dg.ExecuteInProcessResult,
) -> dict[str, dict[str, object]]:
    return {
        event.asset_key.to_user_string(): {
            key: getattr(value, "value", value)
            for key, value in (event.materialization.metadata or {}).items()
        }
        for event in result.get_asset_materialization_events()
    }


@pytest.fixture(scope="module")
def replayed(
    tmp_path_factory: pytest.TempPathFactory,
    coinbase_5m_path: Path,
    kraken_5m_path: Path,
) -> _Replayed:
    """Replay both five-minute recordings, then materialize the daily job."""
    definitions = _definitions()
    root = tmp_path_factory.mktemp("features-e2e")
    raw = root / "raw"
    raw.mkdir()
    for source in (coinbase_5m_path, kraken_5m_path):
        (raw / source.name).write_bytes(source.read_bytes())
    run_replay(raw, root)
    # Compact up front, the way a production root is compacted hourly. The
    # daily job then finds nothing to merge, so the hash comparison below
    # speaks about the feature path and not about compaction, which is
    # documented to rewrite normalized files.
    compact(root, _PARTITION)

    before = _input_hashes(root)

    dagster_home = tmp_path_factory.mktemp("dagster-home")
    previous = os.environ.get("DAGSTER_HOME")
    os.environ["DAGSTER_HOME"] = str(dagster_home)
    try:
        result = definitions.defs.resolve_job_def("daily_job").execute_in_process(
            partition_key=_PARTITION,
            resources={
                "data_root": definitions.DataRootResource(data_root=str(root)),
            },
        )
    finally:
        if previous is None:
            os.environ.pop("DAGSTER_HOME", None)
        else:
            os.environ["DAGSTER_HOME"] = previous
    assert result.success

    return _Replayed(
        root=root,
        before=before,
        after=_input_hashes(root),
        metadata=_materialization_metadata(result),
    )


def _features_tree_hashes(root: Path) -> dict[str, str]:
    """Hashes of the derived tree relative to ``root/features``."""
    return _tree_hashes(root / "features")


def test_daily_job_writes_both_resolutions_per_venue(replayed: _Replayed) -> None:
    expected = {
        features_path(replayed.root, resolution, venue, _PARTITION, _SYMBOL)
        for resolution in ("1s", "1m")
        for venue in _VENUES
    }
    written = {path for path in (replayed.root / "features").rglob("*.parquet")}

    assert written == expected
    for path in expected:
        assert path.stat().st_size > 0

    features_metadata = replayed.metadata["features"]
    assert features_metadata["files"] == len(expected)
    assert int(str(features_metadata["rows_1s"])) > 0
    assert int(str(features_metadata["rows_1m"])) > 0
    # The root was compacted before the job, so the job merged nothing and its
    # normalized input is untouched.
    assert replayed.metadata["compacted_partitions"]["rows"] == 0


def test_sql_returns_the_row_counts_the_job_reported(replayed: _Replayed) -> None:
    result = CliRunner().invoke(
        app,
        [
            "sql",
            "--data",
            str(replayed.root),
            "-c",
            "select resolution, venue, count(*) as bars from features "
            "group by all order by all",
        ],
    )

    assert result.exit_code == 0, result.output
    lines = _plain(result.output).strip().splitlines()
    assert lines[0].split() == ["resolution", "venue", "bars"]
    counts = {(row[0], row[1]): int(row[2]) for row in (line.split() for line in lines[2:])}

    # The files the job wrote are the only source of the counts, so reading
    # their Parquet footers gives the expected answer without trusting the
    # feature computation again.
    expected: dict[tuple[str, str], int] = {}
    for path in (replayed.root / "features").rglob("*.parquet"):
        resolution = path.parents[2].name.removeprefix("resolution=")
        venue = path.parents[1].name.removeprefix("venue=")
        key = (resolution, venue)
        expected[key] = expected.get(key, 0) + pq.read_metadata(path).num_rows
    assert counts == expected

    features_metadata = replayed.metadata["features"]
    assert sum(value for (resolution, _), value in expected.items() if resolution == "1s") == int(
        str(features_metadata["rows_1s"])
    )
    assert sum(value for (resolution, _), value in expected.items() if resolution == "1m") == int(
        str(features_metadata["rows_1m"])
    )
    # The view is backed by the files the job wrote; the day is the only date
    # partition in the tree.
    for path in (replayed.root / "features").rglob("*.parquet"):
        assert f"date={_PARTITION}" in path.parts


def test_cli_features_reproduces_the_dagster_bytes(
    replayed: _Replayed, tmp_path: Path
) -> None:
    out = tmp_path / "cli-out"
    before = _input_hashes(replayed.root)
    result = CliRunner().invoke(
        app,
        [
            "features",
            "--data",
            str(replayed.root),
            "--date",
            _PARTITION,
            "--out",
            str(out),
        ],
    )

    assert result.exit_code == 0, result.output
    output = _plain(result.output)
    rows_1s = int(str(replayed.metadata["features"]["rows_1s"]))
    rows_1m = int(str(replayed.metadata["features"]["rows_1m"]))
    assert f"features 1s: {rows_1s} rows, {len(_VENUES)} files" in output
    assert f"features 1m: {rows_1m} rows, {len(_VENUES)} files" in output
    assert _input_hashes(replayed.root) == before

    dagster_tree = _features_tree_hashes(replayed.root)
    cli_tree = _features_tree_hashes(out)
    assert set(cli_tree) == set(dagster_tree)
    for name, digest in dagster_tree.items():
        assert cli_tree[name] == digest, name


def test_input_trees_are_unchanged(replayed: _Replayed) -> None:
    # The Job's own effect: hashed right after materialization, when the
    # features tree exists and the CLI has not run yet.
    assert replayed.after == replayed.before
    assert any(name.startswith("raw/") for name in replayed.before)
    assert any(name.startswith("norm/") for name in replayed.before)
