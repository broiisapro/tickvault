"""Tests for the Dagster daily job (issue #58).

The data root is built by replaying the committed synthetic session through
``tickvault.pipeline.run_replay`` — the same normalizer -> BookProcessor ->
ParquetWriter path live ingestion uses — then the three assets are
materialized in-process (no daemon, no webserver) for the session's
partition. The test asserts the Epic 1 report numbers the acceptance criteria
name, the report files, the materialization metadata and that the written
manifest verifies.
"""

import json
import sys
import time
from pathlib import Path

import dagster as dg

from tickvault.pipeline import run_replay
from tickvault.storage.manifest import verify_manifest

_PARTITION = "2026-11-01"
_REPO_ROOT = Path(__file__).resolve().parent.parent


def _materialization_metadata(result: dg.ExecuteInProcessResult) -> dict[str, dict[str, object]]:
    return {
        event.asset_key.to_user_string(): {
            key: getattr(value, "value", value)
            for key, value in (event.materialization.metadata or {}).items()
        }
        for event in result.get_asset_materialization_events()
    }


def test_daily_job_materializes_reports_and_manifest(
    tmp_path: Path,
    synthetic_gap_path: Path,
) -> None:
    # pytest does not put the repo root on sys.path and ``orchestration`` is a
    # top-level package next to it, not an installed one, so import it here.
    sys.path.insert(0, str(_REPO_ROOT))
    from orchestration.definitions import (
        DataRootResource,
        compacted_partitions,
        manifest,
        qa_report,
    )

    root = tmp_path / "data"
    started = time.monotonic()
    run_replay(synthetic_gap_path, root)
    result = dg.materialize(
        [compacted_partitions, qa_report, manifest],
        partition_key=_PARTITION,
        resources={"data_root": DataRootResource(data_root=str(root))},
    )
    elapsed = time.monotonic() - started

    assert result.success
    assert elapsed < 30

    report_path = root / "reports" / f"qa-{_PARTITION}.json"
    markdown_path = root / "reports" / f"qa-{_PARTITION}.md"
    assert report_path.is_file()
    assert markdown_path.is_file()

    document = json.loads(report_path.read_text(encoding="utf-8"))
    rows = {(row["venue"], row["symbol"]): row for row in document["rows"]}
    assert set(rows) == {("coinbase", "BTC-USD"), ("kraken", "BTC-USD")}

    coinbase = rows[("coinbase", "BTC-USD")]
    assert coinbase["gaps"] == 1
    assert coinbase["resyncs"] == 1
    assert coinbase["unexplained"] == 0
    kraken = rows[("kraken", "BTC-USD")]
    assert kraken["checksum_failures"] == 1
    assert kraken["resyncs"] == 1
    assert kraken["unexplained"] == 0
    assert sum(row["unexplained"] for row in document["rows"]) == 0

    manifest_path = root / "manifests" / f"date={_PARTITION}" / "manifest.json"
    assert manifest_path.is_file()
    assert verify_manifest(manifest_path) == []

    metadata = _materialization_metadata(result)
    assert metadata["compacted_partitions"]["rows"] == 0
    assert metadata["qa_report"]["rows"] == 2
    assert metadata["qa_report"]["unexplained"] == 0
    assert metadata["qa_report"]["book_missing"] == 0
    assert metadata["qa_report"]["json_report"] == str(report_path)
    assert metadata["qa_report"]["markdown_report"] == str(markdown_path)
    assert metadata["manifest"]["manifest_path"] == str(manifest_path)
