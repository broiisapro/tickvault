"""Tests for the Dagster features asset (issue #119).

The compute and write functions are monkeypatched, so these tests describe the
asset's wiring only and stay independent of the feature implementation.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import dagster as dg
import pyarrow as pa

from tickvault.features import features_path

_PARTITION = "2026-10-15"
_REPO_ROOT = Path(__file__).resolve().parent.parent


def _definitions() -> Any:
    # pytest does not put the repo root on sys.path and ``orchestration`` is a
    # top-level package next to it, not an installed one.
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))
    from orchestration import definitions

    return definitions


def _metadata(result: dg.ExecuteInProcessResult) -> dict[str, dict[str, object]]:
    return {
        event.asset_key.to_user_string(): {
            key: getattr(value, "value", value)
            for key, value in (event.materialization.metadata or {}).items()
        }
        for event in result.get_asset_materialization_events()
    }


def _fake_compute(
    calls: list[tuple[Any, ...]],
) -> Any:
    def compute(
        data_root: str | Path,
        date: str,
        resolutions: Sequence[str] = ("1s", "1m"),
    ) -> dict[str, pa.Table]:
        calls.append(("compute", Path(data_root), date, tuple(resolutions)))
        return {"1s": pa.table({"n": [1, 2, 3]}), "1m": pa.table({"n": [1]})}

    return compute


def _fake_write(calls: list[tuple[Any, ...]]) -> Any:
    def write(tables: Any, out_root: str | Path, date: str) -> list[Path]:
        calls.append(("write", Path(out_root), date))
        assert sorted(tables) == ["1m", "1s"]
        return [
            features_path(out_root, resolution, "kraken", date, "BTC-USD")
            for resolution in sorted(tables)
        ]

    return write


def test_features_asset_calls_compute_and_write_for_the_partition(
    tmp_path: Path, monkeypatch: Any
) -> None:
    definitions = _definitions()
    root = tmp_path / "data"
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr("tickvault.features.compute_features", _fake_compute(calls))
    monkeypatch.setattr("tickvault.features.write_features", _fake_write(calls))

    result = dg.materialize(
        [definitions.features],
        partition_key=_PARTITION,
        resources={"data_root": definitions.DataRootResource(data_root=str(root))},
    )

    assert result.success
    assert calls == [
        ("compute", root, _PARTITION, ("1s", "1m")),
        ("write", root, _PARTITION),
    ]
    metadata = _metadata(result)["features"]
    assert metadata["rows_1s"] == 3
    assert metadata["rows_1m"] == 1
    assert metadata["files"] == 2


def test_features_asset_is_in_the_daily_job_downstream_of_compaction() -> None:
    definitions = _definitions()

    job = definitions.defs.resolve_job_def("daily_job")

    assert dg.AssetKey("features") in job.asset_layer.selected_asset_keys
    assert definitions.features.asset_deps[dg.AssetKey("features")] == {
        dg.AssetKey("compacted_partitions")
    }
    assert definitions.features.partitions_def is definitions.daily_partitions


def test_daily_job_materializes_features_in_process(
    tmp_path: Path, monkeypatch: Any
) -> None:
    definitions = _definitions()
    root = tmp_path / "data"
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr("tickvault.features.compute_features", _fake_compute(calls))
    monkeypatch.setattr("tickvault.features.write_features", _fake_write(calls))

    result = definitions.defs.resolve_job_def("daily_job").execute_in_process(
        partition_key=_PARTITION,
        resources={"data_root": definitions.DataRootResource(data_root=str(root))},
    )

    assert result.success
    materialized = _metadata(result)
    assert set(materialized) == {"compacted_partitions", "qa_report", "manifest", "features"}
    assert materialized["features"]["rows_1s"] == 3
    assert materialized["features"]["rows_1m"] == 1
    assert materialized["features"]["files"] == 2
    assert [call[0] for call in calls] == ["compute", "write"]
    assert calls[0][1:] == (root, _PARTITION, ("1s", "1m"))
