"""Dagster daily job for TickVault.

Three daily-partitioned assets run the same Epic 1 functions the CLI calls —
:func:`tickvault.storage.compact.compact` → :func:`tickvault.qa.report.run_qa`
+ :func:`tickvault.qa.report.write_reports` →
:func:`tickvault.storage.manifest.write_manifest` — for one UTC day. The
assets themselves contain no business logic.

The data root comes from the ``data_root`` resource, which defaults to
``$TICKVAULT_DATA_DIR`` and falls back to ``./data``.
"""

import os
from datetime import UTC, timedelta
from pathlib import Path

import dagster as dg
from pydantic import Field

from tickvault.qa.report import QAReport, run_qa, write_reports
from tickvault.storage.compact import compact
from tickvault.storage.manifest import write_manifest

daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-10-01",
    timezone="UTC",
    end_offset=90,
)
"""One partition per UTC day.

``end_offset`` keeps partitions available past the current day so a future
date can be materialized (the daily job for the previous day is unaffected);
without it Dagster only knows days that have already started.
"""


class DataRootResource(dg.ConfigurableResource):
    """The TickVault data root, from ``TICKVAULT_DATA_DIR`` or ``./data``."""

    data_root: str = Field(
        default_factory=lambda: os.environ.get("TICKVAULT_DATA_DIR", "./data")
    )

    @property
    def path(self) -> Path:
        return Path(self.data_root)


def _unexplained(report: QAReport) -> int:
    return sum(row.unexplained for row in report.rows)


def _book_missing(report: QAReport) -> int:
    return sum(
        1 for row in report.rows if row.messages > 0 and row.venue_snapshots == 0
    )


@dg.asset(
    partitions_def=daily_partitions,
    description="Merge multi-file normalized partitions of one UTC day.",
)
def compacted_partitions(
    context: dg.AssetExecutionContext, data_root: DataRootResource
) -> dg.MaterializeResult:
    result = compact(data_root.path, context.partition_key)
    return dg.MaterializeResult(
        metadata={"partitions": len(result.partitions), "rows": result.rows}
    )


@dg.asset(
    partitions_def=daily_partitions,
    deps=[compacted_partitions],
    description="Write the per-venue/symbol QA report of one UTC day.",
)
def qa_report(
    context: dg.AssetExecutionContext, data_root: DataRootResource
) -> dg.MaterializeResult:
    report = run_qa(data_root.path, context.partition_key)
    json_path, markdown_path = write_reports(
        report, data_root.path / "reports", context.partition_key
    )
    return dg.MaterializeResult(
        metadata={
            "rows": len(report.rows),
            "unexplained": _unexplained(report),
            "book_missing": _book_missing(report),
            "json_report": str(json_path),
            "markdown_report": str(markdown_path),
        }
    )


@dg.asset(
    partitions_def=daily_partitions,
    deps=[qa_report],
    description="Write the SHA-256 manifest of one UTC day's normalized files.",
)
def manifest(
    context: dg.AssetExecutionContext, data_root: DataRootResource
) -> dg.MaterializeResult:
    manifest_path = write_manifest(data_root.path, context.partition_key)
    return dg.MaterializeResult(metadata={"manifest_path": str(manifest_path)})


daily_job = dg.define_asset_job(
    "daily_job",
    selection=[compacted_partitions, qa_report, manifest],
    description="Compact, report and manifest one UTC day of normalized data.",
)


@dg.schedule(
    name="daily_job_schedule",
    cron_schedule="30 0 * * *",
    execution_timezone="UTC",
    job=daily_job,
    description="Run the daily job for the previous UTC day at 00:30 UTC.",
)
def daily_job_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    scheduled = context.scheduled_execution_time
    day = scheduled.astimezone(UTC).date() if scheduled.tzinfo else scheduled.date()
    return dg.RunRequest(partition_key=(day - timedelta(days=1)).isoformat())


defs = dg.Definitions(
    assets=[compacted_partitions, qa_report, manifest],
    schedules=[daily_job_schedule],
    resources={"data_root": DataRootResource()},
)
