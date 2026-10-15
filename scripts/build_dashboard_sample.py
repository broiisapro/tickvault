#!/usr/bin/env python3
"""Build the bundled dashboard sample from raw inputs.

Replays one or more raw files or raw trees with
:func:`tickvault.pipeline.run_replay` into ``--out`` (default
``dashboard/sample``), compacts every date present, writes the per-date
manifests and fails when the finished root exceeds ``--max-mb`` (default
60, decision A44) or any single file exceeds ``--max-file-mb`` (default 45,
GitHub's 50 MB warning line). The output root's ``norm/`` and ``manifests/``
directories are rebuilt from scratch on every run, so the sample is a
deterministic function of its inputs; a ``raw/`` tree under ``--out`` is
never touched, and the script refuses to rebuild a root that holds one.

Usage:
    scripts/build_dashboard_sample.py \\
        --raw tests/fixtures/coinbase_btc_5m.jsonl.zst \\
        --raw tests/fixtures/kraken_btc_5m.jsonl.zst \\
        --out dashboard/sample
    scripts/build_dashboard_sample.py --raw /data/raw --out /tmp/sample --max-mb 50
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from tickvault.pipeline import run_replay
from tickvault.storage.compact import compact
from tickvault.storage.manifest import write_manifest

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "dashboard" / "sample"
DEFAULT_MAX_MB = 60.0
DEFAULT_MAX_FILE_MB = 45.0
_MIB = 1024 * 1024


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the bundled dashboard sample.")
    parser.add_argument(
        "--raw",
        action="append",
        required=True,
        metavar="PATH",
        help="raw file or raw tree to replay; repeat for several inputs",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"output data root (default: {DEFAULT_OUT.relative_to(ROOT)})",
    )
    parser.add_argument(
        "--max-mb",
        type=float,
        default=DEFAULT_MAX_MB,
        help=(
            "fail when the finished root is larger than this many MiB "
            f"(default: {DEFAULT_MAX_MB:g})"
        ),
    )
    parser.add_argument(
        "--max-file-mb",
        type=float,
        default=DEFAULT_MAX_FILE_MB,
        help=(
            "fail when any single file is larger than this many MiB "
            f"(default: {DEFAULT_MAX_FILE_MB:g})"
        ),
    )
    return parser.parse_args(argv)


def _dates(root: Path) -> list[str]:
    pattern = "*/venue=*/date=*"
    return sorted({path.name.removeprefix("date=") for path in (root / "norm").glob(pattern)})


def _tree_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _files_by_size(root: Path) -> list[tuple[Path, int]]:
    return sorted(
        ((path, path.stat().st_size) for path in root.rglob("*") if path.is_file()),
        key=lambda item: item[1],
        reverse=True,
    )


def _holds_raw_tree(root: Path) -> bool:
    return any((root / "raw").rglob("*.jsonl.zst"))


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    for raw in args.raw:
        if not Path(raw).exists():
            print(f"raw input not found: {raw}", file=sys.stderr)
            return 2
    if args.max_mb <= 0:
        print("--max-mb must be positive", file=sys.stderr)
        return 2
    if args.max_file_mb <= 0:
        print("--max-file-mb must be positive", file=sys.stderr)
        return 2

    out: Path = args.out
    if _holds_raw_tree(out):
        print(f"refusing to rebuild {out}: it holds a raw tree", file=sys.stderr)
        return 1
    for stale in (out / "norm", out / "manifests"):
        if stale.is_dir():
            shutil.rmtree(stale)

    messages = 0
    rows = 0
    for raw in args.raw:
        stats = run_replay(raw, out)
        messages += stats.messages
        rows += stats.trades + stats.book_deltas + stats.book_snapshots + stats.events
        print(f"replayed {raw}: {stats.messages} messages, {stats.normalize_errors} malformed")

    dates = _dates(out)
    for date in dates:
        result = compact(out, date)
        manifest = write_manifest(out, date)
        print(
            f"{date}: compacted {result.files_removed} files into {result.files_written}, "
            f"wrote {manifest}"
        )

    total_bytes = _tree_bytes(out)
    total_mb = total_bytes / _MIB
    if total_mb > args.max_mb:
        print(
            f"{out} is {total_mb:.2f} MiB, over --max-mb {args.max_mb:g}",
            file=sys.stderr,
        )
        return 1
    files = _files_by_size(out)
    for path, size in files:
        size_mb = size / _MIB
        if size_mb > args.max_file_mb:
            print(
                f"{path.relative_to(out)} is {size_mb:.2f} MiB, "
                f"over --max-file-mb {args.max_file_mb:g}",
                file=sys.stderr,
            )
            return 1
    largest = "no files" if not files else f"{files[0][0].relative_to(out)} ({files[0][1]} bytes)"
    print(
        f"{out}: {len(dates)} date(s), {messages} messages, {rows} rows, "
        f"{total_bytes} bytes ({total_mb:.2f} MiB) within {args.max_mb:g} MiB; "
        f"largest file: {largest}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
