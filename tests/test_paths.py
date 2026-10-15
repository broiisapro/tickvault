from datetime import UTC, datetime, timedelta
from pathlib import Path

from tickvault.paths import partition_path, raw_file_path

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _ns(
    year: int, month: int, day: int, hour: int, minute: int, second: int, nano: int = 0
) -> int:
    moment = datetime(year, month, day, hour, minute, second, tzinfo=UTC)
    microseconds = (moment - _EPOCH) // timedelta(microseconds=1)
    return int(microseconds) * 1000 + nano


def test_partition_path_is_utc_hourly() -> None:
    root = Path("/data")
    assert partition_path(root, "trades", "coinbase", _ns(2026, 10, 15, 3, 18, 51, 462_959)) == (
        root / "norm" / "trades" / "venue=coinbase" / "date=2026-10-15" / "hour=03"
    )


def test_partition_path_hour_boundaries() -> None:
    root = Path("/data")
    last_ns = _ns(2026, 10, 14, 23, 59, 59, 999_999_999)
    assert partition_path(root, "trades", "kraken", last_ns) == (
        root / "norm" / "trades" / "venue=kraken" / "date=2026-10-14" / "hour=23"
    )
    assert partition_path(root, "trades", "kraken", last_ns + 1) == (
        root / "norm" / "trades" / "venue=kraken" / "date=2026-10-15" / "hour=00"
    )
    midnight = _ns(2026, 10, 15, 0, 0, 0, 0)
    assert partition_path(root, "book_deltas", "kraken", midnight) == (
        root / "norm" / "book_deltas" / "venue=kraken" / "date=2026-10-15" / "hour=00"
    )


def test_raw_file_path_and_rotation_format() -> None:
    root = Path("/data")
    ts_ns = _ns(2026, 10, 14, 23, 59, 59, 999_999_999)
    assert raw_file_path(root, "coinbase", "coinbase-20261014T235959Z-00000001", ts_ns, 1) == (
        root
        / "raw"
        / "venue=coinbase"
        / "date=2026-10-14"
        / "hour=23"
        / "coinbase-20261014T235959Z-00000001-00001.jsonl.zst"
    )


def test_raw_file_path_rolls_over_at_hour_boundary() -> None:
    root = Path("/data")
    ts_ns = _ns(2026, 10, 14, 23, 59, 59, 999_999_999) + 1
    assert raw_file_path(root, "kraken", "kraken-20261014T235959Z-0000000a", ts_ns, 12) == (
        root
        / "raw"
        / "venue=kraken"
        / "date=2026-10-15"
        / "hour=00"
        / "kraken-20261014T235959Z-0000000a-00012.jsonl.zst"
    )
