"""AppTest smoke tests for :mod:`dashboard.app`.

The tests point ``TICKVAULT_DATA_DIR`` at a root built from the tiny synthetic
fixture (replaying the five-minute recordings would only slow the AppTest
down), render the app once and assert it completes without exceptions and
produces the expected elements. The header tests render the committed
``dashboard/sample`` with ``TICKVAULT_DATA_DIR`` unset, which also exercises
the cold-render budget on the real sample. Nothing here touches the network.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from tickvault.book.processor import BookProcessor, ProcessorOutput
from tickvault.models import NormalizedMessage, RawMessage
from tickvault.normalize import coinbase as coinbase_normalizer
from tickvault.normalize import kraken as kraken_normalizer
from tickvault.rawio import iter_raw
from tickvault.storage.parquet_writer import ParquetWriter

_TITLE = "TickVault — crypto market-data quality"
_SOURCE_URL = "https://github.com/broiisapro/tickvault"
_APP_PATH = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
_NORMALIZERS = {"coinbase": coinbase_normalizer.normalize, "kraken": kraken_normalizer.normalize}


def _normalize(raw: RawMessage) -> NormalizedMessage:
    return _NORMALIZERS[raw.venue](raw)


def _write_output(writer: ParquetWriter, output: ProcessorOutput) -> None:
    writer.write("trades", output.trades)
    writer.write("book_deltas", output.book_deltas)
    writer.write("book_snapshots", output.book_snapshots)
    writer.write("events", output.events)


def _replay(root: Path, raw_paths: Sequence[Path]) -> None:
    try:
        from tickvault.pipeline import run_replay
    except ImportError:
        _replay_inline(root, raw_paths)
        return
    for path in raw_paths:
        run_replay(path, root)


def _replay_inline(root: Path, raw_paths: Sequence[Path]) -> None:
    writer = ParquetWriter(root)
    try:
        for path in raw_paths:
            processor = BookProcessor()
            for raw in iter_raw(path):
                _write_output(writer, processor.process(_normalize(raw)))
    finally:
        writer.close()


@pytest.fixture(scope="module")
def app_root(tmp_path_factory: pytest.TempPathFactory, synthetic_gap_path: Path) -> Path:
    root = tmp_path_factory.mktemp("dashboard-app")
    _replay(root, [synthetic_gap_path])
    return root


def _render(monkeypatch: pytest.MonkeyPatch, root: Path) -> AppTest:
    monkeypatch.setenv("TICKVAULT_DATA_DIR", str(root))
    app = AppTest.from_file(str(_APP_PATH), default_timeout=60)
    app.run()
    return app


def _render_bundled_sample(monkeypatch: pytest.MonkeyPatch) -> AppTest:
    monkeypatch.delenv("TICKVAULT_DATA_DIR", raising=False)
    app = AppTest.from_file(str(_APP_PATH), default_timeout=30)
    app.run(timeout=30)
    return app


def test_app_header_describes_the_bundled_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _render_bundled_sample(monkeypatch)
    assert app.exception == []
    assert [title.value for title in app.title] == [_TITLE]
    assert any(_SOURCE_URL in block.value for block in app.markdown)
    root_captions = [
        caption.value for caption in app.caption if caption.value.startswith("Bundled sample:")
    ]
    assert len(root_captions) == 1
    caption = root_captions[0]
    for market in ("coinbase", "kraken", "BTC-USD", "ETH-USD"):
        assert market in caption
    assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", caption) is not None
    assert re.search(r"UTC \(\d+ min\)", caption) is not None
    tiles = {tile.label: tile.value for tile in app.metric}
    assert "Messages/s (last 5 min)" in tiles
    assert float(tiles["Messages/s (last 5 min)"].replace(",", "")) > 0


def test_app_header_shows_the_live_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _render(monkeypatch, tmp_path)
    assert app.exception == []
    live = [
        caption.value
        for caption in app.caption
        if caption.value.startswith("Live data root:")
    ]
    assert len(live) == 1
    assert str(tmp_path) in live[0]
    assert not any(
        caption.value.startswith("Bundled sample:") for caption in app.caption
    )


def test_app_renders_with_data(
    app_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _render(monkeypatch, app_root)
    assert app.exception == []
    assert len(app.metric) == 5
    assert len(app.multiselect) == 2
    assert len(app.dataframe) >= 1
    tiles = {tile.label: tile.value for tile in app.metric}
    assert tiles["Gaps"] == "1"
    assert tiles["Checksum failures"] == "1"
    assert tiles["Reconnects"] == "0"
    assert tiles["Unexplained"] == "0"
    assert float(tiles["Messages/s (last 5 min)"].replace(",", "")) > 0


def test_app_renders_on_empty_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _render(monkeypatch, tmp_path)
    assert app.exception == []
    assert len(app.info) == 1
    assert "No normalized Parquet data" in app.info[0].value
    assert len(app.metric) == 0


def test_app_renders_on_missing_root(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _render(monkeypatch, Path("/nonexistent/tickvault/data"))
    assert app.exception == []
    assert len(app.info) == 1
