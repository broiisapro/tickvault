"""Tests for :mod:`scripts.build_qa_site` (issue #84).

The site is built from a real replay tree produced by
:func:`tickvault.pipeline.run_replay` on the synthetic fixture, so the tests
never depend on the bundled sample's contents (agent B replaces it during the
same wave). A monkeypatched hostile row proves the HTML escaping, and two
builds must be byte-identical.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts import build_qa_site
from tickvault.pipeline import run_replay
from tickvault.qa.metrics import QARow
from tickvault.qa.report import QAReport, run_qa

_LATEST_KEYS = {"date", "source", "rows", "unexplained", "book_missing", "exit_code"}


def _replay_root(tmp_path: Path, synthetic_gap_path: Path) -> Path:
    root = tmp_path / "data"
    run_replay(synthetic_gap_path, root)
    return root


def _partition_dates(root: Path) -> list[str]:
    return sorted(
        {path.name.removeprefix("date=") for path in (root / "norm").glob("*/*/date=*")}
    )


def _tree_digest(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_build_writes_every_expected_file(tmp_path: Path, synthetic_gap_path: Path) -> None:
    root = _replay_root(tmp_path, synthetic_gap_path)
    out = tmp_path / "site"

    build_qa_site.build_site(root, out)

    dates = _partition_dates(root)
    assert dates
    for day in dates:
        for name in (f"qa-{day}.json", f"qa-{day}.md", f"qa-{day}.html"):
            assert (out / name).is_file(), name
    assert (out / "index.html").is_file()
    assert (out / "latest.json").is_file()
    assert (out / ".nojekyll").is_file()
    assert (out / ".nojekyll").read_text(encoding="utf-8") == ""

    index = (out / "index.html").read_text(encoding="utf-8")
    for day in dates:
        assert f'href="qa-{day}.html"' in index
    assert build_qa_site.REPOSITORY_URL in index

    payload = json.loads((out / "latest.json").read_text(encoding="utf-8"))
    assert set(payload) == _LATEST_KEYS
    assert payload["date"] == max(dates)
    assert payload["source"] == str(root)
    latest_rows = [row for row in run_qa(root).rows if row.date == payload["date"]]
    assert payload["rows"] == len(latest_rows) > 0
    assert payload["unexplained"] == 0
    assert payload["book_missing"] == 0
    assert payload["exit_code"] == 0


def test_hostile_rows_are_html_escaped(
    tmp_path: Path, synthetic_gap_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _replay_root(tmp_path, synthetic_gap_path)
    day = _partition_dates(root)[0]

    def hostile(data: str | Path, date: object = None) -> QAReport:
        row = QARow(
            venue='<script>alert("x")</script>',
            symbol="<b>BTC-USD</b>",
            date=day,
            messages=1,
            trades=0,
            book_deltas=0,
            venue_snapshots=0,
            gaps=0,
            resyncs=0,
            checksum_failures=0,
            reconnects=0,
            invariant_violations=0,
            lag_p50_ms=0.0,
            lag_p99_ms=0.0,
            coverage=0.0,
            unexplained=1,
            book_missing=False,
        )
        return QAReport(rows=(row,), generated_from=str(data), date=day)

    monkeypatch.setattr(build_qa_site, "run_qa", hostile)
    out = tmp_path / "site"
    build_qa_site.build_site(root, out)

    for name in (f"qa-{day}.html", "index.html"):
        text = (out / name).read_text(encoding="utf-8")
        assert "<script>" not in text
        assert "&lt;script&gt;" in text
        assert 'class="bad"' in text


def test_two_builds_are_byte_identical(tmp_path: Path, synthetic_gap_path: Path) -> None:
    root = _replay_root(tmp_path, synthetic_gap_path)
    first = tmp_path / "first"
    second = tmp_path / "second"

    build_qa_site.build_site(root, first)
    build_qa_site.build_site(root, second)

    assert _tree_digest(first)
    assert _tree_digest(first) == _tree_digest(second)


def test_empty_root_still_builds_an_index(tmp_path: Path) -> None:
    out = tmp_path / "site"

    build_qa_site.build_site(tmp_path / "empty-data", out)

    index = (out / "index.html").read_text(encoding="utf-8")
    assert "No QA reports yet." in index
    assert (out / ".nojekyll").is_file()
    payload = json.loads((out / "latest.json").read_text(encoding="utf-8"))
    assert set(payload) == _LATEST_KEYS
    assert payload["date"] is None
    assert payload["exit_code"] == 0
