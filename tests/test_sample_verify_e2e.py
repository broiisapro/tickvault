"""End-to-end test of the public-sample round trip (issue #143).

The input is a data root replayed from the two recorded five-minute Coinbase
and Kraken BTC-USD captures, so the sample is built from the same normalized
tables the pipeline writes. The builder's output tree is served over a
loopback ``http.server`` on an ephemeral port and ``tickvault verify`` is run
through Typer's runner against the served manifest URL: the built tree must
verify clean, and one flipped byte in one served Parquet file must make the
command exit 1 while naming that file. ``SHA256SUMS`` is checked with
:mod:`hashlib` independently of the manifest.

Nothing here leaves the machine: the only network traffic is to
``127.0.0.1`` on the port the test itself opened.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import threading
from collections.abc import Iterator
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from typer.testing import CliRunner

from scripts.build_public_sample import main as build_public_sample
from tickvault.cli import app
from tickvault.pipeline import run_replay

DATE = "2026-10-15"
"""UTC day of both recorded five-minute fixtures."""
HOUR = "11"
"""The hour with the most rows: the fixtures start at 11:56:18 and run past noon."""
SYMBOL = "BTC-USD"
MANIFEST = f"manifests/date={DATE}/manifest.json"


@pytest.fixture(scope="module")
def built_sample(
    tmp_path_factory: pytest.TempPathFactory,
    coinbase_5m_path: Path,
    kraken_5m_path: Path,
) -> Path:
    """The sample tree the builder produced from both recorded fixtures."""
    workspace = tmp_path_factory.mktemp("sample-verify")
    data = workspace / "data"
    for raw in (coinbase_5m_path, kraken_5m_path):
        run_replay(raw, data)
    out = workspace / "sample"
    assert (
        build_public_sample(
            [
                "--data",
                str(data),
                "--date",
                DATE,
                "--out",
                str(out),
                "--hour",
                HOUR,
            ]
        )
        == 0
    )
    return out


@pytest.fixture()
def served(built_sample: Path, tmp_path: Path) -> Iterator[tuple[str, Path]]:
    """Serve a private copy of the sample; yield its base URL and directory."""
    root = tmp_path / "served"
    shutil.copytree(built_sample, root)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _handler(root))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    try:
        yield f"http://{host}:{port}", root
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive(), "test HTTP server thread outlived its test"


def _handler(directory: Path) -> type[SimpleHTTPRequestHandler]:
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, directory=str(directory), **kwargs)  # type: ignore[arg-type]

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def _entries(root: Path) -> list[dict[str, object]]:
    document = json.loads((root / MANIFEST).read_text(encoding="utf-8"))
    files = document["files"]
    assert isinstance(files, list) and files
    return files


def _invoke(url: str) -> tuple[int, str]:
    result = CliRunner().invoke(app, ["verify", "--manifest", url])
    return result.exit_code, result.output


def test_a_served_sample_verifies_clean_through_the_cli(served: tuple[str, Path]) -> None:
    base, root = served
    url = f"{base}/{MANIFEST}"
    entries = _entries(root)
    assert {entry["table"] for entry in entries} == {
        "book_deltas",
        "book_snapshots",
        "trades",
    }
    assert {entry["venue"] for entry in entries} == {"coinbase", "kraken"}
    payload = json.loads((root / "sample.json").read_text(encoding="utf-8"))
    assert (payload["date"], payload["hour"], payload["symbol"]) == (DATE, HOUR, SYMBOL)
    assert payload["venues"] == ["coinbase", "kraken"]

    exit_code, output = _invoke(url)

    assert exit_code == 0, output
    assert f"verify {url}: {len(entries)}/{len(entries)} files ok" in output
    assert "mismatch" not in output
    assert "missing file:" not in output


def test_one_flipped_byte_fails_the_cli_and_names_the_file(
    served: tuple[str, Path],
) -> None:
    base, root = served
    entries = _entries(root)
    entry = next(item for item in entries if item["table"] == "trades")
    relative = str(entry["path"])
    target = root / relative
    data = bytearray(target.read_bytes())
    data[len(data) // 2] ^= 0xFF
    target.write_bytes(data)

    exit_code, output = _invoke(f"{base}/{MANIFEST}")

    assert exit_code == 1
    assert f"{len(entries) - 1}/{len(entries)} files ok" in output
    assert f"sha256 mismatch: {relative}" in output


def test_sha256sums_matches_every_file_in_the_sample(built_sample: Path) -> None:
    listed: dict[str, str] = {}
    for line in (built_sample / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, separator, relative = line.partition("  ")
        assert separator == "  ", line
        listed[relative] = digest
    files = {
        path.relative_to(built_sample).as_posix(): path
        for path in built_sample.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    }

    assert list(listed) == sorted(listed)
    assert set(listed) == set(files)
    assert f"{MANIFEST}" in listed
    for relative, path in files.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == listed[relative], relative
