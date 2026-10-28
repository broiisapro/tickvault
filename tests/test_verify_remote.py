"""Tests for manifest verification over a path or URL (issue #126)."""

from __future__ import annotations

import functools
import json
import shutil
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from typer.testing import CliRunner

import tickvault.verify as verify_module
from tickvault.cli import app
from tickvault.pipeline import run_replay
from tickvault.storage.manifest import write_manifest
from tickvault.verify import verify_remote

_DATE = "2026-11-01"
_MANIFEST = f"manifests/date={_DATE}/manifest.json"

_Handler = Callable[[BaseHTTPRequestHandler], None]
_Serve = Callable[[Path], tuple[str, dict[str, _Handler]]]


@pytest.fixture(scope="module")
def data_root(fixtures_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A real data root built by replaying the synthetic gap session."""
    root = tmp_path_factory.mktemp("verify") / "data"
    run_replay(fixtures_dir / "synthetic_gap_session.jsonl.zst", root)
    write_manifest(root, _DATE)
    return root


@pytest.fixture()
def root(data_root: Path, tmp_path: Path) -> Path:
    """A fresh copy of the data root, so mutation tests stay independent."""
    target = tmp_path / "data"
    shutil.copytree(data_root, target)
    return target


@pytest.fixture()
def serve() -> Iterator[_Serve]:
    """Serve a directory on an ephemeral loopback port for one test."""
    running: list[tuple[ThreadingHTTPServer, threading.Thread]] = []

    def start(directory: Path) -> tuple[str, dict[str, _Handler]]:
        routes: dict[str, _Handler] = {}
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), _handler(directory, routes))
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        running.append((httpd, thread))
        host, port = httpd.server_address[:2]
        return f"http://{host}:{port}", routes

    yield start

    for httpd, thread in running:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive(), "test HTTP server thread outlived its test"


def _handler(
    directory: Path, routes: dict[str, _Handler]
) -> type[SimpleHTTPRequestHandler]:
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, directory=str(directory), **kwargs)  # type: ignore[arg-type]

        def log_message(self, format: str, *args: object) -> None:
            return

        def handle_error(self, request: object, client_address: object) -> None:
            # A timed-out client hangs up while a slow route is still writing.
            return

        def do_GET(self) -> None:
            route = routes.get(self.path)
            if route is None:
                super().do_GET()
            else:
                route(self)

    return Handler


def _serve_bytes(handler: BaseHTTPRequestHandler, data: bytes) -> None:
    handler.send_response(200)
    handler.send_header("Content-Type", "application/octet-stream")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _serve_path(handler: BaseHTTPRequestHandler, path: Path) -> None:
    _serve_bytes(handler, path.read_bytes())


def _serve_status(handler: BaseHTTPRequestHandler, status: int) -> None:
    handler.send_error(status)


def _serve_slowly(handler: BaseHTTPRequestHandler, path: Path, delay_s: float) -> None:
    time.sleep(delay_s)
    _serve_path(handler, path)


def _document(root: Path) -> dict[str, object]:
    return json.loads((root / _MANIFEST).read_text(encoding="utf-8"))


def _entry(root: Path, table: str) -> dict[str, object]:
    document = _document(root)
    files = document["files"]
    assert isinstance(files, list)
    return next(entry for entry in files if entry["table"] == table)


def _rewrite_document(root: Path, document: dict[str, object]) -> None:
    (root / _MANIFEST).write_text(json.dumps(document), encoding="utf-8")


# ----------------------------------------------------------------- local paths


def test_a_fresh_manifest_verifies_clean(root: Path) -> None:
    report = verify_remote(str(root / _MANIFEST))

    assert report.manifest == str(root / _MANIFEST)
    assert report.files == 8
    assert report.ok == 8
    assert report.problems == []


def test_a_modified_byte_is_reported(root: Path) -> None:
    entry = _entry(root, "trades")
    target = root / str(entry["path"])
    data = bytearray(target.read_bytes())
    data[len(data) // 2] ^= 0xFF
    target.write_bytes(data)

    report = verify_remote(str(root / _MANIFEST))

    assert report.files == 8
    assert report.ok == 7
    assert f"sha256 mismatch: {entry['path']}" in report.problems
    assert not any(problem.startswith("bytes mismatch") for problem in report.problems)


def test_a_missing_file_is_reported(root: Path) -> None:
    entry = _entry(root, "events")
    target = root / str(entry["path"])
    target.unlink()

    report = verify_remote(str(root / _MANIFEST))

    assert report.ok == 7
    assert f"missing file: {entry['path']}" in report.problems
    assert "totals.files mismatch" in report.problems


def test_a_wrong_row_count_is_reported(root: Path) -> None:
    document = _document(root)
    files = document["files"]
    assert isinstance(files, list)
    files[0]["row_count"] += 1
    _rewrite_document(root, document)

    report = verify_remote(str(root / _MANIFEST))

    assert report.ok == 7
    assert f"row_count mismatch: {files[0]['path']}" in report.problems
    assert not any(problem.startswith("totals.") for problem in report.problems)


def test_an_unlisted_file_is_reported_for_a_local_root(root: Path) -> None:
    source = next(path for path in (root / "norm").rglob("*.parquet"))
    extra = (
        root
        / "norm"
        / "events"
        / "venue=coinbase"
        / f"date={_DATE}"
        / "hour=00"
        / "part-unlisted.parquet"
    )
    extra.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, extra)

    report = verify_remote(str(root / _MANIFEST))

    assert f"unlisted file: {extra.relative_to(root).as_posix()}" in report.problems


def test_a_missing_local_manifest_raises(root: Path) -> None:
    with pytest.raises(ValueError, match="manifest not found"):
        verify_remote(str(root / "manifests" / "date=2020-01-01" / "manifest.json"))


def test_an_unsafe_entry_is_reported_without_reading_outside_the_root(root: Path) -> None:
    document = _document(root)
    files = document["files"]
    assert isinstance(files, list)
    files[0]["path"] = "../outside.parquet"
    _rewrite_document(root, document)

    report = verify_remote(str(root / _MANIFEST))

    assert "unsafe path in manifest: ../outside.parquet" in report.problems
    assert report.ok == 7


# ------------------------------------------------------------------------ URLs


def test_a_url_manifest_verifies_clean(root: Path, serve: _Serve) -> None:
    base, _ = serve(root)

    report = verify_remote(f"{base}/{_MANIFEST}")

    assert report.files == 8
    assert report.ok == 8
    assert report.problems == []


def test_the_data_root_is_the_manifest_location_minus_the_manifests_path(
    root: Path, tmp_path: Path, serve: _Serve
) -> None:
    served = tmp_path / "served"
    shutil.copytree(root, served / "sample")
    base, _ = serve(served)

    report = verify_remote(f"{base}/sample/{_MANIFEST}")

    assert report.problems == []
    assert report.ok == 8


def test_a_downloaded_file_with_modified_bytes_is_reported(
    root: Path, serve: _Serve
) -> None:
    base, routes = serve(root)
    entry = _entry(root, "trades")
    routes[f"/{entry['path']}"] = functools.partial(_serve_bytes, data=b"not parquet")

    report = verify_remote(f"{base}/{_MANIFEST}")

    assert report.ok == 7
    assert f"sha256 mismatch: {entry['path']}" in report.problems
    assert f"bytes mismatch: {entry['path']}" in report.problems


def test_a_404_on_a_listed_file_is_reported(
    root: Path, serve: _Serve
) -> None:
    base, routes = serve(root)
    entry = _entry(root, "book_deltas")
    routes[f"/{entry['path']}"] = functools.partial(_serve_status, status=404)

    report = verify_remote(f"{base}/{_MANIFEST}")

    assert report.ok == 7
    assert f"missing file: {entry['path']} (HTTP 404)" in report.problems


def test_a_wrong_row_count_reaches_the_url_path_too(
    root: Path, serve: _Serve
) -> None:
    document = _document(root)
    files = document["files"]
    assert isinstance(files, list)
    files[0]["row_count"] += 1
    _rewrite_document(root, document)
    base, _ = serve(root)

    report = verify_remote(f"{base}/{_MANIFEST}")

    assert report.ok == 7
    assert f"row_count mismatch: {files[0]['path']}" in report.problems


def test_a_404_on_the_manifest_raises(root: Path, serve: _Serve) -> None:
    base, routes = serve(root)
    routes[f"/{_MANIFEST}"] = functools.partial(_serve_status, status=404)

    with pytest.raises(ValueError, match="HTTP 404"):
        verify_remote(f"{base}/{_MANIFEST}")


def test_a_slow_file_times_out_without_raising(
    root: Path, serve: _Serve
) -> None:
    base, routes = serve(root)
    entry = _entry(root, "book_snapshots")
    target = root / str(entry["path"])
    routes[f"/{entry['path']}"] = functools.partial(
        _serve_slowly, path=target, delay_s=0.05
    )

    report = verify_remote(f"{base}/{_MANIFEST}", timeout_s=0.01)

    assert report.ok == 7
    assert any(
        problem.startswith(f"timeout downloading {entry['path']}")
        for problem in report.problems
    )


def test_a_download_directory_is_created_and_removed(
    root: Path,
    serve: _Serve,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base, _ = serve(root)
    created: list[Path] = []
    real = tempfile.TemporaryDirectory

    def recording(*args: object, **kwargs: object) -> tempfile.TemporaryDirectory[str]:
        directory = real(*args, **kwargs)  # type: ignore[arg-type]
        created.append(Path(directory.name))
        return directory

    monkeypatch.setattr(verify_module, "_TEMPORARY_DIRECTORY", recording)

    report = verify_remote(f"{base}/{_MANIFEST}")

    assert report.problems == []
    assert created
    assert all(not path.exists() for path in created)


def test_an_unsupported_scheme_raises() -> None:
    with pytest.raises(ValueError, match="http and https"):
        verify_remote("ftp://example.com/manifests/date=2026-11-01/manifest.json")


def test_a_manifest_url_outside_the_expected_layout_raises(
    root: Path, serve: _Serve
) -> None:
    base, _ = serve(root)

    with pytest.raises(ValueError, match="manifests/date="):
        verify_remote(f"{base}/manifest.json")


def test_a_non_positive_timeout_raises() -> None:
    with pytest.raises(ValueError, match="timeout_s"):
        verify_remote("data/manifest.json", timeout_s=0.0)


# ------------------------------------------------------------------------- CLI


def test_cli_verifies_a_good_manifest(root: Path) -> None:
    result = CliRunner().invoke(app, ["verify", "--manifest", str(root / _MANIFEST)])

    assert result.exit_code == 0
    assert f"verify {root / _MANIFEST}: 8/8 files ok" in result.output


def test_cli_exits_one_on_a_mismatch(root: Path) -> None:
    document = _document(root)
    files = document["files"]
    assert isinstance(files, list)
    Path(root / str(files[0]["path"])).unlink()

    result = CliRunner().invoke(app, ["verify", "--manifest", str(root / _MANIFEST)])

    assert result.exit_code == 1
    assert f"missing file: {files[0]['path']}" in result.output
