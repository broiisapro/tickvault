"""Package metadata checks for the public releases (issues #82 and #151).

These read the metadata installed by ``uv sync``, so they fail if the project
version or the MIT license declaration drifts from the committed ``LICENSE``.
"""

from __future__ import annotations

import importlib.metadata as metadata
from pathlib import Path

import tickvault

_ROOT = Path(__file__).resolve().parents[1]


def test_version_is_1_1_0_everywhere() -> None:
    assert tickvault.__version__ == "1.1.0"
    assert metadata.version("tickvault") == tickvault.__version__


def test_license_file_is_mit() -> None:
    license_text = (_ROOT / "LICENSE").read_text(encoding="utf-8")
    assert license_text.startswith("MIT License")
    assert "Copyright (c) 2026 Moksh Siruvani" in license_text


def test_installed_metadata_exposes_the_license_expression() -> None:
    project = metadata.metadata("tickvault")
    assert project["License-Expression"] == "MIT"
    assert "LICENSE" in (project.get_all("License-File") or [])
