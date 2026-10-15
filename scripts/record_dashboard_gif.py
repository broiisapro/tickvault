#!/usr/bin/env python3
"""Record a running TickVault dashboard as an animated GIF plus a screenshot.

Drives a headless Chromium with Playwright against a dashboard URL, waits for
every render to settle (the ``Running...`` indicator is gone and the KPI tiles
carry values), captures a frame, reloads on an interval to pick up new data
and assembles the frames with Pillow into a palette-quantized GIF at no more
than ``--gif-width`` pixels wide that plays the whole session in at most
``--max-play-seconds``. After the last frame it saves one full-page screenshot
of the populated dashboard.

Playwright is deliberately not a project dependency: run the script through
``uv run --with``. Pillow is already importable through Streamlit (the
``dashboard`` dependency group), so it needs no overlay.

Usage (from the repository root, with the dashboard up on port 8501)::

    uv run --with playwright==1.64.0 playwright install chromium   # once
    uv run --with playwright==1.64.0 python scripts/record_dashboard_gif.py \\
        --url http://localhost:8501 --duration 150

Defaults record for 120 s at one reload every 10 s and write
``docs/media/dashboard-live.gif`` and ``docs/media/dashboard.png``. The GIF
stays under ``--max-mb`` (default 8) by reducing the palette, dropping frames
and narrowing the width, in that order. The script writes only those two
files: it reads nothing from and writes nothing to a ``data/`` directory, and
it never commits.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "docs" / "media" / "dashboard-live.gif"
DEFAULT_SHOT = ROOT / "docs" / "media" / "dashboard.png"
_MIB = 1024 * 1024
_METRIC = '[data-testid="stMetric"]'
_METRIC_VALUE = '[data-testid="stMetricValue"]'
_RUNNING = '[data-testid="stStatusWidget"]'
_GIF_STEPS: tuple[tuple[int, int, int], ...] = (
    (0, 128, 1),
    (0, 96, 1),
    (0, 64, 1),
    (800, 64, 1),
    (640, 64, 1),
    (640, 32, 1),
    (480, 32, 2),
)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record the live dashboard as a GIF.")
    parser.add_argument(
        "--url",
        default="http://localhost:8501",
        help="dashboard URL (default: http://localhost:8501)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=120.0,
        help="recording wall time in seconds (default: 120)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="seconds between page reloads (default: 10)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"GIF output path (default: {DEFAULT_OUT.relative_to(ROOT)})",
    )
    parser.add_argument(
        "--shot",
        type=Path,
        default=DEFAULT_SHOT,
        help=f"full-page screenshot path (default: {DEFAULT_SHOT.relative_to(ROOT)})",
    )
    parser.add_argument(
        "--no-shot",
        action="store_true",
        help="skip the full-page screenshot",
    )
    parser.add_argument("--width", type=int, default=1280, help="viewport width (default: 1280)")
    parser.add_argument("--height", type=int, default=800, help="viewport height (default: 800)")
    parser.add_argument(
        "--gif-width",
        type=int,
        default=960,
        help="maximum GIF width in pixels (default: 960)",
    )
    parser.add_argument(
        "--max-mb",
        type=float,
        default=8.0,
        help="GIF size budget in MiB (default: 8)",
    )
    parser.add_argument(
        "--shot-max-mb",
        type=float,
        default=1.5,
        help="screenshot size budget in MiB; over it the PNG is requantized (default: 1.5)",
    )
    parser.add_argument(
        "--max-play-seconds",
        type=float,
        default=18.0,
        help="GIF playback time; the session is sped up to fit (default: 18)",
    )
    parser.add_argument(
        "--min-tiles",
        type=int,
        default=5,
        help="KPI tiles that must be visible with values before a frame (default: 5)",
    )
    parser.add_argument(
        "--settle-timeout",
        type=float,
        default=90.0,
        help="seconds to wait for one render to settle (default: 90)",
    )
    parser.add_argument(
        "--settle-ms",
        type=int,
        default=750,
        help="extra milliseconds to wait after settling, for chart animations (default: 750)",
    )
    parser.add_argument("--headed", action="store_true", help="show the browser window")
    return parser.parse_args(argv)


def _require_playwright() -> Any:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - depends on the caller's overlay
        print(
            "playwright is not importable; run this script with "
            "`uv run --with playwright==1.64.0 python scripts/record_dashboard_gif.py` "
            "after `uv run --with playwright==1.64.0 playwright install chromium`",
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
    return sync_playwright


def _wait_settled(page: Any, args: argparse.Namespace) -> None:
    timeout_ms = int(args.settle_timeout * 1000)
    page.wait_for_selector(_METRIC, state="attached", timeout=timeout_ms)
    page.wait_for_selector(_RUNNING, state="detached", timeout=timeout_ms)
    deadline = time.monotonic() + args.settle_timeout
    while True:
        values = page.eval_on_selector_all(
            _METRIC_VALUE, "els => els.map(e => (e.textContent || '').trim())"
        )
        if len(values) >= args.min_tiles and all(values[: args.min_tiles]):
            break
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"dashboard did not settle within {args.settle_timeout:g}s: "
                f"{len(values)} KPI tiles, values {values!r}"
            )
        page.wait_for_timeout(250)
    page.wait_for_timeout(args.settle_ms)


def _capture_frame(page: Any, frames_dir: Path, index: int, args: argparse.Namespace) -> Path:
    path = frames_dir / f"frame-{index:04d}.png"
    page.screenshot(path=path, animations="disabled")
    print(f"frame {index:3d}: {path.name}")
    return path


def _save_shot(page: Any, shot: Path, args: argparse.Namespace) -> None:
    shot.parent.mkdir(parents=True, exist_ok=True)
    height = page.evaluate(
        "() => {"
        " const sels = ['[data-testid=\"stMain\"]', '[data-testid=\"stMainBlockContainer\"]',"
        " '[data-testid=\"stAppViewContainer\"]'];"
        " let h = 0;"
        " for (const sel of sels) { const el = document.querySelector(sel);"
        " if (el) h = Math.max(h, el.scrollHeight); }"
        " return h || document.documentElement.scrollHeight; }"
    )
    viewport = page.viewport_size
    try:
        page.set_viewport_size({"width": args.width, "height": max(args.height, int(height) + 8)})
        page.wait_for_timeout(500)
        page.screenshot(path=shot)
    finally:
        page.set_viewport_size(viewport)
    size_mb = shot.stat().st_size / _MIB
    if size_mb > args.shot_max_mb:
        image = Image.open(shot).convert(
            "P", palette=Image.ADAPTIVE, colors=256, dither=Image.Dither.NONE
        )
        image.save(shot, optimize=True)
        size_mb = shot.stat().st_size / _MIB
    print(f"shot: {shot} ({size_mb:.2f} MiB)")
    if size_mb > args.shot_max_mb:
        print(
            f"warning: {shot} is {size_mb:.2f} MiB, over --shot-max-mb {args.shot_max_mb:g}",
            file=sys.stderr,
        )


def _record(args: argparse.Namespace, frames_dir: Path) -> list[Path]:
    sync_playwright = _require_playwright()
    frames: list[Path] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=not args.headed)
        try:
            context = browser.new_context(
                viewport={"width": args.width, "height": args.height},
                device_scale_factor=1,
            )
            page = context.new_page()
            page.goto(args.url, wait_until="load", timeout=args.settle_timeout * 1000)
            _wait_settled(page, args)
            frames.append(_capture_frame(page, frames_dir, 0, args))

            start = time.monotonic()
            for tick in range(1, int(args.duration // args.interval) + 1):
                remaining = start + tick * args.interval - time.monotonic()
                if remaining > 0:
                    page.wait_for_timeout(int(remaining * 1000))
                page.reload(wait_until="load", timeout=args.settle_timeout * 1000)
                _wait_settled(page, args)
                frames.append(_capture_frame(page, frames_dir, tick, args))

            if not args.no_shot:
                _save_shot(page, args.shot, args)
        finally:
            browser.close()
    return frames


def _assemble_gif(frames: list[Path], out: Path, args: argparse.Namespace) -> Path:
    play_ms = min(args.max_play_seconds * 1000.0, max(args.duration, 0.0) * 1000.0)
    out.parent.mkdir(parents=True, exist_ok=True)
    for width, colors, step in _GIF_STEPS:
        target_width = min(args.gif_width, width) if width else args.gif_width
        selected = frames[::step]
        images: list[Image.Image] = []
        for path in selected:
            image = Image.open(path).convert("RGB")
            if image.width > target_width:
                target_height = round(image.height * target_width / image.width)
                image = image.resize((target_width, target_height), Image.LANCZOS)
            images.append(
                image.convert(
                    "P", palette=Image.ADAPTIVE, colors=colors, dither=Image.Dither.NONE
                )
            )
        delay_ms = max(250, round(play_ms / len(images)))
        images[0].save(
            out,
            save_all=True,
            append_images=images[1:],
            duration=delay_ms,
            loop=0,
            optimize=True,
            disposal=2,
        )
        size_mb = out.stat().st_size / _MIB
        print(
            f"gif attempt: {target_width} px, {colors} colors, step {step}, "
            f"{len(images)} frames, {delay_ms} ms/frame -> {size_mb:.2f} MiB"
        )
        if size_mb <= args.max_mb:
            print(f"gif: {out} ({size_mb:.2f} MiB)")
            return out
    out.unlink(missing_ok=True)
    raise SystemExit(
        f"GIF stays above --max-mb {args.max_mb:g} after every reduction step; "
        "re-record with a shorter --duration or fewer KPI tiles"
    )


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.duration <= 0 or args.interval <= 0:
        print("--duration and --interval must be positive", file=sys.stderr)
        return 2
    if args.gif_width <= 0 or args.max_mb <= 0 or args.min_tiles <= 0:
        print("--gif-width, --max-mb and --min-tiles must be positive", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="tickvault-gif-") as tmp:
        frames = _record(args, Path(tmp))
        _assemble_gif(frames, args.out, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
