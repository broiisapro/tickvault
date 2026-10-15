#!/usr/bin/env python3
"""Record a live venue websocket session into a TickVault raw fixture.

This is the only network-touching script in the repository. It connects to a
public venue websocket, subscribes to the channels TickVault ingests, and
writes every received frame, unmodified, as one raw envelope line per frame in
a single zstd-compressed JSONL file (architecture doc, section "Raw layer").

Envelope key order is fixed and matches the raw layer contract:

    {"v":1,"venue":...,"connection_id":...,"ingest_seq":...,"recv_ts_ns":...,
     "kind":"msg"|"control","payload":"<frame text exactly as received>"}

The envelope writer is implemented here on purpose: this script must stay
standalone (it is not imported by ``tickvault`` and must not import it).

Usage:
    scripts/record_fixture.py --venue coinbase --symbol BTC-USD \
        --seconds 300 --out tests/fixtures/coinbase_btc_5m.jsonl.zst
    scripts/record_fixture.py --venue kraken --symbol BTC-USD \
        --seconds 300 --out tests/fixtures/kraken_btc_5m.jsonl.zst
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import secrets
import sys
import time
from pathlib import Path
from typing import Any

import zstandard
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

COINBASE_URL = "wss://advanced-trade-ws.coinbase.com"
KRAKEN_URL = "wss://ws.kraken.com/v2"

# Coinbase sends the full BTC-USD level2 snapshot (tens of thousands of
# levels) in a single multi-MB text frame.
MAX_SIZE = 64 * 1024 * 1024

ZSTD_LEVEL = 10


def utc_stamp(ns: int) -> str:
    """Format a Unix timestamp in nanoseconds as an RFC3339 UTC string."""
    return dt.datetime.fromtimestamp(ns // 1_000_000_000, tz=dt.UTC).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def connection_id(venue: str, connect_ns: int) -> str:
    """Build ``<venue>-<UTC connect time>-<8 lowercase hex random>``."""
    stamp = utc_stamp(connect_ns).replace("-", "").replace(":", "")
    return f"{venue}-{stamp}-{secrets.token_hex(4)}"


class RawRecorder:
    """Append raw envelope lines to a single zstd frame."""

    def __init__(self, out: Path, venue: str, connection_id: str) -> None:
        self.venue = venue
        self.connection_id = connection_id
        self.msg_lines = 0
        self.control_lines = 0
        self.payload_bytes = 0
        self.out = out
        out.parent.mkdir(parents=True, exist_ok=True)
        self._fh = out.open("wb")
        self._writer = zstandard.ZstdCompressor(level=ZSTD_LEVEL).stream_writer(
            self._fh, closefd=False
        )
        self._ingest_seq = 0

    def _write_line(self, kind: str, payload: str, recv_ts_ns: int) -> None:
        record = {
            "v": 1,
            "venue": self.venue,
            "connection_id": self.connection_id,
            "ingest_seq": self._ingest_seq,
            "recv_ts_ns": recv_ts_ns,
            "kind": kind,
            "payload": payload,
        }
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        self._writer.write(line.encode("utf-8") + b"\n")
        self._ingest_seq += 1

    def write_control(self, recv_ts_ns: int, **fields: Any) -> None:
        payload = json.dumps(fields, separators=(",", ":"), ensure_ascii=False)
        self._write_line("control", payload, recv_ts_ns)
        self.control_lines += 1

    def write_msg(self, frame: str, recv_ts_ns: int) -> None:
        self._write_line("msg", frame, recv_ts_ns)
        self.msg_lines += 1
        self.payload_bytes += len(frame.encode("utf-8"))

    def close(self) -> None:
        self._writer.close()
        self._fh.close()


def coinbase_subscriptions(symbol: str) -> list[dict[str, Any]]:
    return [
        {"type": "subscribe", "product_ids": [symbol], "channel": "heartbeats"},
        {"type": "subscribe", "product_ids": [symbol], "channel": "level2"},
        {"type": "subscribe", "product_ids": [symbol], "channel": "market_trades"},
    ]


def kraken_subscriptions(symbol: str) -> list[dict[str, Any]]:
    pair = symbol.replace("-", "/")
    return [
        {"method": "subscribe", "params": {"channel": "book", "symbol": [pair], "depth": 25}},
        {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": [pair], "snapshot": False},
        },
    ]


async def record(
    url: str, subscriptions: list[dict[str, Any]], seconds: float, recorder: RawRecorder
) -> bool:
    """Connect, subscribe and record frames until ``seconds`` elapse.

    Returns True on a clean duration-limited recording.
    """
    clean = False
    async with connect(url, max_size=MAX_SIZE, open_timeout=15) as ws:
        connect_ns = time.time_ns()
        recorder.write_control(connect_ns, event="connect", url=url, subscriptions=subscriptions)
        for payload in subscriptions:
            await ws.send(json.dumps(payload, separators=(",", ":")))
        deadline = time.monotonic() + seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                clean = True
                break
            try:
                frame = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except TimeoutError:
                clean = True
                break
            recv_ns = time.time_ns()
            if isinstance(frame, bytes):
                frame = frame.decode("utf-8")
            recorder.write_msg(frame, recv_ns)
    return clean


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--venue", required=True, choices=["coinbase", "kraken"])
    parser.add_argument("--symbol", required=True, help="venue-agnostic symbol, e.g. BTC-USD")
    parser.add_argument("--seconds", type=float, required=True, help="recording duration")
    parser.add_argument("--out", type=Path, required=True, help="output .jsonl.zst path")
    args = parser.parse_args(argv)
    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    venue = args.venue
    if venue == "coinbase":
        url = COINBASE_URL
        subscriptions = coinbase_subscriptions(args.symbol)
    else:
        url = KRAKEN_URL
        subscriptions = kraken_subscriptions(args.symbol)

    connect_ns = time.time_ns()
    recorder = RawRecorder(args.out, venue, connection_id(venue, connect_ns))
    exit_code = 1
    reason = "error"
    try:
        clean = asyncio.run(record(url, subscriptions, args.seconds, recorder))
        reason = "duration_elapsed" if clean else "connection_closed"
        exit_code = 0 if clean else 1
    except ConnectionClosed as exc:
        reason = f"connection_closed: code={exc.rcvd.code if exc.rcvd else None}"
        exit_code = 1
    except KeyboardInterrupt:
        reason = "interrupted"
        exit_code = 130
    finally:
        recorder.write_control(time.time_ns(), event="disconnect", reason=reason)
        recorder.close()

    size = args.out.stat().st_size
    duration = (time.time_ns() - connect_ns) / 1e9
    print(
        f"{args.out}: {recorder.msg_lines} msg lines, "
        f"{recorder.control_lines} control lines, "
        f"{recorder.payload_bytes / 1e6:.2f} MB payload, "
        f"{size / 1e6:.2f} MB compressed, {duration:.1f}s wall time"
    )
    if exit_code != 0:
        print(f"recording ended early ({reason}); re-record before using it", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
