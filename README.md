# TickVault

Production-grade crypto market-data pipeline: L2 + trades from Coinbase, Kraken and Bitfinex → validated, append-only Parquet + DuckDB, with gap detection, checksum validation, a lookahead-safe features table and a QA dashboard.

[![CI](https://github.com/broiisapro/tickvault/actions/workflows/ci.yml/badge.svg)](https://github.com/broiisapro/tickvault/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

TickVault streams level-2 order books and trades from Coinbase, Kraken and Bitfinex, stores every raw message immutably, and turns it into partitioned Parquet you can query with DuckDB. Correctness is the point: it detects sequence gaps and resyncs, validates the CRC32 book checksums published by Kraken and Bitfinex, checks book invariants, and publishes a daily QA report. A derived table of 1-second and 1-minute bars uses only data from before each bar's close, and a lookahead test proves it; an optional Kafka-compatible buffer separates ingestion from writing without losing a message when the writer crashes. A replay of the same raw data always produces byte-identical tables.

**Live dashboard:** https://tickvault.streamlit.app (bundled 30-minute sample; may take a few seconds to wake) · **QA report:** https://broiisapro.github.io/tickvault/

![TickVault dashboard during live ingestion](docs/media/dashboard-live.gif)

## What it demonstrates

- **Immutable raw capture with replay.** Every websocket frame is persisted first (zstd JSONL, write-once) with its receive timestamp, connection id and ingest sequence; replay reads the same bytes through the same normalizer → book → writer path, and no command ever rewrites a raw file (`test_raw_files_unchanged_after_all_commands`).
- **Per-connection sequence-gap detection.** Coinbase Advanced Trade's `sequence_num` is tracked on every message; one missing number emits a `gap` event, moves the symbol to SYNCING and requests a fresh snapshot (`test_single_missing_sequence_is_flagged_and_resynced`); a stale delta from the previous connection is never applied after a resync (`test_no_pre_reconnect_delta_applied_after_resync`).
- **Kraken CRC32 checksum on every book update**, computed from the exact decimal strings. Parsing Kraken's JSON numbers as floats loses trailing zeros (`0.00006120` becomes `6.12e-05`) and the checksum fails — verified against the live feed (`test_checksum_matches_reference_vectors`).
- **Book invariants** after every applied update: crossed book, negative size, duplicate level (`test_crossed_book_is_flagged`, `test_invariant_violation_flags_resync_and_syncing`).
- **Deterministic replay:** two replays of the same raw input produce byte-identical Parquet (`test_two_replays_are_byte_identical`, `test_replay_twice_produces_identical_trees`).
- **QA that fails loudly:** `tickvault qa` exits 1 on any unexplained anomaly or missing book, and writes JSON + Markdown reports per day.
- **Tardis.dev reconciliation** of trades by `trade_id`, fixture-tested; the fixture carries deliberate discrepancies that the report must find.
- **Lookahead-safe features.** `tickvault features` derives 1-second and 1-minute bars — touch, spread, top-5 depth imbalance, trade count, volume, signed volume, VWAP, realized volatility — from the normalized tree, using only rows strictly before each bar's close. `test_features_lookahead.py` deletes every row at or after a cutoff and asserts that every earlier bar is unchanged, for fixed cutoffs and a Hypothesis property over the cutoff.
- **Bitfinex checksum with JavaScript number formatting.** Bitfinex hashes the top 25 levels with numbers rendered the way its JavaScript client renders them (`1e-7`, never `1e-07`; no trailing zeros, `0.50` becomes `0.5`), compared as a signed 32-bit CRC. `test_bitfinex_checksum_matches_reference_vectors` pins the venue's documented example and three live-captured vectors.
- **A buffer that survives a writer crash.** With `ingest --buffer` the ingester publishes the exact raw-layer bytes to Redpanda and `tickvault consume` writes them raw-first; offsets commit only after a flush and the raw reader skips exact duplicate lines, so `test_writer_restart_loses_nothing` and `test_replay_after_writer_crash_equals_single_writer_replay` prove a restarted writer loses no message and replay equals a single-writer run.

## Numbers

All figures are from runs on this machine (MacBook Pro, Apple M5, 32 GB). The
benchmark is a replay of recorded sessions and measures processing headroom;
the live message rate is set by the venues.

| Measurement | Value |
|---|---|
| 30-minute live capture (2026-10-15 17:08–17:38 UTC) | 476,965 raw messages — Coinbase 69,618, Kraken 407,347 |
| Rows written | 26,294 trades, 1,840,670 book deltas, 300,926 book snapshots |
| Anomalies, all explained by resyncs | 3 reconnects → 3 resyncs; 0 gaps, 0 checksum failures, 0 invariant violations, `unexplained 0` |
| Receive lag p50 / p99 | Coinbase 65.7–129.2 / 405.3–23,004.9 ms · Kraken 68.4–68.6 / 190.8–197.6 ms (per symbol; the 23 s p99 is reconnect catch-up) |
| Replay throughput (`tickvault bench`, three runs) | 9.6–10.2k msg/s overall — Coinbase 2.3–2.4k, Kraken 7.3–7.7k; peak RSS 223 MB |
| Live message rate (BTC-USD + ETH-USD, 30-minute capture) | ≈ 265 msg/s (476,965 messages / 1,800 s) |
| `tickvault features` on the bundled sample, 2026-10-15 | 0.8 s wall; 7,197 1-second bars and 124 1-minute bars over 4 venue/symbol pairs (`uv run tickvault features --data dashboard/sample --date 2026-10-15 --out /tmp/tv-features`) |
| 120 s three-venue live ingest, 2026-10-29 | 37,905 raw messages — Coinbase 4,081, Kraken 22,545, Bitfinex 11,279; `tickvault qa` reports unexplained 0 and exits 0 (`uv run tickvault ingest --venues coinbase,kraken,bitfinex --symbols BTC-USD,ETH-USD --duration 120 --out /tmp/tv-readme`) |
| Test suite | 748 tests (`uv run pytest -q`) |

## Architecture

```mermaid
flowchart LR
    A[Coinbase, Kraken and Bitfinex<br/>websocket connectors] -->|RawMessage| B[Raw writer<br/>zstd JSONL, write-once]
    A -.->|ingest --buffer| K[(Redpanda<br/>tickvault.raw)] -.->|consume| B
    B --> C[Normalizers<br/>pure functions]
    C --> D[Book processor<br/>SYNCING and LIVE per symbol]
    D -->|trades, deltas, snapshots, events| E[Parquet writer<br/>hourly partitions]
    E --> F[DuckDB views]
    F --> G[QA report<br/>fails on unexplained anomalies]
    F --> H[Streamlit dashboard]
    F --> I[Features table<br/>1s and 1m bars]
    D -.->|resync request| A
```

The live path writes every venue frame to the raw layer before parsing it, then normalizes it and applies deltas to a per-symbol book. A detected anomaly — sequence gap, checksum mismatch, invariant violation — emits an `events` row and a resync request; the symbol returns to SYNCING, and deltas received in the meantime are dropped and counted on the following `resync`. Replay orders raw lines by receive timestamp, connection and ingest sequence and feeds the same normalizer, processor and writer, which is why its output is byte-identical. With `--buffer`, the ingester publishes those same raw lines to a one-partition topic and `tickvault consume` runs the write path in a separate process; delivery is at-least-once and the writer recognises redelivered lines, so a crash costs at most duplicate work, never a lost message. `tickvault features` and the Dagster daily job derive the bars from the normalized tree, and `--metrics-port` exposes Prometheus counters for the ingester and the writer. Downstream, DuckDB views read the hive-partitioned tree; `tickvault qa` aggregates it per venue/symbol/day and exits non-zero on any anomaly no resync explains. Every venue behaviour claimed above is backed by the live checks recorded in `docs/architecture.md` (§2); the Phase 2 amendments are in §14, and the phase notes live in `docs/epic-00-foundation.md`, `docs/epic-01-feeds-book-storage.md`, `docs/epic-02-pipeline.md`, `docs/epic-03-ship.md`, `docs/epic-04-features.md`, `docs/epic-05-third-venue.md` and `docs/epic-06-buffer-metrics.md`.

## Quick start

```bash
git clone https://github.com/broiisapro/tickvault && cd tickvault
uv sync --frozen                      # Python 3.12 environment managed by uv

uv run pytest -q                      # full suite; live network checks excluded
make bench                            # replay the recorded fixtures, report throughput

uv run tickvault replay --input tests/fixtures/synthetic_gap_session.jsonl.zst --out ./data
uv run tickvault qa --data ./data     # quality table; exits 1 on unexplained anomalies
uv run tickvault sql --data ./data -c "select venue, count(*) from trades group by 1"
uv run tickvault ingest --venues coinbase,kraken,bitfinex --symbols BTC-USD,ETH-USD --out ./data

uv run tickvault features --data dashboard/sample --date 2026-10-15 --out ./data
uv run tickvault sql --data ./data -c "select venue, symbol, round(avg(spread_bps), 3) spread_bps, sum(trade_count) trades from features where resolution = '1m' group by 1, 2 order by 1, 2"

uv run streamlit run dashboard/app.py # dashboard over the bundled dashboard/sample
make demo-live                        # docker compose: live ingester + dashboard on :8501
make demo-buffer                      # + Redpanda writer, Prometheus on :9090, Grafana on :3001
make demo-down
```

Ingestion reads public market data only and needs no API keys.

## CLI

| Command | Purpose |
|---|---|
| `tickvault ingest` | stream live venue data into the raw and normalized layers (`--duration`, `--max-gb`, `--buffer`, `--metrics-port`) |
| `tickvault replay` | rebuild the normalized layer from raw files deterministically |
| `tickvault qa` | per venue/symbol/day quality table; exit 1 on unexplained anomalies (`--json`, `--reports`) |
| `tickvault compact` | merge a date's hourly partitions, verify row counts, write a manifest |
| `tickvault sql` | DuckDB views over the normalized tree; `-c` runs one query |
| `tickvault reconcile` | compare captured trades against a Tardis.dev slice (`--tardis-date` or `--fixture`) |
| `tickvault bench` | replay the recorded fixtures and report msg/s and peak RSS |
| `tickvault consume` | write a buffered raw stream into the raw and normalized layers (`--buffer`, `--metrics-port`) |
| `tickvault features` | compute 1-second and 1-minute feature bars for one UTC date (`--data`, `--date`, `--out`, `--resolution`) |
| `tickvault verify` | check a published manifest against the files it describes (local path or URL) |

## Data layout

```
raw/venue=<v>/date=<YYYY-MM-DD>/hour=<HH>/<connection_id>-<rot:05d>.jsonl.zst
norm/<table>/venue=<v>/date=<YYYY-MM-DD>/hour=<HH>/part-<connection_id>-<first ingest_seq:012d>.parquet
features/resolution=<1s|1m>/venue=<v>/date=<YYYY-MM-DD>/part-<symbol>.parquet
```

Four tables under `norm/`: `trades`, `book_deltas`, `book_snapshots` and `events` (plus `manifests/` and `reports/`). Prices and sizes are `decimal(38,18)`; timestamps are int64 nanoseconds UTC parsed by one RFC 3339 parser for 0–9 fractional digits (`test_iso8601_and_rfc3339_parse_to_same_ns`); every row carries `venue`, `symbol`, `exchange_ts_ns`, `recv_ts_ns`, `connection_id` and `ingest_seq`. See `docs/architecture.md` §5–7. `features/` is derived output — one file per resolution, venue, date and symbol, not one of the four normalized tables, so compaction, manifests and QA ignore it and it can be deleted and rebuilt at any time.

## Testing and CI

`uv run pytest -q` runs the suite offline; checks that talk to the public venues are marked `network` and excluded by default (`uv run pytest -m network` to opt in). CI runs `ruff`, `mypy` (strict) and `pytest` as three required jobs on GitHub-hosted `ubuntu-24.04-arm` runners (`.github/workflows/ci.yml`).

## Limitations

- Three venues (Coinbase, Kraken, Bitfinex); reconciliation is fixture-tested against Coinbase only — Kraken, Bitfinex and a real Tardis capture are not covered yet.
- The hosted dashboard serves the fixed 30-minute sample, not a live feed, and has no features view yet.
- The QA page (GitHub Pages) shows the report for the bundled sample; a daily report from always-on capture is Phase 3.
- The buffer and metrics stack (`make demo-buffer`) is a local Compose demo, not an always-on deployment. Docker is not part of CI, so the Compose path is checked on this machine rather than by CI.
- 1-second feature bars take book state from the processor's once-per-second samples, so the touch can be up to about a second old and `realized_vol` on a 1-second bar is a single return; the 1-minute bars are the ones to read volatility from.
- Bitfinex's sequence-number flag (`SEQ_ALL`) is documented by the venue as beta; if it stopped being honoured, gaps would go undetected rather than false-alarm.
- No always-on ingestion; the live Tardis reconciliation of a real capture is planned for 2026-11-01 (v1.0.1).

## Roadmap

- **Phase 2 (v1.1.0, complete):** 1-second and 1-minute feature bars with a lookahead test, Bitfinex as the third venue, and a Redpanda buffer with Prometheus/Grafana metrics.
- **Phase 3:** always-on ingestion, published daily samples checked by `tickvault verify`, and public QA history.

## License

MIT — see [LICENSE](LICENSE).
