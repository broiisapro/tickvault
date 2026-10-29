# TickVault architecture

Status: Phase 2 release (v1.1.0).

## 1. What it is

TickVault streams level-2 order books and trades from Coinbase and Kraken public websockets, stores every received message immutably (raw layer), and turns it into hourly-partitioned Parquet (normalized layer) queryable with DuckDB. Correctness is the point: sequence-gap detection with resync, Kraken CRC32 book checksums on every update, book invariants, deterministic replay, and a QA report that explains every anomaly.

## 2. Verified venue facts (checked live on 2026-10-14/15 UTC)

**Coinbase.**
- Exchange feed (`wss://ws-feed.exchange.coinbase.com`): `level2` now returns "level2, level3, and full channels now require authentication". `level2_batch` and `level2_50` still work without auth, but their `snapshot`/`l2update` messages carry **no sequence number** (only `matches` does, and `matches` sequences skip by design because they are a subset of the full feed). So the Exchange feed cannot drive FR-1.4.
- Advanced Trade public feed (`wss://advanced-trade-ws.coinbase.com`): `level2`, `market_trades` and `heartbeats` work without auth. Every message carries `sequence_num`, a **per-connection** counter shared by all channels on that connection (starts at 0, +1 per message). `level2` sends `l2_data` with event `type: "snapshot"` (full depth, ~40k levels for BTC-USD, several MB in one frame) then `type: "update"`; each update entry has `side` (`bid`/`offer`), `event_time` (RFC3339, microseconds), `price_level`, `new_quantity` (absolute; `"0"` removes). `market_trades` sends a `snapshot` event of the last ~100 trades on subscribe, then `update` events (newest first within a message) with `trade_id`, `price`, `size`, `time`, `side` (`BUY`/`SELL`). Message `timestamp` has **nanosecond** precision (9 fractional digits).
- **Locked:** Advanced Trade feed, channels `level2` + `market_trades` + `heartbeats`.

**Kraken WS v2** (`wss://ws.kraken.com/v2`), no auth.
- `book` channel: `snapshot` then `update` messages; `data` is a list of per-symbol entries with `bids`/`asks` (`price`, `qty` as **JSON numbers**), `checksum` (CRC32, unsigned), `timestamp` (RFC3339, microseconds; on updates). Supported depths 10/25/100/500/1000. No sequence numbers.
- `trade` channel: `update` messages with `side` (taker), `price`, `qty`, `ord_type`, `trade_id`, `timestamp`. Subscribe with `snapshot: false`.
- `status` and `heartbeat` messages also arrive; v2 symbols are `BTC/USD` (not `XBT`).
- Checksum algorithm (docs: `docs.kraken.com/exchange/guides/websockets/book-checksum-v2`): after applying all levels in a message and truncating to subscribed depth, take top 10 asks (ascending) then top 10 bids (descending); for each level, remove the `.` from price and qty text and strip leading zeros, concatenate price+qty; CRC32 of asks-string + bids-string as unsigned 32-bit.
- **The classic bug is real:** parsing Kraken numbers as floats loses trailing zeros (`0.00006120` → `6.12e-05`) and the checksum fails. Verified on a live BTC/USD depth-10 snapshot: raw-text digits give `1264451060` (matches), float parsing gives `3408421861` (mismatch). Prices and sizes must be decoded from the JSON text as `Decimal`.

**Tardis.dev.** `https://datasets.tardis.dev/v1/coinbase/trades/YYYY/MM/01/BTC-USD.csv.gz` downloads without a key on the first day of each month (verified 2026-10-01). Columns: `exchange,symbol,timestamp,local_timestamp,id,side,price,amount`; timestamps are **microseconds** UTC; `id` is the Coinbase `trade_id` (same id space as Advanced Trade `trade_id`); `side` is the taker side per Tardis's normalization.

## 3. Module map (package `tickvault/`)

| Module | Owner (Phase 1) | Purpose |
|---|---|---|
| `models.py` | foundation (frozen) | `RawMessage`, record dataclasses, `NormalizedMessage`, Arrow schemas, enums |
| `paths.py` | foundation | `partition_path(root, table, venue, ts_ns)`, raw path helper |
| `timeutil.py` | foundation | `parse_rfc3339_ns(text) -> int` (0–9 fractional digits, `Z` or offset), ns helpers |
| `rawio.py` | foundation | encode/decode raw envelope lines; read raw files/dirs in canonical order |
| `connectors/base.py` | foundation | `Connector` protocol |
| `connectors/coinbase.py`, `connectors/kraken.py` | WS-A, WS-B | live websocket clients |
| `normalize/coinbase.py`, `normalize/kraken.py` | WS-A, WS-B | pure `RawMessage -> NormalizedMessage` |
| `book/sequence.py` | foundation | `SequenceTracker` |
| `book/orderbook.py` | interface: foundation; impl: WS-B | price-level `OrderBook` |
| `book/processor.py` | interface: foundation; impl: WS-B | per-(venue, symbol) state machine, gap/checksum/invariant handling, event emission |
| `book/kraken_checksum.py`, `book/invariants.py` | WS-B | CRC32 checksum, invariant checks |
| `storage/sink.py` | foundation | `Sink` protocol |
| `storage/raw_writer.py`, `parquet_writer.py`, `compact.py`, `manifest.py`, `sql.py` | WS-C | storage |
| `qa/`, `reconcile/` | WS-D | QA metrics/report, Tardis reconciliation |
| `pipeline.py`, `cli.py` (bodies), `bench.py` | WS-E | wiring live + replay, CLI |
| `orchestration/` (top-level package dir) | Epic 2 | Dagster definitions |
| `dashboard/app.py` | Epic 2 | Streamlit |

## 4. Data flow

```
Connector ──RawMessage──► RawWriter (persist first, always)
     ▲                         │
     │ resync requests         ▼
     │                    Normalizer (pure) ──NormalizedMessage──► BookProcessor ──rows──► ParquetWriter (Sink)
     └─────────────────────────────────────────── resync_requests ◄─┘   (trades, book_deltas, book_snapshots, events)
```

Replay reads raw files in canonical order (§6) and feeds the same Normalizer → BookProcessor → ParquetWriter path. Resync requests are ignored in replay; the snapshot that the live system obtained is already in the raw data.

## 5. Raw layer

- Path: `raw/venue=<v>/date=<YYYY-MM-DD>/hour=<HH>/<connection_id>-<rot:05d>.jsonl.zst`, date/hour from `recv_ts_ns` (UTC). `rot` increments at each hourly rotation within a connection.
- One zstd frame per file; the writer flushes the stream at least every 1 s (`FLUSH_BLOCK`) so a crash loses at most ~1 s. Readers tolerate a truncated final frame/line (count and report it, never crash).
- Each line is one UTF-8 JSON envelope, keys in exactly this order:
  `{"v":1,"venue":"kraken","connection_id":"kraken-20261015T031901Z-3fa2c1d0","ingest_seq":17,"recv_ts_ns":1792034341558201000,"kind":"msg","payload":"<websocket text frame exactly as received>"}`
  - `payload` is the frame as a JSON **string** (escaped), never re-encoded, so the venue bytes are preserved exactly.
  - `kind` is `"msg"` for venue frames or `"control"` for TickVault lifecycle records whose payload is a JSON object string: `{"event":"connect","url":...,"subscriptions":[...]}`, `{"event":"disconnect","reason":...}`, `{"event":"resync_request","symbol":...,"reason":...}`.
  - `ingest_seq` is a 0-based per-connection ordinal over all lines (msg and control).
- `connection_id` = `<venue>-<UTC connect time %Y%m%dT%H%M%SZ>-<8 lowercase hex random>`; generated only live, read from raw in replay.
- No TickVault command opens a closed raw file for writing. Tests hash raw files before/after `replay`, `compact`, `qa`, `reconcile`.

## 6. Canonical order and determinism

- Replay orders lines globally by `(recv_ts_ns, connection_id, ingest_seq)`.
- Nothing in normalized output may depend on wall-clock time, dict iteration of unordered sources, randomness, or process state. Within one venue message, emitted records keep venue order, except `market_trades` updates, which are emitted in ascending `trade_id`.
- Parquet flushing uses a **logical clock** (`recv_ts_ns` of the records): a partition buffer flushes when logical time advances ≥ 60 s past the buffer's first record or the buffer exceeds 200,000 rows, and on close. Live mode adds an idle wall-clock flush (no records for 60 s), which affects only live, never replay.
- Normalized file name: `part-<connection_id>-<first ingest_seq:012d>.parquet` (deterministic in replay).
- Parquet writes: zstd, no pandas metadata, fixed row-group size, `write_statistics=True`; pyarrow version pinned by `uv.lock` so bytes are reproducible.
- Determinism check: two replays of the same input produce byte-identical `norm/` trees (`diff -r`).

## 7. Normalized layer and schemas (defined only in `models.py`)

Partition path: `norm/<table>/venue=<v>/date=<YYYY-MM-DD>/hour=<HH>/` keyed by **exchange timestamp** hour (`exchange_ts_ns`; `events` without an exchange time use `recv_ts_ns`). Partition columns are not stored inside files (hive partitioning).

Common columns: `venue` (string), `symbol` (string, `BASE-QUOTE`), `exchange_ts_ns` (int64), `recv_ts_ns` (int64), `connection_id` (string), `ingest_seq` (int64). Prices and sizes are `decimal128(38, 18)` (exact; analytics cast to DOUBLE in DuckDB).

| Table | Extra columns |
|---|---|
| `trades` | `trade_id` (string), `price`, `size`, `side` (`buy`/`sell`, **taker** side), `sequence` (int64, nullable; Coinbase `sequence_num`) |
| `book_deltas` | `side` (`bid`/`ask`), `price`, `size` (new absolute size; 0 = level removed), `sequence` (nullable) |
| `book_snapshots` | `snapshot_id` (string = `connection_id:ingest_seq`), `source` (`venue` = venue-sent snapshot, `sample` = TickVault top-10 sample), `side`, `level` (int16, 0 = best), `price`, `size` |
| `events` | `event_type` (`gap`, `resync`, `checksum_failure`, `invariant_violation`, `reconnect`, `disconnect`), `symbol` nullable, `exchange_ts_ns` nullable, `channel` nullable, `expected_seq`/`received_seq` (int64 nullable), `detail` (string: JSON with sorted keys) |

`book_deltas` contains only deltas applied to a LIVE book. Deltas dropped while SYNCING are counted in the `detail.deltas_dropped` of the following `resync` event. Market-trade snapshot events (Coinbase subscribe backfill) are kept in raw but not emitted to `trades`.

## 8. Book processing

- `SequenceTracker` (foundation): per key, `observe(key, seq)` → `FIRST | OK | GAP(expected, received) | STALE(received)`; `reset(key)`. Gap iff `received > expected`; `expected = last + 1` (the off-by-one test: one missing number is exactly one GAP with `expected = n+1, received = n+2`).
- `BookProcessor` holds per-(venue, symbol) state: `SYNCING` or `LIVE`, the authoritative `connection_id`, and an `OrderBook`.
  - Snapshot from the authoritative connection → replace book, `LIVE`; if the state was SYNCING because of an anomaly, emit `resync` (with `reason`, `deltas_dropped`).
  - Delta while `SYNCING` → dropped (counted). Delta from a non-authoritative (older) connection → dropped, always. This is the reconnect race guard.
  - Coinbase: `sequence` is tracked per `connection_id`. A gap emits `gap` and moves every symbol on that connection to SYNCING, and emits a resync request (live: unsubscribe+resubscribe `level2` for those products).
  - Kraken: after applying a message's levels and truncating to subscribed depth (25), compute the checksum; mismatch → `checksum_failure`, SYNCING, resync request (live: unsubscribe+resubscribe `book` for that symbol).
  - Invariants after every applied update: best bid < best ask, no negative size, no duplicate levels → `invariant_violation`, SYNCING, resync request.
  - Control `connect` with a new `connection_id` for a venue → that connection becomes authoritative for its symbols, symbols go SYNCING, emit `reconnect` (not for the very first connection of a venue in a session). Control `disconnect` → `disconnect` event.
  - While LIVE, emit a `book_snapshots` `sample` (top 10 per side) for each symbol every 1 s of exchange time (first update at or after each whole second). This feeds the dashboard's top-of-book and spread/mid charts without rebuilding books in the UI.
- **Unexplained anomaly** (QA): a `gap`, `checksum_failure` or `invariant_violation` for (venue, symbol) with no later `resync` for the same (venue, symbol) in the data.

## 9. Connectors

- `websockets` ≥ 17, `from websockets.asyncio.client import connect` (the legacy `websockets.client` API is not used). `max_size` ≥ 32 MiB (Coinbase snapshots are several MB).
- One Coinbase connection per product carrying `level2`, `market_trades`, `heartbeats`, so a sequence gap scopes to one product. One Kraken connection for all symbols (`book` depth 25 + `trade` with `snapshot:false`).
- Reconnect with exponential backoff 1, 2, 4 … capped at 30 s, with jitter; each new connection writes a `connect` control line first.
- Coinbase: verify (and record) whether re-subscribing `level2` on the same connection yields a fresh snapshot; if not, resync = reconnect.

## 10. Storage, QA, reconcile, orchestration, dashboard

- `ParquetWriter` implements `Sink` (`write(records)`, `flush()`, `close()`); SIGTERM/SIGINT → flush and close all files (tested).
- `compact --date d`: per table/venue/date/hour, merge files into one, verify row counts equal before deleting inputs, write `manifest.json` (file list, row counts, SHA-256) per date. Compaction never touches `raw/`.
- `sql`: DuckDB views `trades`, `book_deltas`, `book_snapshots`, `events` via `read_parquet('<root>/norm/<t>/**/*.parquet', hive_partitioning=true)`; `-c` runs a query and prints results; without `-c` prints the view DDL.
- QA: DuckDB/Polars over Parquet, per venue/symbol/day: messages, trades, deltas, snapshots, gaps, resyncs, checksum failures, reconnects, invariant violations, lag p50/p99, coverage, unexplained; exit 1 if unexplained > 0. JSON + Markdown renderers.
- Reconcile: `--tardis-date/--venue/--symbol` (download with `httpx`) and `--fixture/--data` offline; match by `trade_id`; report counts, unmatched ids on each side, price/size/side mismatches.
- Dagster: three daily-partitioned assets (`compacted_partitions` → `qa_report` → `manifest`) calling the same functions as the CLI; run locally with `dagster asset materialize` (no daemon needed); `dagster dev` only for the UI.
- Streamlit dashboard reads DuckDB views with filters (never loads whole tables); works on a live data dir or the bundled sample.
- Docker Compose: `ingester` and `dashboard` services, `linux/arm64`, `python:3.12-slim`, data in a named volume; Dagster optional behind a profile.

## 11. Dependencies (declared once in Epic 0)

Runtime: `websockets>=17.2`, `msgspec>=0.22`, `zstandard>=0.25`, `pyarrow>=26`, `polars>=1.30,<2` (Polars 2.0 shipped 2026-10-06 with row-order and dtype changes; stay on 1.x for Phase 1), `duckdb>=1.5.6,<2`, `typer>=0.27`, `sortedcontainers>=2.4`, `httpx>=0.28`, `prometheus-client>=0.26.0`.
Groups: `dev` = `pytest>=9.1`, `hypothesis>=6.169`, `pytest-asyncio`, `ruff>=0.17`, `mypy>=2.4` (+ stubs as needed); `orchestration` = `dagster>=1.13.26,<2`, `dagster-webserver`; `dashboard` = `streamlit>=1.66`; `buffer` = `aiokafka>=0.14,<0.15`. `[tool.uv] default-groups = ["dev", "orchestration", "dashboard", "buffer"]` so `uv sync --frozen` gives a full dev environment; Docker images sync only what they need.
Only one agent per wave may change `pyproject.toml`/`uv.lock`.

## 12. Amendments after Epic 1

- **Per-symbol connection scope.** Each `connect` control record lists the normalized symbols the connection serves (`"symbols": [...]`). The processor makes a new connection authoritative only for those symbols and emits `reconnect` only for symbols that already had an authoritative connection. Recordings without the field fall back to the first connection that delivers a message for a symbol.
- **QA failure conditions.** `tickvault qa` exits 1 when any venue/symbol/day has an unexplained anomaly or a missing book (messages but no venue snapshot). With `--date`, a `resync` within 300 s after midnight still explains a late anomaly of the previous day.
- **Streaming replay.** The raw reader merges files lazily in canonical order, so replay memory is bounded by the number of open files, not the size of the raw tree.
- **Layout of generated outputs.** Daily QA reports: `<data>/reports/qa-<date>.json|md`. Manifests: `<data>/manifests/date=<date>/manifest.json`. Bundled dashboard sample: `dashboard/sample/` (a data root).
- **Pipeline module.** `tickvault/pipeline.py` provides `run_replay` and `run_live`; every CLI command is a thin wrapper over the pipeline or an Epic 1 function, and Dagster assets call the same functions.

## 13. Amendments for the Phase 1 release

- **Bounded-memory replay.** The raw reader streams each file. Files written in canonical order (the normal case) are passed through directly; a file with an out-of-order line is sorted with an external merge sort over bounded runs in temporary files. Memory no longer depends on file size, and the output order is unchanged.
- **Observable ingestion.** `tickvault ingest` logs a start line, one progress line per interval (default 10 s: elapsed time, raw messages per venue, trades, book deltas, anomalies, resyncs, bytes written) and a final summary, all through Python logging to stderr, so `docker compose logs ingester` shows the run.
- **Disk guard without directory walks.** The raw and Parquet writers count the bytes they write. `--max-gb` compares the data root's size at start-up plus those counters with the limit.
- **Clean shutdown.** Connectors and the live pipeline cancel and await every task they start; closing is idempotent, and a venue closing the socket during shutdown is not an error.
- **Bundled sample.** `dashboard/sample/` is a replay of a 30-minute real capture of BTC-USD and ETH-USD on both venues, compacted with manifests, under 60 MB and QA-clean. The raw capture itself is not published in Phase 1.
- **Hosting.** The dashboard runs on Streamlit Community Cloud from `dashboard/app.py`, installing `dashboard/requirements.txt`, which is exported from `uv.lock` and checked by a test. The QA site is static HTML built by `scripts/build_qa_site.py` (index, one page per day, `latest.json`) and deployed to GitHub Pages by `.github/workflows/pages.yml`. CI runs `lint`, `types` and `tests` on GitHub-hosted `ubuntu-24.04-arm` runners, using the image's system CPython 3.12.

## 14. Amendments for Phase 2 (v1.1.0)

- **Venue registry.** `tickvault/venues.py` holds one `VenueSpec` per venue (normalizer, connector, optional checksum function and depth). The pipeline's normalizer map, the connector factories, the CLI's `--venues` choices and the book processor's checksum map are all derived from it, so adding a venue is one registry entry plus the venue's own modules.
- **Third venue: Bitfinex.** Public websocket v2, one connection per symbol (`book` P0 depth 25 + `trades`), `conf` flags for per-connection sequence numbers, CRC32 book checksums and millisecond timestamps. The checksum interleaves the top 25 bid and ask levels and renders numbers the way JavaScript does, so it is computed from a JavaScript-compatible number formatter rather than from the venue text. Bitstamp was not used because its public book feed carries neither sequence numbers nor checksums. Details: `docs/epic-05-third-venue.md`.
- **Checksum depth per venue.** The processor truncates each checksum-verified book to that venue's depth and passes that many levels to its checksum function (Kraken hashes its top 10 of 25, Bitfinex all 25).
- **Features table.** `tickvault features` (and the Dagster `features` asset) writes 1-second and 1-minute bars to `<root>/features/resolution=<r>/venue=<v>/date=<d>/part-<symbol>.parquet`: mid, spread, top-5 depth imbalance, trades, volume, signed volume, VWAP, realized volatility. A bar `[start, end)` uses only rows with exchange time strictly before `end`; a lookahead test deletes all data after a cutoff and checks every earlier bar is unchanged. Derived values are float64; `features` is not one of the four normalized tables. DuckDB exposes a `features` view when the tree exists. Details: `docs/epic-04-features.md`.
- **Optional buffer between ingestion and writing.** With `ingest --buffer HOST:PORT` the ingester publishes every raw envelope (the exact raw-layer bytes) to a Kafka-API topic (`tickvault.raw`, one partition) and `tickvault consume` writes it: raw first, then the unchanged normalizer → processor → Parquet path. Offsets are committed only after the raw writer flushed; delivery is at-least-once, the writer skips records it already wrote, and the raw reader skips exact duplicate lines, so a replay after a writer crash equals a replay of the same messages written once. A restarted writer asks the ingester for fresh snapshots over `tickvault.control`. Without `--buffer`, ingestion is exactly the Phase 1 path. Redpanda runs under the Compose profile `buffer`.
- **Metrics.** `--metrics-port` serves Prometheus metrics (`tickvault_messages_total`, `rows_total`, `events_total`, `book_live`, `last_message_age_seconds`, `raw_bytes_total`, `normalize_errors_total`, writer `buffer_consumed_total` and `buffer_lag_messages`). The Compose profile `metrics` adds Prometheus and a provisioned Grafana dashboard. Details: `docs/epic-06-buffer-metrics.md`.
- **Published samples.** `scripts/build_public_sample.py` cuts one symbol and one hour of a day into a size-capped tree with a manifest and `SHA256SUMS`; `tickvault verify --manifest <path|url>` checks a published sample against its manifest (size, SHA-256, Parquet row counts). Publishing itself is Phase 3.
