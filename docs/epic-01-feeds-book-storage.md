# Epic 1 — Feeds, book and storage

## Goal

Turn the Epic 0 contracts into a working end-to-end data path: stream level-2
order books and trades from the Coinbase Advanced Trade and Kraken WS v2 public
websockets, persist every received frame immutably (raw layer), normalize the
frames into hourly-partitioned Parquet (normalized layer), and validate the
data — sequence gaps for Coinbase, CRC32 book checksums for Kraken, and book
invariants for both — with deterministic replay, compaction, SHA-256 manifests,
DuckDB views, a QA report and Tardis reconciliation.

Epic 1 ships the domain code and its tests. Pipeline wiring, CLI command
bodies, Dagster, the dashboard and Docker are Epic 2 and are explicitly out of
scope here; the public interfaces they will wire are listed below.

## How it works

### Connectors

Both connectors implement the `Connector` protocol
(`tickvault/connectors/base.py`) and emit `RawMessage` envelopes, never
normalized records. They share the helpers in `tickvault/connectors/_ws.py`:

- `make_connection_id(venue, connect_ns, ...)` produces
  `<venue>-<%Y%m%dT%H%M%SZ UTC>-<8 lowercase hex>` (for example
  `kraken-20261015T031901Z-3fa2c1d0`). Every reconnect gets a fresh id.
- `RawEmitter` stamps `recv_ts_ns` from an injectable clock at the moment a
  frame is received and numbers `ingest_seq` from 0, counting control lines as
  well as venue frames, so a connection's stream is gap-free by construction.
- `Backoff` doubles from 1 s to a 30 s cap with jitter drawn from an injectable
  RNG; jitter only shortens a delay, never lengthens it, so a recorded schedule
  can be exactly reproduced with `jitter=0`.
- First line of every established connection is the `connect` control record
  (`{"event":"connect","url":...,"subscriptions":[...]}`); the last one is
  `disconnect` with `reason="closed"` when `close()` was called and
  `"connection_lost"` otherwise. A failed connect emits no `disconnect`
  because no connection existed.
- Frames are stored exactly as received (bytes decoded as UTF-8), with no
  re-encoding; the normalizers are the only layer that interprets them.

`CoinbaseConnector` (`tickvault/connectors/coinbase.py`) opens one websocket
**per product** against `wss://advanced-trade-ws.coinbase.com`, subscribing to
`heartbeats`, `level2` and `market_trades` in that order (`max_size` 64 MiB,
since a BTC-USD level2 snapshot is several megabytes). A resync request
enqueues a per-product command; the product's own connection task writes a
`resync_request` control line and then sends unsubscribe+subscribe for
`level2` on the same socket: a live check confirmed that re-subscribing
`level2` on the same connection returns a fresh full-depth snapshot within
about half a second, so a processor anomaly never forces a reconnect. Each
product task reconnects with the shared backoff; `close()` wakes a task
sleeping in backoff for prompt shutdown, and `stream()` is one-shot.

`KrakenConnector` (`tickvault/connectors/kraken.py`) uses a **single**
connection for all symbols against `wss://ws.kraken.com/v2`, subscribing to
`book` at depth 25 and `trade` with `snapshot:false`, both listing every
configured symbol. Symbols are translated to Kraken's spelling only on the
wire (`BTC-USD` → `BTC/USD`); the API and every record keep the normalized
`BASE-QUOTE` form. A `success:false` subscribe acknowledgement is logged,
stored as an ordinary frame, and then treated as a connection failure — a
fresh subscribe is the only way to obtain a snapshot, and the backoff bounds
the retry rate. Resync sends unsubscribe+subscribe for `book` on the live
socket; the `depth` parameter must be repeated on the unsubscribe (Kraken
matches subscriptions by all parameters, and omitting it fails with
`Subscription with depth 10 not Found`, followed by `Already subscribed`).

### Normalizers

Normalizers are pure functions `normalize(raw: RawMessage) -> NormalizedMessage`
that raise `NormalizeError` (a `ValueError`) on malformed payloads. Unknown
channels and venue housekeeping frames normalize to a `NormalizedMessage` with
no records; when the venue provides a sequence number it is still attached so
the tracker sees every message.

`tickvault/normalize/coinbase.py`:

- Every `msg` frame must carry `sequence_num` (a missing one would silently
  punch a hole in gap detection); the value rides on the normalized message
  for every channel, including `heartbeats` and subscription acknowledgements,
  which emit no records.
- `l2_data` events produce one `BookUpdate` per event; `type:"snapshot"` maps
  to `is_snapshot=True` with the message-level `timestamp` as the exchange
  time, and `type:"update"` levels are grouped into one `BookUpdate` per
  distinct `event_time`, so a multi-product frame can never merge products.
  `offer` maps to `ask`; `new_quantity` is stored as the absolute size and
  `"0"` means removal (applied by the book).
- `market_trades` rows are emitted in ascending integer `trade_id`.
  `type:"snapshot"` (the subscribe backfill) is kept in the raw layer but
  produces no rows. Prices and sizes are parsed as `Decimal` from the JSON
  **strings**, never through `float`.
- The venue (both Coinbase feeds) reports the **maker** side; the normalized
  `side` is the **taker** side, so the values are inverted
  (`BUY → sell`, `SELL → buy`), with the evidence recorded below.
- Symbols must match `^[A-Z0-9]+-[A-Z0-9]+$` and pass through unchanged.

`tickvault/normalize/kraken.py`:

- The payload is decoded with `json.loads(..., parse_float=Decimal)`, so even
  JSON numbers keep the venue's exact digits; `_decimal` accepts only `int`,
  `str` and `Decimal` and rejects `float` outright. This is what makes
  `0.00006120` survive (a float parse turns it into `6.12e-05` and every
  checksum fails).
- `book` snapshot/update produce one `BookUpdate` per `data` entry (one per
  symbol), preserving venue order and **all** levels. The checksum is
  required on every entry and stored on `BookUpdate.checksum`. A missing
  timestamp falls back to the envelope's `recv_ts_ns`; present timestamps are
  parsed with `parse_rfc3339_ns`.
- `trade` rows preserve message order; Kraken's `side` is already the taker
  side and is mapped unchanged. A trade message with any `type` other than
  `update` is an error (the connector subscribed with `snapshot:false`, so a
  snapshot means subscription drift, unlike Coinbase).
- Kraken v2 has no sequence numbers, so `NormalizedMessage.sequence` is always
  `None`; correctness rests on the per-message book checksum.
- `status`, `heartbeat`, unknown channels and successful subscribe acks are
  empty messages with the channel set; `BTC/USD` becomes `BTC-USD` (A20).

### BookProcessor state machine

`BookProcessor` (`tickvault/book/processor.py`) keeps one state per
`(venue, symbol)`: `SYNCING` or `LIVE`, an authoritative `connection_id`, and
an `OrderBook`. `process(msg)` returns a `ProcessorOutput` with `trades`,
`book_deltas`, `book_snapshots`, `events` and `resync_requests`; trades pass
through unconditionally.

- **Sequence tracking** (`SequenceTracker`, keyed by `connection_id`) observes
  every message that carries a sequence, including Coinbase heartbeats. `GAP`
  discards the revealing message's levels (they are neither emitted nor
  counted as dropped), emits one `gap` event and a resync request per symbol
  known on that connection, and moves those symbols to `SYNCING`. `STALE`
  ignores the message and increments the public `stale_messages` counter.
- **Snapshots** from the authoritative connection replace the book, are
  checksum-verified when the venue has a registered checksum, emit
  `book_snapshots` `source="venue"` rows for the full book
  (`snapshot_id = <connection_id>:<ingest_seq>`) and make the symbol `LIVE`.
  A snapshot received while recovering from an anomaly or a reconnect emits
  one `resync` event whose `detail` carries `reason` and `deltas_dropped`; the
  very first snapshot of a session emits no `resync`.
- **Deltas** while `SYNCING` are dropped and counted (the count appears in the
  next `resync` event, A19); deltas from a non-authoritative connection are
  dropped silently — the reconnect race guard. Applied deltas are emitted as
  one `BookDelta` per level with the absolute new size.
- **Checksums** are a per-venue registry
  (`checksum_registry = {"kraken": kraken_checksum}`); only registered venues
  are truncated (default `kraken_depth=25`) and verified. A mismatch emits
  `checksum_failure` with `{"computed", "expected"}`, moves the symbol to
  `SYNCING`, requests a resync and suppresses the update's rows (for Kraken
  the levels are applied to the internal book first, because the checksum is
  over the resulting book, but nothing is emitted). Coinbase books keep full
  depth.
- **Invariants** run after every applied update; a violation emits
  `invariant_violation` with the codes and moves the symbol to `SYNCING`.
- **Controls**: a `connect` control with a new connection becomes authoritative
  for the venue's known symbols, which go `SYNCING` and each emit a
  `reconnect` event — except the venue's first connection. `disconnect` emits
  one venue-level event (`symbol` is null).
- **Samples**: while `LIVE`, the first applied update of each exchange-time
  second emits a top-10-per-side `book_snapshots` `source="sample"` series for
  that symbol (at most one per second; skipped seconds are not back-filled).
- **Resync requests** are surfaced as `ResyncRequest(venue, connection_id,
  symbol, reason)` in `ProcessorOutput.resync_requests` with reasons `gap`,
  `checksum_failure` or `invariant_violation`; the live loop (Epic 2) hands
  them to `connector.resync`, and replay ignores them.

Event `detail` strings are JSON with sorted keys and compact separators, so
replay is byte-deterministic.

**Correction (Epic 2).** Per-symbol connection scope replaces the venue-wide
rule above (issue #55): every `connect` control now declares the `symbols` its
connection serves, and the connection becomes authoritative for exactly those
symbols, emitting `reconnect` (and moving the symbol to `SYNCING`) only for a
symbol that already had an authoritative connection. Recordings whose
`connect` has no `symbols` keep the venue-wide rule for symbols already known
on the venue; a symbol first seen later belongs to the first connection that
delivers a message for it.

### Kraken checksum

`kraken_checksum(bids, asks) -> int` (`tickvault/book/kraken_checksum.py`)
hashes the top 10 asks (best first) then the top 10 bids (best first). Each
level contributes the price digits followed by the size digits after removing
the decimal point and stripping leading zeros (`0` when nothing remains);
trailing zeros are significant. The digits come from
`format(value, "f")` on `Decimal`, never from `str()`, which can switch to
exponent notation for small values. The CRC32 is returned unsigned. Callers
truncate the book to the subscribed depth first (25 in the processor).

### Book invariants

`check_invariants(book) -> list[str]` (`tickvault/book/invariants.py`)
returns, in order, `crossed` (best bid ≥ best ask), `negative_size` and
`duplicate_level`. It reads O(1) counters maintained by `OrderBook` while
levels are inserted, so it can run after every update even on a full-depth
Coinbase book.

### Raw layer

`RawWriter(root)` (`tickvault/storage/raw_writer.py`) persists every message
before anything else processes it:

- One open file per `connection_id`, path from
  `paths.raw_file_path` with the message's `recv_ts_ns` hour. When the receive
  hour changes the file is closed and the next opened with `rotation + 1`
  (any hour change, including a backwards one, opens a new rotation rather
  than reopening a file).
- Every file is created with `"xb"` (exclusive create): an existing path
  raises `FileExistsError`, so no code path can reopen or overwrite raw data.
- Lines are `rawio.encode_line` bytes in a single zstd frame per file.
  `flush()` emits a zstd block flush, making everything written so far
  decodable while the frame stays open; the writer also block-flushes at
  least once per second of `recv_ts_ns` time. `close()` ends every frame and
  is idempotent.
- A reader of a file copied mid-write decodes everything up to the last flush
  and counts the torn tail as truncated; it never raises.

### Normalized layer (Parquet writer)

`ParquetWriter(root, flush_interval_ns=60_000_000_000, max_rows=200_000,
idle_flush_s=None, clock=time.monotonic)`
(`tickvault/storage/parquet_writer.py`) implements the `Sink` protocol:

- Buffers per `(table, partition directory)`; the partition comes from
  `paths.partition_path` keyed by `exchange_ts_ns`, or `recv_ts_ns` for events
  without an exchange time (A5). Files land only under `<root>/norm/`, which
  is enforced on every write.
- Flush rules: the record that is at least `flush_interval_ns` after the
  buffer's first `recv_ts_ns` (the logical clock) is included in the flush;
  reaching `max_rows` flushes; `flush()`/`close()` flush everything. With
  `idle_flush_s` set (live only), `write()` and the public `flush_idle()`
  flush buffers untouched for that many wall-clock seconds — the injected
  `clock` is used only here, so replay never depends on wall time.
- File name `part-<first row connection_id>-<first row ingest_seq:012d>.parquet`
  with rows in write order; schema from `models.TABLES` via `to_arrow`; zstd,
  statistics on, fixed `row_group_size=65_536`, no pandas metadata; written to
  a temporary name in the same directory and renamed into place. Should a
  `max_rows` split start the next buffer on the same first row, a deterministic
  `-<n>` suffix keeps names unique instead of overwriting.
- `install_signal_flush(writer)` closes the writer on SIGTERM/SIGINT, restores
  the OS default handler and re-sends the signal, so the process exits with
  the conventional signal status after a complete flush.

### Compaction and manifests

`compact(root, date) -> CompactionResult` (`tickvault/storage/compact.py`)
merges every `norm/<table>/venue=*/date=<date>/hour=*` directory with more
than one file into `compacted-<date>-<HH>.parquet`:

- Rows are sorted by `(recv_ts_ns, connection_id, ingest_seq)` with a stable
  sort, so rows that share a key (all rows of one raw message) keep their
  relative order.
- The merged file is written with the Parquet writer settings to a temporary
  name, read back, and verified: row count equals the sum over inputs and the
  multiset of `(connection_id, ingest_seq)` keys is unchanged. Only then is it
  renamed into place and the inputs deleted; a failed check raises
  `CompactionError` with every input untouched (including a previous
  `compacted-*` file that was one of the inputs).
- Single-file directories are left alone, re-running is a no-op, and `raw/` is
  never read or modified.

`write_manifest(root, date)` (`tickvault/storage/manifest.py`) writes
`<root>/manifests/date=<date>/manifest.json` listing every normalized file of
the date — relative path, table, venue, hour, row count, SHA-256, bytes —
sorted by path with sorted keys, plus totals. `verify_manifest(path)`
recomputes all of it and returns human-readable mismatches: missing file,
bytes/sha256/row_count mismatch, unreadable file, unlisted file, and totals
mismatches.

### DuckDB views

`tickvault/sql.py` exposes the normalized tree as four views:

- `view_ddl(root)` emits one `CREATE VIEW` per table backed by
  `read_parquet('<root>/norm/<table>/**/*.parquet', hive_partitioning = true,
  hive_types = {'venue': 'VARCHAR', 'date': 'DATE', 'hour': 'INTEGER'})`.
  `date` is a `DATE` (matching DuckDB's own hive inference and the QA
  queries), `hour` an `INTEGER` (so `hour=05` never becomes a string) and
  `venue` a `VARCHAR`.
- DuckDB refuses to bind a `read_parquet` glob that matches no files, so a
  table without files gets a typed zero-row view generated from the Arrow
  schema, with the table columns in schema order followed by `date` and
  `hour`. `SELECT ... FROM events` therefore works on a fresh data directory.
  `view_ddl` is a pure function of the data tree and is deterministic.
- `connect(root)` opens an in-memory connection with the views created;
  `run_query(root, sql)` runs one statement over fresh views and returns the
  column names and rows.

### QA

`compute_qa(root, date=None) -> list[QARow]` (`tickvault/qa/metrics.py`)
aggregates one row per venue/symbol/partition-day with counts for messages,
trades, book deltas, venue snapshots, gaps, resyncs, checksum failures,
reconnects and invariant violations, plus lag percentiles, coverage and
`unexplained`:

- `messages` is the number of distinct `(connection_id, ingest_seq)` pairs
  contributing at least one row across the three count tables plus
  symbol-level events (A24); raw-layer counts are deferred to Epic 2.
- Lag is `recv_ts_ns - exchange_ts_ns` over trades and deltas (p50/p99);
  coverage is the fraction of exchange-time seconds in the observed range
  that have at least one trade or delta.
- `unexplained` follows A10: a `gap`, `checksum_failure` or
  `invariant_violation` for a `(venue, symbol)` with no later `resync` for the
  same key; anomalies and resyncs at the same event order stay unexplained.

`tickvault/qa/report.py` wraps it: `run_qa`, `totals`, `exit_code` (1 if any
row has `unexplained > 0`), deterministic `to_json` (sorted keys, no
wall-clock fields), `to_markdown`, `to_text_table` and `write_reports` writing
`qa-<date>.json` and `qa-<date>.md`.

**Correction (Epic 2).** `QARow` gained `book_missing` (true when a row has
messages but zero venue snapshots) and `exit_code` now returns 1 for it as
well as for `unexplained > 0` (issue #56). With a `date` filter, a resync for
the same venue/symbol up to 300 s into the next day explains an anomaly of the
filtered day, and the totals line reports the number of rows with a missing
book.

### Reconcile

`tickvault/reconcile/tardis.py` implements the offline path:
`load_tardis_trades` reads a Tardis trades CSV (plain or gzip, sniffed by
magic bytes; microseconds → ns; exact `Decimal` prices), and
`load_tickvault_trades` reads `norm/trades` through DuckDB with both time
bounds inclusive. `reconcile(tardis, tickvault, window=None)` matches by
`trade_id` inside a window that defaults to the exchange-time overlap
`[max(first ts), min(last ts)]`; it reports matched ids, one-sided ids and
price/size/side mismatches, where the mismatch tuples are subsets of the
matched ids and every id list is sorted (A21). `report_to_json` and
`report_to_text` render the report deterministically.

`tickvault/reconcile/download.py` fetches the Tardis dataset used for
reconciliation:

- Free Tardis datasets exist only for the first day of each month; a date that
  is not the 1st raises `ValueError` before any HTTP call. The URL is
  `https://datasets.tardis.dev/v1/<exchange>/trades/<YYYY>/<MM>/<DD>/<SYMBOL>.csv.gz`
  and is fetched with a plain `GET` (with redirects) because the dataset host
  answers `HEAD` with 404 while `GET` needs no API key.
- Downloads stream to a hidden `.download-*.part` file in the target
  directory and are moved into place atomically; failures leave neither a
  partial CSV nor a `.part` file. The cache layout is
  `<cache_dir>/tardis/<exchange>/trades/<YYYY-MM-DD>/<SYMBOL>.csv.gz`, and an
  existing file is trusted and never re-downloaded.
- `reconcile_live_day(root, date, cache_dir, ...)` is the convenience path:
  download one day, load both sides, reconcile on the overlap window.

## Key decisions and reasoning

- **Coinbase taker side.** Both Coinbase feeds report the **maker** side of a
  trade; the normalized `trades.side` is always the **taker** side, so the
  normalizer inverts it (`BUY → sell`, `SELL → buy`). A 60-second live
  comparison of Exchange `matches` against Advanced Trade `market_trades` for
  BTC-USD found the same side (modulo case) for every shared trade id, with
  no numeric price or size differences — only trailing-zero text differences
  such as `0.00000060` vs `0.0000006`, which `Decimal` preserves exactly.
- **Coinbase resync without reconnect.** Re-subscribing `level2` on the same
  connection was verified live to return a fresh full-depth snapshot within
  about half a second (39,792 levels versus 39,782 on the first subscribe).
  Resync therefore unsubscribes and resubscribes `level2` on the live socket,
  keeping the connection id and keeping the `resync_request` control line and
  the fresh snapshot in one per-connection sequence.
- **Kraken resync repeats the depth.** Kraken matches a subscription by all
  of its parameters; unsubscribing without `depth=25` failed live with
  `Subscription with depth 10 not Found`, after which the resubscribe was
  rejected with `Already subscribed`. The connector repeats the depth on both
  frames, and the resubscribe answers with a fresh snapshot on the same
  connection.
- **Exact decimals, never float.** Coinbase prices and sizes arrive as JSON
  strings and are parsed with `Decimal`; Kraken's arrive as JSON numbers and
  are parsed with `json.loads(parse_float=Decimal)` while `float` inputs are
  rejected. The Kraken checksum is the enforcement mechanism: it hashes the
  exact digit text, and a single float conversion breaks it.
- **One connection per Coinbase product.** Sequence numbers are
  per-connection on Advanced Trade, so a gap must scope to one product. Kraken
  has one connection for all symbols because v2 has no sequence numbers at
  all; its per-message CRC32 checksum is the correctness check.
- **Corrections are data.** The Coinbase message that reveals a sequence gap
  and the Kraken update whose checksum fails are discarded: their levels are
  neither emitted nor counted as dropped (A19). Deltas dropped while
  `SYNCING` are counted and reported in the next `resync` event's
  `detail.deltas_dropped`.
- **Samples feed the book without rebuilding it.** While `LIVE`, the
  processor emits a top-10 sample per symbol for the first applied update of
  each exchange-time second (A9), so the dashboard and later features can read
  top-of-book and spread without replaying deltas. There is no catch-up
  series; the cadence is bounded by design.
- **Replay is byte-deterministic.** Every flush decision is a function of the
  records (logical clock, row count) except the live-only idle flush. Event
  `detail` JSON uses sorted keys, Parquet writing uses a pinned pyarrow, fixed
  row groups and no pandas metadata, and compaction output is committed to
  disk only after verification. Two replays of the same raw input produce
  byte-identical `norm/` trees.
- **Raw files are write-once.** Exclusive create means an existing path is an
  error; there is no reopen path. Rotation, zstd block flushes and the
  tolerant reader bound crash loss to about one second of receive time and
  make a torn tail visible as counters, never as an exception.
- **Verification before deletion.** Compaction reads the temporary output back
  and compares row counts and the `(connection_id, ingest_seq)` multiset
  before deleting anything; manifests record SHA-256 per file so later
  mutation is detectable. Neither ever touches `raw/`.
- **Views pin partition types.** `date` is `DATE` (matching the QA filters and
  DuckDB's inference), `hour` is `INTEGER` (so `hour=05` cannot degrade to a
  string) and `venue` is `VARCHAR`. Empty tables get typed zero-row views
  built from the Arrow schema because DuckDB cannot bind an empty glob.
- **QA `unexplained` is literal.** An anomaly is explained only by a
  *later* `resync` for the same `(venue, symbol)`; equal-order pairs remain
  unexplained (A10). The synthetic session yields a Coinbase BTC-USD gap with
  a following resync and a Kraken BTC-USD checksum failure with a following
  resync, so `unexplained` is zero overall.
- **Reconciliation window.** The default window is the exchange-time overlap;
  when one side is empty and no window is given, no filtering happens and the
  other side is reported one-sided rather than dropped (A21).
- **Tardis free datasets.** Free slices exist only for the first of each
  month, require no API key, and must be fetched with `GET` (the host answers
  `HEAD` with 404). Downloads are cached and trusted, with atomic writes.

## Interfaces for future epics

The Epic 2 pipeline wires these; Epic 0 contracts (`models.py`, `paths.py`,
`rawio.py`, `timeutil.py`) are unchanged and documented in
`docs/epic-00-foundation.md`.

### `tickvault/connectors/coinbase.py`

```python
COINBASE_URL = "wss://advanced-trade-ws.coinbase.com"
COINBASE_CHANNELS = ("heartbeats", "level2", "market_trades")
COINBASE_MAX_SIZE = 64 * 1024 * 1024

def subscribe_payload(product: str, channel: str) -> dict[str, object]
def unsubscribe_payload(product: str, channel: str) -> dict[str, object]
def subscribe_payloads(product: str) -> list[dict[str, object]]

class CoinbaseConnector:
    venue = "coinbase"
    def __init__(self, symbols: Sequence[str], url: str = COINBASE_URL, *,
                 max_size: int = COINBASE_MAX_SIZE, open_timeout: float | None = 10.0,
                 close_timeout: float | None = 2.0, backoff_factory: Callable[[], Backoff] = Backoff,
                 sleep: SleepFn = asyncio.sleep, clock: ClockFn = time.time_ns,
                 token_hex: Callable[[int], str] = secrets.token_hex) -> None
    async def subscribe(self, symbols: Sequence[str]) -> None
    async def resync(self, symbol: str, reason: str) -> None
    async def close(self) -> None
    async def stream(self) -> AsyncIterator[RawMessage]
```

### `tickvault/connectors/kraken.py`

```python
KRAKEN_URL = "wss://ws.kraken.com/v2"
KRAKEN_DEPTH = 25
KRAKEN_MAX_SIZE = 64 * 1024 * 1024

class SubscribeRejected(Exception): ...

def venue_symbol(symbol: str) -> str
def subscribe_payloads(venue_symbols: Sequence[str], depth: int = KRAKEN_DEPTH) -> list[dict[str, object]]
def unsubscribe_book_payload(symbol: str, depth: int = KRAKEN_DEPTH) -> dict[str, object]
def subscribe_book_payload(symbol: str, depth: int = KRAKEN_DEPTH) -> dict[str, object]

class KrakenConnector:
    venue = "kraken"
    def __init__(self, symbols: Sequence[str], url: str = KRAKEN_URL, *,
                 depth: int = KRAKEN_DEPTH, max_size: int = KRAKEN_MAX_SIZE,
                 open_timeout: float | None = 10.0, close_timeout: float | None = 1.0,
                 backoff_factory: Callable[[], Backoff] = Backoff,
                 sleep: SleepFn = asyncio.sleep, clock: ClockFn = time.time_ns,
                 token_hex: Callable[[int], str] = secrets.token_hex) -> None
    async def subscribe(self, symbols: Sequence[str]) -> None
    async def resync(self, symbol: str, reason: str) -> None
    async def close(self) -> None
    async def stream(self) -> AsyncIterator[RawMessage]
```

### `tickvault/normalize/`

```python
class NormalizeError(ValueError): ...

# tickvault.normalize.coinbase
def normalize(raw: RawMessage) -> NormalizedMessage

# tickvault.normalize.kraken
def normalize(raw: RawMessage) -> NormalizedMessage
```

### `tickvault/book/processor.py`

```python
type ChecksumFn = Callable[[Sequence[PriceLevel], Sequence[PriceLevel]], int]

@dataclass(frozen=True, slots=True)
class ResyncRequest:
    venue: str
    connection_id: str
    symbol: str
    reason: str

@dataclass(slots=True)
class ProcessorOutput:
    trades: list[Trade]
    book_deltas: list[BookDelta]
    book_snapshots: list[BookSnapshotLevel]
    events: list[Event]
    resync_requests: list[ResyncRequest]

class BookProcessor:
    def __init__(self, kraken_depth: int = 25, sample_interval_ns: int = 1_000_000_000) -> None
    stale_messages: int
    checksum_registry: dict[str, ChecksumFn]
    def process(self, msg: NormalizedMessage) -> ProcessorOutput
```

### `tickvault/book/orderbook.py`, `sequence.py`, `kraken_checksum.py`, `invariants.py`

```python
class OrderBook:
    def apply_snapshot(self, bids: Sequence[PriceLevel], asks: Sequence[PriceLevel]) -> None
    def apply_delta(self, side: Side, price: Decimal, size: Decimal) -> None
    def best_bid(self) -> PriceLevel | None
    def best_ask(self) -> PriceLevel | None
    def top(self, n: int) -> tuple[list[PriceLevel], list[PriceLevel]]
    def truncate(self, depth: int) -> None
    def __len__(self) -> int
    @property
    def negative_size_count(self) -> int
    @property
    def duplicate_level_count(self) -> int

class SequenceTracker:
    def observe(self, key: str, seq: int) -> SeqResult   # FIRST | OK | GAP | STALE
    def reset(self, key: str) -> None
    def last(self, key: str) -> int | None

def kraken_checksum(bids: Sequence[PriceLevel], asks: Sequence[PriceLevel]) -> int
def check_invariants(book: OrderBook) -> list[str]
```

### `tickvault/storage/`

```python
class RawWriter:
    def __init__(self, root: str | Path) -> None
    def write(self, message: RawMessage) -> None
    def flush(self) -> None
    def close(self) -> None

class ParquetWriter:
    def __init__(self, root: str | Path, flush_interval_ns: int = 60_000_000_000,
                 max_rows: int = 200_000, idle_flush_s: float | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None
    def write(self, table: str, records: Sequence[NormalizedRecord]) -> None
    def flush(self) -> None
    def flush_idle(self) -> None
    def close(self) -> None

def install_signal_flush(writer: ParquetWriter) -> None

class CompactionError(RuntimeError): ...

@dataclass(frozen=True, slots=True)
class CompactedPartition:
    table: str
    venue: str
    hour: str
    rows: int
    inputs: int
    output: str

@dataclass(frozen=True, slots=True)
class CompactionResult:
    date: str
    partitions: tuple[CompactedPartition, ...]
    @property
    def rows(self) -> int
    @property
    def files_removed(self) -> int
    @property
    def files_written(self) -> int

def compact(root: str | Path, date: str) -> CompactionResult
def write_manifest(root: str | Path, date: str) -> Path
def verify_manifest(path: str | Path) -> list[str]
```

### `tickvault/sql.py`

```python
def view_ddl(root: str | Path) -> str
def connect(root: str | Path) -> duckdb.DuckDBPyConnection
def run_query(root: str | Path, sql: str) -> tuple[list[str], list[tuple[object, ...]]]
```

### `tickvault/qa/`

```python
@dataclass(frozen=True, slots=True)
class QARow:
    venue: str
    symbol: str
    date: str
    messages: int
    trades: int
    book_deltas: int
    venue_snapshots: int
    gaps: int
    resyncs: int
    checksum_failures: int
    reconnects: int
    invariant_violations: int
    lag_p50_ms: float
    lag_p99_ms: float
    coverage: float
    unexplained: int

def compute_qa(root: str | Path, date: str | Date | None = None) -> list[QARow]

@dataclass(frozen=True, slots=True)
class QAReport:
    rows: tuple[QARow, ...]
    generated_from: str
    date: str | None = None

@dataclass(frozen=True, slots=True)
class QATotals:
    rows: int
    messages: int
    trades: int
    book_deltas: int
    venue_snapshots: int
    gaps: int
    resyncs: int
    checksum_failures: int
    reconnects: int
    invariant_violations: int
    unexplained: int

def run_qa(root: str | Path, date: str | Date | None = None) -> QAReport
def totals(report: QAReport) -> QATotals
def exit_code(report: QAReport) -> int
def to_json(report: QAReport) -> str
def to_markdown(report: QAReport) -> str
def to_text_table(report: QAReport) -> str
def write_reports(report: QAReport, reports_dir: str | Path, date: str | Date) -> tuple[Path, Path]
```

### `tickvault/reconcile/`

```python
@dataclass(frozen=True, slots=True)
class TardisTrade:
    exchange: str
    symbol: str
    exchange_ts_ns: int
    local_ts_ns: int
    trade_id: str
    side: TradeSide
    price: Decimal
    amount: Decimal

@dataclass(frozen=True, slots=True)
class TickVaultTrade:
    venue: str
    symbol: str
    exchange_ts_ns: int
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    trade_id: str
    side: TradeSide
    price: Decimal
    size: Decimal

@dataclass(frozen=True, slots=True)
class ReconcileWindow:
    start_ns: int
    end_ns: int

@dataclass(frozen=True, slots=True)
class ReconcileReport:
    window: ReconcileWindow | None
    tardis_trades: int
    tickvault_trades: int
    matched: int
    missing_in_tickvault_ids: tuple[str, ...]
    extra_in_tickvault_ids: tuple[str, ...]
    price_mismatch_ids: tuple[str, ...]
    size_mismatch_ids: tuple[str, ...]
    side_mismatch_ids: tuple[str, ...]

def window_from_iso(start: str, end: str) -> ReconcileWindow
def load_tardis_trades(path: str | Path) -> list[TardisTrade]
def load_tickvault_trades(root: str | Path, venue: str, symbol: str,
                          start_ns: int | None = None, end_ns: int | None = None) -> list[TickVaultTrade]
def reconcile(tardis: Sequence[TardisTrade], tickvault: Sequence[TickVaultTrade],
              window: ReconcileWindow | tuple[int, int] | None = None) -> ReconcileReport
def report_to_json(report: ReconcileReport) -> str
def report_to_text(report: ReconcileReport) -> str

def tardis_ids(venue: str, symbol: str) -> tuple[str, str]
def tardis_url(exchange: str, symbol: str, date: str | Date) -> str
def download_tardis_trades(date: str | Date, cache_dir: str | Path, exchange: str = "coinbase",
                           symbol: str = "BTC-USD", client: httpx.Client | None = None) -> Path
def reconcile_live_day(root: str | Path, date: str | Date, cache_dir: str | Path, *,
                       venue: str = "coinbase", symbol: str = "BTC-USD",
                       client: httpx.Client | None = None) -> ReconcileReport
```

### CLI

Every command still has a stub body (prints `not implemented yet` to stderr and
exits 2); Epic 2 fills the bodies calling the functions above.

```
tickvault ingest   --venues TEXT --symbols TEXT --out PATH [--duration FLOAT] [--max-gb FLOAT]
tickvault replay   --input PATH --out PATH
tickvault qa       --data PATH [--date TEXT]
tickvault compact  --data PATH --date TEXT
tickvault sql      --data PATH [-c TEXT]
tickvault reconcile [--tardis-date TEXT] [--venue TEXT] [--symbol TEXT] [--fixture PATH] [--data PATH]
tickvault bench    [--input PATH]
```

## Deviations from the plan

- **`view_ddl` is state-dependent.** A table without files gets a typed
  zero-row view instead of a `read_parquet` statement, because DuckDB 1.x
  refuses to bind an empty glob. The text stays a pure, deterministic
  function of the data tree.
- **Unique-name suffix in the Parquet writer.** The specified file name has no
  room for two buffers that start on the same `(connection_id, ingest_seq)`;
  that can only happen when a single message is split by `max_rows`, and the
  writer then appends a deterministic `-<n>` suffix rather than overwriting.
- **Compaction verifies the temporary file.** The deletion of inputs happens
  only after the renamed file is read back; a failed check never leaves a
  half-replaced `compacted-*` file behind.
- **QA runs its own DuckDB queries.** `qa/metrics.py` does not depend on
  `sql.py`; it creates per-table views only for tables that have files. This
  keeps the QA path usable before the CLI `sql` command is wired.
- **QA `messages` is normalized-layer only.** Distinct `(connection_id,
  ingest_seq)` pairs from the normalized tables (A24); raw-layer message
  counting arrives in Epic 2 when a raw tree is present.
- **Reconcile download signature order.** `download_tardis_trades` takes
  `(date, cache_dir, exchange, symbol, client)` because the sketched order
  (`date, exchange=..., symbol=..., cache_dir, client=None`) is invalid
  Python (a non-default parameter after a default one). Names and defaults
  are as planned.
- **Tardis hashing deferred.** A cached download is trusted when present; no
  size or hash re-check yet (noted for Epic 2).
- **No `pipeline.py`.** Live/replay wiring was not part of this epic; the
  normalizer → processor seam is covered end-to-end offline over the recorded
  and synthetic fixtures.
- **Bounded sample coverage.** Samples are emitted at most once per
  exchange-time second with no catch-up series; a quiet second simply has no
  sample.

## Known limitations and deferred items

Deferred by scope, not broken:

- `pipeline.py`, the CLI command bodies, Dagster definitions, the Streamlit
  dashboard and Docker Compose are Epic 2.
- `iter_raw` loads raw files into memory before sorting (A27); replay at
  multi-hour scale replaces it with a streaming k-way merge without changing
  the signature.
- QA does not count raw-layer messages yet (A24) and does not compute
  `unexplained` across days — the lookup is per date.
- The Tardis cache trusts existing files; a content check is deferred.
- Live recording and live network checks are marked `network` and excluded
  from CI; the recorded fixtures are frozen 5-minute captures.
- Coinbase books keep full depth (about 40k levels); only Kraken truncates to
  the subscribed depth. Compaction and QA handle both without special cases.
- A live-versus-Tardis reconciliation capture on 2026-11-01 remains optional
  (A17).

Nothing from the Epic 1 issue set is left unmerged; this document reflects
`main`.
