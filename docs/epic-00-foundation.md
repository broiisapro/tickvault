# Epic 0 — Foundation

## Goal

Give the project a fresh-clone installable package with a pinned toolchain and
a CI gate (`lint`, `types`, `tests`) on the self-hosted macOS ARM64 runners,
and define every shared contract that the Epic 1 workstreams meet at, so those
workstreams never edit shared files. Epic 0 ships no venue, book, storage or
pipeline implementation; it ships the interfaces, the fixtures and this
documentation.

## How it works

### Contract modules

| Module | Provides |
|---|---|
| `tickvault/models.py` | The single source of truth for record types and Arrow schemas: `RawMessage`; `Trade`, `BookDelta`, `BookSnapshotLevel`, `Event`; `BookUpdate`; `NormalizedMessage`; the literals `RawKind`, `Side`, `TradeSide`, `EventType`, `SnapshotSource`; `PriceLevel` and `NormalizedRecord`; `TRADES_SCHEMA`, `BOOK_DELTAS_SCHEMA`, `BOOK_SNAPSHOTS_SCHEMA`, `EVENTS_SCHEMA`, `TABLES`; `to_arrow`. |
| `tickvault/paths.py` | `partition_path` for `norm/<table>/venue=<v>/date=<YYYY-MM-DD>/hour=<HH>` and `raw_file_path` for the hourly raw files. UTC only. |
| `tickvault/rawio.py` | The raw envelope codec: `encode_line`, `decode_line`, `iter_raw` (canonical order, truncated-tail tolerance) and `RawReadStats`. |
| `tickvault/timeutil.py` | `parse_rfc3339_ns` (0–9 fractional digits, `T`/space, `Z`/`±HH:MM`), `us_to_ns`, `ns_to_iso`. |
| `tickvault/book/sequence.py` | `SequenceTracker` and the result types `FIRST`, `OK`, `GAP(expected, received)`, `STALE(received)`. |
| `tickvault/book/orderbook.py` | The `OrderBook` interface (price-level book); the implementation lands in Epic 1. |
| `tickvault/book/processor.py` | The `BookProcessor` interface, `ProcessorOutput` and `ResyncRequest`; the state-machine rules from the architecture document, §8, are stated verbatim in the class docstring. |
| `tickvault/connectors/base.py` | The `Connector` protocol. |
| `tickvault/storage/sink.py` | The `Sink` protocol. |
| `tickvault/cli.py` | The Typer entry point with `ingest`, `replay`, `qa`, `compact`, `sql`, `reconcile`, `bench`: full option sets, stub bodies that print `not implemented yet` to stderr and exit 2. |

Package `__init__.py` files exist for `connectors`, `normalize`, `book`,
`storage`, `qa` and `reconcile`.

### Project scaffolding

- `pyproject.toml`: package `tickvault`, `requires-python = ">=3.12,<3.13"`,
  console script `tickvault = "tickvault.cli:app"`, hatchling backend, the
  runtime dependencies pinned there, and the `dev`, `orchestration` and
  `dashboard` groups (all three default, so `uv sync --frozen` installs a full
  development environment). Ruff (line length 100; `E,F,I,B,UP,SIM,RUF`),
  mypy (`strict = true`) and pytest (`asyncio_mode = "auto"`, the `network`
  marker, `addopts = "-m 'not network'"`) are configured here.
- `uv.lock` committed; `.python-version` = `3.12`; `Makefile` with `sync`,
  `lint`, `types`, `test`, `check`; `.env.example` with `TICKVAULT_DATA_DIR`
  and the note that Phase 1 needs no secrets; `.gitignore` covering caches,
  build output, `reports/` and local data (no global `*.parquet` / `*.zst`
  ignores, so committed fixtures stay trackable).
- `.github/workflows/ci.yml`: workflow `CI` on `pull_request` and `push` to
  `main`, workflow-level `concurrency: ci-${{ github.ref }}` with
  cancel-in-progress, and three jobs whose ids and names are exactly `lint`,
  `types`, `tests`, each on `[self-hosted, macOS, ARM64]` with
  `actions/checkout`, `astral-sh/setup-uv` and `uv sync --frozen`. Python 3.12
  comes from `.python-version` through uv; there is no `actions/setup-python`.

### Fixtures

Produced by committed scripts; tests never touch the network.

| Fixture | Size | Contents |
|---|---|---|
| `tests/fixtures/coinbase_btc_5m.jsonl.zst` | 1,095,768 B | 5-minute Advanced Trade recording (`heartbeats`, `level2`, `market_trades`, BTC-USD), 6,822 lines, recorded 2026-10-15T11:56:18Z. |
| `tests/fixtures/kraken_btc_5m.jsonl.zst` | 488,265 B | 5-minute WS v2 recording (`book` depth 25, `trade`, BTC/USD), 21,829 lines, recorded 2026-10-15T11:56:19Z. |
| `tests/fixtures/synthetic_gap_session.jsonl.zst` | 3,315 B | Deterministic session with one Coinbase sequence gap and one Kraken checksum failure, both followed by a resync. |
| `tests/fixtures/synthetic_gap_session.expected.json` | 2,360 B | Exact expected counts and the ordered event list for the synthetic session. |
| `tests/fixtures/tardis_coinbase_trades_sample.csv.gz` | 34,737 B | Tardis BTC-USD trades slice for 2026-10-01T12:00:00Z–12:10:00Z, 2,089 rows, original text. |
| `tests/fixtures/tv_coinbase_trades_sample/` | 52,675 B | The same slice as a TickVault data root (one Parquet file, `TRADES_SCHEMA`), 2,087 rows, with the documented deliberate discrepancies. |
| `tests/fixtures/reconcile_expected.json` | 1,239 B | Exact expected reconciliation counts and id lists. |
| `tests/fixtures/fixtures.json` | 726 B | Path, SHA-256, line count, venue, recorded-at and generator for each recorded fixture. |

Total committed fixture size is about 1.7 MB against the 50 MB budget.

Generators:

- `scripts/record_fixture.py`: the only network code in the repository;
  captures the two 5-minute live recordings.
- `scripts/make_synthetic_fixtures.py`: deterministic (no randomness, no wall
  clock), and `--check` regenerates in memory and fails unless the committed
  files are byte-identical.
- `scripts/make_reconcile_fixtures.py`: downloads the free first-of-month
  Tardis CSV once and derives the TickVault-format sample with fixed-index
  discrepancies; outputs are committed.

Test harness: `tests/conftest.py` exposes `fixtures_dir`, `coinbase_5m_path`,
`kraken_5m_path`, `synthetic_gap_path`, `synthetic_expected`,
`tardis_sample_path`, `tv_trades_sample_root`, `reconcile_expected` and
`raw_lines`. `tests/test_fixtures.py` re-checks every fixture invariant
(decodability, envelope shape, contiguous `ingest_seq`, trailing-zero
preservation, fixture hashes, size budget, synthetic `--check`, reconciliation
counts).

## Key decisions and reasoning

- **Raw envelope byte fidelity.** `payload` is stored as a JSON string with
  the frame text unmodified, keys in the fixed order
  `{"v","venue","connection_id","ingest_seq","recv_ts_ns","kind","payload"}`,
  compact separators, `ensure_ascii=False` and a trailing newline. The exact
  bytes are unit-tested against the architecture document's example.
- **`decimal128(38, 18)`, never float.** Prices and sizes stay `Decimal` from
  venue text through Arrow; the tests assert `Decimal("0.00006120")` keeps
  scale 18 (`0.000061200000000000`).
- **Nanosecond timestamps.** `datetime.fromisoformat` truncates to
  microseconds, so `timeutil.parse_rfc3339_ns` parses the fractional digits
  itself and right-pads to nine. Lowercase `t`/`z` are accepted as RFC 3339
  permits.
- **Gap semantics.** `expected = last + 1`; `received > expected` is one
  `GAP(expected, received)` and advances the last value; `received < expected`
  is `STALE` and leaves it unchanged. Tests cover the off-by-one case, and
  Hypothesis properties cover no-false-positives on consecutive streams and
  exact gaps when numbers are removed.
- **Raw order is canonical globally.** `iter_raw` reads all files, then yields
  by `(recv_ts_ns, connection_id, ingest_seq)`. Fixtures and replay inputs are
  the Epic 0 scale; the internals can become a streaming merge later without
  changing the signature.
- **Truncation tolerance.** A partial trailing line is skipped and counted as
  `truncated_lines`; a `ZstdError` is caught and counted as `truncated_files`.
  Both paths are tested.
- **`Connector.stream` is declared `def -> AsyncIterator[RawMessage]`** so
  implementations can be async generators; `subscribe(symbols)` and
  `resync(symbol, reason)` are `async def`.
- **`OrderBook.best_bid()`/`best_ask()` return the whole `(price, size)`
  level** (or `None`); `top(n)` returns `(bids, asks)` best-first, which is
  also the input the Kraken checksum needs after `truncate(25)`.
- **`pyarrow-stubs` in the `dev` group.** pyarrow ships no `py.typed` marker,
  so strict mypy cannot analyze `models.py` without stubs; issue #1's dev
  group allows any needed type stubs.
- **CLI options via `Annotated[...]`.** This keeps `typer.Option` calls out of
  argument defaults, so the `B008` ruff rule does not need a per-file ignore.
- **CI action pinning.** `astral-sh/setup-uv@v7` because the action's floating
  major tag stops at `v7`; newer releases exist only as full version tags.
  `actions/checkout@v7`.

## Interfaces for future epics

### `tickvault/models.py`

```python
@dataclass(frozen=True, slots=True)
class RawMessage:
    venue: str
    connection_id: str
    ingest_seq: int
    recv_ts_ns: int
    kind: RawKind          # "msg" | "control"
    payload: str

@dataclass(frozen=True, slots=True)
class Trade:
    venue: str
    symbol: str
    exchange_ts_ns: int
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    trade_id: str
    price: Decimal
    size: Decimal
    side: TradeSide        # "buy" | "sell" (taker)
    sequence: int | None = None

@dataclass(frozen=True, slots=True)
class BookDelta:
    venue: str
    symbol: str
    exchange_ts_ns: int
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    side: Side             # "bid" | "ask"
    price: Decimal
    size: Decimal          # new absolute size; 0 removes the level
    sequence: int | None = None

@dataclass(frozen=True, slots=True)
class BookSnapshotLevel:
    venue: str
    symbol: str
    exchange_ts_ns: int
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    snapshot_id: str       # "<connection_id>:<ingest_seq>"
    source: SnapshotSource # "venue" | "sample"
    side: Side
    level: int             # 0 = best
    price: Decimal
    size: Decimal

@dataclass(frozen=True, slots=True)
class Event:
    venue: str
    recv_ts_ns: int
    connection_id: str
    ingest_seq: int
    event_type: EventType  # gap | resync | checksum_failure | invariant_violation | reconnect | disconnect
    symbol: str | None = None
    exchange_ts_ns: int | None = None
    channel: str | None = None
    expected_seq: int | None = None
    received_seq: int | None = None
    detail: str = "{}"     # JSON text with sorted keys

@dataclass(frozen=True, slots=True)
class BookUpdate:
    symbol: str
    is_snapshot: bool
    bids: tuple[PriceLevel, ...]   # (price, size) pairs, exact Decimal
    asks: tuple[PriceLevel, ...]
    exchange_ts_ns: int
    checksum: int | None = None

@dataclass(frozen=True, slots=True)
class NormalizedMessage:
    raw: RawMessage
    channel: str | None = None
    sequence: int | None = None
    trades: tuple[Trade, ...] = ()
    book_updates: tuple[BookUpdate, ...] = ()
    control: dict[str, object] | None = None

TABLES: dict[str, pa.Schema]
def to_arrow(table: str | pa.Schema, records: Sequence[NormalizedRecord]) -> pa.Table
```

### `tickvault/paths.py`

```python
def partition_path(root: str | Path, table: str, venue: str, ts_ns: int) -> Path
def raw_file_path(root: str | Path, venue: str, connection_id: str, recv_ts_ns: int, rotation: int) -> Path
```

### `tickvault/rawio.py`

```python
@dataclass
class RawReadStats:
    files: int = 0
    messages: int = 0
    truncated_files: int = 0
    truncated_lines: int = 0
    bad_lines: int = 0

def encode_line(message: RawMessage) -> bytes
def decode_line(line: bytes) -> RawMessage
def iter_raw(path_or_dir: str | Path, *, stats: RawReadStats | None = None) -> Iterator[RawMessage]
```

### `tickvault/connectors/base.py`

```python
class Connector(Protocol):
    venue: str
    def stream(self) -> AsyncIterator[RawMessage]: ...
    async def subscribe(self, symbols: Sequence[str]) -> None: ...
    async def resync(self, symbol: str, reason: str) -> None: ...
    async def close(self) -> None: ...
```

### `tickvault/storage/sink.py`

```python
class Sink(Protocol):
    def write(self, table: str, records: Sequence[NormalizedRecord]) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...
```

### `tickvault/book/orderbook.py`

```python
class OrderBook:
    def apply_snapshot(self, bids: Sequence[PriceLevel], asks: Sequence[PriceLevel]) -> None
    def apply_delta(self, side: Side, price: Decimal, size: Decimal) -> None
    def best_bid(self) -> PriceLevel | None
    def best_ask(self) -> PriceLevel | None
    def top(self, n: int) -> tuple[list[PriceLevel], list[PriceLevel]]
    def truncate(self, depth: int) -> None
    def __len__(self) -> int
```

### `tickvault/book/processor.py`

```python
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
    def process(self, msg: NormalizedMessage) -> ProcessorOutput
```

### `tickvault/book/sequence.py`

```python
class SequenceTracker:
    def observe(self, key: str, seq: int) -> SeqResult   # FIRST | OK | GAP | STALE
    def reset(self, key: str) -> None
    def last(self, key: str) -> int | None
```

### `tickvault/timeutil.py`

```python
def parse_rfc3339_ns(text: str) -> int
def us_to_ns(microseconds: int) -> int
def ns_to_iso(ts_ns: int) -> str
```

### `tickvault/cli.py`

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

- **`iter_raw` sorts in memory** instead of a streaming k-way merge; fixture
  and replay-scale inputs fit comfortably, and the signature is stable if the
  internals change.
- **Truncated zstd frame detection is best-effort.** A frame that decodes to a
  prefix is handled by the partial-line counter; a frame that decodes to
  nothing is indistinguishable from an empty file at this API level. A
  `ZstdError` path is caught and counted when the decoder raises.
- **`pyarrow-stubs` was added to the `dev` group** (pyarrow has no `py.typed`
  marker and mypy runs strict).
- **`astral-sh/setup-uv` is pinned to `v7`**, the newest floating major tag
  the action publishes.
- **Stats exposure for `iter_raw`** is the optional `RawReadStats` parameter
  the issue allowed ("returned stats object or attribute").

## Known limitations and deferred items

Deferred by scope, not broken:

- `OrderBook` and `BookProcessor` are interfaces with `NotImplementedError`
  bodies; connectors, normalizers, checksum, invariants, raw/Parquet writers,
  compaction, QA, reconcile, pipeline, CLI bodies, Dagster, dashboard and
  Docker are Epic 1/2 work.
- `README.md` is a placeholder until ship time.
- `sortedcontainers` has no `py.typed` marker; the first module that imports
  it under `tickvault` will need `types-sortedcontainers` (or a mypy override)
  through the next wave's lockfile owner.
- The live fixtures were recorded once on 2026-10-15 and are frozen; a live
  re-record is a deliberate, network-touching action (`scripts/record_fixture.py`,
  not run in CI).
- A live-versus-Tardis reconciliation capture on 2026-11-01 remains optional
  (decision A17).

Nothing from issues #1–#8 is left unmerged; this document reflects `main`.
