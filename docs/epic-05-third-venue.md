# Epic 5 — Third venue (Bitfinex)

## Goal

Bitfinex BTC-USD and ETH-USD books and trades flow through the same pipeline as
Coinbase and Kraken — raw writer first, normalizer, `BookProcessor`, Parquet
writer, replay and QA — behind the unchanged `Connector` interface (FR-2.2),
with per-connection sequence-gap detection and CRC32 book checksums. Adding the
venue must not change the existing normalizers, the storage modules or the
replay semantics: it is one `VenueSpec` entry plus the venue's own three
modules (`bitfinex.py` normalizer, `bitfinex.py` connector,
`bitfinex_checksum.py`), all merged earlier in Phase 2 and wired into the
registry by issue #139.

## How it works

### Connection layout

One websocket per symbol, `wss://api-pub.bitfinex.com/ws/2`, no authentication,
so a connection-wide sequence number always scopes to one symbol (A65). Each
connection sends three frames before the venue streams anything:

```
{"event":"conf","flags":229376}
{"event":"subscribe","channel":"book","symbol":"tBTCUSD","prec":"P0","len":"25"}
{"event":"subscribe","channel":"trades","symbol":"tBTCUSD"}
```

`229376` is `SEQ_ALL` (65536) + `OB_CHECKSUM` (131072) + `TIMESTAMP` (32768).
`SEQ_ALL` puts a sequence number on every frame after the four unsequenced dict
frames; `OB_CHECKSUM` makes the venue send a `cs` frame after each book-update
batch; `TIMESTAMP` adds an epoch-millisecond timestamp to book frames, which
otherwise carry none. `prec` is exact-price `P0` and `len` is the **string**
`"25"`, repeated verbatim on every resubscribe because the venue matches a
subscription on its params.

Symbols are translated only on the wire: `BTC-USD` → `tBTCUSD`, and
`t<BASE>:<QUOTE>` when either asset is longer than three letters
(`USDT-USD` → `tUSDT:USD`). Control lines and every normalized row use the
`BASE-QUOTE` form.

The connector yields each connection's `connect` control line (declaring its
one symbol), every received frame unmodified, resync requests, and the final
`disconnect` line. Nothing is filtered or rewritten before the raw writer, so
the raw layer is the venue's bytes and replay can rebuild everything.

### Frame shapes

Real frames from `tests/fixtures/bitfinex_btc_5m.jsonl.zst` (300 s, one
`tBTCUSD` connection, 8241 raw lines), trimmed where noted:

```
{"event":"info","version":2,"serverId":"b176bd51-1ada-4465-b4e1-4dd441fc0b4d","platform":{"status":1}}
{"event":"conf","status":"OK","flags":229376}
{"event":"subscribed","channel":"trades","chanId":258792,"symbol":"tBTCUSD","pair":"BTCUSD"}
{"event":"subscribed","channel":"book","chanId":396954,"symbol":"tBTCUSD","prec":"P0","freq":"F0","len":"25","pair":"BTCUSD"}

[258792,[[2000903768,1793158765067,0.00016296,81173], ...],1,1793158778228]   # trades snapshot
[396954,[[81164,1,0.00022],[81163,1,0.0002822], ...],2,1793158779209]          # book snapshot
[396954,[81154,0,1],3,1793158779620]                                          # book update
[396954,"cs",-717955449,61,1793158781868]                                     # checksum
[258792,"hb",298,1793158792999]                                               # heartbeat
[258792,"te",[2000903773,1793158780785,0.00016301,81170],26,1793158780786]    # executed trade
[258792,"tu",[2000903773,1793158780785,0.00016301,81170],32,1793158780846]    # updated trade
```

Array frames are `[channel_id, payload, sequence, venue_ts_ms]`, except the two
frame types that carry an extra field before the sequence: `cs` is
`[chanId, "cs", checksum, sequence, ts_ms]` and `te`/`tu` are
`[chanId, "te"|"tu", [id, ts_ms, amount, price], sequence, ts_ms]`. The
`subscribed` acknowledgement is the only place a channel id is announced; every
later array frame starts with it.

A book level is `[price, count, amount]`: `amount > 0` is a bid, `amount < 0` an
ask, and the stored size is `|amount|`. `count == 0` removes the level, with
`amount` exactly `1` or `-1` (the magnitude carries no size information), so it
is emitted as a size-0 row. A level leaving the venue's 25-level window is
removed with the same count-0 frame before the level that replaces it, so a
client that applies frames in order never holds more than 25 levels per side; a
thin book can have fewer on one side. Trades are `te` only: `tu` finalizes the
same trade id a few milliseconds later and yields no row (the same rule as the
Coinbase trades backfill), while `tu` still consumes its sequence number.

### Normalization and per-connection state

`tickvault/normalize/bitfinex.py` turns one raw envelope into one
`NormalizedMessage`:

| raw frame | rows |
| --- | --- |
| book snapshot | one `BookUpdate(is_snapshot=True)` with all levels |
| book update | one `BookUpdate` with the single changed level |
| `cs` | one empty `BookUpdate` carrying the venue checksum (no invented rows) |
| `te` | one `Trade`, side from the amount sign |
| `hb`, `tu`, trades snapshot | no rows, but the sequence number |
| dict frames | no rows, no sequence |

Prices and amounts are parsed with `json.loads(..., parse_float=Decimal)` so the
venue's digits stay exact (`0.00007191` would otherwise render as
`7.191e-05` and break the checksum). Floats are never constructed for storage.

A normalizer is otherwise a pure function of one raw message, but Bitfinex
array frames name only a numeric channel id, and that id is announced once in
the `subscribed` acknowledgement. The module therefore keeps per-connection
state: a `connect` control line replaces the entry and takes the symbol from its
declared `symbols`; `subscribed` adds `chanId → (channel, symbol)`; `disconnect`
drops the entry. That state is rebuilt **only** from raw lines in canonical
`(recv_ts_ns, connection_id, ingest_seq)` order, so a replay in a fresh process
and a replay in a process that already replayed the same file produce identical
output. A data frame whose channel id has no acknowledgement yet raises
`NormalizeError` rather than guessing. The package docstring
(`tickvault/normalize/__init__.py`) states the rule for any future venue.

### Sequence tracking

`SEQ_ALL` gives one strictly increasing counter per connection, not per channel:
the trades snapshot is sequence 1, the book snapshot 2, and both channels share
the counter afterwards. In the fixture 8235 sequenced frames are numbered
exactly 1..8235 with no gap and no duplicate (the four dict frames consume no
number). Heartbeats, `cs` frames, snapshots, `te` and `tu` all consume one.

Because a connection carries one symbol, any missing value is a connection-level
gap that the processor scopes to that symbol: one `gap` event, the symbol goes
`SYNCING`, deltas are dropped until a snapshot arrives, and the drop count lands
in the next `resync` event's `detail.deltas_dropped` (A19). The normalizer
attaches the sequence to dataless frames too, so heartbeats and `tu` keep the
tracker honest (A25). A malformed array frame is a `NormalizeError`, counted and
skipped by the pipeline, never a silent sequence hole.

### Checksum

After a batch of book changes Bitfinex sends
`[chanId, "cs", signed_checksum, sequence, ts_ms]`. The digest is CRC32 over the
top 25 levels per side (the subscribed `len`), interleaved by rank with the bid
first and ask amounts negated:

```
bid0:size0:ask0:-size0:bid1:size1:ask1:-size1: ...
```

`tickvault/book/bitfinex_checksum.py` reproduces it. The numbers are rendered
the way the venue's JavaScript reference client renders them —
`Number.prototype.toString`: shortest round-trip digits, plain notation for
`1e-6 <= |x| < 1e21`, exponent form outside it (`1e-7`, never Python's `1e-07`;
`1e+21`), no trailing zeros (`0.50` → `0.5`), never `-0`. This is the Bitfinex
form of the Kraken trailing-zero trap (A2): the values are stored as `Decimal`
but the checksum input is formatted from the float64, because that is what the
venue hashes. The CRC is a **signed** 32-bit integer — 47 of the fixture's 91
live values are negative — so it is converted from `zlib.crc32`'s unsigned
value before comparison.

The processor's checksum registry maps venue → `(function, depth)`; Bitfinex is
registered at depth 25 alongside Kraken, and the legacy `kraken_depth`
constructor argument still overrides only Kraken (A23/A66). A mismatch emits a
`checksum_failure` event, moves the symbol to `SYNCING` and requests a resync.
Because the `cs` frame itself carries no levels, nothing is discarded at the
failure point: the state the venue hashed was emitted by the update frames
before it.

In the committed fixture, rebuilding the book from the snapshot and applying
every update in order reproduces **91 of 91** `cs` values; replay reports zero
checksum failures.

### Resync path

A resync for Bitfinex is an unsubscribe plus a resubscribe of `book` on the same
connection; the venue answers with a fresh snapshot on the same channel id, and
the connection sequence continues across the gap (no reset, no hole). Frames of
the old subscription still in flight are yielded like any other frame; the
symbol is `SYNCING` and drops them until the snapshot. If no snapshot arrives
within 10 s, the connection is rebuilt with the existing backoff schedule. Info
code 20051 ("please reconnect") also rebuilds it. Info code 20060 (maintenance
start) defers a requested resync because the venue answers nothing during the
window, and 20061 (maintenance end) unsubscribes the acknowledged channels and
resubscribes both, which brings a fresh snapshot.

## Key decisions and reasoning

**Bitfinex, not Bitstamp (A64).** The PRD named Bitstamp as a candidate *if* it
had sequence or checksum semantics. Its public `diff_order_book` channel carries
timestamps only — no sequence number and no checksum — so sequence gaps would
be undetectable and the third venue would not exercise the anomaly paths the
project is about. Bitfinex's public v2 websocket offers both on the same
connection with no authentication, so a gap is detectable per connection and a
wrong book state is detectable per update batch. Alternative venues were
rejected for reachability, not merit: OKX left Canada in 2023, and Gemini's
exchange name collides with the project's no-AI-attribution list (H2), so it is
never considered.

**One connection per symbol (A65).** Bitfinex's sequence counter is
connection-wide, so a single connection carrying BTC-USD and ETH-USD would turn
one symbol's missing frame into a gap for both. One connection per symbol keeps
the existing processor model — connection = symbol authority (A29) — exact, and
reuses the Coinbase layout. It costs one extra socket per symbol, well inside
the venue's connection-rate limit that the existing backoff already respects.
`SEQ_ALL` is documented by the venue as beta; see the limitations below.

**Checksum at depth 25 through the registry.** The venue hashes what a `len=25`
subscription can hold, so the depth is a property of the feed, not of the
processor. The registry's per-venue depth map (from wave 07) keeps Kraken's
top-10 slice and Bitfinex's 25-level window in one place, and the default
processor picks up Bitfinex without any construction argument:
`BookProcessor().checksum_depths == {"kraken": 25, "bitfinex": 25}`.

## Interfaces for future epics

Adding a venue is deliberately small, and Bitfinex is the second proof of it
(the first was Kraken). The exact steps:

1. Write the venue's modules in their own files:
   `tickvault/normalize/<venue>.py` (raw envelope → `NormalizedMessage`),
   `tickvault/connectors/<venue>.py` (a `Connector` implementation), and, only
   if the feed carries a book checksum, `tickvault/book/<venue>_checksum.py`.
   None of them may need a change to `models.py`, the storage modules or the
   pipeline.
2. Add the recorded fixture under `tests/fixtures/` and its entry in
   `fixtures.json`, so the fixture inventory test covers it.
3. Add **one** entry to `VENUES` in `tickvault/venues.py`:

   ```python
   "bitfinex": VenueSpec(
       name="bitfinex",
       normalize=bitfinex_normalize.normalize,
       connector=BitfinexConnector,
       checksum=bitfinex_checksum,
       checksum_depth=25,
   ),
   ```

   plus the corresponding imports at the top of that file.
4. Update only the existing assertions that hard-code the registry size:
   `tests/test_venues.py` and the two default-checksum-map tests in
   `tests/test_book_processor.py`.

Nothing else changes. `tickvault.pipeline` derives `_NORMALIZERS` and
`default_connector_factories()` from `VENUES`; `tickvault.cli` derives
`--venues` choices from `venue_names()`; the processor's default checksum map
comes from `checksum_registry()`; QA, the SQL views, compaction, manifests and
the dashboard iterate over whatever venues the data contains. Issue #139 found
no other two-venue assumption in code: the full suite plus a repository grep for
venue lists turned up only fixture-specific test data, the Compose ingester's
argument (ship wave) and a one-line CLI help example.

## Verification on the Mac (2026-10-28)

Fixture replay, twice, byte-identical:

```
$ uv run tickvault replay --input tests/fixtures/bitfinex_btc_5m.jsonl.zst --out /tmp/tv-bfx-e5
replay stopped: completed
raw messages: 8241
normalized messages: 8241 (normalize errors: 0)
rows: trades=135 book_deltas=7832 book_snapshots=5950 events=1
events by type: gap=0 resync=0 checksum_failure=0 invariant_violation=0 reconnect=0 disconnect=1
resync requests: 0
$ uv run tickvault replay --input tests/fixtures/bitfinex_btc_5m.jsonl.zst --out /tmp/tv-bfx-e5b
$ diff -r /tmp/tv-bfx-e5/norm /tmp/tv-bfx-e5b/norm        # no output
```

QA over that replay:

```
$ uv run tickvault qa --data /tmp/tv-bfx-e5
venue     symbol   date        messages  trades  book_deltas  venue_snapshots  gaps  resyncs  checksum_failures  reconnects  invariant_violations  lag_p50_ms  lag_p99_ms  coverage  unexplained  book_missing
--------  -------  ----------  --------  ------  -----------  ---------------  ----  -------  -----------------  ----------  --------------------  ----------  ----------  --------  -----------  ------------
bitfinex  BTC-USD  2026-10-28      7968     135         7832                1     0        0                  0           0                     0      54.759     146.258    0.9833            0         False
   TOTAL        -           -      7968     135         7832                1     0        0                  0           0                     0           -           -         -            0             0
$ echo $?
0
```

Live Bitfinex connector check (30 s, `@pytest.mark.network`):

```
$ uv run pytest -q -s -m network tests/test_bitfinex_live.py
bitfinex BTC-USD 30s: 470 frames, 1 snapshots, 425 book updates, 5 checksums, 15 trades, 4 heartbeats, 1 connection(s), 471 raw lines
.
1 passed in 30.34s
```

Three-venue live ingest, 120 s, BTC-USD and ETH-USD on Coinbase, Kraken and
Bitfinex (the 10-second progress lines are trimmed to the last one):

```
$ uv run tickvault ingest --venues coinbase,kraken,bitfinex --symbols BTC-USD,ETH-USD --duration 120 --out /tmp/tv-3v
2026-10-28 19:19:09,589 INFO tickvault.pipeline: ingest progress: elapsed=120s raw_msgs=16099 (coinbase=4179 kraken=5621 bitfinex=6299) trades=890 book_deltas=42151 anomalies=0 resyncs=0 written=2.8MB
2026-10-28 19:19:09,998 INFO tickvault.pipeline: ingest finished: stopped=duration elapsed=121s raw_msgs=16107 (coinbase=4184 kraken=5622 bitfinex=6301) trades=890 book_deltas=42154 anomalies=0 resyncs=0 written=3.1MB
live stopped: duration
raw messages: 16107
normalized messages: 16107 (normalize errors: 0)
rows: trades=890 book_deltas=42154 book_snapshots=74854 events=5
events by type: gap=0 resync=0 checksum_failure=0 invariant_violation=0 reconnect=0 disconnect=5
resync requests: 0
```

The five disconnects are the five connections closed by the graceful stop (two
Coinbase products, one Kraken connection, two Bitfinex symbols). The raw layer
from this run holds 71 Bitfinex `cs` frames; all 71 were verified against the
book with no `checksum_failure`.

QA over the live run — six venue/symbol rows, unexplained 0, no missing book:

```
$ uv run tickvault qa --data /tmp/tv-3v
venue     symbol   date        messages  trades  book_deltas  venue_snapshots  gaps  resyncs  checksum_failures  reconnects  invariant_violations  lag_p50_ms  lag_p99_ms  coverage  unexplained  book_missing
--------  -------  ----------  --------  ------  -----------  ---------------  ----  -------  -----------------  ----------  --------------------  ----------  ----------  --------  -----------  ------------
bitfinex  BTC-USD  2026-10-28      2870      69         2800                1     0        0                  0           0                     0      79.166    3538.727    0.9823            0         False
bitfinex  ETH-USD  2026-10-28      3222      25         3196                1     0        0                  0           0                     0       74.09    3767.611    0.9741            0         False
coinbase  BTC-USD  2026-10-28      2355     551        16148                1     0        0                  0           0                     0      54.123    3294.364       1.0            0         False
coinbase  ETH-USD  2026-10-28      1583     155        12471                1     0        0                  0           0                     0      97.215     3375.68       1.0            0         False
  kraken  BTC-USD  2026-10-28      1596      43         2100                1     0        0                  0           0                     0      65.662    4569.868    0.8879            0         False
  kraken  ETH-USD  2026-10-28      3902      47         5439                1     0        0                  0           0                     0      64.944    4291.567    0.9831            0         False
   TOTAL        -           -     15528     890        42154                6     0        0                  0           0                     0           -           -         -            0             0
$ echo $?
0
```

DuckDB over the same tree shows three venues with trades:

```
$ uv run tickvault sql --data /tmp/tv-3v -c "select venue, count(*) from trades group by 1 order by 1"
venue     count_star()
--------  ------------
bitfinex            94
coinbase           706
  kraken            90
```

## Deviations from the plan

- The issue's plan said "nothing else in shared files may need to change". Two
  existing test modules pinned the registry contents and had to be updated:
  `tests/test_venues.py` (registry order, the per-venue spec test, the checksum
  registry and the pipeline-derived maps — four tests) and
  `tests/test_book_processor.py::test_default_checksum_map_comes_from_the_venue_registry`
  plus `::test_kraken_depth_argument_overrides_the_registered_depth` (their
  expected checksum maps). Both are mechanical two-venue assumptions, not design
  changes.
- The wave 08 contract request from #120 asked for a docstring widening in
  `tickvault/normalize/__init__.py` so a normalizer may keep per-connection
  state; that docstring change landed with this issue, as the issue allowed.
- `compose.yaml`'s ingester command still lists `coinbase,kraken`: Compose venue
  changes are the Phase 2 ship wave's (the issue marks them out of scope), so
  the Compose demo does not yet ingest Bitfinex by default. The three-venue
  evidence above comes from the CLI.
- `README.md` and `docs/architecture.md` are untouched (A73); the README's
  three-venue section and architecture §14 are the ship wave's.

## Known limitations

- `SEQ_ALL` is documented by Bitfinex as beta. If the venue ever stops honouring
  the flag, connections would carry no sequence numbers and the tracker would
  see a single unsequenced stream, i.e. gaps would go undetected rather than
  false-alarm. Nothing in this epic can be done about that short of a different
  gap signal; the raw layer records the venue's `conf` acknowledgement, so a
  changed flag echo stays visible in the recording.
- The info codes 20051, 20060 and 20061 are implemented from the venue's API
  documentation but were never observed: about a dozen minutes of live sessions
  (the wave 07 exploratory runs plus this wave's live checks) and the recorded
  fixture contain none, so those branches are covered by unit tests with
  synthetic frames, not by live evidence.
- The checksum compares the venue's JavaScript number rendering against
  Python's float64 shortest-repr. The two can disagree only for a value with 17
  significant digits where two shortest representations both round-trip; the
  venue sends far fewer digits, and the recorded fixture's 91 live checksums all
  match.
- Only the spot `book` channel at `P0`/`len=25` and `te` trades are in scope.
  Bitfinex funding, derivatives, `R0` books and authenticated channels are out
  of scope, as is Tardis reconciliation for Bitfinex (Phase 1 reconciles
  Coinbase only, A35).
- `tickvault` now has three venues in the registry, but the dashboards' bundled
  sample and the public QA site still describe the two-venue Phase 1 capture;
  that stays correct until the ship wave regenerates them.
