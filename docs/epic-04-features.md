# Epic 4 — Features table

## Goal

Give any TickVault data root a research-ready derived table: 1-second and
1-minute bars per venue/symbol/day with the book touch, spread, top-5 depth
imbalance, trade count, volume, signed volume, VWAP and realized volatility,
computed only from data strictly before each bar's end. The table is
reproducible byte for byte, queryable through DuckDB and materialized by the
Dagster daily job (FR-2.1, decisions A67 and A68).

## How it works

### Bars

The unit is a half-open interval `[bar_start_ns, bar_end_ns)`. Both
resolutions are fixed widths on the UTC nanosecond clock: `1s` is
1,000,000,000 ns and `1m` is 60,000,000,000 ns. A bar belongs to the date of
its start, which is also the `date=` hive partition of the file it lands in.

Bars cover the span of that day's data for a venue/symbol, not all 86,400
seconds of the day: the first bar starts at the second containing the first
sample or trade, the last bar ends at the boundary after the last sample or
trade. A quiet minute between two trades is still emitted, with
`trade_count = 0`, zero volumes and a null `vwap`, because the span has to stay
contiguous for a time series.

### Book state and the 5-second staleness limit

Book columns come from the last `book_snapshots` row with `source = 'sample'`
(the processor's once-per-exchange-second top-10 sample, A9) whose
`exchange_ts_ns` is strictly earlier than `bar_end_ns`. If that sample is more
than 5 seconds older than `bar_end_ns` (`MAX_SAMPLE_AGE_NS`), the bar gets no
book state at all: `best_bid`, `best_ask`, `mid`, `spread_bps`,
`depth_imbalance_5` and `book_sample_ts_ns` are null. There is no forward fill
past that limit.

Level 0 of each side gives the touch. `mid` and `spread_bps` are computed from
the exact `Decimal` prices before the result is cast to float64:

```
mid        = (best_bid + best_ask) / 2
spread_bps = (best_ask - best_bid) / mid × 10⁴
```

`depth_imbalance_5` sums sizes of levels 0–4 on each side and normalizes:

```
depth_imbalance_5 = (Σ bid size L0–4 − Σ ask size L0–4) / (Σ bid size L0–4 + Σ ask size L0–4)
```

Snapshots are collapsed to one row per `snapshot_id` first, so a snapshot's
levels cannot be counted twice. A one-sided book keeps its touch and a null
mid, while its imbalance is ±1 (see Known limitations).

### Trades window

Trades use the closed-open window `bar_start_ns ≤ exchange_ts_ns <
bar_end_ns`: a trade exactly at the bar start belongs to this bar, a trade
exactly at the close belongs to the next one. From those rows:

- `trade_count` — number of trades;
- `volume` — Σ size;
- `signed_volume` — Σ size with taker `buy` positive and `sell` negative;
- `vwap` — Σ(price × size) / Σ size, null when the bar has no trades.

Prices and sizes are summed as `Decimal` and only the finished values are cast
to float64. The side is the taker side of the normalized trades table (A7).

### Realized volatility

`realized_vol = sqrt(Σ r_k²)` over every whole second `T_k` with
`bar_start_ns < T_k ≤ bar_end_ns`, where

```
r_k = ln(m(T_k) / m(T_k − 1 s))
```

and `m(T)` is the as-of mid at `T`: the mid of the last sample with
`exchange_ts_ns < T`, provided it is at most 5 s old. A 1-second bar therefore
has one return and a 1-minute bar has sixty. The as-of mids are evaluated once
on the shared whole-second grid, so consecutive returns share endpoints:
`m(T_k − 1 s)` is exactly the previous grid value. Returns with a missing
endpoint are skipped; when none survive, the value is null.

### No lookahead

`tests/test_features_lookahead.py` deletes every normalized row with
`exchange_ts_ns ≥ t`, recomputes the features and asserts that every bar with
`bar_end_ns ≤ t` is identical to the reference, for fixed cutoffs (including
one in the middle of a minute) and a Hypothesis property over `t`. It also
requires a non-zero number of comparable bars, so a cutoff cannot pass
vacuously.

### Storage layout

```
<out>/features/resolution=<1s|1m>/venue=<venue>/date=<YYYY-MM-DD>/part-<symbol>.parquet
```

One file per resolution/venue/date/symbol, rows sorted by
`(symbol, bar_start_ns)`, zstd compression and the same fixed row-group size as
the normalized writer, so two runs produce byte-identical files. The tree is
derived output: it is written outside the four normalized tables and can be
deleted and rebuilt from `norm/` at any time.

### Entry points

**CLI** — `tickvault features` writes the tree and prints one line per
resolution:

```
tickvault features --data <root> --date YYYY-MM-DD --out <root2> [--resolution 1s,1m]
```

**SQL** — `tickvault sql` adds a `features` view whenever `<data>/features/`
holds Parquet files. The hive keys `resolution` and `venue` are `VARCHAR` and
`date` is `DATE`; a features-only root still gets the four typed empty
normalized views, which is what makes the Epic 4 definition of done queryable
without the normalized tree.

**Dagster** — the `features` asset runs `compute_features` → `write_features`
for its daily partition, downstream of `compacted_partitions`, and reports
`rows_1s`, `rows_1m` and `files` in its materialization metadata. It is part of
`daily_job`, so one materialization compacts, reports QA, writes the manifest
and writes the features for a day.

## Key decisions and reasoning

- **Half-open bars (A67).** `[start, end)` is the only convention under which
  every timestamp belongs to exactly one bar: a sample or trade exactly at a
  boundary goes to the next bar, with no double counting and no gap. It also
  makes the lookahead property expressible as one inequality.
- **The strict `< bar_end_ns` rule (A67).** The PRD says "at or before the
  close"; the implementation deliberately uses "before the close" for book
  samples and trades. A row stamped exactly at the close is information from
  the next bar, and admitting it would make the classic bug (using the future
  to describe the past) reachable at every boundary. The lookahead test is
  built around this inequality.
- **float64 for derived values (A68).** Features are research values:
  mid, spread and volatility are computed to be consumed by analysis, not to
  reproduce venue decimals. The exact `decimal128(38, 18)` prices stay in the
  normalized tables, and every feature is computed from those decimals before
  the final cast, so no intermediate rounding happens in float.
- **Features are not a normalized table (A68).** `FEATURES_SCHEMA` lives in
  `tickvault/models.py` for one place of truth, but it is not in `TABLES`.
  Compaction, manifests and QA keep operating on exactly the four ingested
  tables, and the derived tree can be rebuilt without touching them.
- **Recompute from samples per call.** The bars are a pure function of the
  day's normalized partitions; there is no incremental state and no
  cross-day read. That is slower than an incremental build but makes a
  recomputation for any date reproducible and keeps the lookahead guarantee
  local to one call.
- **As-of mids on the shared second grid.** Evaluating the mid once per whole
  second, instead of once per bar, guarantees that adjacent 1-second bars use
  the same endpoint value and that a 1-minute volatility is exactly the
  aggregation of its sixty 1-second returns.
- **Deterministic writing.** The file layout, the row order and the Parquet
  options are fixed in `features_path`/`write_features`, which is what lets
  the CLI and the Dagster asset be compared byte for byte (and what the
  end-to-end test asserts).
- **DuckDB for reads.** The day's partitions are opened once with a
  `read_parquet` over the exact `date=` files, so a feature run never touches
  another day and never loads the whole normalized tree.

## Interfaces for future epics

```python
# tickvault/features.py
RESOLUTIONS: Mapping[str, int]          # {"1s": 1_000_000_000, "1m": 60_000_000_000}
MAX_SAMPLE_AGE_NS: int = 5_000_000_000

features_path(out_root, resolution, venue, date, symbol) -> Path

compute_features(
    data_root: str | Path,
    date: str,
    resolutions: Sequence[str] = ("1s", "1m"),
) -> dict[str, pa.Table]

write_features(
    tables: Mapping[str, pa.Table],
    out_root: str | Path,
    date: str,
) -> list[Path]
```

`FEATURES_SCHEMA` (`tickvault.models`):

| column | Arrow type | nullable |
| --- | --- | --- |
| `venue` | string | no |
| `symbol` | string | no |
| `resolution` | string | no |
| `bar_start_ns` | int64 | no |
| `bar_end_ns` | int64 | no |
| `best_bid` | float64 | yes |
| `best_ask` | float64 | yes |
| `mid` | float64 | yes |
| `spread_bps` | float64 | yes |
| `depth_imbalance_5` | float64 | yes |
| `book_sample_ts_ns` | int64 | yes |
| `trade_count` | int64 | no |
| `volume` | float64 | no |
| `signed_volume` | float64 | no |
| `vwap` | float64 | yes |
| `realized_vol` | float64 | yes |

The `features` SQL view is `SELECT *` over the tree plus the hive keys, so its
columns are the schema above in order followed by `date DATE`. The Dagster
asset keys are `features` (asset), `rows_1s`, `rows_1m`, `files`
(materialization metadata). Downstream work can add resolutions by extending
`RESOLUTIONS`; bars are only emitted for the labels a caller requests.

File layout:

```
<out>/features/
  resolution=1s/venue=coinbase/date=2026-10-15/part-BTC-USD.parquet
  resolution=1m/venue=kraken/date=2026-10-15/part-ETH-USD.parquet
```

## Deviations from the plan

- **The CLI tests live in `tests/test_cli_phase2.py`.** The roadmap listed
  `tests/test_cli_features.py`, but the `features` command and its tests were
  frozen in wave 07 together with the rest of the Phase 2 CLI surface (#113),
  which already owns that file.
- **The definition-of-done fixture is `dashboard/sample` (A69).** The PRD
  pointed at `tests/fixtures/tv_sample --date 2026-11-01`, a fixture that was
  never planned; the DoD now runs against the bundled 30-minute real capture
  for 2026-10-15.
- **1-second bar counts are not exactly 1,800.** The DoD expects about 1,800
  per venue/symbol; the actual range is 1,798–1,801 because each venue's
  30-minute capture starts and ends mid-second, so the first and last partial
  seconds of the span differ by venue. Coverage of book state is 100 % for
  Coinbase's 1-second bars and 98.5 % for Kraken's: the sample's Kraken feed
  drops at 17:28:57 and reconnects at 17:29:29, so the 27 bars from 17:29:02
  to 17:29:28 are more than 5 s past the last sample and correctly have null
  book columns. The final 1-minute bar closes 31 s after the last sample and
  is null for the same reason.
- **The end-to-end test compacts its replayed root before materializing the
  daily job.** The job's first asset compacts by design, so leaving the
  multi-file replay in place would make the "input trees unchanged" hash
  comparison measure compaction rather than the features path. With the root
  already compacted, the job merges nothing (`rows == 0`) and the hashes speak
  about the feature read/write only.
- **Querying a features-only root works, but is not the sample's shape.** The
  DoD passes `--data <features-only root>` to `tickvault sql`; the SQL module
  already handles that with typed empty normalized views, so the feature query
  does not need `dashboard/sample` mounted too.

## Known limitations

- **The 1-second sample cadence bounds 1-second features.** Book state for a
  1-second bar is the last processor sample from before the bar's close, so
  the touch can be up to about a second old and is not the bar's closing
  quote. `realized_vol` on a 1-second bar is a single return; it becomes a
  meaningful volatility estimate at `1m` and longer.
- **No dashboard view yet.** Features are computed, stored and queryable, but
  the Streamlit app still charts the normalized tables; a features view is
  future work (out of scope for Epic 4).
- **A one-sided book's imbalance is ±1.** When only bids (or only asks) have
  levels 0–4, the numerator equals the denominator (or its negation) and the
  imbalance saturates at `±1`; the missing touch makes `mid` and `spread_bps`
  null.
- **Whole-second grid.** The volatility grid assumes bar boundaries fall on
  whole seconds, which holds for `1s` and `1m`; a sub-second resolution would
  need a finer as-of grid.
- **No trades means no VWAP.** A quiet bar has `vwap = null`, not the previous
  price; consumers must decide whether to carry a price forward, and the
  feature table deliberately does not.
- **The tree is rebuilt, not maintained.** A new capture or a corrected
  normalized partition requires recomputing the affected day; compaction,
  manifests and QA do not track the derived tree.

## Worked example

One 1-minute bar from the bundled sample (`dashboard/sample`, 2026-10-15,
Coinbase BTC-USD, the bar starting at 17:12:00 UTC). First the bar itself:

```console
$ uv run tickvault sql --data /tmp/tv-feat -c "
    select timezone('UTC', to_timestamp(bar_start_ns / 1e9)) as bar_start_utc,
           best_bid, best_ask, mid, spread_bps, depth_imbalance_5, book_sample_ts_ns,
           trade_count, volume, signed_volume, vwap, realized_vol
    from features
    where resolution = '1m' and venue = 'coinbase' and symbol = 'BTC-USD'
      and bar_start_ns = 1792084320000000000"
bar_start_utc        best_bid  best_ask  mid        spread_bps             depth_imbalance_5    book_sample_ts_ns    trade_count  volume      signed_volume  vwap              realized_vol
-------------------  --------  --------  ---------  ---------------------  -------------------  -------------------  -----------  ----------  -------------  ----------------  ----------------------
2026-10-15 17:12:00  82094.23  82094.24  82094.235  0.0012181123315175542  -0.3148907011266394  1792084379043509000          280  1.02045724    -0.46067244  82112.52767257097  0.0006767521716519581
```

The book sample used is the five-level snapshot at
`exchange_ts_ns = 1792084379043509000`, 0.956 s before the bar's
`bar_end_ns = 1792084380000000000` and therefore fresh:

```console
$ uv run tickvault sql --data dashboard/sample -c "
    select side, level, price, size from book_snapshots
    where venue='coinbase' and symbol='BTC-USD' and source='sample'
      and exchange_ts_ns = 1792084379043509000 and level < 5
    order by side desc, level"
side  level  price                     size
----  -----  ------------------------  ------------------------
bid       0  82094.230000000000000000  0.053402400000000000
bid       1  82092.580000000000000000  0.024362740000000000
bid       2  82092.130000000000000000  0.073069690000000000
bid       3  82091.750000000000000000  0.000111950000000000
bid       4  82091.140000000000000000  0.029550040000000000
ask       0  82094.240000000000000000  0.027000760000000000
ask       1  82095.890000000000000000  0.024361760000000000
ask       2  82098.860000000000000000  0.001685700000000000
ask       3  82099.960000000000000000  0.109668900000000000
ask       4  82099.970000000000000000  0.183700000000000000
```

And the trades inside `[17:12:00, 17:13:00)`, grouped by taker side:

```console
$ uv run tickvault sql --data dashboard/sample -c "
    select side, count(*) trades, sum(size) volume,
           sum(CAST(price AS DOUBLE) * CAST(size AS DOUBLE)) notional
    from trades
    where venue='coinbase' and symbol='BTC-USD'
      and exchange_ts_ns >= 1792084320000000000
      and exchange_ts_ns < 1792084380000000000
    group by side order by side"
side  trades  volume                notional
----  ------  --------------------  ------------------
buy       72  0.279892400000000000  22983.722851324197
sell     208  0.740564840000000000  60808.60050685119
```

The arithmetic from those rows:

- `mid = (82094.23 + 82094.24) / 2 = 82094.235`
- `spread_bps = (82094.24 − 82094.23) / 82094.235 × 10⁴ = 0.01 / 82094.235 × 10⁴ = 0.0012181123315…`
- bid sizes L0–4 sum to `0.18049682`, ask sizes L0–4 sum to `0.34641712`, so
  `depth_imbalance_5 = (0.18049682 − 0.34641712) / (0.18049682 + 0.34641712) = −0.3148907011…`
- `trade_count = 72 + 208 = 280`
- `volume = 0.2798924 + 0.74056484 = 1.02045724`
- `signed_volume = (+0.2798924) + (−0.74056484) = −0.46067244`
- `vwap = (22983.722851324197 + 60808.60050685119) / 1.02045724 = 83792.323358175387 / 1.02045724 = 82112.5276725709…`

`realized_vol` is not hand-computable from a handful of rows: it aggregates the
sixty one-second mid returns of the bar from the same sample stream.
