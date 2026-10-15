# Epic 2 — Pipeline, CLI, orchestration, dashboard

## Goal

Wire the Epic 1 domain code into the Phase 1 product: one pipeline module shared
by live ingestion and deterministic replay, a complete CLI over it, a Dagster
daily job for compaction, QA reports and manifests, a Streamlit dashboard with a
bundled sample, and arm64 Docker images running under Compose — plus an
end-to-end Compose check that ingests real venue data and reports its own
quality.

## How it works

### Pipeline (`tickvault/pipeline.py`)

The pipeline owns both execution modes; the CLI and the Dagster assets are thin
wrappers over it.

`run_replay(input, out)` reads a raw file or raw tree in canonical
`(recv_ts_ns, connection_id, ingest_seq)` order through the streaming k-way
merge (A32), so memory is bounded by the number of open files, not by the size
of the raw tree. Every message goes through the venue normalizer, the
`BookProcessor` and a `ParquetWriter`; only `out/norm/` is written, the raw
layer is never copied or modified, malformed frames are counted and skipped, and
resync requests are counted and discarded (replay has nothing to resynchronize).
Because every flush decision is a function of the records, two replays of the
same input produce byte-identical `norm/` trees.

`run_live(venues, symbols, out, duration=None, max_gb=None)` builds one
connector per venue (`coinbase` opens one websocket per product so a sequence
gap scopes to a product; `kraken` uses one connection for all symbols), merges
their `RawMessage` streams through one `asyncio.Queue`, and for every message
writes the raw envelope **before** normalizing and processing it exactly like
replay. Resync requests from the processor are routed back to the venue's
connector, which unsubscribes and resubscribes the affected channel on the live
socket. The loop ticks once per second to flush idle Parquet buffers (60 s) and
to enforce `--max-gb` against the data-root size. A graceful stop on
`--duration`, SIGTERM/SIGINT or `--max-gb` closes the connectors, drains the
queue, and closes the raw and Parquet writers, so every file on disk is
complete.

Both entry points return a frozen `PipelineStats` (raw lines, normalized
messages, rows per table, events per type, resync requests, `stopped_by`) with a
stable `to_dict()` for tests and tooling.

### CLI

Every command is a thin layer over the pipeline or an Epic 1 function.

```
tickvault ingest   --venues TEXT --symbols TEXT --out PATH [--duration FLOAT] [--max-gb FLOAT]
tickvault replay   --input PATH --out PATH
tickvault qa       --data PATH [--date TEXT] [--json] [--reports]
tickvault compact  --data PATH --date TEXT
tickvault sql      --data PATH [-c TEXT]
tickvault reconcile [--tardis-date TEXT] [--venue TEXT] [--symbol TEXT] [--fixture PATH] [--data PATH] [--json]
tickvault bench    [--input PATH] [--json]
```

- `ingest` / `replay` print the pipeline summary; `ingest` exits `3` when
  `--max-gb` stopped the run.
- `qa` prints the per-venue/symbol/day text table (or the JSON report with
  `--json`) and exits `1` when any row has an unexplained anomaly or a missing
  book (messages but no venue snapshot, A30). `--reports` also writes
  `<data>/reports/qa-<date>.json` and `.md`.
- `compact` merges multi-file partitions and then writes the date's manifest
  under `<data>/manifests/date=<date>/manifest.json`.
- `sql` prints the DuckDB view DDL, or runs one query with `-c` and prints an
  aligned table.
- `reconcile` runs offline (`--fixture <csv.gz>` with `--data <root>`) or
  downloads a first-of-month Tardis slice (`--tardis-date` with `--data`,
  cached under `<data>/cache/`); matches are by `trade_id` (A21).
- `bench` replays the two recorded 5-minute fixtures by default (A26) and
  reports throughput and peak RSS against the PRD targets.
- Exit codes: `0` success, `1` runtime error, `2` invalid usage, `3` ingest
  stopped by `--max-gb`.

### Dagster daily job (`orchestration/definitions.py`)

Three daily-partitioned assets wrap the same functions the CLI calls:

- `compacted_partitions` → `tickvault.storage.compact.compact(root, date)`;
  metadata `partitions`, `rows`.
- `qa_report`, depending on it → `tickvault.qa.report.run_qa(root, date)` plus
  `write_reports(report, <root>/reports, date)`; metadata `rows`,
  `unexplained`, `book_missing`, `json_report`, `markdown_report`.
- `manifest`, depending on `qa_report` → `tickvault.storage.manifest.write_manifest(root, date)`.

The data root comes from a Dagster `ConfigurableResource` named `data_root`,
which defaults to `$TICKVAULT_DATA_DIR` and falls back to `./data`.
`daily_job` selects the three assets; `daily_job_schedule` fires at 00:30 UTC
and requests the previous UTC day's partition (defined, not required to run).
There is no business logic in the asset bodies, and no daemon is needed to run
them:

```
dagster asset materialize -m orchestration.definitions --select '*' --partition 2026-11-01
```

### Dashboard

`dashboard/app.py` is a Streamlit app that reads the normalized layer through
the same DuckDB views as `tickvault sql` (`tickvault.sql.connect`). All queries
live in `dashboard/queries.py`; each chart runs one aggregated SQL statement,
every series is capped at the 2000 newest points, and results are cached for
10 s, so the app never loads a whole table and a live root refreshes
periodically.

The sidebar selects the data root (default: `TICKVAULT_DATA_DIR`, else the
bundled sample) and filters by venue and symbol. The body shows five KPI tiles
(message rate over the last 5 minutes, gaps, checksum failures, reconnects,
unexplained), the message-throughput chart, events by venue, top-of-book and
spread/mid charts built from the processor's top-10 `sample` snapshots (A9), and
the QA history table. With no normalized data under the root the app renders an
explanatory empty state instead of failing.

The bundled sample (`dashboard/sample/`, a plain data root with `norm/` and
`manifests/`) holds two recorded hours of 2026-10-15 for both venues: 14 files,
171,986 rows, about 2.2 MB. It is built deterministically by
`scripts/build_dashboard_sample.py` from raw inputs and is replaced by a longer
real capture in Epic 3 (A33).

### Docker and Compose

`docker/Dockerfile` is a multi-stage build on `python:3.12-slim`
(`--platform=linux/arm64`) that installs dependencies with uv from `uv.lock`
and produces three targets:

- `ingester` — base dependencies only (`uv sync --frozen --no-dev --no-default-groups`);
- `dashboard` — base plus the `dashboard` group (Streamlit);
- `orchestration` — base plus the `orchestration` group (Dagster + webserver), optional.

No image contains uv, build tools, dev dependencies or data; every image runs as
non-root uid/gid 10001 `tickvault` and owns `/data`, which is where the shared
named volume mounts.

`compose.yaml` (project `tickvault`) starts:

- `ingester`: `tickvault ingest --venues coinbase,kraken --symbols BTC-USD,ETH-USD --out /data`,
  `restart: unless-stopped`;
- `dashboard`: `streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0`
  with `TICKVAULT_DATA_DIR=/data`, published on 8501, healthcheck
  `curl -fsS http://localhost:8501/_stcore/health`;
- `dagster` (profile `orchestration`): `dagster dev -m orchestration.definitions`
  on port 3000, reading the same volume.

Both default services mount the `tickvault-data` named volume at `/data`. The
`Makefile` adds `docker-build`, `demo-live` (up + dashboard URL) and
`demo-down`. Docker is verified locally, not in CI (A34).

`scripts/compose_smoke.sh` is the end-to-end check used at the epic audit: it
builds and starts the stack, waits 60 s of live ingestion, checks the dashboard
health endpoint, prints the ingester log tail, runs `tickvault qa --data /data`
inside the ingester and asserts positive trades and book deltas for both venues,
then tears the stack down. It exits non-zero on any failure and cleans up on
exit.

## Key decisions and reasoning

- **Per-symbol connection scope (A29).** Each `connect` control record carries
  the normalized symbols the connection serves. A new connection becomes
  authoritative only for those symbols, and only symbols that already had an
  authoritative connection emit `reconnect` and go back to `SYNCING`. With two
  Coinbase products on separate sockets, a venue-wide reconnect would otherwise
  starve the second product's book; the recordings without the `symbols` field
  fall back to the first connection that delivers a message for a symbol.
- **QA fails on a missing book (A30).** A venue/symbol/day with messages but no
  venue snapshot is an ingestion failure, not a zero: `book_missing` is a
  first-class field of every QA row and makes `tickvault qa` exit 1.
- **Cross-midnight grace (A31).** With `--date`, a `resync` up to 300 s into the
  next day still explains an anomaly late on the reported day, so a session that
  spans midnight is not reported as unexplained on either date.
- **Streaming raw reader (A32).** `rawio.iter_raw` merges per-file iterators
  with a heap so replay memory is bounded by open files, not tree size; the
  output order is identical to the previous in-memory reader (A27 retired).
- **Output locations (A33).** Reports go to `<data>/reports/qa-<date>.json|md`,
  manifests to `<data>/manifests/date=<date>/manifest.json`, and the dashboard
  sample is a committed data root under `dashboard/sample/`. Generated files
  live inside one data root, so Compose, Dagster and the dashboard all share
  one mount.
- **One pipeline module (A36).** `run_replay` and `run_live` are the only
  orchestration of connector, normalizer, processor and writers; the CLI and the
  Dagster assets call them (or the Epic 1 functions) directly, which is what
  keeps replay and live behavior identical.
- **Docker is local-only (A34),** like the live network checks: CI stays
  lint/types/tests, and the Compose build plus smoke run are verified on the
  arm64 builder and at the epic audit.
- **Dagster partitions carry an end offset.** The acceptance command
  materializes `2026-11-01`, a date in the future at build time; without
  `end_offset=90` Dagster only knows days that have already started and rejects
  the partition. The schedule still targets the previous day, and the offset
  only widens which partitions can be requested.
- **Image dependency groups are explicit.** `[tool.uv] default-groups` includes
  dev, orchestration and dashboard, so `--no-dev` alone would still install
  Dagster and Streamlit into every image. Each target syncs
  `--no-default-groups` plus exactly the group it needs.
- **Non-root images with a prepared data volume.** The runtime stage creates
  uid/gid 10001 `tickvault`, makes it own `/data`, and drops root, so the named
  volume initializes with the right ownership and raw/Parquet files are never
  written as root. The only apt package added is curl, for the dashboard
  healthcheck.
- **The smoke script tolerates the 60 s flush window.** The live pipeline's
  first normalized partitions flush on the 60 s logical clock, within a second
  of the mandated 60 s wait, so the QA step is retried (same command, bounded to
  120 s) until the first clean report. Without the retry the check can lose a
  sub-second race and report zero rows on healthy runs.
- **Version pin for uv.** The Dockerfile pins the uv image to the version used
  on the build machine, so a rebuild months later produces the same resolver.

## Interfaces for future epics

- `tickvault.pipeline.run_replay(input, out) -> PipelineStats` and
  `run_live(venues, symbols, out, duration=None, max_gb=None, ...) -> PipelineStats`;
  `PipelineStats.to_dict()` is stable and JSON-serializable.
- The CLI command names, options and exit codes above are the Phase 1 contract;
  Epic 3's README and public demo build on them.
- `orchestration.definitions` exports `defs` (the Dagster `Definitions`), the
  three asset functions, `daily_job`, `daily_job_schedule` and
  `DataRootResource`; Epic 3 can add assets without touching the Epic 1 code.
- `TICKVAULT_DATA_DIR` is the environment contract shared by the dashboard, the
  Dagster resource and Compose; `--data`/`--out`/`--input` remain explicit
  where the CLI declares them (A26).
- `dashboard/sample` is a plain data root: Epic 3 replaces its contents (A33)
  without changing `dashboard/app.py`.
- `scripts/build_dashboard_sample.py --input <raw tree> --out <data root>`
  rebuilds the sample from a longer capture.
- `scripts/compose_smoke.sh` is the orchestrator's end-to-end hook; it requires
  outbound network access to both venues.

## Measured benchmark

`tickvault bench` replays the two recorded 5-minute fixtures (28,651 messages)
into a temporary directory and reports throughput and peak RSS against the PRD
targets (at least 2,000 messages/s per venue, peak RSS below 400 MB). Measured
on the Apple M5 build machine (self-hosted runner host):

```
inputs: 2 file(s)
messages: 28,651 (coinbase 6,822, kraken 21,829)
wall time: 2.719 s
throughput: 10,537 msg/s overall (coinbase 2,509, kraken 8,028)
peak RSS: 227.7 MB
targets: >= 2,000 msg/s per venue and < 400 MB RSS: PASS
```

The benchmark replays the recorded fixtures by default; `--input` accepts any
raw file or raw tree for a measurement on other data.

## Deviations from the plan

- **Dagster partitions need `end_offset=90`.** Documented above; the acceptance
  criterion materializes a future date, which Dagster rejects without it.
- **The Compose smoke retries the QA step.** Documented above; the retry uses
  the same command and only makes the wait deterministic.
- **`tests/test_orchestration.py` builds its data root directly** with the
  normalizer, `BookProcessor` and `ParquetWriter` (the fallback the issue
  allowed), because `tickvault.pipeline` was not merged when the test was
  written. It works unchanged now that it is; switching to `run_replay` is a
  small follow-up, not a correctness issue.
- **`book_missing` metadata is computed in the asset** from the QA row fields
  (`messages > 0 and venue_snapshots == 0`) rather than read from `QAReport`,
  which exposes counts but not that derived flag.
- **The orchestration test inserts the repository root into `sys.path`.**
  `orchestration` is a top-level package next to `tickvault`, not an installed
  one, and the pytest configuration that could add it (`pythonpath = ["."]`)
  lives in `pyproject.toml`, which the orchestration issue did not own.
- **`uv`'s group selection is explicit in the Dockerfile** for the reason
  recorded under Key decisions.
- **The ingester logs nothing while it runs.** `tickvault ingest` prints its
  summary only at exit, and the pipeline configures no logger, so
  `docker compose logs ingester` is empty during a healthy run. The smoke issue
  reported this precisely; the fix (configure a logger in `cli.py ingest` and
  log progress periodically from the live loop) belongs to a future change.
- **QA `messages` is still normalized-layer only.** A24 deferred raw-layer
  message counts to Epic 2 "when a raw tree is present"; the QA path today
  counts distinct `(connection_id, ingest_seq)` pairs from the normalized
  tables, and the raw tree is not consulted. The column's meaning is unchanged
  from Epic 1.
- **The Tardis cache still trusts existing files** (Epic 1 deferral): a cached
  download is reused without a size or hash re-check.
- **No `make bench` target.** The bench issue flagged the Makefile as owned by
  the Docker issue, whose acceptance criteria did not include it, and the docs
  issue does not own the Makefile either. `make bench` would be one line.

## Known limitations and deferred items

Deferred by scope, not broken:

- The bundled dashboard sample is two recorded hours (about 2.2 MB); A33
  schedules its replacement with a longer real capture in Epic 3.
- A live-versus-Tardis reconciliation capture on 2026-11-01 remains optional
  (A17); the offline fixture path and the cached download path are complete.
- Kraken reconciliation against Tardis is deferred (A35); Phase 1 reconciles
  Coinbase only.
- Live network tests (connectors and the Compose smoke) are excluded from CI;
  the recorded fixtures are frozen captures.
- The Dagster UI (`dagster dev`, the optional Compose profile) is provided but
  not part of the daily job's DoD; the job itself runs with
  `dagster asset materialize` and no daemon.
- `tickvault ingest` is unobservable from `docker compose logs` while running
  (see Deviations); the data path is unaffected.
- The first normalized partitions of a live run appear about 60 s after the
  first message (logical-clock flush); the smoke script accounts for it.
- Live ingestion keeps no retention policy and has no `--max-gb` default; raw
  files rotate hourly, but the data root grows until the operator stops the run.

Nothing from the Epic 2 issue set is left unmerged; this document reflects
`main`.
