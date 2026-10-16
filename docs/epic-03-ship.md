# Epic 3 — Phase 1 ship (v1.0.0)

## Goal

Make TickVault a public, self-explanatory Phase 1 release: a README anyone can
follow, an MIT license, a QA-clean 30-minute real-capture sample bundled in the
repository, the dashboard hosted on Streamlit Community Cloud, the daily QA
report published as a static site on GitHub Pages, CI running on GitHub-hosted
arm64 runners, and no secrets anywhere in the history.

## How it works

### Bundled sample

`dashboard/sample` is a plain data root holding only `norm/` and `manifests/`;
the raw capture is not published in Phase 1 (that is Phase 3, Epic 8). It is
built from a 30-minute capture recorded on 2026-10-15 between 17:08 and 17:38
UTC (BTC-USD and ETH-USD on Coinbase and Kraken) by

```
scripts/build_dashboard_sample.py --raw <capture tree> --out dashboard/sample
```

which replays the raw tree through `tickvault.pipeline.run_replay`, compacts
every date present, writes the per-date manifest and fails when the finished
root exceeds `--max-mb` (default 60, A44) or any single file exceeds
`--max-file-mb` (default 45, under GitHub's 50 MiB warning line). The rebuild
is deterministic: `norm/` and `manifests/` are removed and rebuilt from the
inputs on every run, and a root holding a `raw/` tree is refused.

The committed root is 24.3 MiB in 9 files (8 Parquet + one manifest,
2,167,901 rows) and QA-clean: `uv run tickvault qa --data dashboard/sample`
exits 0 with 471,575 normalized messages, 26,294 trades, 1,840,670 book
deltas, zero gaps, checksum failures and invariant violations, `unexplained 0`
and no missing book. Its three reconnect/disconnect cycles are each explained
by a `resync`.

### Hosted dashboard

`dashboard/app.py` runs on Streamlit Community Cloud at
https://tickvault.streamlit.app (public, Python 3.12). Cloud installs
`dashboard/requirements.txt`, which sits next to the entrypoint and therefore
takes precedence over the root `uv.lock` in its dependency lookup; the file is
the export of the `dashboard` dependency group (`make requirements`) and
`tests/test_requirements_export.py` fails when the lockfile moves ahead of it.

The sample root is the app's default data directory, so the hosted page needs
no secrets and no network. Every time-axis chart is an Altair spec from
`dashboard/charts.py` with an explicit `alt.Scale(type="utc")` and a 24-hour
`%H:%M` tick format, so the ticks read in UTC for every viewer; tooltips are
preformatted in UTC as well.

### QA site

`scripts/build_qa_site.py` computes the same per-day report as
`tickvault qa --date` over `dashboard/sample` and writes a deterministic
static site: `index.html`, `qa-<date>.html`, `qa-<date>.md`, `qa-<date>.json`,
`latest.json` and `.nojekyll`, using only the standard library and the
`tickvault` package (no Jekyll). `.github/workflows/pages.yml` builds it on
pushes that touch the sample, the builder or `tickvault/qa/**`, and deploys it
with `actions/deploy-pages` to https://broiisapro.github.io/tickvault/. Both
jobs are guarded by the repository variable `TICKVAULT_PAGES == 'on'`, which
was set when Pages was enabled at the ship gate, so the workflow was inert
while the repository was private.

### CI

`.github/workflows/ci.yml` runs the three required jobs — `lint`, `types`,
`tests` — on GitHub-hosted `ubuntu-24.04-arm` runners, each with
`uv sync --frozen` and no network access to the venues (`-m "not network"` is
the suite default). The jobs use the image's system CPython 3.12 (3.12.3)
deliberately (A50). The self-hosted runners used while the repository was
private were deregistered before the visibility change, and Docker stays out
of CI (A34): the Compose stack and its health check are verified locally.

### Engineering carryover

- **Streaming raw reader.** `rawio.iter_raw` merges per-file iterators with
  `heapq.merge`; an ordered file streams straight through and an unordered
  file takes a bounded-memory external sort. The merge iterator and every
  per-file stream are closed in a `finally`, and the external-sort temporary
  directory is removed deterministically when a consumer stops early instead
  of waiting for the garbage collector (the 3.12.3 leak fix).
- **Ingest observability.** `tickvault ingest` configures stderr logging
  (`--log-level`, default INFO) and the live loop logs a start line, an
  `ingest progress` line every `--progress-interval` seconds (default 10, 0
  disables) and a final summary, so `docker compose logs ingester` shows a
  healthy run.
- **Disk guard from byte counters.** `--max-gb` is enforced from
  `RawWriter.bytes_written` plus `ParquetWriter.bytes_written` plus the data
  root's size measured once at start-up; the live loop never walks the tree.
- **Clean connector cancellation.** The shared websocket helper cancels and
  awaits every pump task when a connection closes or the wait itself is
  cancelled, so a closing connector leaves no pending task behind; a test pins
  this with a recording exception handler.
- **Stop priority with a bounded queue.** A set stop event (`--duration`,
  SIGTERM/SIGINT, `--max-gb`) is observed before another item is dequeued:
  items are taken with `get_nowait` on the hot path and only an empty queue
  falls back to waiting on the item and the stop together. The merge queue is
  bounded at `_LIVE_QUEUE_MAXSIZE` (10,000) so a slow consumer backpressures
  the connectors, and a graceful stop closes the connectors and then drains
  every frame that already reached the queue through the raw writer and the
  processor.

## Key decisions and reasoning

- **60 MiB sample cap (tightened from 100).** Clone and hosted-install speed
  dominate at this size; the committed root (24.3 MiB) has room to grow by a
  real capture with a longer window without approaching the limit, and the
  builder fails the run instead of committing an oversized root.
- **CI stays on the distribution's Python 3.12.3.** Pinning a newer patch
  would have hidden the raw-reader cleanup bug the hosted image exposed;
  keeping the system Python means CI continues to exercise that path.
- **The Streamlit dependency file lives next to the entrypoint.**
  `dashboard/requirements.txt` exported from `uv.lock` (dashboard group +
  runtime deps) is what Community Cloud installs, which keeps the hosted
  environment small and exactly reproducible from the lockfile; the export is
  checked by a test.
- **Pages guarded by a repository variable.** The workflow could be merged
  while the repository was private without ever running, and the site only
  started deploying once the orchestrator enabled Pages and set
  `TICKVAULT_PAGES = 'on'`.
- **Replay benchmark and live rate are separate numbers.** The README reports
  `tickvault bench` throughput (processing headroom on recorded sessions)
  separately from the live message rate, which is set by the venues; every
  figure comes from a pasted command output.
- **MIT license.** Permissive and unambiguous for a public data-engineering
  showcase.

## Interfaces for future epics

- **`latest.json`** (`date`, `source`, `rows`, `unexplained`, `book_missing`,
  `exit_code`) is the machine-readable summary of the newest QA report; the
  Phase 3 always-on QA site keeps those fields, and per-day
  `qa-<date>.json|md|html` stay the detail pages.
- **Sample root layout**: `norm/` + `manifests/` only, built by
  `scripts/build_dashboard_sample.py`; the dashboard, the QA-site builder and
  the tests all treat it as an ordinary data root.
- **`make requirements` after any lockfile change**; the export test enforces
  that `dashboard/requirements.txt` matches `uv.lock`.
- **`RawWriter.bytes_written` / `ParquetWriter.bytes_written`** are monotonic
  counters (bytes flushed to disk, no directory walks); `--max-gb` already
  builds on them, and retention or quota work can too.
- **CI check names** `lint`, `types`, `tests` are the ruleset's required
  contexts; the runner image change did not rename them.

## Deviations from the plan

- **The CI move finished in a follow-up wave.** The last wave before the ship
  gate ended with the test suite failing only on the hosted image's CPython
  3.12.3; rather than pinning a newer patch, a short follow-up wave fixed the
  rawio cleanup bug and landed the runner move.
- **Sample cap tightened from 100 MiB to 60 MiB** (S9 to A44) after weighing
  clone and hosted-install speed.
- **The live Tardis reconciliation moved past the release.** v1.0.0 ships the
  fixture-tested reconciliation; a real capture reconciled against the free
  2026-11-01 Tardis file becomes a small post-release README update.
- **The hosted dashboard initially mislabelled time axes.** It formatted the
  timestamps in the viewer's local zone while the axis titles said UTC; the
  charts were rebuilt as UTC Altair specs and the README media re-recorded
  before the tag.

## Known limitations and deferred items

- Two venues (Coinbase, Kraken); Tardis reconciliation is fixture-tested
  against Coinbase only. Kraken reconciliation and a real capture are
  deferred (A35, A41).
- The hosted dashboard and the Pages QA site both serve the bundled fixed
  sample; always-on ingestion and a live QA history are Phase 3.
- The 30-minute capture recorded three venue-side disconnects; all three
  recovered through resync before the capture ended, so the sample QA reports
  `unexplained 0`. The Coinbase BTC-USD p99 receive lag of about 23 s comes
  from one reconnect catch-up (the resubscribe snapshot burst), not from
  steady-state delivery.
- Live-network checks remain outside CI; the recorded fixtures are frozen
  captures and Docker builds are verified locally, not in CI (A34).
- Ingestion has no retention policy and no `--max-gb` default; raw files
  rotate hourly, but a long-running live root grows until the operator stops
  the run.
