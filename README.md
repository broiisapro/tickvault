# TickVault
Live crypto market-data pipeline: raw websocket capture, normalized Parquet, sequence and book validation.
Under construction.
Test with `uv sync --frozen && uv run pytest -q`.
Architecture: `docs/architecture.md`; Epic 0 notes: `docs/epic-00-foundation.md`.

## Quick start

```bash
uv sync --frozen                      # Python 3.12 environment managed by uv
uv run pytest -q                      # full suite; live network checks excluded

uv run tickvault bench                # replay the recorded fixtures, report throughput
uv run tickvault replay --input tests/fixtures/synthetic_gap_session.jsonl.zst --out ./data
uv run tickvault qa --data ./data     # quality table; exits 1 on unexplained anomalies
uv run tickvault sql --data ./data -c "select venue, count(*) from trades group by 1"
uv run tickvault ingest --venues coinbase,kraken --symbols BTC-USD,ETH-USD --out ./data

uv run streamlit run dashboard/app.py # dashboard; TICKVAULT_DATA_DIR picks the data root
docker compose up -d --build          # ingester + dashboard on http://localhost:8501
docker compose down                   # or: make demo-live / make demo-down
```

Phase 1 reads public market data only and needs no API keys. The pipeline,
CLI, Dagster job, dashboard and Docker stack are documented in
`docs/epic-02-pipeline.md`.
