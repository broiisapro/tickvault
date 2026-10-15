#!/usr/bin/env bash
# End-to-end Compose check for TickVault (issue #60).
#
# Builds and starts the ingester and dashboard, lets the ingester stream live
# Coinbase and Kraken data, checks the dashboard health endpoint, prints the
# ingester log tail, asserts that both venues produced trades and book deltas,
# and tears the stack down. Any failing step exits non-zero.
#
# Run it from anywhere; the repository root is resolved from this script.
# Set TICKVAULT_SMOKE_WAIT_SECONDS to shorten the live-data window.

set -euo pipefail

cd "$(dirname "$0")/.."

WAIT_SECONDS="${TICKVAULT_SMOKE_WAIT_SECONDS:-60}"
HEALTH_URL="http://localhost:8501/_stcore/health"

cleanup() {
    docker compose down
}
trap cleanup EXIT

echo "== docker compose up -d --build =="
docker compose up -d --build

echo "== waiting ${WAIT_SECONDS}s for live data =="
sleep "$WAIT_SECONDS"

echo "== dashboard health: curl -fsS ${HEALTH_URL} =="
health="$(curl -fsS "$HEALTH_URL")"
echo "$health"
if [[ "$health" != "ok" ]]; then
    echo "dashboard health returned '$health', expected 'ok'" >&2
    exit 1
fi

echo "== ingester logs =="
docker compose logs --tail 20 ingester

echo "== data quality: tickvault qa --data /data =="
if ! qa_output="$(docker compose exec -T ingester tickvault qa --data /data)"; then
    printf '%s\n' "$qa_output"
    echo "tickvault qa failed" >&2
    exit 1
fi
printf '%s\n' "$qa_output"

echo "== checking both venues produced trades and book deltas =="
if ! summary="$(printf '%s\n' "$qa_output" | awk '
    NR == 1 { for (i = 1; i <= NF; i++) { if ($i == "trades") t = i; if ($i == "book_deltas") d = i } }
    $1 == "coinbase" || $1 == "kraken" { seen[$1] = 1; trades[$1] += $t; deltas[$1] += $d }
    END {
        if (!t || !d) { print "could not find trades/book_deltas columns in the qa output"; exit 1 }
        ok = 1
        for (venue in seen) {
            printf "%s trades=%d book_deltas=%d\n", venue, trades[venue], deltas[venue]
            if (trades[venue] <= 0 || deltas[venue] <= 0) ok = 0
        }
        if (!("coinbase" in seen) || !("kraken" in seen)) { print "missing venue row"; exit 1 }
        exit ok ? 0 : 1
    }
')"; then
    printf '%s\n' "$summary"
    echo "expected trades > 0 and book_deltas > 0 for both venues" >&2
    exit 1
fi
printf '%s\n' "$summary"

echo "== docker compose down =="
docker compose down
trap - EXIT

echo "compose smoke ok"
