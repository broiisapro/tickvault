#!/usr/bin/env bash
# End-to-end Compose check for TickVault (issue #60).
#
# Builds and starts the ingester and dashboard, lets the ingester stream live
# Coinbase and Kraken data, checks the dashboard health endpoint, prints the
# ingester log tail, asserts that both venues produced trades and book deltas,
# and tears the stack down. Any failing step exits non-zero.
#
# Run it from anywhere; the repository root is resolved from this script.
#
# The live pipeline flushes normalized Parquet buffers on a 60 s logical
# clock, so the first partitions land right around the 60 s mark; the QA step
# is retried (same command) for up to TICKVAULT_SMOKE_QA_TIMEOUT seconds so a
# sub-second race between the mandated wait and the first flush cannot fail
# the check. Set TICKVAULT_SMOKE_WAIT_SECONDS to shorten the live-data window.

set -euo pipefail

cd "$(dirname "$0")/.."

WAIT_SECONDS="${TICKVAULT_SMOKE_WAIT_SECONDS:-60}"
QA_TIMEOUT_SECONDS="${TICKVAULT_SMOKE_QA_TIMEOUT:-120}"
HEALTH_URL="http://localhost:8501/_stcore/health"

cleanup() {
    docker compose down
}
trap cleanup EXIT

# Print "venue trades=<n> book_deltas=<n>" per venue and exit 0 only when both
# venues have a positive trade and book-delta count. Reads the fixed-width QA
# table on stdin; column names come from its header line.
parse_qa() {
    awk '
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
    '
}

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
deadline=$((SECONDS + QA_TIMEOUT_SECONDS))
while :; do
    qa_status=0
    qa_output="$(docker compose exec -T ingester tickvault qa --data /data)" || qa_status=$?
    if summary="$(printf '%s\n' "$qa_output" | parse_qa)" && ((qa_status == 0)); then
        break
    fi
    if ((SECONDS >= deadline)); then
        printf '%s\n' "$qa_output"
        printf '%s\n' "${summary:-no venue summary}"
        echo "no trades and book deltas for both venues within ${QA_TIMEOUT_SECONDS}s" >&2
        exit 1
    fi
    sleep 5
done
printf '%s\n' "$qa_output"
printf '%s\n' "$summary"

echo "== docker compose down =="
docker compose down
trap - EXIT

echo "compose smoke ok"
