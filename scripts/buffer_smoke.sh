#!/usr/bin/env bash
# Buffered pipeline smoke check for TickVault (issue #125).
#
# Starts the Phase 2 stack with `make demo-buffer` (ingester publishing to
# Redpanda, writer consuming from it, Prometheus and Grafana), waits for the
# first raw and normalized data, kills the writer, waits, starts it again and
# then checks that the restart lost nothing: for every connection in the raw
# layer the ingest sequence numbers must cover 0..max without a hole (read with
# tickvault.rawio.iter_raw inside the writer container, which shares the data
# volume). It also checks that `tickvault qa --data /data` exits 0, that the
# ingester's Prometheus endpoint reports received venue messages and that the
# writer's reports a non-zero tickvault_buffer_consumed_total. Every check
# prints PASS or FAIL and the script exits non-zero if any of them failed.
#
# The run starts from empty volumes and the exit trap tears the project down
# including its volumes, so consecutive runs are independent and no data from
# an earlier demo can mask a problem. No other container, image or volume on
# the machine is stopped or removed: the Compose project is `tickvault`, as
# named in compose.yaml.
#
# Optional knobs (seconds unless noted):
#   TICKVAULT_SMOKE_DATA_TIMEOUT     180  wait for the first partitions
#   TICKVAULT_SMOKE_KILL_WAIT        30   writer downtime
#   TICKVAULT_SMOKE_RESTART_TIMEOUT  180  wait for the writer to recover
#   TICKVAULT_SMOKE_METRICS_PORT     9109 writer metrics port
#   TICKVAULT_SMOKE_INGEST_PORT      9108 ingester metrics port

set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE=(docker compose --profile buffer --profile metrics)
DATA_TIMEOUT="${TICKVAULT_SMOKE_DATA_TIMEOUT:-180}"
KILL_WAIT="${TICKVAULT_SMOKE_KILL_WAIT:-30}"
RESTART_TIMEOUT="${TICKVAULT_SMOKE_RESTART_TIMEOUT:-180}"
METRICS_PORT="${TICKVAULT_SMOKE_METRICS_PORT:-9109}"
INGEST_METRICS_PORT="${TICKVAULT_SMOKE_INGEST_PORT:-9108}"

PASSES=0
FAILURES=0

pass() {
    printf 'PASS: %s\n' "$*"
    PASSES=$((PASSES + 1))
}

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    FAILURES=$((FAILURES + 1))
}

teardown() {
    local status=$?
    set +e
    echo "== docker compose --profile buffer --profile metrics down --volumes --remove-orphans =="
    "${COMPOSE[@]}" down --volumes --remove-orphans
    return "$status"
}
trap teardown EXIT

# Wait until the writer has written both a raw file and a normalized partition.
wait_for_partitions() {
    local label="$1"
    local timeout="$2"
    local deadline=$((SECONDS + timeout))
    echo "== waiting up to ${timeout}s for ${label} =="
    while :; do
        if "${COMPOSE[@]}" exec -T writer sh -c \
            'ls /data/raw/venue=*/*/*/*.jsonl.zst >/dev/null 2>&1 &&
             ls /data/norm/trades/venue=*/*/*/*.parquet >/dev/null 2>&1'; then
            echo "== ${label} present =="
            return 0
        fi
        if ((SECONDS >= deadline)); then
            echo "timed out waiting for ${label}" >&2
            return 1
        fi
        sleep 5
    done
}

# Run tickvault qa until it exits 0 or the timeout expires. The output and exit
# status of the last attempt are kept for the check below.
qa_output=""
qa_status=1
wait_for_qa() {
    local timeout="$1"
    local deadline=$((SECONDS + timeout))
    echo "== waiting up to ${timeout}s for tickvault qa to exit 0 =="
    while :; do
        qa_status=0
        qa_output="$("${COMPOSE[@]}" exec -T writer tickvault qa --data /data)" || qa_status=$?
        if ((qa_status == 0)); then
            return 0
        fi
        if ((SECONDS >= deadline)); then
            return 1
        fi
        sleep 5
    done
}

check_raw_contiguity() {
    local output=""
    local status=0
    output="$(
        "${COMPOSE[@]}" exec -T writer python - <<'PY'
from collections import defaultdict

from tickvault.rawio import iter_raw

sequences = defaultdict(list)
for message in iter_raw("/data/raw"):
    sequences[message.connection_id].append(message.ingest_seq)

if not sequences:
    raise SystemExit("no raw messages under /data/raw")

total = 0
problems = []
for connection_id, seqs in sorted(sequences.items()):
    total += len(seqs)
    lowest = min(seqs)
    highest = max(seqs)
    missing = sorted(set(range(lowest, highest + 1)) - set(seqs))
    if lowest != 0 or missing:
        problems.append((connection_id, lowest, highest, missing))

print(f"{total} raw messages over {len(sequences)} connections")
for connection_id, lowest, highest, missing in problems:
    sample = ", ".join(str(value) for value in missing[:10])
    print(
        f"  {connection_id}: ingest_seq {lowest}..{highest}, "
        f"{len(missing)} missing ({sample})"
    )
if problems:
    raise SystemExit(1)
print("every connection covers ingest_seq 0..max with no hole")
PY
    )" || status=$?
    printf '%s\n' "$output"
    if ((status == 0)); then
        pass "raw ingest_seq is contiguous from 0 for every connection"
    else
        fail "raw ingest_seq has a hole or starts above 0 (see above)"
    fi
}

check_qa() {
    if ((qa_status != 0)); then
        printf '%s\n' "$qa_output"
        fail "tickvault qa --data /data exited ${qa_status}"
    elif ((data_ok == 0)); then
        fail "tickvault qa --data /data exited 0, but no raw or normalized data was written"
    else
        pass "tickvault qa --data /data exited 0"
    fi
}

check_metrics() {
    local url="http://localhost:${METRICS_PORT}/metrics"
    local deadline=$((SECONDS + 30))
    local body=""
    local value=""
    # The writer was just restarted; give its HTTP endpoint a moment to bind.
    until body="$(curl -fsS "$url" 2>/dev/null)"; do
        if ((SECONDS >= deadline)); then
            fail "writer metrics endpoint ${url} is not reachable"
            return
        fi
        sleep 2
    done
    value="$(printf '%s\n' "$body" | awk '$1 == "tickvault_buffer_consumed_total" { print $2; exit }')"
    if [[ -z "$value" ]]; then
        fail "tickvault_buffer_consumed_total is missing from ${url}"
        return
    fi
    if awk -v value="$value" 'BEGIN { exit !(value + 0 > 0) }'; then
        pass "tickvault_buffer_consumed_total is ${value} (> 0)"
    else
        fail "tickvault_buffer_consumed_total is ${value}, expected > 0"
    fi
}

# The ingester is the other half of the buffered path: it must report received
# venue messages, otherwise the buffer is only carrying control frames.
check_ingest_metrics() {
    local url="http://localhost:${INGEST_METRICS_PORT}/metrics"
    local body=""
    local matched=""
    if ! body="$(curl -fsS "$url")"; then
        fail "ingester metrics endpoint ${url} is not reachable"
        return
    fi
    matched="$(printf '%s\n' "$body" | grep -E '^tickvault_messages_total\{.*\} [1-9]' || true)"
    if [[ -n "$matched" ]]; then
        printf '%s\n' "$matched"
        pass "tickvault_messages_total is non-zero for at least one venue"
    else
        fail "no tickvault_messages_total series above 0 on ${url}"
    fi
}

echo "== docker compose --profile buffer --profile metrics down --volumes --remove-orphans (clean start) =="
"${COMPOSE[@]}" down --volumes --remove-orphans

echo "== make demo-buffer =="
make demo-buffer

data_ok=0
if wait_for_partitions "first raw and normalized data" "$DATA_TIMEOUT"; then
    data_ok=1
    pass "the writer produced raw and normalized data"
else
    fail "the writer never produced raw and normalized data"
fi

echo "== docker compose kill writer =="
if "${COMPOSE[@]}" kill writer; then
    pass "writer killed"
else
    fail "could not kill the writer"
fi

echo "== waiting ${KILL_WAIT}s with the writer down =="
sleep "$KILL_WAIT"

echo "== docker compose up -d writer =="
if "${COMPOSE[@]}" up -d writer; then
    pass "writer restarted"
else
    fail "could not restart the writer"
fi

wait_for_qa "$RESTART_TIMEOUT" || true

check_raw_contiguity
check_qa
check_metrics
check_ingest_metrics

echo "== summary: ${PASSES} passed, ${FAILURES} failed =="
if ((FAILURES > 0)); then
    echo "== docker compose ps -a =="
    "${COMPOSE[@]}" ps -a || true
    echo "== ingester, writer and redpanda logs =="
    "${COMPOSE[@]}" logs --tail 30 ingester writer redpanda || true
    exit 1
fi
echo "buffer smoke ok"
