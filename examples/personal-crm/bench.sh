#!/usr/bin/env bash
# Reproduce FRICTION.md #9's measurement: what does one read cost, as the log gets longer?
#
# The claim under test is not "is the app fast" — it is **what the cost of a read is a function of**.
# #9 measured a per-read cost that tracked the branch head rather than the size of the request:
# 18.4 ms at L441 and 53.0 ms at L1391, identical on a 281-read list and a 7-read detail. That is the
# signature of an `O(log)` store open happening once per read, and it is what makes the list view
# `O(n²)`.
#
# So this script grows a store one contact at a time and, at checkpoints, times **both** routes and
# records the branch head. Read the *per-read* columns: a flat curve says the cost of a read is
# a fact about the read, and a rising one says it is a fact about the log.
#
#     usage: ./bench.sh [--checkpoints "45 60 80 140 300"]
#
#       BORG_BIN=…   which binary to measure. Point it at another build to compare two.
#       API_PORT=…   defaults to 8791, so this does not collide with a dev.sh you have running.
#
# It boots the whole stack through `dev.sh --headless` — a real store, a real `borg serve`, the real
# api over the real socket — because a measurement taken through a stand-in measures the stand-in.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

API_PORT="${API_PORT:-8791}"
BORG_BIN="${BORG_BIN:-$ROOT/target/debug/borg}"
CHECKPOINTS="45 60 80 100 140"
while [ $# -gt 0 ]; do
    case "$1" in
        --checkpoints) CHECKPOINTS="$2"; shift 2 ;;
        *) echo "usage: ./bench.sh [--checkpoints \"45 60 80\"]" >&2; exit 2 ;;
    esac
done

LOG="$(mktemp -t borg-bench-XXXXXX.log)"
API="http://localhost:$API_PORT/api"

say() { printf '\033[1m▸ %s\033[0m\n' "$*" >&2; }
die() { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; [ -f "$LOG" ] && tail -30 "$LOG" >&2; exit 1; }

# `--reset` throws the store away, so every run measures a log this run built. Backgrounded, and its
# own trap tears down the server, the api and the workers when we kill it.
say "booting the crm on $BORG_BIN (port $API_PORT) — log: $LOG"
BORG_BIN="$BORG_BIN" API_PORT="$API_PORT" "$HERE/dev.sh" --reset --headless >"$LOG" 2>&1 &
DEV=$!
cleanup() { kill "$DEV" 2>/dev/null || true; wait "$DEV" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

for _ in $(seq 300); do
    curl -sf "$API/health" >/dev/null 2>&1 && up=1 && break
    sleep 0.5
done
[ "${up:-0}" = 1 ] || die "the api never came up"

head_now() { curl -sf "$API/health" | sed -n 's/.*"head": *"\([^"]*\)".*/\1/p'; }

# One `GET`, in milliseconds. `time_total` is the whole request, which is what the browser waits for.
time_ms() { curl -sf -o /dev/null -w '%{time_total}' "$1" | awk '{printf "%.0f", $1 * 1000}'; }

add_contact() {
    curl -sf -X POST "$API/contacts" -H 'content-type: application/json' \
        -d "{\"firstName\":\"Contact\",\"lastName\":\"$1\",\"email\":\"c$1@example.com\"}" \
        | sed -n 's/.*"id": *"\([^"]*\)".*/\1/p'
}

printf '| contacts | branch head | POST /contacts | GET /contacts | GET /contacts/:id | ms per read (list) | ms per read (detail) |\n'
printf '|---:|---:|---:|---:|---:|---:|---:|\n'

count=0
last_id=""
for target in $CHECKPOINTS; do
    # The whole batch, so the per-write figure is an average over this stretch of the log rather than
    # one sample that may or may not have caught a derivation round.
    written=0
    started="$(date +%s%N)"
    while [ "$count" -lt "$target" ]; do
        count=$((count + 1))
        written=$((written + 1))
        # `|| true` so that a failed request reaches `die` with the server log, rather than
        # tripping `pipefail` and killing the script with nothing to look at.
        id="$(add_contact "$count" || true)"
        [ -n "$id" ] || die "POST /contacts answered no id at $count"
        last_id="$id"
    done
    post_ms="$(awk "BEGIN{printf \"%.0f\", ($(date +%s%N) - $started) / 1000000 / $written}")"

    # A warm-up read that is not measured, so the first timing is not paying for a cold connection.
    curl -sf -o /dev/null "$API/contacts"
    list_ms="$(time_ms "$API/contacts")"
    detail_ms="$(time_ms "$API/contacts/$last_id")"
    head="$(head_now)"

    # `list` is `1 + 2n` reads (the enumeration, then displayName and email per contact); `detail` is
    # one existence check plus the six fields. Both counts are the app's, not the store's — see #9.
    list_reads=$((1 + 2 * count))
    detail_reads=7
    printf '| %d | %s | %s ms | %s ms | %s ms | %s | %s |\n' \
        "$count" "$head" "$post_ms" "$list_ms" "$detail_ms" \
        "$(awk "BEGIN{printf \"%.1f\", $list_ms / $list_reads}")" \
        "$(awk "BEGIN{printf \"%.1f\", $detail_ms / $detail_reads}")"
done
