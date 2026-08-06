#!/usr/bin/env bash
# Drive the whole CRM headless and check it still works. A tool, not a scenario.
#
# `README.md` says why nothing here is in `check.sh`: scenarios assert, this observes. What this
# script is for is the observation being repeatable — booting the stack by hand, clicking three
# views and reading a JSON body is how the last few regressions in this example were found, and
# doing it the same way twice is worth a file.
#
# Two things it checks:
#
#   1. **the app** — boot, create a contact, list contacts, read one in detail, including the
#      derived `displayName` and the §10.4 envelope the detail view renders;
#   2. **FRICTION #11** — start the api with *no server running*, assert it says so usefully rather
#      than dying, then start a server and assert the api recovers **without being restarted**.
#
# It hosts its own data directory on its own socket and its own port, so it never touches a `dev.sh`
# you have running.
#
#     usage: ./smoke.sh

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

DATA="$HERE/data-smoke"
SOCK="$DATA/borg.sock"
REGISTRY="personal-crm"
URL="borg+unix://$SOCK/$REGISTRY"
API_PORT="${API_PORT:-8793}"
API="http://localhost:$API_PORT/api"

BORG="${BORG_BIN:-$ROOT/target/debug/borg}"
BORG_SERVER="${BORG_SERVER_BIN:-$ROOT/target/debug/borg-server}"
LOG="$(mktemp -t crm-smoke-XXXXXX.log)"

say() { printf '\033[1m▸ %s\033[0m\n' "$*"; }
pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
die() { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; tail -40 "$LOG" >&2 2>/dev/null || true; exit 1; }

server() { "$BORG_SERVER" --data-dir "$DATA" --socket "$SOCK" "$@"; }

DEV=""
API_PID=""
cleanup() {
    [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true
    [ -n "$DEV" ] && kill "$DEV" 2>/dev/null && wait "$DEV" 2>/dev/null || true
    CRM_DATA="$DATA" BORG_SERVER_BIN="$BORG_SERVER" "$HERE/dev.sh" --stop >/dev/null 2>&1 || true
    rm -rf "$DATA"
}
trap cleanup EXIT INT TERM

# One dotted path out of a JSON body on stdin. `node` rather than `jq`, because everything else
# here already needs node and nothing else here needs jq.
jval() { node -e '
let raw = ""; process.stdin.on("data", (c) => (raw += c)).on("end", () => {
  let value = JSON.parse(raw);
  for (const key of process.argv[1].split(".")) value = value?.[key];
  process.stdout.write(String(value));
});' "$1"; }

# ── 1. The app ─────────────────────────────────────────────────────────────────────────────────────

say "booting the crm on port $API_PORT (data $DATA) — log: $LOG"
rm -rf "$DATA"
CRM_DATA="$DATA" API_PORT="$API_PORT" BORG_BIN="$BORG" BORG_SERVER_BIN="$BORG_SERVER" \
    "$HERE/dev.sh" --reset --headless >"$LOG" 2>&1 &
DEV=$!

for _ in $(seq 300); do
    curl -sf "$API/health" >/dev/null 2>&1 && up=1 && break
    sleep 0.5
done
[ "${up:-0}" = 1 ] || die "the api never came up"

health="$(curl -sf "$API/health")"
printf '%s' "$health" | jval clientVersion | grep -q '^L[0-9]' || die "health has no ClientVersion"
pass "health answers with the branch, its head and the def-version the client was generated at"

created="$(curl -sf -X POST "$API/contacts" -H 'content-type: application/json' \
    -d '{"firstName":"Ada","lastName":"Lovelace","email":"ada@example.com"}')"
id="$(printf '%s' "$created" | jval id)"
[ -n "$id" ] || die "POST /contacts answered no id: $created"
pass "a contact is created in one transaction, under an id nothing here chose ($id)"

listed="$(curl -sf "$API/contacts")"
printf '%s' "$listed" | grep -q "$id" || die "the new contact is not in the listing: $listed"
pass "…and it is in the listing"

# The derived field is the whole reason this is a Borg app rather than a CRUD app: nothing in the
# api computes it, and reading it answers an envelope rather than a value.
detail="$(curl -sf "$API/contacts/$id")"
printf '%s' "$detail" | jval fields.displayName.value | grep -q "Ada Lovelace" \
    || die "displayName was not derived: $detail"
pass "the detail view shows displayName, computed by the pipeline and never by this app"
[ "$(printf '%s' "$detail" | jval fields.displayName.origin)" = "derived" ] \
    || die "displayName does not say it was derived"
[ "$(printf '%s' "$detail" | jval fields.firstName.origin)" = "source" ] \
    || die "firstName does not say it is source"
pass "…with the §10.4 envelope saying which of them is ground truth"

# **An id that is well-formed and names nothing.** Getting one is itself the §3.1 demonstration: a
# PID is `(branch, allocator, counter)`, and the `Contact#N` shorthand names counter N under the
# *hand-authored* allocator — which is not the one anything allocates from, so an id spelled this way
# can never be an id the server issued, whichever was created first. `borg get` canonicalises it,
# which is how a shorthand becomes the PID text this api's route takes. Reading it must be a 404 and
# not a 200 with six nulls, because every field of a non-existent object is a perfectly ordinary
# absent value (FRICTION #4).
scratch="$(mktemp -d)"
"$BORG" --store "$scratch/borg.db" init >/dev/null
unused="$("$BORG" --store "$scratch/borg.db" get 'Contact#999999' | head -1 | sed 's/^Contact://')"
rm -rf "$scratch"
[ -n "$unused" ] || die "could not mint a well-formed id that names nothing"

status="$(curl -s -o /dev/null -w '%{http_code}' "$API/contacts/$unused")"
[ "$status" = 404 ] || die "an id nobody created should be a 404, got $status for $unused"
pass "and an object nobody created is a 404 rather than six nulls"

kill "$DEV" 2>/dev/null || true
wait "$DEV" 2>/dev/null || true
DEV=""

# ── 2. FRICTION #11: the api starts before the server, and recovers when one appears ───────────────

say "stopping the server, and starting the api against nothing"
server stop >/dev/null

BORG_URL="$URL" PORT="$API_PORT" node "$HERE/api/server.ts" >>"$LOG" 2>&1 &
API_PID=$!

# It must *come up* — the whole complaint was that this was fatal — and answer usefully.
for _ in $(seq 100); do
    body="$(curl -s "$API/health" || true)"
    [ -n "$body" ] && break
    sleep 0.1
done
kill -0 "$API_PID" 2>/dev/null || die "the api died because no server was running — that is #11"
pass "the api starts with no server running, and stays up"

status="$(curl -s -o /dev/null -w '%{http_code}' "$API/health")"
[ "$status" = 503 ] || die "expected a 503 with no server, got $status"
message="$(curl -s "$API/health" | jval message)"
case "$message" in
    "no borg server at $SOCK — start one with: borg-server start") ;;
    *) die "the api's error does not name the address and the fix: $message" ;;
esac
pass "…and says: $message"

say "starting a server underneath it — no api restart"
server start >/dev/null

# **The same process.** Not a new context, not a reconnect the app wrote: the next request works.
for _ in $(seq 100); do
    status="$(curl -s -o /dev/null -w '%{http_code}' "$API/health")"
    [ "$status" = 200 ] && break
    sleep 0.1
done
[ "$status" = 200 ] || die "the api did not recover once a server appeared (last status $status)"
pass "the api recovers without a restart — the SDK reconnected on the next request"

# And it can still do everything, over the connection it dialled itself.
again="$(curl -sf "$API/contacts")"
printf '%s' "$again" | grep -q "$id" || die "the recovered api cannot read: $again"
pass "…and the contact created before the bounce is still there"

printf '\n\033[1;32mcrm smoke passed\033[0m\n'
