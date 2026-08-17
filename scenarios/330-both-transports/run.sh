#!/usr/bin/env bash
# **One server, two transports, one registry.** SPEC.md §17.5, §17.6, §17.7.
#
# 300 proved a server can host several registries on one socket; 310 proved a client is configured
# by one string and outlives its server. This is the transport that arrives with the network: a
# WebSocket, which is what a browser can open and what an ordinary load balancer already carries.
#
# The claims:
#
#   * **both listeners are up at once**, and the unix socket is not traded away for the network one
#     — every local `borg` invocation still speaks it, and the advisory lock's liveness test is it;
#   * **`GET /health` on the WebSocket's port**, answering the server version and how many
#     registries are hosted. One HTTP endpoint, for a load balancer and a supervisor; it names no
#     registry, because it is unauthenticated and a registry name is tenancy;
#   * **the CLI is a WebSocket client too** — `borg --url borg+ws://…` generates from a server it
#     reaches over the network, which is the same `ask` the unix path uses with the framing swapped;
#   * **S2 across transports**: two transactions on one registry, one arriving over a WebSocket and
#     one over the unix socket, held open at the same time. The second commit is rejected and names
#     the guard cell — decided by the engine, which cannot tell the two apart and must not be able
#     to;
#   * **a registry the server does not host is refused at construction**, naming it — the protocol-2
#     acknowledgement, which is what closes the deviation `ROADMAP.md` recorded;
#   * **a WebSocket bounce mid-session**, with a transaction opened before it committing after it.
#
# *Failing means the transport is a second protocol rather than a second framing — which would make
# every guarantee in §17.5 per-transport, and the browser client a different product.*
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

DATA="$WORK/data"
SOCK="$WORK/borg.sock"
server() { "$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" "$@"; }

# Stop the server on the way out, whichever way we leave, and before `setup`'s trap removes the
# scratch directory — this scenario restarts servers, and one whose socket has been deleted out
# from under it is one nothing can then reach.
trap 'server stop >/dev/null 2>&1 || true; rm -rf "$WORK"' EXIT

if ! command -v python3 >/dev/null 2>&1; then
    echo "  ⚠ SKIPPED: python3 is not installed" >&2
    echo "    The unix half of the cross-transport conflict is scenarios/250-serve's client.py," >&2
    echo "    which is eighty lines of python and no dependencies." >&2
    exit 0
fi

# A port nothing is listening on. Racy in principle and settled in practice; the alternative is
# `--listen ws://127.0.0.1:0` and grepping the log for the port the server chose, which is a second
# thing to get wrong for a scenario that also has to *restart* onto the same address.
PORT="$(python3 -c 'import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()')"
WS_URL="borg+ws://127.0.0.1:$PORT/crm"

server create crm >/dev/null
cat >"$WORK/schema.json" <<'JSON'
{"repo": 1, "events": [
  {"DeclareField": {"struct_name": "Company", "field": "headcount", "ty": "Int"}},
  {"DeclareField": {"struct_name": "Contact", "field": "name", "ty": "String"}}
]}
JSON
"$BORG_BIN" --store "$DATA/crm/borg.db" def push "$WORK/schema.json" >/dev/null

# **Both at once.** The unix socket is always there; the WebSocket is what `--listen` adds.
server start --listen "ws://127.0.0.1:$PORT" >/dev/null

# ── The one HTTP endpoint ─────────────────────────────────────────────────────────────────────────
#
# Asked with bash's own `/dev/tcp` rather than with curl, because the claim is about raw HTTP on the
# same port and a scenario should not acquire a dependency to check a health check.
#
# *Failing means a supervisor has to open a second port, or parse a log, to know the server is up.*
health() {
    exec 3<>"/dev/tcp/127.0.0.1/$PORT"
    printf 'GET %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n' "$1" >&3
    cat <&3
    exec 3<&-
}
probe="$(health /health)"
assert_contains "$probe" "200 OK" "GET /health on the websocket's port answers 200…"
assert_contains "$probe" '"status":"ok"' "…as json a load balancer can read"
assert_contains "$probe" '"registries":1' "…saying how many registries are hosted"
assert_contains "$probe" 'application/json' "…and saying so it is json"
# *Failing means an unauthenticated endpoint is leaking tenancy.*
case "$probe" in
    *crm*) fail "the health endpoint named a registry, and it is unauthenticated" ;;
    *) pass "and it names no registry, because a registry name is tenancy" ;;
esac
assert_contains "$(health /)" "404" "one HTTP endpoint, not two — the API is §17.5"

# ── The CLI over a websocket ──────────────────────────────────────────────────────────────────────
#
# `borg generate` is the CLI's protocol client (§17.7), and it is the same `ask` over both
# transports. *Failing means the network transport is the SDK's alone, and every deployment needs
# node to read a schema.*
"$BORG_BIN" --url "$WS_URL" generate --lang ts -o "$WORK/gen-ws" >/dev/null
assert_contains "$(cat "$WORK/gen-ws/borg.generated.ts")" "export interface Company" \
    "the CLI generates over borg+ws://, from the registry the url named"

# A registry the server does not host, over a websocket, from the CLI — refused at the handshake.
assert_rejected 'nope' "a registry nobody hosts is refused over a websocket too…" \
    -- "$BORG_BIN" --url "borg+ws://127.0.0.1:$PORT/nope" generate --lang ts -o "$WORK/gen"
assert_rejected 'crm' "…naming what is hosted, at the handshake" \
    -- "$BORG_BIN" --url "borg+ws://127.0.0.1:$PORT/nope" generate --lang ts -o "$WORK/gen"

# ── The SDK: two transports, one registry, one conflict ───────────────────────────────────────────
#
# What is left needs a JavaScript runtime, so it skips loudly rather than failing — and everything
# above has already been asserted.

source "$HERE/../ts-lib.sh"
need_node "330's SDK half needs node and pnpm." \
          "The transport itself is covered by crates/borg-server/src/serve.rs's tests and by the" \
          "CLI assertions above, which need only cargo."
build_sdk

PROGRAM="$WORK/program"
cp -r "$HERE/program" "$WORK/"
link_sdk "$PROGRAM"

cat >"$WORK/bounce.sh" <<BOUNCE
set -euo pipefail
"$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" stop >/dev/null
"$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" start --listen "ws://127.0.0.1:$PORT" >/dev/null
BOUNCE

tsc_check "$PROGRAM/tsconfig.json"
pass "a websocket client compiles against the same SDK entry point a unix one uses"

out() { sed -n "s/^$2=//p" <<<"$1" | head -1; }
result="$(cd "$PROGRAM" && node client.ts \
    "$WS_URL" \
    "borg+ws://127.0.0.1:$PORT/nope" \
    "$HERE/../250-serve/client.py" \
    "$SOCK" \
    "$WORK/bounce.sh")"

# **Refused where it was configured, not where it was first used.** This is the deviation closing:
# `createBorgContext` used to resolve happily against a registry that does not exist, because the
# server acknowledged nothing and had nowhere to put the refusal.
assert_eq "$(out "$result" missing_kind)" "BorgClientError" \
    "a registry the server does not host fails at construction…"
assert_contains "$(out "$result" missing_says)" "nope" "…naming the registry that was asked for…"
assert_contains "$(out "$result" missing_says)" "crm" "…and the ones that exist"

assert_eq "$(out "$result" structs)" "Company,Contact" \
    "a websocket client reads the schema of the registry its url named"

# *Failing means the engine can tell which transport a transaction arrived over — which would make
# isolation a property of the connection rather than of the store.*
assert_eq "$(out "$result" unix_saw)" "10" "the unix client reads the cell…"
assert_eq "$(out "$result" ws_saw)" "10" "…and the websocket client reads the same one"
assert_contains "$(out "$result" ws_committed)" "L" "the websocket commit lands"
assert_eq "$(out "$result" unix_outcome)" "conflict" \
    "and the unix commit, guarded on a cell a websocket moved, is refused"
assert_contains "$(out "$result" unix_conflict_cell)" "headcount" \
    "…naming the guard cell, which is what a client needs to decide whether to retry"
assert_eq "$(out "$result" unix_conflict_reason)" "guard" "…and why"
assert_eq "$(out "$result" headcount)" "11" \
    "the increment happened once, not twice with one silently lost"

# *Failing means the reconnect guarantee is per-transport, and a browser client has to own
# connection lifecycle the way FRICTION #11 said no client should have to.*
assert_eq "$(out "$result" bounced)" "ok" "the server is stopped and started mid-websocket-session"
# The same hard case 310 asserts over a unix socket, over this one: a client that was busy right
# through the outage discovers it by failing, with an error saying it was not retried. Identical
# words and identical class, because the guarantee is the connection's and not the transport's.
assert_eq "$(out "$result" stale_socket)" "BorgDisconnectedError" \
    "an operation that met the dead websocket fails with the same error class a socket gives…"
assert_contains "$(out "$result" stale_says)" "not retried" \
    "…saying it was not retried, in the same words"
assert_eq "$(out "$result" dropped)" "false" "and the connection is torn down rather than reused"
assert_contains "$(out "$result" resumed)" "L" \
    "a transaction opened before the bounce commits after it, over a websocket that did not exist \
when it was opened"
assert_eq "$(out "$result" listed)" "true" "and the object it created is in the listing"
assert_eq "$(out "$result" reconnected)" "true" \
    "the context reconnected on its own, over the transport it was configured with"

server stop >/dev/null
