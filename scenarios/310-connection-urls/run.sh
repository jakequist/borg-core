#!/usr/bin/env bash
# **One string configures a client, and a client outlives its server.** SPEC.md §17.7,
# `examples/personal-crm/FRICTION.md` #11.
#
# 300 proved a server can host several registries and route between them. This is the other side of
# that socket: how a *client* is told which one it wants, and what happens to a long-lived one when
# the server goes away and comes back.
#
# The claims:
#
#   * **a connection url names a socket and a registry in one string**, and the name routes — two
#     urls differing only in their last segment reach two stores with two schemas;
#   * **a url that is not one quotes itself back**, and `borg+ws://` — the transport that does not
#     exist yet — is refused *by name* rather than left for somebody to invent a spelling for;
#   * **nothing listening says how to start one.** `no borg server at <addr> — start one with:
#     borg-server start`, identically from the CLI and from the SDK, because a message a user learns
#     to recognise has to be the same message everywhere. This is the sentence the first application
#     on this system wanted and did not have;
#   * **a schema is pushed into a running server by url**, which is the CLI half of retiring
#     "pushing a schema means stopping the server";
#   * **a session survives a bounce**, and a transaction opened before it commits after it — which
#     is the reconnect story §12.2 was designed for, actually working.
#
# *Failing means a client is configured by two variables that can disagree, or that a server restart
# is an application restart — which is what it was.*
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

DATA="$WORK/data"
SOCK="$WORK/borg.sock"
ABSENT="$WORK/nothing-here.sock"
server() { "$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" "$@"; }

# **Stop the server on the way out, whichever way we leave**, and before `setup`'s trap removes the
# scratch directory. This scenario restarts servers, so a failure part-way through can leave one
# running — and a server whose socket has been deleted out from under it is one nothing can then
# reach. Chained ahead of the existing trap rather than replacing it.
trap 'server stop >/dev/null 2>&1 || true; rm -rf "$WORK"' EXIT

# Two registries, so that "the name in the url routed" is a statement about *which* store answered
# and not about there being only one.
server create crm >/dev/null
server create analytics >/dev/null

# Two schemas, deliberately different. `analytics` gets one field and no producer; `crm` gets 030's
# repo, pushed below **through the running server**.
cat >"$WORK/analytics.json" <<'JSON'
{"repo": 1, "events": [{"DeclareField": {"struct_name": "Metric", "field": "value", "ty": "Int"}}]}
JSON
"$BORG_BIN" --store "$DATA/analytics/borg.db" def push "$WORK/analytics.json" >/dev/null

CRM_URL="borg+unix://$SOCK/crm"
ANALYTICS_URL="borg+unix://$SOCK/analytics"

# ── A url that is not one, before anything is even running ────────────────────────────────────────
#
# Parsed before it is dialled, deliberately: a malformed url is a mistake and an unreachable one is
# an outage, and telling a user "connection refused" about a typo would send them to restart a
# server that was never the problem.

assert_rejected 'is not a borg url' "a url that is not one is refused…" \
    -- "$BORG_BIN" --url 'localhost/crm' generate --lang ts -o "$WORK/gen"
assert_rejected '`localhost/crm`' "…and quotes itself back, because a url lives in a variable" \
    -- "$BORG_BIN" --url 'localhost/crm' generate --lang ts -o "$WORK/gen"
assert_rejected 'not a registry name' "a registry name the server could never accept is caught here" \
    -- "$BORG_BIN" --url 'borg://localhost/has.dot' generate --lang ts -o "$WORK/gen"

# *Failing means the next transport gets invented independently by everybody who needs one first.*
assert_rejected 'not yet supported' "borg+ws:// is reserved for the browser transport…" \
    -- "$BORG_BIN" --url 'borg+ws://borg.example/crm' generate --lang ts -o "$WORK/gen"
assert_rejected 'borg+ws://' "…and the refusal names it, rather than saying 'unknown scheme'" \
    -- "$BORG_BIN" --url 'borg+ws://borg.example/crm' generate --lang ts -o "$WORK/gen"

# A url names a *server*, so a command that operates on a store directly is told so rather than
# quietly given an answer about --store.
assert_rejected 'generate' "--url on a command that does not connect is refused, naming the two that do" \
    -- "$BORG_BIN" --url "$CRM_URL" get 'Company#1.headcount'

# ── Nothing listening ──────────────────────────────────────────────────────────────────────────────
#
# *Failing means the commonest failure a client has is reported as an errno.* This is the exact
# complaint `examples/personal-crm/FRICTION.md` records: a socket that answers nothing, and a
# message that sends you to read strace instead of to start a server.

assert_rejected "no borg server at $ABSENT — start one with: borg-server start" \
    "an address with nothing on it says exactly what is wrong and exactly what to do" \
    -- "$BORG_BIN" --url "borg+unix://$ABSENT/crm" generate --lang ts -o "$WORK/gen"

# ── One socket, two urls, two stores ───────────────────────────────────────────────────────────────

server start >/dev/null

# `repo push` over a url is the *server* running the push against a path on its own disk (§17.6) —
# the CLI half of "a schema can be pushed into a running server".
cp -r "$HERE"/../030-shell-pipeline/repo "$WORK/repo"
pushed="$("$BORG_BIN" --url "$CRM_URL" repo push "$WORK/repo")"
assert_contains "$pushed" "is_investible" \
    "a repo is pushed into crm by url, by the server, while it is serving"

# *Failing means the registry in a url is decoration and one socket is one store.*
"$BORG_BIN" --url "$CRM_URL" generate --lang ts -o "$WORK/gen-crm" >/dev/null 2>&1
"$BORG_BIN" --url "$ANALYTICS_URL" generate --lang ts -o "$WORK/gen-analytics" >/dev/null 2>&1
assert_contains "$(cat "$WORK/gen-crm/borg.generated.ts")" "export interface Company" \
    "the url ending in /crm generates crm's schema…"
assert_contains "$(cat "$WORK/gen-analytics/borg.generated.ts")" "export interface Metric" \
    "…and the one ending in /analytics generates a different store's, over the same socket"

# `$BORG_URL` is the ambient form, which is what makes a deployment configurable without a flag.
assert_contains "$(BORG_URL="$ANALYTICS_URL" "$BORG_BIN" generate --lang ts -o "$WORK/gen-env" 2>&1)" \
    "$SOCK" "\$BORG_URL configures a client that was given no flag"
assert_eq "$(cat "$WORK/gen-env/borg.generated.ts")" "$(cat "$WORK/gen-analytics/borg.generated.ts")" \
    "…to exactly the same place the flag would have"

# **The deferred routing error, observed.** A handshake that cannot be routed is not refused at the
# handshake — the server does not acknowledge an accepted hello, so there is nowhere to put the
# refusal — and the error is handed to the first request that needs a registry instead
# (`ROADMAP.md`, *The handshake names a registry*). For a client this looks like a connection that
# succeeded and an operation that failed.
assert_rejected 'nope' "a registry the server does not host is named back…" \
    -- "$BORG_BIN" --url "borg+unix://$SOCK/nope" generate --lang ts -o "$WORK/gen"
assert_rejected 'crm' "…beside the ones that exist, at the first request rather than at the handshake" \
    -- "$BORG_BIN" --url "borg+unix://$SOCK/nope" generate --lang ts -o "$WORK/gen"

# ── The SDK: one url, and a session that outlives the server ───────────────────────────────────────
#
# Everything above needed only cargo. What is left needs a JavaScript runtime, so it skips loudly
# rather than failing — and the claims above have already been made.

source "$HERE/../ts-lib.sh"
need_node "310's SDK half needs node and pnpm." \
          "The url grammar itself is covered by crates/borg-protocol/src/url.rs and the CLI" \
          "assertions above, which need only cargo."
build_sdk

PROGRAM="$WORK/program"
cp -r "$HERE/program" "$WORK/"
link_sdk "$PROGRAM"

# What the client runs to bounce the server from inside its own session. `stop` waits for the
# process rather than for the socket, which is what makes the restart below not a race.
cat >"$WORK/bounce.sh" <<BOUNCE
set -euo pipefail
"$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" stop >/dev/null
"$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" start >/dev/null
BOUNCE

tsc_check "$PROGRAM/tsconfig.json"
pass "a client configured by a url compiles"

out() { sed -n "s/^$2=//p" <<<"$1" | head -1; }
result="$(cd "$PROGRAM" && node client.ts "$CRM_URL" "$WORK/bounce.sh" "borg+unix://$ABSENT/crm")"

assert_eq "$(out "$result" address)" "$SOCK" "the SDK dials the socket the url named"
assert_eq "$(out "$result" structs)" "Company" \
    "…and the registry the url named, which is a different schema from the other one on this socket"

# *Failing means the reconnect story §12.2 was designed for does not work, and a server restart is
# an application restart — which is what FRICTION #11 recorded.*
assert_eq "$(out "$result" bounced)" "ok" "the server is stopped and started mid-session"

# **What a client that was busy right through the outage sees.** The program bounces the server with
# a blocking call, so node cannot deliver the socket's close and the next request goes out on a
# socket this process still believes is live. It fails, and the failure names itself — never a
# silent retry, because a `tx_commit` that reached the server and lost its answer on the way back is
# indistinguishable from one that never arrived, and re-sending it could apply twice.
assert_eq "$(out "$result" stale_socket)" "BorgDisconnectedError" \
    "an operation that met the dead socket fails with an error of its own…"
assert_contains "$(out "$result" stale_says)" "not retried" \
    "…which says it was not retried, rather than pretending nothing happened"
assert_eq "$(out "$result" dropped)" "false" "and the connection is torn down rather than reused"

assert_contains "$(out "$result" committed)" "L" \
    "a transaction opened before the bounce commits after it, over a connection that did not exist \
when it was opened"
assert_eq "$(out "$result" website)" "acme.ai" "and what it wrote is there"
assert_eq "$(out "$result" listed)" "true" "…and the object it created is in the listing"
assert_eq "$(out "$result" reconnected)" "true" \
    "the context reconnected on its own: no new context, no restart, nothing retried"

# The same sentence the CLI printed above, from the SDK, with its own error class so that an
# application can tell "there is no server" from "the server said no".
assert_eq "$(out "$result" unreachable_kind)" "BorgUnreachableError" \
    "an unreachable address is its own error class, not a generic protocol failure"
assert_eq "$(out "$result" unreachable)" \
    "no borg server at $ABSENT — start one with: borg-server start" \
    "…carrying word for word the sentence the CLI prints, because one message has to be learnable"

server stop >/dev/null
