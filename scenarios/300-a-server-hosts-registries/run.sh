#!/usr/bin/env bash
# **One server, two registries.** SPEC.md §17.6.
#
# `borg-server` hosts a *data directory* of registries, and the registry — not the connection, not
# the branch, not the process — is the unit of tenancy. This is the local shape of a multi-tenant
# platform, so what has to be true here is what has to be true there:
#
#   * two clients address two registries **by name over one socket**, and neither had to know the
#     other existed;
#   * a write to one is invisible to the other, including a *schema* write — a repo pushed into one
#     registry leaves the other's definitions exactly where they were;
#   * a handshake that **names a registry nobody hosts is refused at the handshake**, naming the
#     ones that exist — the routing decision belongs to the connection (§17.5, §17.6);
#   * a handshake that names *nothing* against two registries is accepted and settles on none, so
#     that `registries` can still be asked; the ambiguity is reported by the first request that
#     needs a store, because at two there is no obvious default and any guess would be a coin toss
#     over somebody's data;
#   * a registry is opened by the first request that needs it and not at boot, which `status`
#     reports rather than hiding;
#   * `start`, `status`, `stop` behave, and every failure says how to start one.
#
# *Failing means the tenancy seam is decoration: one process would be serving one store with extra
# words, and the platform would need a different server rather than this one with a name in a
# handshake.*
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

DATA="$WORK/data"
SOCK="$WORK/borg.sock"
server() { "$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" "$@"; }
# The client from 250: a socket, `json`, and no SDK. `--registry` is the only thing that differs
# between the two clients below.
client() { python3 "$HERE"/../250-serve/client.py "$SOCK" "$@"; }

jval() {
    python3 -c '
import json, sys
value = json.loads(sys.argv[1])
for key in sys.argv[2].split("."):
    value = value[key]
print("" if value is None else value)
' "$1" "$2"
}

# --- Nothing is running yet, and every command says so usefully -----------------------------------

# *Failing means a developer who typed the wrong data dir is told "not running" and nothing else.*
assert_rejected "borg-server start" "status against nothing says how to start one" \
    -- server status
assert_rejected "borg-server start" "and so does stop, which is the same confusion from the other end" \
    -- server stop

# --- Two registries, made before anything serves --------------------------------------------------

# A data directory has to be fillable before there is a server to fill it, which is why `create`
# works both ways: directly now, and through the server once one is up (asserted below).
server create crm >/dev/null
server create analytics >/dev/null

# Two schemas, deliberately different, so that "the other registry is untouched" is a statement about
# definitions and not only about values.
for registry in crm analytics; do
    cat >"$WORK/$registry.json" <<JSON
{"repo": 1, "events": [{"DeclareField": {"struct_name": "Company", "field": "headcount", "ty": "Int"}}]}
JSON
    "$BORG_BIN" --store "$DATA/$registry/borg.db" def push "$WORK/$registry.json" >/dev/null
done

# --- Start, and say what is hosted ----------------------------------------------------------------

server start >/dev/null
status="$(server status)"
assert_contains "$status" "$SOCK" "status names the address a client should speak to"
assert_contains "$status" "crm" "and every registry the server hosts…"
assert_contains "$status" "analytics" "…both of them, on one socket"

# *Failing means every registry's log is replayed to answer a request about one of them, which is
# what makes a data directory of a hundred registries an expensive thing to start.*
assert_eq "$(printf '%s' "$status" | grep -c 'not opened')" "2" \
    "nothing is opened at boot: locking a store is a file write, opening one replays its log"

# --- Two clients, two registries, one socket ------------------------------------------------------

write_headcount() {
    client --registry "$1" \
        '{"tx_begin":{}}' \
        "{\"tx_set\":{\"tx\":\"%TX%\",\"cell\":\"Company#1.headcount\",\"value\":\"$2\"}}" \
        '{"tx_commit":{"tx":"%TX%"}}' >/dev/null
}
read_headcount() {
    jval "$(client --registry "$1" '{"get":{"cell":"Company#1.headcount"}}')" cell.value
}

write_headcount crm 10
write_headcount analytics 99

# *Failing means the handshake's registry is decoration and one socket is one store.*
assert_eq "$(read_headcount crm)" "10" "a client naming crm reads what it wrote to crm"
assert_eq "$(read_headcount analytics)" "99" \
    "and the same cell in analytics is a different cell, in a different store"

# The registry that was asked about is open; so is the other now, because both were used. The claim
# that matters is the one above at boot — this is the pair to it.
assert_eq "$(printf '%s' "$(server status)" | grep -c 'not opened')" "0" \
    "both registries are open once both have been used"

# --- A handshake that names nothing, with two on offer ---------------------------------------------

# **A hello naming nothing has made no claim that could be wrong**, so it is accepted and settles on
# no registry — which is exactly the connection an administrative client makes. The ambiguity is
# reported by the first request that needs a store.
#
# *Failing means the n=1 convenience survived contact with n=2, and a client that forgot to say which
# registry it meant is silently answered about whichever one sorted first.*
ambiguous="$(client '{"branch_list":{}}')"
message="$(jval "$ambiguous" error.message)"
assert_contains "$message" "crm" "a request on a connection that settled no registry is refused, naming…"
assert_contains "$message" "analytics" "…every registry it could have meant"

# And the one question that needs no registry still answers, which is how a client that guessed wrong
# finds out what to name instead of being hung up on.
assert_contains "$(client '{"registries":{}}')" '"crm"' \
    "asking what the server hosts needs no registry, so a misrouted client can still ask"

# **A name nobody hosts is refused at the handshake**, with the list rather than with "not found".
# The client exits non-zero having printed the refusal on stderr, which is what a handshake that was
# turned away *is* — there was never a session to answer a request on. Until protocol 2 this error
# arrived at the first request instead, because the server had nowhere to put it (`ROADMAP.md`,
# *The handshake names a registry*).
assert_rejected 'crmm' "a registry nobody hosts is refused at the handshake, named back…" \
    -- client --registry crmm '{"branch_list":{}}'
assert_rejected 'analytics' "…beside the ones that exist, before any request is sent" \
    -- client --registry crmm '{"branch_list":{}}' 

# --- A push to one registry leaves the other exactly where it was ----------------------------------

# *Failing means schema is server-wide rather than registry-wide, which is the whole of what tenancy
# means here.*
before="$(client --registry analytics '{"def_view":{}}')"
assert_contains "$before" '"headcount"' "analytics starts with the one field it declared"

cp -r "$HERE"/../030-shell-pipeline/repo "$WORK/repo"
pushed="$(client --registry crm "{\"repo_push\":{\"path\":\"$WORK/repo\"}}")"
assert_contains "$pushed" '"pushed"' "a repo is pushed into crm, by the server, while it is serving"

assert_contains "$(client --registry crm '{"def_view":{}}')" '"is_investible"' \
    "crm has the definitions the push landed…"
assert_eq "$(client --registry analytics '{"def_view":{}}')" "$before" \
    "…and analytics has not moved one byte"

# The derived half, to be sure this was a real push and not a def-layer with nothing behind it: the
# pipeline the push registered ran, in the server that took the push.
assert_eq "$(jval "$(client --registry crm '{"get":{"cell":"Company#1.is_investible"}}')" cell.origin)" \
    "derived" "and the pipeline it registered has run in crm"

# A push may name its registry on the message, which is what a deploy client pushing to several
# needs — so this connection is for `analytics` and the push is not.
retry="$(client --registry analytics "{\"repo_push\":{\"registry\":\"crm\",\"path\":\"$WORK/repo\"}}")"
assert_contains "$retry" "unchanged" \
    "a push may name another registry than the connection did, and the repeat still emits nothing"

# --- A registry created through the running server -------------------------------------------------

# *Failing means a directory appearing under a running server's data dir is a store it has not
# locked and will not route to — which is exactly why creating one is a server operation.*
server create reporting >/dev/null
assert_contains "$(server status)" "reporting" "a registry created while serving is hosted at once"
assert_contains "$(client --registry reporting '{"branch_list":{}}')" '"main"' \
    "…and is addressable by name immediately, with the root branch every store starts with"

# --- One process serves a store, all of them ---------------------------------------------------------

# *Failing means two processes are writing one store's sidecars and one store's sequencer.*
assert_rejected "$SOCK" "every hosted store refuses other borg invocations, naming the socket" \
    -- "$BORG_BIN" --store "$DATA/crm/borg.db" get 'Company#1.headcount'
assert_rejected "as \`analytics\`" "and the refusal says what the server calls that store" \
    -- "$BORG_BIN" --store "$DATA/analytics/borg.db" get 'Company#1.headcount'

# --- Stop, and give every store back -----------------------------------------------------------------

server stop >/dev/null
for registry in crm analytics reporting; do
    [ -e "$DATA/$registry/borg.serving.json" ] && \
        fail "the server left $registry's lock behind on a clean shutdown"
done
[ -e "$SOCK" ] && fail "the server left its socket behind on a clean shutdown"

# And the stores are ordinary stores again, with everything the socket clients wrote.
assert_eq "$("$BORG_BIN" --store "$DATA/crm/borg.db" get 'Company#1.headcount' --value)" "10" \
    "the CLI has crm back"
assert_eq "$("$BORG_BIN" --store "$DATA/analytics/borg.db" get 'Company#1.headcount' --value)" "99" \
    "…and analytics, still holding its own answer to the same address"

# --- The log a backgrounded server leaves behind ------------------------------------------------------

# *Failing means a server that would not start is a server with nowhere to say why.*
assert_contains "$(server logs)" "serving" "the log says what the server did, after it has gone"
assert_rejected "borg-server start" "and status is honest once nothing is answering" \
    -- server status
