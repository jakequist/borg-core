#!/usr/bin/env bash
# `borg-server`: the same client surface, over a socket instead of over argv. SPEC.md §17.5, §17.6.
#
# Two clients speak the protocol directly — `client.py` is a socket and `json`, no SDK — and the
# claims are the ones an SDK will rest on:
#
#   * a transaction opened over the socket behaves exactly as `borg tx` does, guards included, and a
#     rejected commit **names the cell that moved** rather than saying "conflict";
#   * a transaction lives in the store, not in the connection, so a client that disconnects has
#     abandoned a transaction rather than destroyed one — and the idle reaper collects it (§12.3);
#   * the §10.4 envelope that comes back over the socket is the envelope `borg get` prints, field
#     for field, because it is the same read rendered twice;
#   * one process serves a store, and everyone else is turned away by name;
#   * **a schema can be pushed into a server that is running**, which is the sentence this scenario
#     used to have to work around.
#
# The server is `borg-server` and hosts a *data directory of registries*; this one holds exactly
# one, which is the case where a client names no registry at all and gets it (§17.6).
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

DATA="$WORK/data"
SOCK="$WORK/borg.sock"
server() { "$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" "$@"; }
client() { python3 "$HERE/client.py" "$SOCK" "$@"; }

# One registry, made before anything is serving — which is the other half of `create` existing:
# a data directory has to be fillable before there is a server to fill it.
server create main >/dev/null
STORE="$DATA/main/borg.db"

# One field of one JSON response, without needing `jq` to agree with us about numbers.
jval() {
    python3 -c '
import json, sys
value = json.loads(sys.argv[1])
for key in sys.argv[2].split("."):
    value = value[key]
print("" if value is None else value)
' "$1" "$2"
}

# `borg-server start` backgrounds and **waits until the server answers** before it returns, which is
# the whole reason a scenario no longer needs its own retry loop: a socket file exists a moment
# before anything is listening on it, and every caller used to have to know that.
start_serve() {
    if ! server start >"$WORK/start.out" 2>&1; then
        echo "server never came up:" >&2
        cat "$WORK/start.out" >&2
        cat "$DATA/borg-server.log" >&2 2>/dev/null || true
        exit 1
    fi
}

# `borg-server stop` sends SIGTERM and waits for the socket to go quiet, so nothing after this line
# is racing a shutdown. If it did not, the CLI assertions after each stop would be asserting the
# stale-lock fallback instead of a clean release.
stop_serve() {
    server stop >/dev/null
    [ -e "$DATA/main/borg.serving.json" ] && fail "the server left its lock behind on a clean shutdown"
    return 0
}

# A repo with a real pipeline, so that what comes back over the socket includes *derived* data —
# origin, producer and freshness are the fields an envelope exists for, and a store of pure source
# data would let all three be stubbed without anyone noticing.
borg repo push "$HERE"/../030-shell-pipeline/repo >/dev/null
borg set 'Company#1.website' acme.ai >/dev/null
borg set 'Company#1.headcount' 40 >/dev/null

start_serve

# --- The store is served, so nothing else may touch it -----------------------------------------------

# *Failing means two processes are writing the same sidecars and the same sequencer.*
#
# Not a rule serve invented: sidecars and `InProcessSequencer` were never multi-process safe. What
# serve changes is that the second process is now likely rather than hypothetical, so the assumption
# is enforced instead of assumed — and the refusal names the socket, because the answer to "why was I
# refused" should be the address of the thing that refused you.
assert_rejected "$SOCK" "a served store turns other borg invocations away, naming the socket" \
    -- borg get 'Company#1.headcount'

# --- S2 over the socket: both read, both write, the second is rejected --------------------------------

# *Failing means guards do not reach the socket, and two SDK clients silently lose a write.*
#
# Two clients, two connections, interleaved — the same shape as `140-transaction-conflicts`, except
# that here each `client.py` call is its own connection. That is the load-bearing part: the handle
# survives the socket that made it, so "client A" is not a process or a connection but an id.
a="$(jval "$(client '{"tx_begin":{}}')" tx.tx)"
b="$(jval "$(client '{"tx_begin":{}}')" tx.tx)"
assert_contains "$a" "tx-" "a transaction opened over the socket answers with a handle"

# Read-modify-write, both of them. The read precedes the write, so it observed the parent and is
# guarded — which is what makes compare-and-swap fall out of reading a cell first (§12.1).
increment() {
    local tx="$1" seen
    seen="$(jval "$(client "{\"tx_get\":{\"tx\":\"$tx\",\"cell\":\"Company#1.headcount\"}}")" cell.value)"
    client "{\"tx_set\":{\"tx\":\"$tx\",\"cell\":\"Company#1.headcount\",\"value\":\"$((seen + 1))\"}}" >/dev/null
}
increment "$a"
increment "$b"

first="$(client "{\"tx_commit\":{\"tx\":\"$a\"}}")"
assert_contains "$first" '"committed"' "the first commit lands, and says which layer it landed in"

second="$(client "{\"tx_commit\":{\"tx\":\"$b\"}}")"
assert_eq "$(jval "$second" conflict.reason)" "guard" \
    "the second is refused as a guard conflict, not as a generic error"
assert_contains "$(jval "$second" conflict.cell)" "headcount" \
    "and the reply names the cell that moved — which is what a client needs to decide about retrying"

# The increment happened once, not twice with one silently lost. Read on a *fresh* connection, which
# is also the proof that a read outside a transaction sees the merged result.
assert_eq "$(jval "$(client '{"get":{"cell":"Company#1.headcount"}}')" cell.value)" "41" \
    "so the increment happened exactly once"

# The rejected transaction is still open: its read-set is what a client needs in order to decide
# whether to retry, and throwing it away at the rejection would leave them holding an error and
# nothing else.
assert_contains "$(client "{\"tx_abort\":{\"tx\":\"$b\"}}")" '"ok"' \
    "the rejected transaction is still there to abort"

# --- One connection, several messages, and the derived data a commit caused ---------------------------

# *Failing means the loop answers one request and stops, or a commit over the socket skips the
# auto-derivation a `borg tx commit` performs.*
#
# `%TX%` is the handle from the previous response: begin, write, commit, all down one socket. The
# pipeline reads `headcount`, so the commit owes a re-derivation — and the envelope that comes back
# afterwards must show it, produced by the pipeline and current.
client '{"tx_begin":{}}' \
       '{"tx_set":{"tx":"%TX%","cell":"Company#1.headcount","value":"40"}}' \
       '{"tx_commit":{"tx":"%TX%"}}' >/dev/null

investible="$(client '{"get":{"cell":"Company#1.is_investible"}}')"
assert_eq "$(jval "$investible" cell.value)" "true" \
    "a commit over the socket catches the branch up, exactly as borg tx commit does"
assert_eq "$(jval "$investible" cell.origin)" "derived" "and the envelope says the value is derived"
assert_contains "$(jval "$investible" cell.by)" "P" "naming the producer that wrote it"

# `explain` and `def_show` are here because codegen and debugging will want them, and a message that
# nobody exercises is a message that does not work.
assert_contains "$(client '{"explain":{"cell":"Company#1.is_investible"}}')" "headcount" \
    "explain reports the inputs a derived value came from"
assert_contains "$(client '{"def_show":{"struct":"Company"}}')" '"is_investible"' \
    "def_show answers structurally — this is what codegen reads, not a table for a human"

# --- Two connections open at the same time ---------------------------------------------------------

# *Failing means the server answers one client at a time, or two clients racing corrupt the
# transaction table.*
#
# Everything above interleaved two clients by taking turns. These two are genuinely simultaneous:
# two processes, two open sockets, two transactions on different cells. Neither reads, so neither has
# a guard and both must land — a rejection here would be a conflict the model does not have.
#
# The sharper claim is about the transaction table. It is a JSON file, and `tx_begin` reads it,
# appends and writes it back; two of those interleaved would lose a handle. What prevents it is that
# the server holds the store for a whole operation, which is the same discipline
# process-per-command gave the CLI for nothing.
write_through_its_own_connection() {
    client '{"tx_begin":{}}' \
           "{\"tx_set\":{\"tx\":\"%TX%\",\"cell\":\"$1.headcount\",\"value\":\"$2\"}}" \
           '{"tx_commit":{"tx":"%TX%"}}'
}
write_through_its_own_connection 'Company#2' 7 >"$WORK/two.out" 2>&1 &
two=$!
write_through_its_own_connection 'Company#3' 9 >"$WORK/three.out" 2>&1 &
three=$!
wait "$two" || fail "the first of two simultaneous clients failed: $(cat "$WORK/two.out")"
wait "$three" || fail "the second of two simultaneous clients failed: $(cat "$WORK/three.out")"

assert_contains "$(cat "$WORK/two.out")" '"committed"' "two clients hold connections at once…"
assert_eq "$(jval "$(client '{"get":{"cell":"Company#2.headcount"}}')" cell.value)" "7" \
    "…and both of their transactions landed"
assert_eq "$(jval "$(client '{"get":{"cell":"Company#3.headcount"}}')" cell.value)" "9" \
    "with neither handle lost to a read-modify-write race on the transaction table"

# --- A line a client got wrong -------------------------------------------------------------------------

# *Failing means a shell client that fat-fingers a line watches the socket go quiet.*
#
# Two bad lines and a good one, down one connection. Both codecs are self-delimiting — a newline
# here, a length prefix for MessagePack — so a message that could not be read consumed exactly one
# message and the stream is still aligned behind it. Answering is therefore possible, and a protocol
# meant to be hand-written has to do it.
recovered="$(client 'raw:{"nonsense":{}}' 'raw:not json at all' '{"branch_list":{}}')"
assert_eq "$(printf '%s' "$recovered" | grep -c '')" "3" \
    "a malformed message is answered, not fatal — the connection survives to answer the next one"
assert_contains "$(printf '%s' "$recovered" | head -1)" '"error"' \
    "and what comes back names what could not be read"
assert_contains "$(printf '%s' "$recovered" | tail -1)" '"branches"' \
    "and the good message behind the bad ones is answered normally"

# --- Pushing a schema into a server that is running ----------------------------------------------------

# *Failing means the dev loop is back to "stop the server, push, start the server" — and, worse,
# that a running server can be left executing code the log no longer describes.*
#
# `repo push` reads a **directory** off a filesystem, so a second process doing it while a store is
# served would be the second writer the advisory lock exists to refuse — and it still is:
assert_rejected "$SOCK" "a served store still refuses a repo push from another process" \
    -- borg repo push "$HERE"/../030-shell-pipeline/repo

# The answer is not to let the client write. It is for the **server** to run the push, against a
# path on its own disk (§17.6). Local-only semantics, said out loud: this path means nothing to a
# server on another machine, and the remote form is an uploaded artifact.
cp -r "$HERE"/../030-shell-pipeline/repo "$WORK/repo"
unchanged="$(client "{\"repo_push\":{\"path\":\"$WORK/repo\"}}")"
assert_contains "$unchanged" "unchanged" \
    "pushing a repo the branch already believes emits nothing — which is what makes this affordable"

# Now the code changes and nothing else does: no field moves, no producer is added, and the only
# thing that can notice is the implementation fingerprint (§9.2). `is_investible` was true at a
# threshold of 7 and this company scores 8.
sed -i 's/-ge 7/-ge 9/' "$WORK/repo/pipelines/is_investible.sh"
pushed="$(client "{\"repo_push\":{\"path\":\"$WORK/repo\"}}")"
assert_contains "$pushed" "implementation changed" \
    "an edited pipeline body is a change, and the server says so"

assert_eq "$(jval "$(client '{"get":{"cell":"Company#1.is_investible"}}')" cell.value)" "false" \
    "…and the running server recomputed with the new code, without being restarted"

# The other half, and the one a stale worker pool would break: the server's pool was built from the
# producer table as it stood at boot, and the pool is keyed on the command's *path* — which did not
# move. A server that kept its idle workers would have answered from the old program.
assert_contains "$(client '{"explain":{"cell":"Company#1.is_investible"}}')" "headcount" \
    "and the value still reports the inputs it was computed from"

# --- The envelope over the socket is the envelope borg prints ------------------------------------------

# Read here, as the last thing the server does, and deliberately not reused from further up: every
# write above advanced the branch, and `fresh as of` is a claim about how far the branch had got —
# so an envelope compared against a `borg get` taken later would differ for a reason that has nothing
# to do with either of them being wrong.
socket_envelope="$(client '{"get":{"cell":"Company#1.is_investible"}}')"

stop_serve

# *Failing means there are two read paths and one of them will drift.*
#
# The server is stopped, so the CLI can have the store back — which is itself the assertion that the
# lock is released rather than merely taken.
cli="$(borg get 'Company#1.is_investible')"
assert_eq "$(jval "$socket_envelope" cell.state)" "$(field "$cli" 'state')" \
    "the state the socket reported is the state borg get prints"
assert_eq "$(jval "$socket_envelope" cell.fresh_as_of)" "$(field "$cli" 'fresh as of')" \
    "and so is fresh-as-of, in the same L-form, with no reformatting in between"
assert_eq "$(jval "$socket_envelope" cell.landed_at)" "$(field "$cli" 'landed at')" \
    "and landed-at"
assert_eq "$(jval "$socket_envelope" cell.value)" "$(borg get 'Company#1.is_investible' --value)" \
    "and the value itself"

# --- A client that begins a transaction and walks away -------------------------------------------------

borg tx timeout 2s >/dev/null

start_serve

# *Failing means a browser tab that closed leaks a transaction forever, or — worse — that closing a
# socket silently aborts work a client meant to resume.*
#
# `client.py` exits after its one request, which closes the connection. Nothing is sent to say so;
# the server simply reads end-of-stream. The transaction is untouched from that moment, and silence
# is exactly what the reaper measures (§12.3) — idle, not elapsed.
abandoned="$(jval "$(client '{"tx_begin":{}}')" tx.tx)"
assert_contains "$abandoned" "tx-" "a client opens a transaction and hangs up"

sleep 3

# Any request opens the store, and opening the store is when the sweep happens — no daemon.
client '{"branch_list":{}}' >/dev/null

expired="$(client "{\"tx_get\":{\"tx\":\"$abandoned\",\"cell\":\"Company#1.headcount\"}}")"
assert_contains "$(jval "$expired" error.message)" "expired after" \
    "the abandoned transaction was reaped, and the client is told it expired"
assert_contains "$(jval "$expired" error.message)" "$abandoned" \
    "by name — never 'unknown transaction', which would send a client hunting its own bookkeeping"

stop_serve

# --- And the store is a normal store again --------------------------------------------------------------

assert_eq "$(borg tx list)" "no open transactions" \
    "nothing is left open once the server has gone"
assert_eq "$(borg get 'Company#1.headcount' --value)" "40" \
    "and the CLI has the store back, with everything the socket clients wrote"
