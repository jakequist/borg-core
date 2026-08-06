#!/usr/bin/env bash
# Objects: making one, and finding the ones that exist. SPEC.md §3.1, §9.6, §17.5.
#
# Every write before this one addressed an object the caller had already named — `Company#1`, a pid
# out of a reference. That is enough to build a demo and not enough to build an application: an
# application creates a contact without inventing an id for it, and later asks which contacts there
# are. §9.6 excluded the second on purpose ("enumeration is not exposed as a user-facing query in
# v1") and the exclusion is what this scenario reverses, together with the allocation the first
# needs.
#
# The claims:
#
#   * `borg tx create` allocates an id nobody chose, writes the object's existence cell in the
#     transaction, and prints the id — so it appears when the transaction commits and never if it
#     aborts;
#   * ids come from an **allocator of the server's own**, so a `Contact#5` typed by hand and a
#     contact the store created can never be the same object, whichever came first;
#   * the counter lives beside the store, so ids do not repeat across processes — and every `borg`
#     command here is its own process;
#   * `borg list` names every object of a struct and **skips the deleted ones**;
#   * both work identically over the socket, and two clients creating at once never conflict.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

DATA="$WORK/data"
SOCK="$WORK/borg.sock"

# The half of this scenario that goes over the socket needs a *server*, and a server hosts a data
# directory of registries (§17.6). One registry, so nothing below names it.
"$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" create main >/dev/null
STORE="$DATA/main/borg.db"
# The same eighty-line client `250-serve` uses: a socket and `json`, no SDK. Shared rather than
# copied, because a second copy is a second thing to keep in step with the protocol.
client() { python3 "$HERE/../250-serve/client.py" "$SOCK" "$@"; }

# One field of one JSON response. Arrays print one element per line, which is what `ids` is.
jval() {
    python3 -c '
import json, sys
value = json.loads(sys.argv[1])
for key in sys.argv[2].split("."):
    value = value[key]
if isinstance(value, list):
    print("\n".join(str(v) for v in value))
else:
    print("" if value is None else value)
' "$1" "$2"
}

count() { printf '%s' "$1" | grep -c '' ; }

borg def push "$HERE/contacts.json" >/dev/null

# --- Creating an object without inventing an id ---------------------------------------------------

# *Failing means an application has to make its own ids up, and two of them eventually make the same
# one.*
#
# Three contacts, three separate `borg` processes, one transaction. The id is printed and nothing
# else, so `id=$(borg tx create Contact)` is the whole of how a shell holds one.
tx="$(borg tx begin)"
ada="$(borg tx create Contact --tx "$tx")"
grace="$(borg tx create Contact --tx "$tx")"
barbara="$(borg tx create Contact --tx "$tx")"
borg tx set "Contact:$ada.name" Ada --tx "$tx" >/dev/null
borg tx set "Contact:$grace.name" Grace --tx "$tx" >/dev/null
borg tx set "Contact:$barbara.name" Barbara --tx "$tx" >/dev/null

assert_contains "$ada" "o-" "creating an object answers a pid, in the canonical text form"
[ "$ada" != "$grace" ] && [ "$grace" != "$barbara" ] && [ "$ada" != "$barbara" ] \
    || fail "three creations must be three objects"
pass "and three creations in three processes are three different ids"

# *Failing means a transaction is not what isolates a creation.* Nothing exists on main until the
# transaction merges — the same rule every other write follows (§12).
assert_eq "$(borg list Contact)" "" "a creation is invisible on the parent until it commits"
borg tx commit --tx "$tx" >/dev/null

assert_eq "$(count "$(borg list Contact)")" "3" "…and all three are there once it does"
assert_contains "$(borg list Contact)" "$ada" "by the ids the creations answered with"
assert_eq "$(borg get "Contact:$grace.name" --value)" "Grace" \
    "and the fields written against those ids are on the objects they named"

# *Failing means the counter is in memory, and the next process re-issues ids that already exist.*
#
# Every command above was a separate process, so the ids being distinct is already the claim. This
# says where the fact lives: beside the store, with the pause flags and the transaction table, moved
# on *before* the write it names so that a crash burns an id rather than handing one out twice.
assert_eq "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["next"])' \
    "${STORE%.db}.allocations.json")" "4" \
    "the next id to issue is written down beside the store, so a new process resumes rather than restarting"

# --- A hand-authored object, beside the allocated ones --------------------------------------------

# *Failing means an application creating objects can silently overwrite a fixture, or vice versa.*
#
# `Contact#5` is counter 5 under **allocator 0**, which is the one the shorthand names and which
# belongs to whoever is typing. The store allocates under one of its own, so the two id spaces are
# disjoint by construction — this is what `(branch, allocator, counter)` is for (§3.1), arriving one
# node before there is a second node.
borg set 'Contact#5.name' Hedy >/dev/null

listed="$(borg list Contact)"
assert_eq "$(count "$listed")" "4" "an object written the old way is listed beside the created ones"
assert_eq "$(borg get 'Contact#5.name' --value)" "Hedy" \
    "…and is still reachable by the shorthand that made it"
hand="$(printf '%s\n' "$listed" | grep -v -e "$ada" -e "$grace" -e "$barbara")"
[ "$(count "$hand")" = "1" ] || fail "exactly one of the four should be the hand-authored contact"
assert_fails "and no id the store issued can collide with one somebody typed" \
    -- sh -c "printf '%s\n' '$ada' '$grace' '$barbara' | grep -qx '$hand'"

# The buffers are per struct, so a Company and a Contact are separate populations even though one
# counter issues both ids.
tx="$(borg tx begin)"
acme="$(borg tx create Company --tx "$tx")"
borg tx commit --tx "$tx" >/dev/null
assert_eq "$(count "$(borg list Contact)")" "4" "creating a Company does not add to the Contacts"
assert_eq "$(borg list Company)" "$acme" "and the Company is listed under its own struct"

# --- A deleted object is not one of the objects ---------------------------------------------------

# *Failing means a deleted contact keeps showing up in the list, or the list reports it as an object
# with no fields.*
#
# Deleting an object tombstones its existence cell (§8.1). The scan behind `list` answers that
# tombstone like any other record — deciding it means absence is the enumeration's job, and this is
# the assertion that it does.
borg delete "Contact:$grace" >/dev/null

remaining="$(borg list Contact)"
assert_eq "$(count "$remaining")" "3" "a deleted object drops out of the listing"
assert_fails "by name — it is the one that was deleted and not some other" \
    -- sh -c "printf '%s\n' '$remaining' | grep -qx '$grace'"
assert_field "$(borg get "Contact:$grace")" "state" "tombstoned" \
    "and the object is gone rather than merely unlisted"
assert_contains "$(borg list Contact)" "$ada" "while everything else is untouched"

# A struct nobody declared is a typo, and an empty list is exactly what a typo would look like — so
# it is refused by name, as `borg def show` refuses one.
assert_rejected "Wombat" "listing a struct nobody declared is refused rather than answered with nothing" \
    -- borg list Wombat
assert_rejected "Wombat" "and so is creating one — a creation is a write, checked like any other" \
    -- sh -c "tx=\$($BORG_BIN --store '$STORE' tx begin); $BORG_BIN --store '$STORE' tx create Wombat --tx \"\$tx\""

# --- The same two things over the socket ----------------------------------------------------------

server() { "$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" "$@"; }
start_serve() {
    if ! server start >"$WORK/start.out" 2>&1; then
        echo "server never came up:" >&2
        cat "$WORK/start.out" >&2
        cat "$DATA/borg-server.log" >&2 2>/dev/null || true
        exit 1
    fi
}
stop_serve() { server stop >/dev/null; }
start_serve

# *Failing means the SDK has to reach for the CLI to create an object, which it cannot: while a
# store is served, the CLI is refused.*
#
# `%TX%` is the handle from the previous response, so this is begin, create, write, commit down one
# connection — and the `created` reply is where the id comes from.
socket_created="$(client '{"tx_begin":{}}' \
                         '{"tx_create":{"tx":"%TX%","struct":"Contact"}}' \
                         '{"tx_commit":{"tx":"%TX%"}}' | sed -n 2p)"
katherine="$(jval "$socket_created" created.id)"
assert_contains "$katherine" "o-" "a creation over the socket answers the id it allocated"

over_socket="$(jval "$(client '{"list":{"struct":"Contact"}}')" ids)"
assert_eq "$(count "$over_socket")" "4" "and the listing over the socket sees it"
assert_contains "$over_socket" "$ada" "along with everything the CLI created earlier"
assert_fails "and still not the deleted one" \
    -- sh -c "printf '%s\n' '$over_socket' | grep -qx '$grace'"

# *Failing means two clients creating objects at the same time conflict, or worse, agree on an id.*
#
# Two processes, two connections, two transactions, each creating. Neither reads anything and the
# cells they write are distinct **by construction**, so there is no guard to trip: creation is the
# one write two clients can always both do. A rejection here would be a conflict the model does not
# have.
create_through_its_own_connection() {
    client '{"tx_begin":{}}' \
           '{"tx_create":{"tx":"%TX%","struct":"Contact"}}' \
           '{"tx_commit":{"tx":"%TX%"}}'
}
create_through_its_own_connection >"$WORK/one.out" 2>&1 &
one=$!
create_through_its_own_connection >"$WORK/two.out" 2>&1 &
two=$!
wait "$one" || fail "the first of two simultaneous creations failed: $(cat "$WORK/one.out")"
wait "$two" || fail "the second of two simultaneous creations failed: $(cat "$WORK/two.out")"

first="$(jval "$(sed -n 2p "$WORK/one.out")" created.id)"
second="$(jval "$(sed -n 2p "$WORK/two.out")" created.id)"
assert_contains "$(sed -n 3p "$WORK/one.out")" '"committed"' "two clients create at once…"
assert_contains "$(sed -n 3p "$WORK/two.out")" '"committed"' "…and both commits land"
[ "$first" != "$second" ] || fail "two concurrent creations must be two objects"
pass "with two different ids, because neither client chose one"

assert_eq "$(count "$(jval "$(client '{"list":{"struct":"Contact"}}')" ids)")" "6" \
    "and the store holds all six contacts: created here, created earlier, and typed by hand"

stop_serve

# --- And the CLI has the store back ---------------------------------------------------------------

assert_eq "$(count "$(borg list Contact)")" "6" \
    "the same six, through the CLI, because there is one enumeration and not two"
assert_eq "$(borg list Contact)" "$(borg list Contact)" \
    "listed in the same order twice — stable, so a diff of a listing is news rather than noise"
