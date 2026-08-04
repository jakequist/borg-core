#!/usr/bin/env bash
# Transactions are the only client write path. SPEC.md §12, §13.
#
# A client never writes to a shared branch. It forks, writes in isolation, and merges — and because
# the fork's read path is bounded at the fork point, everything it reads is one consistent snapshot.
# Guards re-evaluated against the parent since that fork point were already the merge-conflict
# detector, so snapshot isolation with optimistic concurrency falls out of machinery that existed.
#
# This scenario is the surface and the two claims that say guards do not *over*-reject. The claims
# that say they reject what they must are next door, in 140.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg def push "$HERE/schema.json" >/dev/null

# --- The surface ------------------------------------------------------------------------------------

tx="$(borg tx begin)"
assert_contains "$tx" "tx-" "begin prints a transaction handle"
assert_contains "$(borg branch list)" "$tx" "which is a real branch, forked from the one being written"

borg tx set --tx "$tx" 'Company#1.name' Acme >/dev/null
assert_eq "$(borg tx get --tx "$tx" 'Company#1.name' --value)" "Acme" \
    "a transaction sees its own writes"
assert_eq "$(borg get 'Company#1.name' --value)" "" \
    "and nobody else does, because they are on a branch of their own"

landed="$(borg tx commit --tx "$tx")"
assert_eq "$(borg get 'Company#1.name' --value)" "Acme" \
    "commit merges the transaction onto the branch it forked"
assert_contains "$(borg layer list)" "${landed#L}" \
    "and prints the layer it landed in, which is a layer on the parent"

# The handle is spent. Not "unknown" — the CLI knows exactly what happened to it.
assert_rejected "already committed" "a committed transaction cannot be used again" \
    -- borg tx get --tx "$tx" 'Company#1.name'

# --- Abort ------------------------------------------------------------------------------------------

doomed="$(borg tx begin)"
borg tx set --tx "$doomed" 'Company#1.name' Wrong >/dev/null
before="$(borg layer head)"
borg tx abort --tx "$doomed" >/dev/null
assert_eq "$(borg get 'Company#1.name' --value)" "Acme" "an aborted transaction never happened"
assert_eq "$(borg layer head)" "$before" "and commits no layer on the parent"

# --- S4: a transaction does not conflict with itself ------------------------------------------------

# *Failing means the parent-reads-only rule is wrong and every read-modify-write deadlocks itself.*
#
# Write X, then read X. That read returned the transaction's own write, not the parent's state, so
# it expresses no dependency on the parent and must contribute no guard. The naive rule — "every
# read is a guard" — makes this transaction fail on a cell only it has touched.
self="$(borg tx begin)"
borg tx set --tx "$self" 'Company#1.headcount' 40 >/dev/null
assert_eq "$(borg tx get --tx "$self" 'Company#1.headcount' --value)" "40" \
    "a transaction reading back its own write sees its own write"
borg tx set --tx "$self" 'Company#1.headcount' 41 >/dev/null
borg tx commit --tx "$self" >/dev/null
assert_eq "$(borg get 'Company#1.headcount' --value)" "41" \
    "and committing it conflicts with nothing"

# --- S5: guards do not over-reject ------------------------------------------------------------------

# *Failing means guards are object-granular in practice and cell granularity is fiction.*
#
# Two transactions, one object, different fields. Every mechanism in Borg is cell-granular — guards,
# dependencies, ownership — and this is where that stops being a design statement and becomes an
# observable one: if either transaction guarded the *object*, the second would lose.
#
# Note that `Company#1` already exists here. Both transactions probe its existence cell on their way
# to writing a property (§8, implied existence), and neither writes it, so neither disturbs what the
# other read. When the object does *not* already exist that probe is exactly what makes concurrent
# creates conflict — which is 140's S3, and the same mechanism seen from the other side.
one="$(borg tx begin)"
two="$(borg tx begin)"
borg tx set --tx "$one" 'Company#1.name' 'Acme Corp' >/dev/null
borg tx set --tx "$two" 'Company#1.region' emea >/dev/null
borg tx commit --tx "$one" >/dev/null
borg tx commit --tx "$two" >/dev/null

assert_eq "$(borg get 'Company#1.name' --value)" "Acme Corp" "both transactions land:"
assert_eq "$(borg get 'Company#1.region' --value)" "emea" \
    "  writing different fields of one object is not a conflict"

# --- A bare `borg set` is an implicit one-shot transaction -------------------------------------------

# begin, set, commit, in one process. That is what keeps the common case one command while making
# "every client write is a transaction" literally true rather than aspirationally true. It reads
# nothing it did not write, so it carries no guard on the cell it writes and is honestly
# last-write-wins there — which is what every database does with a blind write.
before_branches="$(borg branch list | wc -l)"
landed="$(borg set 'Company#1.headcount' 42)"
assert_eq "$(borg get 'Company#1.headcount' --value)" "42" "a bare set still writes"
assert_contains "$(borg layer list)" "${landed#L}" \
    "and still prints one layer on the branch, which is what a client awaits"
if [ "$(borg branch list | wc -l)" -le "$before_branches" ]; then
    fail "a bare set did not fork — it is writing to the shared branch directly"
fi
pass "and it got there by forking and merging like every other write"

# The layer it landed in is a *merge* layer naming the transaction's events, so the value's
# authorship survives the trip (§4.3, §13): authored on the transaction branch, landed here.
out="$(borg get 'Company#1.headcount')"
if [ "$(field "$out" "authored at")" = "$(field "$out" "landed at")" ]; then
    fail "the write was copied rather than named — authorship did not survive the merge"
fi
pass "authored on the transaction branch, landed on the one it merged into"
