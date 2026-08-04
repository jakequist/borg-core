#!/usr/bin/env bash
# Guards are automatic, and this is what they are for. SPEC.md §12, §13.
#
# A transaction records what it read; at commit those reads become its guards, re-evaluated against
# the parent since the fork point. "Did the parent touch this while I was working?" is the definition
# of a merge conflict, so nothing new detects it — the read-set is simply no longer optional.
#
# Three failure classes, each with something specific that breaks if it is not caught. The explicit
# handle is what makes them expressible at all: two transactions are open at once here, interleaved
# across separate processes, which is an ordering a single-process API cannot produce.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg def push "$HERE/schema.json" >/dev/null
borg set 'Company#1.headcount' 10 >/dev/null
borg set 'Company#2.name' Rival >/dev/null

# --- S2: a stale transaction is rejected in either merge order ---------------------------------------

# *Failing means order-enforcement crept back in.*
#
# Both transactions read `headcount` and then write it — the ordinary read-modify-write, and the one
# case where "guard what you read and did not write" has to be read carefully. The read happened
# *before* the write, so it observed the parent and is a real dependency on it; it is only a read
# that follows the transaction's own write that must not be guarded (which is 130's S4). Collapse the
# two and compare-and-swap becomes impossible, which SPEC-DRAFT §2 promises falls straight out of
# reading a cell before writing it.

# increment <tx> — read the counter through a transaction, then write one more than what it said.
increment() {
    local tx="$1" seen
    seen="$(borg tx get --tx "$tx" 'Company#1.headcount' --value)"
    borg tx set --tx "$tx" 'Company#1.headcount' "$((seen + 1))" >/dev/null
}

a="$(borg tx begin)"
b="$(borg tx begin)"
increment "$a"
increment "$b"
assert_eq "$(borg tx commit --tx "$a" >/dev/null && echo ok)" "ok" \
    "the first transaction to commit lands"
assert_rejected "no longer holds against the parent" \
    "and the second is rejected: what it read moved underneath it" \
    -- borg tx commit --tx "$b"
assert_eq "$(borg get 'Company#1.headcount' --value)" "11" \
    "so the increment happened exactly once, not twice with one silently lost"
borg tx abort --tx "$b" >/dev/null

# The same again with the *older* transaction committing second, which is the half an ordering rule
# would get wrong: age is not what decides this, divergence is.
c="$(borg tx begin)"
d="$(borg tx begin)"
increment "$c"
increment "$d"
assert_eq "$(borg tx commit --tx "$d" >/dev/null && echo ok)" "ok" \
    "the younger transaction commits first this time"
assert_rejected "no longer holds against the parent" \
    "and the older one is rejected just the same" \
    -- borg tx commit --tx "$c"
assert_eq "$(borg get 'Company#1.headcount' --value)" "12" \
    "one increment again — the rejection is a property of what moved, not of who started first"
borg tx abort --tx "$c" >/dev/null

# --- S3: absence is a guarded read -------------------------------------------------------------------

# *Failing means absence tracking is decorative and concurrent creates silently duplicate.*
#
# Neither transaction ever runs a `get`. Writing a property implies the object exists (§8), and the
# probe that decides whether to write the existence cell is a **read** — of a cell that is absent.
# Two transactions each conclude `Company#9` does not exist and each create it; if that probe is not
# in the read-set, both creates land and the second silently overwrites a decision the first made.
assert_eq "$(borg get 'Company#9.name' --value)" "" "Company#9 does not exist yet"

e="$(borg tx begin)"
f="$(borg tx begin)"
borg tx set --tx "$e" 'Company#9.name' First >/dev/null
borg tx set --tx "$f" 'Company#9.name' Second >/dev/null
assert_eq "$(borg tx commit --tx "$e" >/dev/null && echo ok)" "ok" "one of the two creates the object"
assert_rejected "no longer holds against the parent" \
    "and the other loses, on a cell neither of them named" \
    -- borg tx commit --tx "$f"
assert_eq "$(borg get 'Company#9.name' --value)" "First" \
    "absence is a legitimate thing to have acted on, so acting on a stale absence is a conflict"
borg tx abort --tx "$f" >/dev/null

# --- S6: deleting an object conflicts with writing to it ----------------------------------------------

# *This is the test for "implicit reads count": the writer's existence probe is what makes it a
# conflict.*
#
# The deleter names the existence cell outright, so it probes nothing and carries no guard — a blind
# delete, last-write-wins, exactly as §2 says a write with no reads must be. The writer never
# mentions existence at all, and is the one that conflicts: its implicit read of `Company:o-…` is the
# whole mechanism, and without it a field would quietly attach itself to a deleted object.
g="$(borg tx begin)"
h="$(borg tx begin)"
borg tx delete --tx "$g" 'Company#2' >/dev/null
borg tx set --tx "$h" 'Company#2.region' emea >/dev/null
assert_eq "$(borg tx commit --tx "$g" >/dev/null && echo ok)" "ok" "the delete lands"

rejection="$(borg tx commit --tx "$h" 2>&1 || true)"
assert_contains "$rejection" "no longer holds against the parent" \
    "and the write to a field of that object is rejected"
assert_contains "$rejection" "Company:o-" \
    "named by the existence cell it never mentioned, which is the read that saved it"
borg tx abort --tx "$h" >/dev/null

assert_field "$(borg get 'Company#2')" "state" "tombstoned" "the object stays deleted"
assert_eq "$(borg get 'Company#2.region' --value)" "" \
    "and nothing from the rejected transaction is on the parent"
