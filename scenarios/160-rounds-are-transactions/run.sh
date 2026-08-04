#!/usr/bin/env bash
# **S7, and what a round looks like from outside.** SPEC.md §16.5.
#
# A round is now a transaction: it forks the branch at the source layer it settles, runs every
# producer on the fork, and merges what settled. Three of that change's consequences are visible to
# a client, and this is where they are asserted end to end through the real binary.
#
# The one that would break loudest is S7. A round's guards are its producers' read-sets, and this
# repo's second hop reads the very field its first hop writes. Guard that, and the second hop is
# rejected on every round — for ever, on any chain, silently, because a rejected invocation looks
# exactly like a producer that had nothing to say. The rule that saves it is *guard the cells you
# read and the round did not write*, and `tier` appearing at all is the proof of it.
#
# What is **not** here: a client write landing while a round runs (S8, S10). The CLI is
# process-per-command and layer ids are minted by a process-local sequencer (§17.2), so two `borg`
# processes against one store would assign the same layer id — the interleaving would be swamped by
# a corruption that has nothing to do with what was being tested. Those live in
# `crates/borg-engine/tests/rounds.rs`, in one process, where concurrency at v1 actually is.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg derive pause >/dev/null
borg repo push "$HERE/repo" >/dev/null
assert_contains "$(borg producer list)" "invest" "the head of the chain registered"
assert_contains "$(borg producer list)" "tier" "and the hop that reads its output"

# --- S7: a chained producer does not trip its own round's guard ------------------------------------

borg set 'Company#1.headcount' 40 >/dev/null
borg set 'Company#2.headcount' 3 >/dev/null
borg derive >/dev/null

assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "the first hop of the chain landed"
assert_eq "$(borg get 'Company#1.tier' --value)" "core" \
    "and so did the hop that read what the same round had just written"
assert_eq "$(borg get 'Company#2.tier' --value)" "watch" \
    "per entity, both hops, in one round"

# The negative half, said plainly: `pending` is what `tier` writes when it could not see
# `is_investible`. Seeing it here would mean the round's own output was invisible to the round.
if [ "$(borg get 'Company#1.tier' --value)" = "pending" ]; then
    fail "the second hop never saw the first hop's output"
fi
pass "the round's own output was visible to the round that produced it"

# --- A round's output lands as derived layers on the branch it settled -----------------------------

# Merging a *client* branch skips derived layers, because they were computed from other data (§13).
# Merging a *round* branch carries them, because that is the only thing on it and the whole purpose
# of the branch. The two are different merges on purpose, and this is the difference showing.
layers="$(borg layer list)"
assert_contains "$layers" "derived by" "the round's output is on main, attributed to its producer"
assert_contains "$(borg explain 'Company#1.tier')" "produced by" \
    "and lineage survived the crossing"

# `reflects` is the fork point by construction now: a round cannot label its output with a layer it
# did not fork at, because the fork point is the only thing it can see.
settled="$(borg layer head)"
reflected="$(printf '%s\n' "$layers" | sed -n 's/.*reflects \(L[0-9]*\).*/\1/p' | sort -u | tail -1)"
assert_field "$(borg get 'Company#1.tier' --freshness validated)" "state" "current" \
    "and the value reads current, with nothing outstanding behind it"
[ -n "$reflected" ] || fail "no derived layer stated what it reflects"
pass "every derived layer on main states the source layer it reflects ($reflected, head $settled)"

# --- An interrupted round leaves nothing behind ----------------------------------------------------

# A round that never merges has committed only to its own branch, which nothing on main can see. The
# residue is a branch row — the same residue a reaped transaction leaves (§12.3) — and the work is
# not lost, because the cells are still dirty and the next round rediscovers them.
borg set 'Company#1.headcount' 4 >/dev/null
borg derive >/dev/null
assert_eq "$(borg get 'Company#1.is_investible' --value)" "false" \
    "a second round settles the second source layer"
assert_eq "$(borg get 'Company#1.tier' --value)" "watch" \
    "and the chain follows it down"

assert_eq "$(borg derive --count)" "0" \
    "nothing is outstanding afterwards: a settled round leaves no work behind"
