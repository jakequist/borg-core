#!/usr/bin/env bash
# **A backlog settles as one round.** SPEC.md §6.3, §16.5.
#
# Several source layers commit before any of them is settled — the ordinary shape of a paused branch,
# a burst of writes, or a deriver that fell behind. A round used to settle **one source layer**, and
# that made a backlog a treadmill: the round settling the first layer computed from the world at that
# layer, and was rejected at merge by its own guard, because the second layer had moved its input
# while it ran. The guard was right every time. The *schedule* had guaranteed the work was stale
# before it ran, and under sustained backlog most derivation work was run and then thrown away.
#
# A round now covers `[watermark+1 … head]`. It has nothing to be stale about, because the top of the
# range is where it forks.
#
# **What is asserted, and why it is these two things.** The settled values, which are the promise; and
# the number of **round branches**, which is how "one round" is visible from outside — a round forks
# exactly one branch of its own (§16.5), so counting branches across a single `borg derive` counts
# rounds without pinning how many invocations ran inside one. How many invocations run is precisely
# what §9.6 leaves to the scheduler, and `scenarios/200-determinism` is the scenario that lets it
# vary.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# Paused, which is what builds the backlog: every `borg set` commits and nothing chases it.
borg derive pause >/dev/null
borg repo push "$HERE/repo" >/dev/null

# --- three writes to one field, none of them settled ------------------------------------------------

# The *same* field three times on purpose. Each write invalidates the invocation the one before it
# dirtied, so under a round per source layer the first two rounds were guaranteed to lose their guard
# on `headcount` to the layer that came after them.
borg set 'Company#1.headcount' 40 >/dev/null
borg set 'Company#1.headcount' 3 >/dev/null
borg set 'Company#1.headcount' 12 >/dev/null
top="$(borg layer head)"

assert_eq "$(borg get 'Company#1.is_investible' --value)" "" \
    "nothing has been derived: three source layers are outstanding"

# --- one derive ------------------------------------------------------------------------------------

branches_before="$(borg branch list | wc -l)"
borg derive >/dev/null
branches_after="$(borg branch list | wc -l)"

assert_eq "$((branches_after - branches_before))" "1" \
    "three source layers, one round: the whole backlog settled in a single fork-and-merge"

assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "computed from the newest write, not from the oldest"
assert_eq "$(borg get 'Company#1.tier' --value)" "core" \
    "and the hop that reads it settled in the same round"
assert_field "$(borg get 'Company#1.tier')" "state" "current" \
    "with nothing outstanding behind it"

# --- one derived layer per producer, reflecting the top of the range --------------------------------

# §6.3's rule after the change: one derived layer per producer per **round**, labelled with the top
# source layer of the range. Not one per `(producer, source layer)` — that was v1's no-coalescing
# rule, and coalescing across a range is what retires it.
layers="$(borg layer list)"
derived="$(printf '%s\n' "$layers" | grep -c 'derived by' || true)"
assert_eq "$derived" "2" \
    "two producers, two derived layers on main — not two per source layer"

reflected="$(printf '%s\n' "$layers" | sed -n 's/.*reflects \(L[0-9]*\).*/\1/p' | sort -u)"
assert_eq "$reflected" "$top" \
    "both reflecting the top of the range ($top), which is the layer their watermark now names"

assert_derives 0 \
    "and the branch settles rather than chasing the derived layers it just merged"

# --- a fourth write is an ordinary round ------------------------------------------------------------

# The range is `[watermark+1 … head]`, so a branch that is already settled has a range of one layer
# and the ordinary case is unchanged.
borg set 'Company#1.headcount' 2 >/dev/null
branches_before="$(borg branch list | wc -l)"
borg derive >/dev/null
assert_eq "$(($(borg branch list | wc -l) - branches_before))" "1" \
    "a settled branch taking one more write settles it in one round, as it always did"
assert_eq "$(borg get 'Company#1.tier' --value)" "watch" \
    "and follows the chain down to the value the last write implies"
