#!/usr/bin/env bash
# Derived data is honest about how stale it is. This is the headline feature, so it gets a scenario
# that shows the lag rather than hiding it.
#
# Lag is normally brief — a write catches its branch up before the command exits (§9.6) — so the way
# to *see* it is to stop the automation. `borg derive pause` is a branch-scoped switch living beside
# the store, and a paused branch is self-documenting: its frontier stops advancing, and every read of
# derived data already says how far behind it is. Nothing below reports a "paused" flag, because a
# pause *is* lag and the freshness envelope already describes lag.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg repo push "$HERE"/../030-shell-pipeline/repo
borg set 'Company#1.website' acme.ai
borg set 'Company#1.headcount' 40

# Nothing asked for that. The write caught the branch up on its way out.
assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "derivation runs on its own: a write is enough"
assert_field "$(borg get 'Company#1.is_investible')" "state" "current" "and the read says current"

# --- Freeze the automation -----------------------------------------------------------------------

borg derive pause
assert_contains "$(borg derive status)" "paused" "auto-derivation can be frozen, per branch"

borg set 'Company#1.headcount' 3

out="$(borg get 'Company#1.is_investible')"
assert_field "$out" "state" "stale" \
    "a derived value whose input moved says so rather than lying"
assert_contains "$out" "fresh as of" "and states exactly what it does reflect"
assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "the stale value is still served — labelled, not withheld"

assert_contains "$(borg frontier)" "invest" "the frontier reports how far each producer has caught up"

# The same question as a *query*. `borg derive --quiet` answers "is anything outstanding" by doing
# the work and reporting how much it did; this one asks the frontier and the log and runs nothing,
# which is why the value underneath is still stale afterwards.
assert_contains "$(borg derive --outstanding)" "invest" \
    "a read-only query reports what a producer has yet to incorporate"
assert_field "$(borg get 'Company#1.is_investible')" "state" "stale" \
    "and asking derived nothing — the query does not run producers"

# Pausing stops the automation, not the engine — which is what makes it useful in an emergency.
assert_derives 1 "borg derive still works on a paused branch"
assert_eq "$(borg get 'Company#1.is_investible' --value)" "false" "and the value follows the input"
assert_eq "$(borg derive status --outstanding)" "nothing outstanding" \
    "and once the round has run, the query says so"

# A write the pipeline never read must not make anything stale.
borg set 'Company#1.employees' 40
assert_field "$(borg get 'Company#1.is_investible')" "state" "current" \
    "an unrelated write leaves derived data current — field-granular, not object-granular"

# --- Resume ----------------------------------------------------------------------------------------

borg derive resume
assert_contains "$(borg derive status)" "running" "and the switch goes back the other way"

borg set 'Company#1.headcount' 40
assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "with automation back on, a write is once again enough"
