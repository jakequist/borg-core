#!/usr/bin/env bash
# Derived data is honest about how stale it is. This is the headline feature, so it gets a scenario
# that shows the lag rather than hiding it.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg repo push "$HERE"/../030-shell-pipeline/repo
borg set 'Company#1.website' acme.ai
borg set 'Company#1.headcount' 40
borg derive

assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" "derived once caught up"
assert_field "$(borg get 'Company#1.is_investible')" "state" "current" "and reported as current"

# Now change an input and deliberately do NOT derive.
borg set 'Company#1.headcount' 3

out="$(borg get 'Company#1.is_investible')"
assert_field "$out" "state" "stale" \
    "a derived value whose input moved says so rather than lying"
assert_contains "$out" "fresh as of" "and states exactly what it does reflect"
assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "the stale value is still served — labelled, not withheld"

# A write the pipeline never read must not make anything stale.
borg derive
borg set 'Company#1.employees' 40
assert_field "$(borg get 'Company#1.is_investible')" "state" "current" \
    "an unrelated write leaves derived data current — field-granular, not object-granular"

assert_contains "$(borg frontier)" "invest" "the frontier reports how far each producer has caught up"
