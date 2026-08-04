#!/usr/bin/env bash
# A pipeline written in bash, spoken to over stdio. The point is not that bash is a good language for
# this — it is that if bash can do it, the protocol has no hidden client-library complexity.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg repo push "$HERE"/../030-shell-pipeline/repo
assert_contains "$(borg producer list)" "invest" \
    "the script described itself, and the server recorded a producer definition"

borg set 'Company#1.website' 9
borg set 'Company#2.website' 1
borg derive

assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "the bash pipeline derived a field over stdio"
assert_eq "$(borg get 'Company#2.is_investible' --value)" "false" \
    "and it ran per entity, not once globally"

assert_field "$(borg get 'Company#1.is_investible')" "origin" "derived" \
    "the output is marked derived, and attributed to its producer"

# Dependency capture is automatic: the script declared nothing, the server watched what it read.
assert_contains "$(borg explain 'Company#1.is_investible')" "Company#1.website" \
    "lineage shows what the script actually read, without it declaring anything"

# The invalidation story, end to end through a subprocess.
borg set 'Company#2.website' 99
assert_eq "$(borg derive --count)" "1" \
    "changing one input re-runs exactly one invocation"
assert_eq "$(borg get 'Company#2.is_investible' --value)" "true" "and the output follows"

borg set 'Company#1.employees' 40
assert_eq "$(borg derive --count)" "0" \
    "writing a field the script never read runs nothing at all"
