#!/usr/bin/env bash
# A pipeline written in bash, spoken to over stdio. The point is not that bash is a good language for
# this — it is that if bash can do it, the protocol has no hidden client-library complexity.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg repo push "$HERE"/../030-shell-pipeline/repo
assert_contains "$(borg producer list)" "invest" \
    "the script described itself, and the server recorded a producer definition"

# …and its definitions came from the same `describe`, in the same def layer. The repo is the only
# thing that knows `is_investible` exists, so after B it is the only thing that *can* declare it.
schema="$(borg def show Company)"
assert_contains "$schema" "website" "the repo's struct definitions landed with its producer"
assert_contains "$schema" "derived by P" \
    "and the derived field names the producer that owns it, resolved from the name in describe"

# The other side of declared ownership: a client cannot write a field a producer owns.
assert_rejected "may not write" "a client may not write a derived field" \
    -- borg set 'Company#1.is_investible' true

# The spec's own motivating example: `company.website.ends_with('.ai')`, plus a headcount threshold.
# The script reads a string and a number, and never learns that one of them is interned.
borg set 'Company#1.website' acme.ai
borg set 'Company#1.headcount' 40
borg set 'Company#2.website' example.com
borg set 'Company#2.headcount' 40
borg derive

assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "the bash pipeline derived a field over stdio"
assert_eq "$(borg get 'Company#2.is_investible' --value)" "false" \
    "and it ran per entity, not once globally"

assert_field "$(borg get 'Company#1.is_investible')" "origin" "derived" \
    "the output is marked derived, and attributed to its producer"

# Dependency capture is automatic: the script declared nothing, the server watched what it read.
# Lineage names cells canonically, so ask the CLI what that address is rather than assuming.
website="$(borg get 'Company#1.website' | head -1)"
assert_contains "$(borg explain 'Company#1.is_investible')" "$website" \
    "lineage shows what the script actually read, without it declaring anything"

# The invalidation story, end to end through a subprocess — driven by the *string* field.
borg set 'Company#2.website' rival.ai
assert_eq "$(borg derive --count)" "1" \
    "changing one input re-runs exactly one invocation"
assert_eq "$(borg get 'Company#2.is_investible' --value)" "true" "and the output follows"

# The numeric field it also reads, to show the two are tracked alike.
borg set 'Company#2.headcount' 3
assert_eq "$(borg derive --count)" "1" "the number it read is a tracked dependency too"
assert_eq "$(borg get 'Company#2.is_investible' --value)" "false" "and it flips the answer back"

borg set 'Company#1.employees' 40
assert_eq "$(borg derive --count)" "0" \
    "writing a field the script never read runs nothing at all"
