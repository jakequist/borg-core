#!/usr/bin/env bash
# The smallest complete round trip: write a cell, read it back with provenance.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# A cell is addressed as Struct:pid.field. `Company#100` is a documented input shorthand meaning
# "the root branch, allocator 0, counter 100" — convenient to type, and never printed back.
borg set 'Company#100.website' 9

assert_eq "$(borg get 'Company#100.website' --value)" "9" \
    "a value written is a value read"

# What comes back names the whole PID: kind, branch, allocator and counter. The shorthand carried
# only the counter, so it meant a different object depending on what you assumed.
canonical="$(borg get 'Company#100.website' | head -1)"
assert_contains "$canonical" "Company:o-" "a cell reads back canonically, not in the shorthand"
assert_eq "$(borg get "$canonical" --value)" "9" \
    "and the canonical address is itself a cell address you can read"

# Reads never return a bare value. Source data is ground truth, so it is always current.
out="$(borg get 'Company#100.website')"
assert_field "$out" "origin" "source" "provenance says where the value came from"
assert_field "$out" "state" "current" "source data is never stale"

assert_eq "$(borg get 'Company#999.website' --value)" "" \
    "a cell never written reads as absent, not as an error"

# Existence is a cell like any other, so deleting is just a write.
borg set 'Company#100' true
borg delete 'Company#100.website'
assert_field "$(borg get 'Company#100.website')" "state" "tombstoned" \
    "a deleted cell is tombstoned, and says so"

assert_contains "$(borg layer list)" "2" "each write is its own layer in the log"
