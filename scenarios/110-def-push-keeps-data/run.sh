#!/usr/bin/env bash
# Declaring a field must not hide the data already stored in the *other* fields.
#
# Six commands, no producers, no migrations, nothing to lag: declare a field, write it, declare an
# unrelated second field, read the first one back. Nothing about that story mentions `name`'s shape
# changing, so nothing about it should make `name` unreadable.
#
# It did. A cell record was keyed by the writing actor's whole-schema ClientVersion, which advances
# on *every* def push, while §5.3 defines a def-version **per definition** — the def-layer that last
# mutated that field. `borg def show` reported the second thing and storage the first, so a reader
# whose ClientVersion had moved past the write looked for a version nothing was stored at, found no
# migration route to it (there is no migration: the field never changed), and concluded the value
# was unreachable.
#
# The assertion below is deliberately about the *whole envelope* and not just the value: `broken`
# was the loudest part of the symptom, and a fix that returned the value while still calling the
# cell broken would be half a fix.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg def push "$HERE/name.json" >/dev/null
at_declare="$(borg def version)"
borg set 'Company#1.name' acme >/dev/null

# An unrelated field, on the same struct, from the same repo. `name` is untouched by this push —
# and that is the whole point: the branch's def-version moves, `name`'s does not.
borg def push "$HERE/city.json" >/dev/null
assert_contains "$(borg def show Company)" "vL1" \
    "declaring city leaves name's own def-version where it was"

out="$(borg get 'Company#1.name')"
assert_field "$out" "value" "acme" "and the value written before the push is still readable"
assert_field "$out" "state" "current" "not broken: nothing owes this cell a migration"
assert_field "$out" "origin" "source" "and it is still the client's own write, not a derived shadow"

# The same claim from the other end: an old client, pinned to the def-version it was written
# against, reads what it wrote. This is §5.4's backwards-compatibility promise, and it held before
# the fix only because that client's ClientVersion happened to equal the storage key.
assert_eq "$(borg get 'Company#1.name' --value --client-version "$at_declare")" "acme" \
    "a client pinned to the older def-view reads the same cell, through no migration at all"

# And the second field is writable and readable at the new version, so the fix is not "ignore
# versions".
borg set 'Company#1.city' berlin >/dev/null
assert_eq "$(borg get 'Company#1.city' --value)" "berlin" \
    "the newly declared field works too"

# A third push, to prove this is not a one-push tolerance: `name` has now survived two def-layers it
# was never mentioned in.
borg def push "$HERE/city.json" >/dev/null
assert_eq "$(borg get 'Company#1.name' --value)" "acme" \
    "and survives a further push it has nothing to do with"
