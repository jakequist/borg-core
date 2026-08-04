#!/usr/bin/env bash
# A schema change is a branch-scoped mutation like any other. It is invisible to the parent until it
# is merged — and, now that writes consult definitions (§5.1, §8), *unusable* there too. The read
# half of this was always testable; the write half is what milestone B made possible.
#
# The second half goes two forks deep. Nothing else in the project exercises a branch chain longer
# than one fork, and `read_path` walking arbitrary depth is exactly the kind of "should work" that
# deserves a test through the real binary.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# Something on main to fork from — and a struct that has nothing to do with the rest of this.
borg def push "$HERE/bootstrap.json" >/dev/null
borg branch fork main --at "$(borg layer head)" --name feature >/dev/null

# --- One fork deep --------------------------------------------------------------------------------

borg --branch feature def push "$HERE/company.json" >/dev/null

assert_contains "$(borg def show Company --branch feature)" "name" \
    "the child sees the struct it declared"
assert_fails "and the parent cannot see it" \
    -- borg def show Company --branch main

borg --branch feature set 'Company#1.name' Acme >/dev/null
assert_eq "$(borg get 'Company#1.name' --value --branch feature)" "Acme" \
    "the child can write the struct it declared"

# The half that only became possible in B: not seeing a def and not being able to *use* one are the
# same fact, and now the write path says so.
assert_rejected "no struct named \`Company\`" \
    "and the parent cannot write it either, because the def is not in force there" \
    -- borg --branch main set 'Company#1.name' Acme

# --- Two forks deep -------------------------------------------------------------------------------

borg branch fork feature --at "$(borg layer head --branch feature)" --name experiment >/dev/null
borg --branch experiment def push "$HERE/founded.json" >/dev/null

out="$(borg def show Company --branch experiment)"
assert_contains "$out" "name" "a fork of a fork inherits through two fork points"
assert_contains "$out" "founded" "as well as its own declaration"

borg --branch experiment set 'Company#1.founded' 1999 >/dev/null
assert_eq "$(borg get 'Company#1.founded' --value --branch experiment)" "1999" \
    "and can write the field it added"

assert_fails "the middle branch does not see the grandchild's field" \
    -- sh -c "borg def show Company --branch feature | grep -q founded"
assert_rejected "no field \`founded\`" \
    "nor can it write one — the struct is there, the field is not" \
    -- borg --branch feature set 'Company#1.founded' 1999
assert_rejected "no struct named \`Company\`" \
    "and main still has neither" \
    -- borg --branch main set 'Company#1.founded' 1999

# --- Until merged ---------------------------------------------------------------------------------

borg branch merge feature --defs-only >/dev/null
assert_contains "$(borg def show Company --branch main)" "name" \
    "a def-only merge carries the schema to the parent"
borg --branch main set 'Company#1.name' 'Acme Corp' >/dev/null
assert_eq "$(borg get 'Company#1.name' --value --branch main)" "Acme Corp" \
    "and the write the parent could not make now lands"

assert_rejected "no field \`founded\`" \
    "the grandchild's field was not part of that merge, and is still not writable on main" \
    -- borg --branch main set 'Company#1.founded' 1999
