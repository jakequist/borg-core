#!/usr/bin/env bash
# Definitions travel the log. Two teams extend one struct; a collision is caught at push time.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg def push "$HERE"/../020-definitions/sales.json
assert_contains "$(borg def show Company)" "name" "a declared field shows up in the def"

# A different repo, the same struct. There is no `extends` — declarations merge.
borg def push "$HERE"/../020-definitions/finance.json
out="$(borg def show Company)"
assert_contains "$out" "name" "the first repo's field survives"
assert_contains "$out" "revenue" "the second repo's field lands on the same struct"

assert_fails "two repos declaring the same field is a hard error" \
    -- borg def push "$HERE"/../020-definitions/collision.json

assert_contains "$(borg def show Company)" "name" \
    "and the rejected push leaves the def untouched"

# Definitions are branch-scoped like everything else.
fork_point="$(borg layer head)"
borg branch fork main --at "$fork_point" --name schema-change
borg --branch schema-change def push "$HERE"/../020-definitions/website.json

assert_contains "$(borg def show Company --branch schema-change)" "website" \
    "the child sees its own schema change"
assert_fails "and the parent does not" \
    -- sh -c "borg def show Company --branch main | grep -q website"
