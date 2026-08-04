#!/usr/bin/env bash
# The value model, through the CLI. Every field in Borg was an integer until interning was wired up,
# so this is the scenario that proves a field can now hold what a real one holds.
#
# The claim under test is not "interning works" — storage could always intern. It is that **a client
# never has to know it exists**: you write text, you read the same text back, and the content hash
# in between is the engine's business.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# The `interned:` line of `borg get` names the content-addressed PID behind a value, and appears
# only when there is one. It is the surface that shows deduplication actually happening.
interned() { borg get "$1" | sed -n 's/^[[:space:]]*interned:[[:space:]]*//p'; }

# --- Strings -----------------------------------------------------------------------------------

# No quotes, no prefix, no separate "create a string" step. A bare word is a string.
borg set 'Company#1.website' acme.ai
assert_eq "$(borg get 'Company#1.website' --value)" "acme.ai" \
    "a string is written and read as itself"

borg set 'Company#1.name' 'Acme Corporation'
assert_eq "$(borg get 'Company#1.name' --value)" "Acme Corporation" \
    "including one with spaces in it"

# What comes out must go back in. A client that cannot copy a value between cells cannot do
# anything, and this is the round trip that proves the printed form is the accepted form.
value="$(borg get 'Company#1.website' --value)"
borg set 'Company#2.website' "$value"
assert_eq "$(borg get 'Company#2.website' --value)" "acme.ai" \
    "a string survives a round trip out through get and back in through set"

# --- Interning ----------------------------------------------------------------------------------

# Equal content is one stored value, registry-wide and branch-independently. Two companies that
# happen to share a website share the storage for it, and nobody asked for that to happen.
assert_eq "$(interned 'Company#1.website')" "$(interned 'Company#2.website')" \
    "two identical strings are one interned value with one PID"

assert_contains "$(interned 'Company#1.website')" "@s-" \
    "and that PID says it is a string — the kind is part of the identifier"

borg set 'Company#3.website' rival.ai
if [ "$(interned 'Company#1.website')" = "$(interned 'Company#3.website')" ]; then
    fail "different strings must not share a PID"
fi
pass "different strings are different interned values"

# A string field prints its content, never the `@s-…` that is physically stored.
assert_field "$(borg get 'Company#1.website')" "value" "acme.ai" \
    "the stored reference is resolved to content on the way out"

# --- Binary and bigints ---------------------------------------------------------------------------

# One mechanism, three kinds. `0x…` is binary; whole octets only, because half a byte has no
# canonical reading and two spellings of one blob would intern twice.
borg set 'Company#1.logo' 0xdeadbeef
assert_eq "$(borg get 'Company#1.logo' --value)" "0xdeadbeef" "binary round trips through its hex"
assert_contains "$(interned 'Company#1.logo')" "@b-" "and is interned as binary, not as a string"

# A trailing `n` on digits is a bigint — arbitrary precision, well past what an Int holds.
borg set 'Company#1.valuation' 170141183460469231731687303715884105728n
assert_eq "$(borg get 'Company#1.valuation' --value)" \
    "170141183460469231731687303715884105728n" \
    "a bigint round trips past the range of an Int"
assert_contains "$(interned 'Company#1.valuation')" "@n-" "and is interned as a bigint"

borg set 'Company#1.debt' -129n
assert_eq "$(borg get 'Company#1.debt' --value)" "-129n" \
    "negative bigints too — the encoding is two's complement, and minimal"

# --- What is *not* a string -----------------------------------------------------------------------

# The older forms win, which is the documented cost of parsing without a declared type. Milestone B
# makes parsing type-directed and resolves it; until then, this is what borg guesses.
borg set 'Company#1.founded' 1999
assert_eq "$(interned 'Company#1.founded')" "" "an integer is a primitive — nothing to intern"
assert_eq "$(borg get 'Company#1.founded' --value)" "1999" "and reads back as an integer"

borg set 'Company#1.is_public' true
assert_eq "$(interned 'Company#1.is_public')" "" "a bool is a primitive too"
assert_eq "$(borg get 'Company#1.is_public' --value)" "true" \
    "so the text 'true' is a Bool, and a string field cannot hold it yet"

borg set 'Company#1.owner' '@Company#2'
assert_contains "$(borg get 'Company#1.owner' --value)" "@o-" \
    "and @<pid> is still a reference, not the text of one"
