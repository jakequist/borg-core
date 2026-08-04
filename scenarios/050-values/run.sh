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

# Nine fields, one per thing a value can be. Declaring them is what lets the text below be parsed
# *against* a type rather than guessed at from its syntax (§3.4, §5.1).
borg def push "$HERE/schema.json"

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

# --- Primitives and references ----------------------------------------------------------------

borg set 'Company#1.founded' 1999
assert_eq "$(interned 'Company#1.founded')" "" "an integer is a primitive — nothing to intern"
assert_eq "$(borg get 'Company#1.founded' --value)" "1999" "and reads back as an integer"

borg set 'Company#1.is_public' true
assert_eq "$(interned 'Company#1.is_public')" "" "a bool is a primitive too"

borg set 'Company#1.owner' '@Company#2'
assert_contains "$(borg get 'Company#1.owner' --value)" "@o-" \
    "a reference is a PID, and reads back as one"

# --- Parsing is directed by the declared type -----------------------------------------------------

# This is the reservation §3.4 recorded as temporary, lifted. Parsing used to guess a type from the
# syntax alone, so `true`, `42`, `0x` and `@jake` all meant something *other* than their text and no
# string field could hold them. Now the field says what it holds, and the text is just text.
for literal in true 42 1.5 0x 7n '@jake' 99999999999999999999999; do
    borg set 'Company#1.slogan' "$literal" >/dev/null
    assert_eq "$(borg get 'Company#1.slogan' --value)" "$literal" \
        "a String field holds the literal text \`$literal\`"
    assert_contains "$(interned 'Company#1.slogan')" "@s-" \
        "…and it is interned as a string, not read as $literal's old meaning"
done

# The same knowledge, used the other way: a field that cannot hold a value says so instead of
# storing something that looks almost right.
assert_rejected "declared Int" "an Int field refuses a word" \
    -- borg set 'Company#1.founded' acme
assert_rejected "declared Bool" "a Bool field refuses anything but true or false" \
    -- borg set 'Company#1.is_public' yes
assert_rejected "declared Company" "a reference field refuses a bare word" \
    -- borg set 'Company#1.owner' acme

# `~` stays reserved on every field whatever its type: deletion has to be expressible (§8.1).
borg delete 'Company#1.slogan'
assert_field "$(borg get 'Company#1.slogan')" "state" "tombstoned" \
    "a tombstone is accepted by every declared type"
