#!/usr/bin/env bash
# Definitions are load-bearing. Every write is checked against the def-view of its branch (§5.1, §8),
# and this scenario is the proof — including that the rejections are worth reading, because a
# rejection nobody can act on is barely better than a crash.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# Before any schema exists, *nothing* is writable. This is the line that used to succeed.
assert_rejected "no struct named \`Wombat\`" \
    "with no schema at all, an invented struct is refused" \
    -- borg set 'Wombat#1.nonsense' 7

# `is_investible` is declared derived and owned by producer 7 — the def file speaks the log's own
# form, so it names the producer by id. A repo names it by name and `borg repo push` resolves it
# (see 030).
borg def push "$HERE/schema.json" >/dev/null

# --- What a declaration buys ---------------------------------------------------------------------

borg set 'Company#1.name' Acme >/dev/null
borg set 'Company#1.headcount' 40 >/dev/null
assert_eq "$(borg get 'Company#1.name' --value)" "Acme" "a declared field accepts a value that fits"

out="$(borg def show Company)"
assert_contains "$out" "source" "the def says which fields clients write"
assert_contains "$out" "derived by P7" "and which producer owns the rest"

# --- The four ways a write is refused -------------------------------------------------------------

# 1. The struct is not declared. A struct exists because someone declared a field on it (§5.2), so
#    an unknown name is not an empty struct — it is a typo.
assert_rejected "no struct named \`Wombat\`" "an undeclared struct is refused" \
    -- borg set 'Wombat#1.nonsense' 7

# 2. The field is not declared. The rejection lists what *is* declared, so a typo is a one-line fix
#    rather than a second command.
assert_rejected "it has: headcount, is_investible, name" \
    "an undeclared field is refused, and the alternatives are named" \
    -- borg set 'Company#1.nonsense' 7

# 3. The value does not fit the declared type. Parsing is directed by that type (§3.4), so this is
#    caught before a value exists at all.
assert_rejected "declared Int" "a value of the wrong type is refused" \
    -- borg set 'Company#1.headcount' acme

# 4. The writer is not the one the declaration names. Ownership is *declared*, not discovered, so
#    this is caught on the first wrong write rather than on a later collision.
assert_rejected "may not write" "a client may not write a derived field" \
    -- borg set 'Company#1.is_investible' true
assert_rejected "derived by producer P7" "and is told who does own it" \
    -- borg set 'Company#1.is_investible' true

# --- A refused write leaves nothing behind --------------------------------------------------------

before="$(borg layer head)"
assert_fails "the refused writes above all failed" -- borg set 'Company#1.nonsense' 7
assert_eq "$(borg layer head)" "$before" \
    "a rejected write commits no layer — the branch head does not move"

assert_eq "$(borg get 'Company#1.headcount' --value)" "40" \
    "and the value that was legitimately there is untouched"
