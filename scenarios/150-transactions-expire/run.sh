#!/usr/bin/env bash
# Transactions are ephemeral and reaped; branches are durable and explicit. SPEC.md §12.
#
# A `tx begin` with no commit would otherwise leak a branch and its read-set forever. The answer is
# an **idle** timeout rather than an elapsed one, so that a long but active transaction survives and
# an abandoned short one does not — and reaping sweeps opportunistically when a process opens the
# store, which is where the indexes are already rebuilt, so there is no daemon and an idle store
# sweeps nothing.
#
# The line this draws is worth naming: a client that wants to walk away and come back wanted a
# branch. `borg branch fork` is how you say that, and nothing reaps it.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg def push "$HERE/schema.json" >/dev/null
borg set 'Company#1.name' Acme >/dev/null

assert_contains "$(borg tx timeout)" "1d" "the default timeout is generous"
borg tx timeout 2s >/dev/null
assert_contains "$(borg tx timeout)" "2s" "and configurable, beside the store like every other switch"

# --- Idle, not elapsed --------------------------------------------------------------------------------

# This transaction lives longer than the timeout and is never reaped, because it is touched inside
# it. That is the property that makes reaping safe to turn on: the predictor of an abandoned
# transaction is silence, not age.
busy="$(borg tx begin)"
for _ in 1 2 3; do
    sleep 1
    borg tx get --tx "$busy" 'Company#1.name' >/dev/null
done
borg tx set --tx "$busy" 'Company#1.name' 'Acme Corp' >/dev/null
borg tx commit --tx "$busy" >/dev/null
assert_eq "$(borg get 'Company#1.name' --value)" "Acme Corp" \
    "a transaction open longer than the timeout survives, as long as it is being used"

# --- Abandoned -----------------------------------------------------------------------------------------

abandoned="$(borg tx begin)"
borg tx set --tx "$abandoned" 'Company#1.name' Never >/dev/null
assert_contains "$(borg tx list)" "$abandoned" "an open transaction is listed"

sleep 3
# The sweep happens on the *next* command against this store, whatever it is — nothing here runs a
# daemon and nothing polls.
assert_eq "$(borg tx list)" "no open transactions" "and is gone once it has been idle too long"

# **The error is the whole point.** "Unknown transaction" tells a client it made a mistake; this
# tells it what happened and, by naming the timeout, what to do about it.
assert_rejected "expired after 2 seconds idle" \
    "touching a reaped transaction says it expired, not that it never existed" \
    -- borg tx commit --tx "$abandoned"
assert_rejected "expired after 2 seconds idle" "on a read too" \
    -- borg tx get --tx "$abandoned" 'Company#1.name'

assert_eq "$(borg get 'Company#1.name' --value)" "Acme Corp" \
    "and what it was going to write never lands"

# A handle nobody ever issued is a different fact, and says so.
assert_rejected "unknown transaction" "an invented handle is still an invented handle" \
    -- borg tx get --tx tx-9999 'Company#1.name'

# --- Branches are the durable form ----------------------------------------------------------------------

# Same shape of work, said the other way, and nothing sweeps it. This is the distinction the timeout
# forces a client to make, and it is a better question than "how long is your timeout".
borg branch fork main --name long-running >/dev/null
borg --branch long-running set 'Company#1.name' Considered >/dev/null
sleep 3
borg tx list >/dev/null
assert_eq "$(borg get 'Company#1.name' --value --branch long-running)" "Considered" \
    "a branch outlives any timeout, because a branch is not a transaction"
