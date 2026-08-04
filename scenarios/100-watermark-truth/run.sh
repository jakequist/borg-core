#!/usr/bin/env bash
# S1, the claim that must not be false: **every watermark tells the truth.**
#
# §10.1 defines a watermark by what it promises — *"if you replayed the world at layer W, you would
# get exactly this value"* — and until now nothing checked the promise. Every read of derived data
# carries one, every scenario in this directory reads derived data, and all of them took the label at
# its word.
#
# So this one does the replay. For each derived cell: read the watermark it states, fork there,
# recompute from scratch on the fork, and compare. Identical, or the label is a lie.
#
# The bugs that shape this are ordering bugs — a backfill that failed one run in six, an inline
# computation advancing a watermark it had no right to, a round's ceiling admitting a layer belonging
# to another writer. None of them corrupt a value in a way a feature test would notice. All of them
# produce a value whose stated watermark is wrong, which is exactly and only what this measures.
#
# **Recomputing has to genuinely recompute.** A fork inherits its parent's derived layers by ancestry
# (§7.4), so forking and reading hands back the very value under test — a check that passes forever
# and proves nothing. `borg derive --rebuild` is what closes that: it rewinds the fork's watermarks
# so every producer owes its whole source buffer again, and the layers it writes shadow the ones it
# inherited. §6.3 licenses this — derived layers are "a cache that happens to live in the log", and
# recompute is always their fallback. Two assertions per cell keep it honest: the value must match,
# and the recomputed value's `written at` must name a layer **on the fork**, which is false the moment
# anything is inherited rather than recomputed.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# --- A store with enough shape to be worth checking -------------------------------------------------

# One producer reading another's output, plus a migration, so a recomputed value has to travel a real
# chain: `rating` = f(arr, band, founded), where `band` is a pipeline's output and `founded` becomes a
# migration's. A single-hop store would let an inheriting check pass by luck.
borg repo push "$HERE/repo-v1" >/dev/null
v1="$(borg def version)"
assert_contains "$(borg producer list)" "band" "the repo registered the head of the chain"
assert_contains "$(borg producer list)" "rating" "and the producer that reads its output"

# The schema move happens here, before any data exists: `founded` becomes an Int, and `founded_up` is
# appointed to carry the old shape forward. The whole story below therefore runs at one def-version,
# which is what keeps this scenario about watermarks — a def push landing *after* data is written
# raises a separate question, and mixing the two would leave a failure ambiguous.
borg repo push "$HERE/repo-v2" >/dev/null
assert_contains "$(borg def show Company)" "Int" "founded is declared an Int"
assert_contains "$(borg producer list)" "founded_up" "with a migration appointed to get values there"

# Auto-derivation is left running, unlike most scenarios here. Each write catches the branch up
# before the next lands, which is what an ordinary client does and what gives the cells below
# genuinely different histories — some recomputed several times over, some untouched since they were
# first written.
companies="1 2 3 4"
borg set 'Company#1.employees' 120 >/dev/null
borg set 'Company#1.arr' 900 >/dev/null
borg set 'Company#2.employees' 4 >/dev/null
borg set 'Company#2.arr' 20 >/dev/null
borg set 'Company#3.employees' 30 >/dev/null
borg set 'Company#3.arr' 150 >/dev/null
# Deliberately given no `employees`, so `band` derives `unknown` from an *absent* input. Absence is a
# tracked dependency (§9.4), and a recomputation that quietly loses one is the kind of thing this
# scenario exists to find.
borg set 'Company#4.arr' 5 >/dev/null

# `founded` is written by a client authored *before* the type changed — the ordinary case §5.4 exists
# for, and what gives `founded_up` something to migrate. A write is stored at its author's
# ClientVersion and never coerced (§5.4), so these are dates, and what a current reader sees is the
# migration's output rather than what was written.
borg set 'Company#1.founded' 1999-06-01 --client-version "$v1" >/dev/null
borg set 'Company#2.founded' 2015-01-20 --client-version "$v1" >/dev/null
borg set 'Company#3.founded' 2008-11-30 --client-version "$v1" >/dev/null
borg set 'Company#4.founded' 2021-07-07 --client-version "$v1" >/dev/null
borg derive >/dev/null

assert_field "$(borg get 'Company#1.founded')" "origin" "derived" \
    "an old client's date reads as a migration's output"
assert_eq "$(borg get 'Company#1.rating' --value)" "large/900/1999" \
    "and the chain runs through it: source data, a pipeline's output, and a migration's"
assert_eq "$(borg get 'Company#4.band' --value)" "unknown" \
    "an absent input is an input like any other"

# --- The check --------------------------------------------------------------------------------------

# Forks are cached per watermark: every cell claiming layer W is checked against one replay of W, and
# recomputing the same world twice would only cost time. `generation` is what a later section bumps to
# force a fresh replay after it has changed what the producers compute.
generation=0
replayed_already=" "

# replay_at <layer> — a branch holding the world at <layer>, with every derived value recomputed
# there rather than inherited. The name lands in $REPLAY_BRANCH; it cannot be printed, because the
# assertions below it print too.
replay_at() {
    local at="$1" name="replay-$1-g$generation"
    REPLAY_BRANCH="$name"
    if [ "$replayed_already" = "${replayed_already% $name *}" ]; then
        borg branch fork main --at "$at" --name "$name" >/dev/null
        # Paused before anything can catch it up by itself. `--rebuild` below is the only derivation
        # this branch is allowed, because a replay that also ran `catch_up` would be answering a
        # question about head rather than about <layer>.
        borg derive pause --branch "$name" >/dev/null
        borg derive --rebuild --branch "$name" >/dev/null

        # The fork is the same schema as main, so a value that differs cannot be differing through a
        # different lens. This holds because every watermark checked below sits above the def push;
        # were that ever to stop being true, the comparison would be comparing two shapes and this is
        # where it would say so.
        assert_eq "$(borg def version --branch "$name")" "$(borg def version)" \
            "the replay of $at reads at the same def-version as main"
        replayed_already="$replayed_already$name "
    fi
}

# check_watermark <cell> <freshness> — replay the cell's stated watermark and compare.
#
# Returns non-zero on a disagreement rather than calling `fail`, because one section below needs to
# assert that it *does* disagree. Everything it prints on failure is what a reader needs to act:
# which cell, which layer, and both values.
check_watermark() {
    local cell="$1" mode="$2" envelope value at branch replayed replayed_at

    envelope="$(borg get "$cell" --freshness "$mode")"
    value="$(field "$envelope" "value")"
    at="$(field "$envelope" "fresh as of")"
    if [ "$(field "$envelope" "origin")" != "derived" ]; then
        fail "$cell is not derived data — this check has nothing to say about source cells"
    fi
    if [ -z "$value" ] || [ "$value" = "<absent>" ]; then
        fail "$cell has no value, so the sweep would be checking nothing"
    fi

    replay_at "$at"
    branch="$REPLAY_BRANCH"
    replayed="$(borg get "$cell" --branch "$branch")"
    replayed_at="$(field "$replayed" "written at")"

    # **The check that the check is real.** A fork inherits its parent's derived layers, so a value
    # that was never recomputed here comes back written at one of *main's* layers. Only a layer
    # belonging to the fork proves the producer ran again.
    if ! borg layer list --branch "$branch" | grep -q "^$replayed_at[[:space:]]"; then
        fail "$cell was inherited from main, not recomputed: the replay reports it written at
      $replayed_at, which is not a layer on $branch — the check is not checking anything"
    fi

    if [ "$(field "$replayed" "value")" != "$value" ]; then
        printf '      watermark disagreement\n' >&2
        printf '      %s claims to be fresh as of %s\n' "$cell" "$at" >&2
        printf '      stored:    %s\n' "$value" >&2
        printf '      replayed:  %s\n' "$(field "$replayed" "value")" >&2
        return 1
    fi
    return 0
}

# --- Sweep one: the watermark a client is actually served -------------------------------------------

# `validated` is the default a client gets, and it reports the highest layer validation can justify:
# head where nothing in the read-set has moved since, and the layer the value was computed at
# otherwise (§10.2). Both are §10.1 claims and both are checked here, because both are *inferences* —
# validation walks the dependency index and runs no producer, so an edge the index never recorded is
# invisible to it and visible to nothing else. This is where an inference and a recomputation part
# company.
for n in $companies; do
    for f in band rating founded; do
        check_watermark "Company#$n.$f" validated || fail "a validated watermark was not true"
    done
done
pass "every settled cell recomputes to itself when the world is replayed at the layer it claims"

# --- Sweep two: a watermark that is behind, and must still be true -----------------------------------

# The harder half, and where an ordering bug would live. Derivation is paused and the world moves on,
# so the stored values are *stale* — and a stale value still makes the §10.1 claim, about the layer it
# names rather than about head. `--freshness any` is what asks for the stored label instead of a
# validated one (§10.5).
borg derive pause >/dev/null
borg set 'Company#2.employees' 500 >/dev/null
borg set 'Company#3.arr' 4000 >/dev/null

assert_field "$(borg get 'Company#2.band' --freshness any)" "state" "unvalidated" \
    "with the branch paused, a derived value is behind and says so"

for n in $companies; do
    for f in band rating founded; do
        check_watermark "Company#$n.$f" any || fail "a stated watermark was not true"
    done
done
pass "and a value the world has overtaken is still exactly what its own layer would produce"

# Catch main back up, so what follows is measured against a settled branch rather than a paused one.
borg derive >/dev/null
borg derive resume >/dev/null

# --- The instrument, checked against a value that is wrong ------------------------------------------

# A check for a property nothing has ever verified is worth nothing until it has been seen to fail. So
# the producer is changed underneath the values it produced: `band` now answers `BIG` where it
# answered `large`, and main is not re-derived. Every stored `band` therefore still claims a watermark
# it no longer satisfies — which is precisely the symptom of the ordering bugs above, arrived at by a
# route the scenario can drive.
#
# The edit is to the CLI's producer-implementation table (§9.2) — the sidecar that resolves a producer
# id to code — and not to the log, because *which code a producer is* was never log data. Deploying a
# different build of a pipeline is an ordinary operational event; the only unusual part is declining
# to re-derive afterwards.
#
# The entry is found by its command path rather than by producer id, and edited with `sed` rather than
# `jq`, because a producer id is a u64 and `jq` parses every number as a double: 1.6 renders this
# repo's `band` as `11104421596272648000`, which matches nothing and corrupts the file on the way back
# out.
impls="$WORK/borg.producers.json"
assert_contains "$(cat "$impls")" "repo-v2/pipelines/band.sh" \
    "the sidecar resolves band to the code the repo was pushed with"
sed "s|$HERE/repo-v2/pipelines/band.sh|$HERE/repo-drift/pipelines/band.sh|" \
    "$impls" > "$impls.new" && mv "$impls.new" "$impls"
assert_contains "$(borg producer list)" "repo-drift/pipelines/band.sh" \
    "and now resolves it to code that answers differently"

generation=1
if check_watermark 'Company#1.band' validated; then
    fail "the sweep passed a value that no longer replays to itself — it is not measuring anything"
fi
pass "a stored value that its own watermark no longer reproduces is caught, and named"

# The chain too, and this is the stronger statement of the two: `rating` never read `employees` and
# its own code did not change. It disagrees because the *input it inherited* was recomputed — which is
# the thing that has to work for a multi-hop check to mean anything.
if check_watermark 'Company#1.rating' validated; then
    fail "a downstream value read its inherited input instead of the recomputed one"
fi
pass "and a disagreement propagates down the chain rather than stopping at the producer that moved"

# Nothing here touched main. The lie was in the label, and the store is exactly as it was.
assert_eq "$(borg get 'Company#1.band' --value)" "large" \
    "checking a watermark leaves the branch it checked untouched"
