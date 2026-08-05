#!/usr/bin/env bash
# The client controls over freshness. SPEC.md §10.5.
#
# §10's claim is that derived data lags *and says so*, which is only half a design unless the client
# has something to do about it. There are three things:
#
#   * a **freshness requirement** per read — `any`, `validated` or `current`. Only the last one
#     computes, and it computes at the call site. That is what makes lazy materialization a per-read
#     client mode rather than a system architecture: whoever needs a fresh answer pays for it, and
#     everyone else takes the lag.
#   * **awaiting the frontier** — `borg frontier reaches <layer>`, which is read-after-write
#     consistency without making the system synchronous.
#   * **two consistency modes** — the ragged head, where every field is as new as it happens to be,
#     and the settled frontier, where everything visible agrees with everything else visible.
#
# The pause switch is here too, because a paused branch is the only reliable way to hold a system
# still enough to look at — and because `borg derive` going on working while paused is what makes
# the switch usable in an emergency rather than a foot-gun.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# The same bash pipeline as 030: `is_investible` is true when the website ends `.ai` *and* the
# headcount is over ten.
borg repo push "$HERE"/../030-shell-pipeline/repo

# --- Derivation runs without being asked -----------------------------------------------------------

borg set 'Company#1.website' acme.ai >/dev/null
landed="$(borg set 'Company#1.headcount' 40)"

# `borg set` prints the layer the write landed in, which is exactly the layer to await. Nothing has
# to be slept on: the branch has already incorporated it by the time the write returns.
assert_contains "$(borg frontier reaches "$landed")" "reached" \
    "read-after-write: the frontier has already reached the layer the write landed in"
assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "and the pipeline ran without anybody asking it to"

# --- Stop the automation, and make one cell stale --------------------------------------------------

borg derive pause >/dev/null
stale_at="$(borg set 'Company#1.headcount' 3)"

assert_fails "a paused branch stops advancing, and awaiting its newest layer fails rather than hangs" \
    -- borg frontier reaches "$stale_at"

# --- Three requirements, one stale cell ------------------------------------------------------------

before="$(borg layer head)"

any="$(borg get 'Company#1.is_investible' --freshness any)"
assert_field "$any" "value" "true" "\`any\` serves what is stored"
assert_field "$any" "state" "unvalidated" "…and does not even look at whether it still holds"

validated="$(borg get 'Company#1.is_investible' --freshness validated)"
assert_field "$validated" "value" "true" "\`validated\` serves the same stored value"
assert_field "$validated" "state" "stale" \
    "…but walks the read-set and reports that an input moved"

# Neither of them ran user code, and the log is the proof: a producer run is a committed layer.
assert_eq "$(borg layer head)" "$before" \
    "neither cheap mode ran a producer — no layer was committed"

current="$(borg get 'Company#1.is_investible' --freshness current)"
assert_field "$current" "value" "false" \
    "\`current\` is the only one that computes, and the answer follows the input that moved"
assert_field "$current" "state" "current" "reported as current, because now it is"
if [ "$(borg layer head)" = "$before" ]; then
    fail "computing inline must commit the value it computed"
fi
pass "and it committed a layer to do it, on a branch whose automation is paused"

# One cell computed on demand is not a producer that has caught up. The watermark is a claim about
# *all* of a producer's output, so an inline computation deliberately leaves it where it was — which
# is also what makes the work self-healing rather than silently skipped.
assert_fails "computing one cell inline does not claim the producer has caught up" \
    -- borg frontier reaches "$stale_at"

# --- `borg derive` still works while paused ---------------------------------------------------------

assert_derives 1 \
    "pause means do not auto-derive, not refuse to derive: the round still runs when asked"
assert_contains "$(borg frontier reaches "$stale_at")" "reached" \
    "and a round, unlike an inline computation, does advance the frontier"

# --- Ragged head versus settled frontier -------------------------------------------------------------

# One more write nothing has incorporated. The branch is now genuinely in two minds.
borg set 'Company#1.headcount' 40 >/dev/null

assert_eq "$(borg get 'Company#1.headcount' --value)" "40" \
    "at the ragged head, the source field is the newest thing written"
ragged="$(borg get 'Company#1.is_investible')"
assert_field "$ragged" "value" "false" "beside a derived field computed from the previous one"
assert_field "$ragged" "state" "stale" "which is why it is labelled stale"

# The settled frontier is the highest layer through which *everything* is caught up. Reading there
# hides the write nothing has incorporated yet, so the pair agrees — a coherent snapshot, slightly in
# the past. A dashboard wants the reading above; a report wants this one.
assert_eq "$(borg get 'Company#1.headcount' --value --settled)" "3" \
    "at the settled frontier, the source field is the one the derived field was computed from"
settled="$(borg get 'Company#1.is_investible' --settled)"
assert_field "$settled" "value" "false" "the same derived value…"
assert_field "$settled" "state" "current" \
    "…and nothing in that snapshot is behind anything else in it"

# --- Resume -----------------------------------------------------------------------------------------

borg derive resume >/dev/null
landed="$(borg set 'Company#1.website' rival.ai)"
assert_contains "$(borg frontier reaches "$landed")" "reached" \
    "with automation resumed, the frontier keeps up with the writes again"
assert_eq "$(borg get 'Company#1.is_investible' --value --settled)" "true" \
    "and the settled read catches up to the ragged one, because there is no gap left"
