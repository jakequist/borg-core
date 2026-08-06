#!/usr/bin/env bash
# **Changing a pipeline's code invalidates its output.** SPEC.md §9.2, FRICTION.md #17.
#
# §9.2 has always said that pushing new pipeline source moves the producer's ClientVersion and
# invalidates all of its prior output. The machinery to do that was all present — a ClientVersion is
# a def-layer, a producer standing below its own is handed its whole source buffer, poisonings expire
# against it — and none of it ever fired, because nothing *told* it. `borg repo push` is a diff
# (ROADMAP: *a schema change is a diff, not an instruction*), the diff compared name, source buffer
# and declared fields, and an edit to a pipeline body touches none of those. So an edited pipeline
# diffed as unchanged, no def event was emitted, and the store went on serving values computed by
# code that no longer existed — labelled `current`, beside values from the new code, with nothing in
# the envelope to tell the two apart.
#
# The fix is one opaque string per producer whose only contract is that it changes when the code
# changes, carried in the `PushProducer` event because *which program this is* belongs to a
# producer's definition. This scenario is the measurement FRICTION #17 made, run again.
#
# It also asserts the other half, which is not a bonus but the same property from the other side: a
# push that changes **nothing** must emit nothing. A mechanism that invalidates on every push would
# pass every assertion below and be useless, because `O(all entities)` per push is what a dev loop
# does forty times an hour.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../ts-lib.sh"

need_node "290 needs node and pnpm to edit a real pipeline; the diff itself is covered by" \
          "borg-cli's producer_change tests and borg-engine's tests/def_events.rs, which need neither."
build_sdk

source "$HERE/../lib.sh"
setup

cp -r "$HERE/repo" "$WORK/"
link_sdk "$WORK/repo"

PIPELINE="$WORK/repo/pipelines/display_name.ts"

# fallback <text> — rewrite the one literal in the pipeline body, and nothing else.
fallback() {
    sed -i "s/\"(\(no name\|nobody\))\"/\"($1)\"/" "$PIPELINE"
}

# landed <cell> — the layer whose membership carried this value here. A recompute writes a new
# event on a new layer, so this moving is what "it ran again" looks like from outside.
landed() { field "$(borg get "$1")" "landed at"; }

# ── A contact, derived under the first build ───────────────────────────────────────────────────────

borg repo push "$WORK/repo" >/dev/null
v1="$(borg def version)"

borg set 'Contact#1.firstName' Ada >/dev/null
borg set 'Contact#2.notes' 'met at a conference' >/dev/null

assert_eq "$(borg get 'Contact#1.displayName' --value)" "Ada" \
    "the pipeline derived a name for the contact that has one"
assert_eq "$(borg get 'Contact#2.displayName' --value)" "(no name)" \
    "and the fallback for the contact that does not"

named_at="$(landed 'Contact#1.displayName')"
unnamed_at="$(landed 'Contact#2.displayName')"

# ── Pushing the same code again is a no-op ─────────────────────────────────────────────────────────

# The property that makes the rest affordable. A def-version *is* a layer id (§5.3), so an unchanged
# push emitting a def event would be visible right here — and would mean every boot of a dev script
# walking the schema forward and recomputing everything derivable.
borg repo push "$WORK/repo" >/dev/null
assert_eq "$(borg def version)" "$v1" \
    "pushing an unedited repo creates no def layer at all — the def-version has not moved"
assert_eq "$(landed 'Contact#1.displayName')" "$named_at" \
    "and nothing was recomputed, because nothing said anything had changed"

assert_contains "$(borg repo push "$WORK/repo")" "unchanged" \
    "and the push says so, rather than reporting a schema nobody pushed"

# ── Edit only the body ─────────────────────────────────────────────────────────────────────────────

# One string literal. No field moves, no name moves, no `writes` moves — `borg def show` is identical
# either side of this, which is exactly why the diff could not see it.
before_schema="$(borg def show Contact)"
fallback nobody
borg repo push "$WORK/repo" >/dev/null

assert_eq "$(borg def show Contact)" "$before_schema" \
    "the schema is untouched: a fingerprint change is not a schema change, and demands no migration"
if [ "$(borg def version)" = "$v1" ]; then
    fail "editing a pipeline body must land a def layer — the producer's ClientVersion is that layer (§9.2)"
fi
pass "editing a pipeline body lands a def layer, so the producer's ClientVersion moves"
v2="$(borg def version)"

# **The assertion FRICTION #17 exists for.** The old value was computed by code that no longer
# exists; serving it labelled `current` is the watermark lie S1 measures, arrived at by a route S1
# cannot see (a fork at `reflects` recomputes with today's code and disagrees).
assert_eq "$(borg get 'Contact#2.displayName' --value)" "(nobody)" \
    "the value the old code produced was recomputed under the new code"

# The other half, and the one that is easy to miss: a value that comes out the *same* has still been
# produced by a different program, and must have been re-derived rather than left alone. Nothing
# about `Ada` changes when the fallback does — so the value proves nothing and the layer proves
# everything.
if [ "$(landed 'Contact#1.displayName')" = "$named_at" ]; then
    fail "a value whose text did not change was left standing: it is still the old build's output"
fi
pass "and so was every other value the producer owns, including the ones whose text did not change"
if [ "$(landed 'Contact#2.displayName')" = "$unnamed_at" ]; then
    fail "the recomputed value landed on the layer it was already on"
fi
pass "each of them landed on a new layer, which is what a recompute looks like from outside"

assert_field "$(borg get 'Contact#1.displayName')" "state" "current" \
    "and the label is honest again: current means computed by the code that is deployed"

# ── Two contacts, one program ──────────────────────────────────────────────────────────────────────

# FRICTION #17's actual measurement. A contact created *after* the edit derived under the new body;
# one created before derived under the old. The store used to hold both, both labelled `current`,
# with `state`, `origin` and `by` identical — same struct, same field, same producer id, two
# different programs, and nothing in the envelope to tell them apart.
borg set 'Contact#3.notes' 'no name either' >/dev/null
assert_eq "$(borg get 'Contact#3.displayName' --value)" "$(borg get 'Contact#2.displayName' --value)" \
    "a contact created after the edit and one created before it now agree"

# And the reverse edit, so this is a property rather than a direction.
fallback 'no name'
borg repo push "$WORK/repo" >/dev/null
assert_eq "$(borg get 'Contact#2.displayName' --value)" "(no name)" \
    "reverting the body carries every value back with it"
assert_eq "$(borg get 'Contact#3.displayName' --value)" "(no name)" \
    "…including the ones written while the other build was deployed"

# Reverting is a code change like any other, so it lands its own layer. Nothing tries to be clever
# about a fingerprint the branch has seen before: a ClientVersion is the layer a producer was pushed
# at, and the producer has been pushed again.
if [ "$(borg def version)" = "$v2" ]; then
    fail "the revert should have landed a def layer of its own"
fi
pass "and it did so through a def layer of its own, because it is a push like any other"

# ── The invalidation is scoped to the producer that moved ──────────────────────────────────────────

# A recompute costs `O(that producer's source buffer)` and must not cost more. Nothing else in this
# repo moved, so nothing else is re-derived — and the ordinary field-granular invalidation the rest
# of the suite tests is untouched by any of this.
borg derive pause >/dev/null
borg set 'Contact#1.notes' 'a field the pipeline never reads' >/dev/null
assert_derives 0 "a write to a field the pipeline never read still runs nothing"
borg derive resume >/dev/null

# ── A push that changes the code lands exactly once ────────────────────────────────────────────────

# The dev loop, which is what FRICTION #2 is about: push, push, push. Only the first of these had
# anything to say.
settled="$(borg def version)"
borg repo push "$WORK/repo" >/dev/null
borg repo push "$WORK/repo" >/dev/null
assert_eq "$(borg def version)" "$settled" \
    "two further pushes of the same code emit nothing between them"
assert_derives 0 "and leave the branch a fixpoint"
