#!/usr/bin/env bash
# **S13 — a migration and a concurrent client write.** SPEC-DRAFT §9, SPEC.md §9.3, §12, §16.5.
#
# Migrations work (`080-migration`). Concurrency works (`140`, `160`). They had never met, and this
# is the pair that broke: a migration is the **only** producer whose output shares a `CellRef` with a
# cell clients write. `founded` at the old def-version is source data a client owns; `founded` at the
# new one is the migration's output. Same cell, two versions — and every guard question in the system
# is asked about a `CellRef`.
#
# Two questions, and the scenario is in two halves because they are asked from opposite sides:
#
#   1. Does the **migration's round** guard against a client write it could not see? A round forks at
#      the source layer it settles, so a client write above that fork point is invisible to it — and
#      must therefore stop its merge, or the migration publishes a projection of a value that is no
#      longer there.
#   2. Does the **client's transaction** guard correctly when the migration's merge lands first? A
#      migration's output is derived, and guarding a shadow is meaningless (§12.4) — so it must *not*
#      conflict. What must conflict is another client moving the underlying value, even at a
#      different def-version than the one this client read.
#
# **What this found.** The round's guard set is *what it read minus what the round wrote*, and the
# subtraction was keyed on `CellRef`. A migration reads `founded` and writes `founded`, so its guard
# on the cell it migrates from was subtracted away and a stale migration round landed happily. In the
# merge order the CLI produces it merely wrote a value the next round overwrote; with two rounds in
# flight it was a lost update that no later round would ever correct. `ROADMAP.md` has the entry and
# `crates/borg-engine/tests/composition.rs` has the interleaving; below is the half a single process
# can show honestly.
#
# **Why the interleaving here is a backlog rather than a race.** The CLI is process-per-command and
# layer ids come from a process-local sequencer (§17.2), so two `borg` processes against one store
# would mint the same id. What a single process *can* express exactly is the state the guard actually
# asks about: a round forked at `L` whose trunk has already moved past `L`. Committing both source
# layers before deriving produces precisely that — the round settling the earlier layer merges into a
# trunk that has since been written, which is what "mid-round" means to a guard.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# Paused throughout, on purpose: every claim below is about *which round* did what, and a branch that
# catches itself up after each write would settle each layer before the next one could contend with
# it — which is the one interleaving that cannot fail.
borg derive pause >/dev/null

borg repo push "$HERE/repo-v1" >/dev/null
v1="$(borg def version)"
borg set 'Company#1.founded' 1999-06-01 >/dev/null
old_write="$(borg layer head)"

# The schema moves, and appoints the two scripts that bridge it. Nothing has run yet.
borg repo push "$HERE/repo-v2" >/dev/null
assert_contains "$(borg def show Company)" "Int" "the schema moved: founded is now a year"
assert_contains "$(borg producer list)" "founded_up" "with a migration to get there"

# --- 1. the migration's round guards against a write it could not see ------------------------------

# A client authored against v1 writes the old shape again — which is not a legacy curiosity but §5.4's
# promise, and therefore the ordinary way a value under migration moves. This lands *above* the layer
# the first round will fork at.
borg set 'Company#1.founded' 1998-07-02 --client-version "$v1" >/dev/null
new_write="$(borg layer head)"

borg derive >/dev/null

# The observable claim, and it is about layers rather than values: **no derived layer on main reflects
# the superseded source layer.** A round that read `founded` as it stood at `L` and merged into a
# trunk where `founded` had since moved is exactly the stale round S8 forbids, and the record it
# leaves behind is a derived layer stamped `reflects L`. Asserting on the value alone would not
# distinguish it — the later round's output sits above the earlier one's either way, and the read
# would say `1998` whether or not the stale round landed underneath it.
layers="$(borg layer list)"
if printf '%s\n' "$layers" | grep -q "reflects ${old_write}$"; then
    fail "a round that forked at $old_write landed after $new_write moved the cell it read
      $layers"
fi
pass "the round settling $old_write was rejected by its own guard on the cell it migrated from"
assert_contains "$layers" "reflects ${new_write}" \
    "and the round that forked above the client's write landed instead"

assert_eq "$(borg get 'Company#1.founded' --value)" "1998" \
    "so the migrated view is a projection of the value that is actually there"
assert_field "$(borg get 'Company#1.founded')" "state" "current" \
    "and it says current, because it is"
assert_eq "$(borg get 'Company#1.founded' --value --client-version "$v1")" "1998-07-02" \
    "while the author's own value is untouched, as a migration always leaves it (§5.3)"

# The dropped invocation is not lost work — its edges were recorded on the trunk when it ran, and the
# layer that failed its guard is a source layer some later round settles. That round has already run
# here, which is why nothing is outstanding.
assert_eq "$(borg derive --count)" "0" \
    "the rejected round cost a re-run, not the value: nothing is left outstanding"

# --- 2. a migration's merge landing under an open transaction is not a conflict --------------------

# The mirror image, from the client's side. A transaction reads the *migrated* view of a cell, and the
# migration that produces it merges into the parent while the transaction is still open.
borg set 'Company#2.founded' 1970-05-05 --client-version "$v1" >/dev/null

tx="$(borg tx begin)"
# Read before the migration has run: the new version is honestly reported as behind rather than
# invented (§10.4), and the read is recorded either way — absence is a legitimate thing to have acted
# on (§12.1).
assert_field "$(borg tx get 'Company#2.founded' --tx "$tx")" "state" "stale" \
    "a transaction reads the migrated view before anything has materialized it"

borg derive >/dev/null
assert_eq "$(borg get 'Company#2.founded' --value)" "1970" \
    "and the migration lands on the parent while that transaction is open"

borg tx set 'Company#2.rating' 5 --tx "$tx" >/dev/null
borg tx commit --tx "$tx" >/dev/null
assert_eq "$(borg get 'Company#2.rating' --value)" "5" \
    "the transaction commits anyway: what landed under it was derived, and guarding a shadow is \
meaningless (§12.4)"

# --- 3. …but another client moving the same cell is, at either def-version -------------------------

# The half that would be silently deleted if a guard were keyed on the version as well as the cell.
# This transaction read `founded` at the *new* version — a migration's output — and the write that
# must stop it is at the *old* one, which is the only version a client can write. A guard is a
# question about a cell, and this is why.
borg set 'Company#3.founded' 1980-02-02 --client-version "$v1" >/dev/null
borg derive >/dev/null

tx="$(borg tx begin)"
assert_eq "$(borg tx get 'Company#3.founded' --tx "$tx" --value)" "1980" \
    "a second transaction reads the migrated view, this time materialized"
borg tx set 'Company#3.rating' 9 --tx "$tx" >/dev/null

borg set 'Company#3.founded' 1981-03-03 --client-version "$v1" >/dev/null

assert_rejected "guard on" \
    "and is rejected when another client moves the cell underneath it, at the older version" \
    -- borg tx commit --tx "$tx"
assert_eq "$(borg get 'Company#3.rating' --value)" "" \
    "nothing of the rejected transaction landed"

# The store is still usable afterwards, which is the whole point of rejecting rather than wedging.
borg derive >/dev/null
assert_eq "$(borg get 'Company#3.founded' --value)" "1981" \
    "and the write that won is migrated like any other"
assert_eq "$(borg derive --count)" "0" "with nothing outstanding behind it"
