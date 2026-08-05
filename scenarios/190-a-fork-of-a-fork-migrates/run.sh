#!/usr/bin/env bash
# **S15 — a second-order fork with a migration.** SPEC-DRAFT §9, SPEC.md §7.4, §9.3, §13.
#
# `080-migration` proves a migration works across **one** fork. Branch chains deeper than one fork
# have been exercised for definitions (`070-branch-visibility`) and never for migrations, and the two
# meet in a place worth checking: a migration is a producer mapping over a *buffer*, and on a
# grandchild that buffer is assembled from three branches. The values it must carry forward were
# written by three different authors at three different depths, and none of those authors is on the
# branch doing the carrying.
#
# The claims, in the order the scenario makes them:
#
#   * A migration pushed on a fork-of-a-fork migrates data inherited through **both** levels — the
#     grandparent's value and the parent's, neither of which is on the branch the migration runs on.
#   * **Both ancestors stay untouched.** Not merely "the parent"; a def push two levels down must not
#     be visible one level up, and a migration's output must not leak into a branch that never asked
#     for the schema change. A fork's read path bounds it at the fork point, so this is a claim about
#     which direction data flows, and it is the claim a chain of length two can falsify and a chain of
#     length one cannot.
#   * **Merging inward one level at a time works at each step**, and each step moves exactly one
#     branch. The schema arrives; the values follow when a round runs; the branch above is unmoved
#     until its own turn.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# Paused everywhere, on every branch as it is created. Each step below is a claim about *when* a
# migration has run and on which branch, and a branch that catches itself up would answer before the
# question could be asked. Pausing is per branch and a fork does not inherit it, because it is not a
# fact in the log.
borg derive pause >/dev/null

# --- three branches, three authors ------------------------------------------------------------------

borg repo push "$HERE/repo-v1" >/dev/null
v1="$(borg def version)"
borg set 'Company#1.founded' 1999-06-01 >/dev/null

borg branch fork main --at "$(borg layer head)" --name child >/dev/null
borg derive pause --branch child >/dev/null
borg --branch child set 'Company#2.founded' 1985-04-04 >/dev/null

borg branch fork child --at "$(borg --branch child layer head)" --name grandchild >/dev/null
borg derive pause --branch grandchild >/dev/null

assert_eq "$(borg get 'Company#1.founded' --value --branch grandchild)" "1999-06-01" \
    "the grandchild inherits the root's value through two forks"
assert_eq "$(borg get 'Company#2.founded' --value --branch grandchild)" "1985-04-04" \
    "and its parent's through one"

# --- the schema change lands on the grandchild only ---------------------------------------------------

borg --branch grandchild repo push "$HERE/repo-v2" >/dev/null
assert_contains "$(borg def show Company --branch grandchild)" "Int" \
    "the grandchild's schema says founded is a year"
assert_eq "$(borg def version --branch child)" "$v1" "its parent's has not moved"
assert_eq "$(borg def version)" "$v1" "nor has the root's"

borg derive --branch grandchild >/dev/null

# The interesting one. Both of these values live on *other branches* — one two levels up, one one
# level up — and the migration wrote neither of them, it wrote a new record at a new def-version on
# the branch it ran on. That is what "a migration adds a version, it does not rewrite one" means once
# there is more than one branch to say it about (§5.3, §7.4).
assert_eq "$(borg get 'Company#1.founded' --value --branch grandchild)" "1999" \
    "the grandchild migrates a value it inherited from its grandparent"
assert_eq "$(borg get 'Company#2.founded' --value --branch grandchild)" "1985" \
    "and one it inherited from its parent"
assert_field "$(borg get 'Company#1.founded' --branch grandchild)" "origin" "derived" \
    "both reported as computed rather than written"
assert_eq "$(borg get 'Company#1.founded' --value --branch grandchild --client-version "$v1")" \
    "1999-06-01" "while an old client on the same branch still reads the shape it knows"

# --- both ancestors are untouched ---------------------------------------------------------------------

assert_eq "$(borg get 'Company#1.founded' --value)" "1999-06-01" \
    "the root still reads a date, in the type it declared"
assert_field "$(borg get 'Company#1.founded')" "origin" "source" \
    "as ground truth, with nothing derived over it"
assert_eq "$(borg get 'Company#2.founded' --value --branch child)" "1985-04-04" \
    "and so does the branch in the middle"
assert_field "$(borg get 'Company#2.founded' --branch child)" "origin" "source" \
    "which never asked for the schema change and did not get it"

# A fork's derived output is on the fork. Nothing on either ancestor was written by the migration —
# which is what makes reading up the chain safe, and is why `--defs-only` below has anything to do.
if borg layer list | grep -q "derived by"; then
    fail "the root has derived layers on it, and only the grandchild ever ran a producer
      $(borg layer list)"
fi
pass "no derived layer reached either ancestor"

# --- merge inward, one level at a time -----------------------------------------------------------------

borg branch merge grandchild --defs-only >/dev/null
assert_contains "$(borg def show Company --branch child)" "Int" \
    "a def-only merge carries the type change up one level"
assert_eq "$(borg def version)" "$v1" \
    "and exactly one level: the root's schema is still where it was"

# A merge replays the child's *source* layers only — the grandchild's derived values were computed
# from the grandchild's world (§13) — so the parent has the new schema and nothing materialized at it.
assert_eq "$(borg get 'Company#2.founded' --value --branch child)" "" \
    "the parent has the new schema and nothing yet at the new version"
borg derive --branch child >/dev/null
assert_eq "$(borg get 'Company#2.founded' --value --branch child)" "1985" \
    "and its own value migrates once a round runs there"
assert_eq "$(borg get 'Company#1.founded' --value --branch child)" "1999" \
    "including the one it inherited from the root, which is now two migrations of one value on \
two branches, neither of which rewrote it"
assert_eq "$(borg get 'Company#1.founded' --value)" "1999-06-01" \
    "the root, still untouched, one merge later"

borg branch merge child --defs-only >/dev/null
assert_contains "$(borg def show Company)" "Int" "the second merge carries it the rest of the way"
borg derive >/dev/null
assert_eq "$(borg get 'Company#1.founded' --value)" "1999" \
    "and the root's own value reads through the new lens at last"
assert_eq "$(borg get 'Company#1.founded' --value --client-version "$v1")" "1999-06-01" \
    "with the value its author wrote still underneath it"

# `Company#2` was authored on the child. A def-only merge carries definitions and no data, so it is
# not on the root — and asserting that is what keeps `--defs-only` from quietly meaning `--all`.
assert_eq "$(borg get 'Company#2.founded' --value)" "" \
    "and the data the middle branch authored did not cross with its schema"

assert_eq "$(borg derive --count)" "0" "the root settles"
assert_eq "$(borg derive --count --branch child)" "0" "so does the branch in the middle"
assert_eq "$(borg derive --count --branch grandchild)" "0" "and so does the one that started it"
