#!/usr/bin/env bash
# §18's first acceptance scenario, end to end through the real binary.
#
#   Start with a field. Fork. On the fork, change that field's type, supplying a migration. Old data
#   on the fork reads correctly *through the new lens*, while the parent is untouched. Then merge
#   def-only, and the parent's existing values migrate too.
#
# This is the thing the whole system was designed around: a schema change is a branchable, mergeable
# event in the log, and the values that predate it are not rewritten — they are *read through* a
# migration, in whichever direction the reader needs.
#
# Two ideas do all the work here and are worth naming before reading on:
#
#   * **Every actor carries a ClientVersion** (§5.4) — the def-layer its view was built from. Writes
#     are stored at their author's ClientVersion and never coerced, and both the *shape* a write must
#     fit and the *shape* a read comes back in are resolved at that version. `borg def version` prints
#     it; `--client-version` pins an older one, which is how a client authored before the schema
#     change is spelled on a command line.
#   * **A migration is just a producer** (§9.1) — a script in a repo, like a pipeline. `up` carries
#     values forward; `down` carries them back, and is what keeps old clients working.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# Auto-derivation is paused throughout, on both branches. Every step below is about *when* a
# migration has run and what a read says before it has, and a branch that catches itself up would
# answer those questions before they could be asked. Pausing is per branch and does not stop
# `borg derive`, so the story is the same one, stepped.
borg derive pause >/dev/null

# --- A field, and some data ------------------------------------------------------------------------

# `Company.founded` starts life as a String holding an ISO date. The repo is a repo of pure schema:
# no producers at all, which §17.4 says is legitimate.
borg repo push "$HERE/repo-v1" >/dev/null
v1="$(borg def version)"
assert_contains "$(borg def show Company)" "String" "founded starts out declared String"

borg set 'Company#1.founded' 1999-06-01 >/dev/null
assert_eq "$(borg get 'Company#1.founded' --value)" "1999-06-01" "and holds a date, as text"

# --- Fork, then change the field's type on the fork --------------------------------------------------

borg branch fork main --at "$(borg layer head)" --name feature >/dev/null

# The switch is per branch, and a fork does not inherit it — a fork is a new branch with its own
# operational settings, and pausing is not a fact in the log to be carried across.
borg derive pause --branch feature >/dev/null

# The same repo, one version on: `founded` is now an `Int` holding the year, and the field names the
# two scripts that bridge the change. `borg repo push` sees a declared field whose type has moved and
# turns that into a `MutateField` — the def change and the migrations that make it survivable land in
# **one def layer**, or neither lands.
borg --branch feature repo push "$HERE/repo-v2" >/dev/null

# A def-version *is* a layer id (§5.3), so the fork's schema visibly moved on and main's did not.
if [ "$(borg def version --branch feature)" = "$v1" ]; then
    fail "pushing a def mutation must move the branch's def-version"
fi
pass "a def-version is the def-layer that last moved the schema, and the fork's has"

assert_contains "$(borg def show Company --branch feature)" "Int" \
    "the fork's schema says founded is an Int"
assert_contains "$(borg producer list)" "founded_up" \
    "and the migration that gets it there is a producer like any other"

# Nothing has run yet, so the new version is not materialized. The read says so rather than
# inventing a value: there is a path from the old version to the new one, it just has not been
# walked (§10.4).
assert_field "$(borg get 'Company#1.founded' --branch feature)" "state" "stale" \
    "before deriving, the new view is honestly reported as behind"

borg derive --branch feature >/dev/null

# --- The fork reads the old data through the new type ------------------------------------------------

out="$(borg get 'Company#1.founded' --branch feature)"
assert_field "$out" "value" "1999" "old data reads through the new lens: the date became a year"
assert_field "$out" "origin" "derived" "and says plainly that it was computed, not written"
assert_contains "$out" "produced by" "attributed to the migration that produced it"
assert_field "$out" "state" "current" "with a watermark, like any other derived value"

# The value its author wrote is untouched — a migration adds a version, it does not rewrite one.
assert_eq "$(borg get 'Company#1.founded' --value --branch feature --client-version "$v1")" \
    "1999-06-01" "the original is still there, at the version it was written at"

# --- The parent is untouched ------------------------------------------------------------------------

assert_eq "$(borg def version)" "$v1" "main's schema has not moved"
assert_eq "$(borg get 'Company#1.founded' --value)" "1999-06-01" \
    "so main still reads a date, in the type it declared"
assert_field "$(borg get 'Company#1.founded')" "origin" "source" \
    "as ground truth, not as anything derived"

# --- Merge the schema change, def-only ---------------------------------------------------------------

borg branch merge feature --defs-only >/dev/null
assert_contains "$(borg def show Company)" "Int" "a def-only merge carries the type change to main"

# A merge replays the child's *source* layers only: the fork's derived values were computed from the
# fork's world and are wrong on main by construction (§13). So main has the new schema and nothing
# materialized at the new version yet — and the migrations to fix that came with it, because they
# were events in the same def layer.
assert_eq "$(borg get 'Company#1.founded' --value)" "" \
    "main's value is not yet materialized at the new version"
borg derive >/dev/null
assert_eq "$(borg get 'Company#1.founded' --value)" "1999" \
    "and now main's existing value reads through the new lens too"

# --- An older client keeps working ---------------------------------------------------------------

# This is §5.4's promise and the reason `down` exists. A client authored against v1 still writes the
# old shape — the write is validated against *its* def-view, not against the branch's — and still
# reads the old shape back.
borg set 'Company#3.founded' 1888-03-04 --client-version "$v1" >/dev/null
assert_eq "$(borg get 'Company#3.founded' --value --client-version "$v1")" "1888-03-04" \
    "an old client writes the old shape long after the schema moved"

borg derive >/dev/null
assert_eq "$(borg get 'Company#3.founded' --value)" "1888" \
    "and a new client sees it through the new lens"

# The other direction: a *new* client writes an Int, and the old client reads a date — because `down`
# ran. Nothing about the old client changed; the system met it where it was.
borg set 'Company#4.founded' 2020 >/dev/null
borg derive >/dev/null
assert_eq "$(borg get 'Company#4.founded' --value --client-version "$v1")" "2020-01-01" \
    "a new client's write reaches an old client through the down migration"

out="$(borg get 'Company#4.founded' --client-version "$v1")"
assert_field "$out" "origin" "derived" "the old client is told its view is a computed one"

# `up` and `down` are two projections of one value, not a cycle. Neither is triggered by the other's
# output, so the world settles instead of ping-ponging until the cycle detector fires (§9.3, §16.6).
assert_eq "$(borg derive --count)" "0" "everything has settled — the two directions do not chase"

# --- Without a `down`, an old client is told it is broken --------------------------------------------

# A second repo, so the point is made on a field of its own. `Note.tag` moves String → Int with an
# `up` and no `down`: the def-push knowingly breaks clients on the old version, and §9.3 says the
# system's job is then to say so rather than to serve something plausible.
borg repo push "$HERE/repo-notes-v1" >/dev/null
note_v1="$(borg def version)"
borg set 'Note#1.tag' 42 >/dev/null

borg repo push "$HERE/repo-notes-v2" >/dev/null
borg set 'Note#2.tag' 7 >/dev/null
borg derive >/dev/null

assert_eq "$(borg get 'Note#1.tag' --value)" "42" "up still carries the old values forward"
assert_eq "$(borg get 'Note#1.tag' --value --client-version "$note_v1")" "42" \
    "and what the old client itself wrote is still readable, at its own version"

# But a value written *after* the change cannot be shown to the old client at all: there is no path
# back to its version, and there is no honest value to serve.
out="$(borg get 'Note#2.tag' --client-version "$note_v1")"
assert_field "$out" "state" "broken" \
    "a new value is unreachable from a version with no down migration, and is reported broken"
assert_field "$out" "value" "<absent>" "not silently wrong, and not silently empty either"
