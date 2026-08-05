#!/usr/bin/env bash
# **S14 — a def-only merge landing while a round is owed under the older def.**
# `ROADMAP.md`, *Acceptance scenarios*; SPEC.md §5.3, §13, §16.5.
#
# Two features that had never met. A **def-only merge** carries a schema change from a fork into a
# trunk without any of the fork's data (§13). A **round** forks the trunk at the top of the range it
# settles and labels everything it produces with the top source layer of it. Put them together and
# the trunk is
# holding unsettled source data at the moment its schema moves underneath it — so the round that
# eventually runs computes under a def-view that arrived after the data it is settling was written,
# and the def layer the merge landed is itself the top of the range.
#
# What must not happen, and is what this is written to catch:
#
#   * **Output that claims a def-version it was not computed under.** A record is keyed by the
#     def-version of its own field (§5.3), and a merge that moves one field's version must not move
#     any other's — or the pipeline's earlier output becomes unreachable and every dependency
#     recorded against it silently stops matching, which is the failure `120` exists for, arriving
#     this time through a merge instead of a push.
#   * **A wedged branch.** Whatever the answer is, the trunk has to keep working afterwards.
#
# **The true mid-flight interleaving is not here**, and cannot be: the CLI is process-per-command and
# layer ids come from a process-local sequencer (§17.2), so a second `borg` process merging while a
# first one derives is a corruption rather than an interleaving.
# `crates/borg-engine/tests/composition.rs` holds a round open inside a producer and lands the merge
# underneath it. What one process expresses exactly is the durable half — the trunk owes a round, its
# schema moves, and then the round runs — which is the state the round's def-fold actually asks about.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

# Paused on both branches. Every claim below is about which round ran under which def-view, and a
# branch that catches itself up after each command answers those questions before they can be asked.
borg derive pause >/dev/null

# --- a trunk with a pipeline, settled --------------------------------------------------------------

borg repo push "$HERE/repo-v1" >/dev/null
v1="$(borg def version)"
borg set 'Company#1.founded' 1999-06-01 >/dev/null
borg derive >/dev/null
assert_eq "$(borg get 'Company#1.decade' --value)" "1990s" \
    "a pipeline derives the decade from the date, under the schema as it stands"

# --- a fork changes the type of the field that pipeline reads ---------------------------------------

borg branch fork main --at "$(borg layer head)" --name next >/dev/null
borg derive pause --branch next >/dev/null
borg --branch next repo push "$HERE/repo-v2" >/dev/null
assert_contains "$(borg def show Company --branch next)" "Int" \
    "the fork moves founded to a year, with the migrations that bridge it"
assert_contains "$(borg def show Company)" "String" \
    "and main's schema has not moved"

# --- meanwhile the trunk takes a write nothing has settled ------------------------------------------

# This is what makes the merge land *during* a round rather than between two: the layer is committed,
# the branch is paused, and the round that will settle it has not been opened yet.
borg set 'Company#2.founded' 1985-04-04 >/dev/null
owed="$(borg layer head)"
assert_eq "$(borg get 'Company#2.decade' --value)" "" \
    "the trunk is holding a source layer nothing has derived from"

borg branch merge next --defs-only >/dev/null
v2="$(borg def version)"
# The top of the range the owed round will settle: everything from the watermark to head, which is
# now this def layer (§16.5). Captured before the round runs, because that is what its output claims.
top="$(borg layer head)"
assert_contains "$(borg def show Company)" "Int" "the schema change crosses, def-only"
if [ "$v2" = "$v1" ]; then
    fail "a def-only merge must move the trunk's def-version"
fi
pass "and main's def-version moved with it"

# --- the owed round now runs under a schema that arrived after its data ------------------------------

borg derive >/dev/null

# The pipeline was authored against v1 and reads `founded` at *its own* def-version, which is still
# the ISO date it was written as — writes are never coerced, and a record is keyed by the version of
# its own field rather than by whoever last moved the schema (§5.3). Had the merge moved every field's
# version, this read would have found nothing and the pipeline would have computed from absence.
assert_eq "$(borg get 'Company#2.decade' --value)" "1980s" \
    "the pipeline still reads the field it depends on, at the version it recorded"
assert_eq "$(borg get 'Company#1.decade' --value)" "1990s" \
    "and its earlier output is still reachable, not orphaned at a version nobody asks for"

# The migration the merge appointed ran in the same round, over the same data.
assert_eq "$(borg get 'Company#2.founded' --value)" "1985" \
    "the migration that arrived with the merge materialized the new view"
assert_eq "$(borg get 'Company#2.founded' --value --client-version "$v1")" "1985-04-04" \
    "while the value its author wrote is untouched"

# Every derived layer on the trunk states a layer in the **source** stream, and states the same one:
# a round settles a range and emits one derived layer per producer reflecting the top of it (§6.3,
# §16.5). The owed write is inside that range, so what the round names is the range's top rather than
# the layer that dirtied it — which is why `$owed` is not what is looked for here.
layers="$(borg layer list)"
assert_contains "$layers" "reflects ${top}" \
    "the round that settled the owed write names the top of the range it settled"
if printf '%s\n' "$layers" | grep -E 'derived by .*reflects' | grep -qv 'reflects L[0-9]*$'; then
    fail "a derived layer reflects something that is not a layer id
      $layers"
fi
pass "and nothing derived claims a def layer as its watermark"

assert_derives 0 "the trunk settles rather than chasing itself"

# --- a second schema move, onto records that are already at the version it supersedes ----------------

# Now the sharp end of S14: the trunk's `founded` records were produced at `v2`, and a def-only merge
# makes `v3` current. Those records are labelled at a def-version that has just stopped being the
# newest — which is exactly the state the mid-round interleaving produces, reached here by a route one
# process can walk.
borg branch fork main --at "$(borg layer head)" --name later >/dev/null
borg derive pause --branch later >/dev/null
borg --branch later repo push "$HERE/repo-v3" >/dev/null
borg set 'Company#3.founded' 1975-09-09 --client-version "$v1" >/dev/null
borg branch merge later --defs-only >/dev/null
v3="$(borg def version)"
borg derive >/dev/null

# The old records did not move and did not lie. Each is still readable at the version it was filed
# at, which is the whole of what "never coerced" buys.
assert_eq "$(borg get 'Company#1.founded' --value --client-version "$v2")" "1999" \
    "a record produced at v2 is still exactly that, after v3 became current"
assert_eq "$(borg get 'Company#1.founded' --value --client-version "$v1")" "1999-06-01" \
    "and so is the source value underneath it"

# **The second step of the chain is reached by a plain catch-up.** `founded@v3`'s input is
# `founded@v2`, which nothing but a *derived* layer has ever written — and a derived layer opens no
# round of its own. While a round settled one source layer at a time, nothing triggered the second
# migration and `borg derive --rebuild` was the only way to it. A round settles the range
# `[watermark+1 … head]` (§16.5), so the derived layer the previous round merged is in its opening
# wave and triggers whoever reads what it wrote.
out="$(borg get 'Company#1.founded')"
assert_field "$out" "value" "1990s" \
    "a catch-up walks both steps of the chain: 1999-06-01 → 1999 → 1990s"
assert_field "$out" "state" "current" "and says current, because nothing is behind it"
assert_eq "$(borg get 'Company#3.founded' --value --client-version "$v3")" "1970s" \
    "including for the value written while the second change was still on the fork"
assert_derives 0 "the trunk settles rather than chasing itself"

# Not wedged, and the rebuild agrees with the catch-up rather than correcting it — which is the
# statement worth keeping now that both routes reach the same place. §6.3 licenses the comparison:
# derived layers are a cache, and recompute is always their fallback.
borg derive --rebuild >/dev/null
assert_eq "$(borg get 'Company#1.founded' --value)" "1990s" \
    "a rebuild reaches the same value the catch-up did"
assert_eq "$(borg get 'Company#1.decade' --value)" "1990s" \
    "and the pipeline that reads the oldest version of the field is unaffected by any of it"
assert_derives 0 "with the branch settled afterwards"
