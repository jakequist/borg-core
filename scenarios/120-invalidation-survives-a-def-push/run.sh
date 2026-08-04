#!/usr/bin/env bash
# **Invalidation must survive a def push.** The severe half of the same bug as `110`, and the half
# that has no symptom.
#
# A read-set entry is a `CellAt` — cell *plus* def-version (§9.4) — and the dependency index keys on
# it. So if a source write's stored version can be moved by a schema change the field was not part
# of, then after any def push the writes a client makes no longer match the dependencies a producer
# recorded, and the producer is never woken again. Nothing fails. No error, no `stale`, no lagging
# watermark: the derived value simply stops following its input, forever, and every label on it goes
# on saying it is current.
#
# That is what this scenario is for. It writes an input, lets it derive, declares an unrelated
# field, writes the input again — and then asks the only question that can catch it: does the output
# still equal the input?
#
# Note what this is *not*. S1 (`100-watermark-truth`) replays a value at the watermark it claims and
# compares. It cannot see this: the producer was authored at the old def-version and genuinely reads
# the old world, so the replay faithfully reproduces the frozen value and the two agree. The result
# is self-consistent and wrong, which is precisely the class a replay cannot measure — so this
# scenario compares the derived cell against the *source cell it is a copy of* instead.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

borg repo push "$HERE/repo" >/dev/null
assert_contains "$(borg producer list)" "score" "the pipeline is registered"

borg set 'Company#1.arr' 900 >/dev/null
assert_eq "$(borg get 'Company#1.score' --value)" "900" \
    "the pipeline follows its input, before anything else happens"

# The unrelated schema change. A second repo declaring a second field on the same struct: it says
# nothing about `arr`, nothing about `score`, and appoints no migration because nothing changed
# shape. The branch's def-version moves; neither field's does.
borg def push "$HERE/city.json" >/dev/null
assert_contains "$(borg def show Company)" "city" "an unrelated field is declared"

borg set 'Company#1.arr' 950 >/dev/null
borg derive >/dev/null

# The whole scenario is this one comparison. `arr` is source data and always tells the truth;
# `score` is a copy of it. They disagree only if the write never invalidated the invocation that
# read it.
arr="$(borg get 'Company#1.arr' --value)"
score="$(borg get 'Company#1.score' --value)"
assert_eq "$arr" "950" "the new source value is stored"
assert_eq "$score" "$arr" \
    "and the derived copy followed it across a def push — the dependency still matches"

# The label has to be honest too. A `current` claim on a value that has stopped tracking its input
# is worse than the wrong value, because it is the wrong value plus a reason to trust it.
assert_field "$(borg get 'Company#1.score')" "state" "current" \
    "and says so: a value that tracks its input is current, not merely unvalidated"

# Once more, so this is a property and not an off-by-one. A second push, a third value.
borg def push "$HERE/city.json" >/dev/null
borg set 'Company#1.arr' 1000 >/dev/null
borg derive >/dev/null
assert_eq "$(borg get 'Company#1.score' --value)" "1000" \
    "and keeps following it across every push after that"

# The other direction: a *deletion* must still reach the producer too. A tombstone is a write like
# any other (§8.1), and it travels the same read-set edge that has just been proved intact. This
# pipeline copies its input, so it copies the tombstone — what is being asserted is that the run
# happened at all, and `produced by` is what says it was this producer's doing rather than a
# cascade.
borg delete 'Company#1.arr' >/dev/null
borg derive >/dev/null
out="$(borg get 'Company#1.score')"
assert_field "$out" "state" "tombstoned" \
    "and a tombstone invalidates across a def push as well as a value does"
assert_contains "$out" "produced by" \
    "by re-running the producer, not by anything cascading around it"
