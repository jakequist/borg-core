#!/usr/bin/env bash
# The failure model, end to end through the real binary. SPEC.md §14.
#
# A producer that throws is poisoned — scoped to the producer, never to the branch — and its cells
# report `broken` with lineage that says why. Recovery is pushing fixed code.
#
# Every assertion here is made from a **fresh process**, which is the whole point: the CLI is
# process-per-command, so a poisoning that lived only in the process that discovered it would be
# invisible to every command that followed, and the cells it broke would report ordinary lag.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

attempts() { [ -f ./attempts ] && wc -l < ./attempts | tr -d ' ' || echo 0; }

borg repo push "$HERE/repo" >/dev/null
borg set 'Company#1.headcount' 40 >/dev/null
assert_eq "$(borg get 'Company#1.risk' --value)" "low" "the working build derived its field"
assert_field "$(borg get 'Company#1.risk')" "state" "current" "and the cell is current"

# A bad deploy of the same producer: same name, same id, same schema, throwing code. A def push
# commits no data and so runs nothing, which is why the write below is what invokes it.
borg repo push "$HERE/repo-broken" >/dev/null
broke="$(borg set 'Company#1.headcount' 5 2>&1 >/dev/null)"
[ "$(attempts)" -gt 0 ] || fail "the new build ran at least once before it was judged broken"
pass "the new build ran before it was judged broken"
assert_contains "$broke" "score is now broken" \
    "the write that discovered the failure is the one that reports it"
assert_contains "$broke" "score exploded" "and says what the producer said"

# **The claim §14 makes, read back from a process that never saw the failure.**
broken="$(borg get 'Company#1.risk')"
assert_field "$broken" "state" "broken" \
    "a poisoned producer's cells report broken from a fresh process, not stale"
assert_eq "$(borg get 'Company#1.risk' --value)" "low" \
    "and the last good value is still served — a broken cell is labelled, not withheld"

# Scoped to the producer. Source data is untouched and everything else keeps working (§14).
assert_field "$(borg get 'Company#1.headcount')" "state" "current" \
    "source data is unaffected: main does not break because a pipeline did"

assert_contains "$(borg explain 'Company#1.risk')" "score exploded" \
    "lineage explains why the cell is broken"
assert_contains "$(borg derive status)" "broken      score" \
    "and the branch's derivation status names it too"

# `borg derive` does not blindly retry. It says whose fault it is, in a sentence with the error in
# it, and runs nothing.
note="$(borg derive 2>&1 >/dev/null)"
assert_contains "$note" "score" "borg derive names the producer it is skipping"
assert_contains "$note" "score exploded" "and repeats the error rather than making you go looking"

before="$(attempts)"
borg derive >/dev/null 2>&1
assert_eq "$(attempts)" "$before" "a broken producer is skipped, not run again"

borg derive --retry-broken >/dev/null 2>&1
[ "$(attempts)" -gt "$before" ] || fail "--retry-broken runs it again"
pass "--retry-broken runs it again"
assert_field "$(borg get 'Company#1.risk')" "state" "broken" \
    "and a retry that fails the same way leaves it broken"

# **Recovery is a def push.** Pushing the working build gives the producer a new ClientVersion, and
# the poison names the version it was recorded against — so nothing has to remember to clear it.
borg repo push "$HERE/repo" >/dev/null
assert_eq "$(borg derive 2>&1 >/dev/null)" "" "nothing is skipped once fixed code is pushed"
assert_field "$(borg get 'Company#1.risk')" "state" "current" \
    "the cell recovers without anyone clearing anything by hand"
assert_eq "$(borg get 'Company#1.risk' --value)" "high" \
    "and it reflects the input that landed while the producer was broken"
