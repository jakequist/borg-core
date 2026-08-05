#!/usr/bin/env bash
# **S16 — the determinism sweep.** `ROADMAP.md`, *Acceptance scenarios*; SPEC.md §9.6, §16.5.
#
# One workload with real contention in it, replayed from an empty store many times at high
# parallelism, asserting that the **settled result is byte-identical every run**. Nothing here
# asserts a schedule: not how many invocations ran, not which rounds were rejected, not what any
# layer is numbered. §9.6's whole licence is that scheduling policy cannot affect correctness, and
# the only honest way to check a licence like that is to vary the schedule and compare the answers.
#
# ## The knob, and why the default is small
#
# **Frequency is the point.** Milestone C's ordering bug appeared about one run in six; the `EPIPE`
# panic `borg get | head -1` caused appeared one in forty, and only under load. Both read as flakes
# and neither was. Fewer than ~50 runs is not evidence.
#
# Fifty runs is also a couple of minutes of wall clock, which is not a thing to put in `./check.sh`
# unattended. So the count is an environment variable and the default is a smoke test:
#
#     bash run.sh                            # 5 runs — what check.sh runs
#     BORG_DETERMINISM_RUNS=50 bash run.sh   # the number that counts as evidence
#     BORG_DETERMINISM_RUNS=200 bash run.sh  # when hunting something specific
#
# `BORG_DERIVE_PARALLELISM` is pinned high rather than left at one-per-core, for the reason
# `crates/borg-engine/tests/concurrency.rs` records for its sixteen: what is being hunted is an
# interleaving, and oversubscribing the runtime produces more of them per second than matching the
# machine does. Export it yourself to override.
#
# ## What is in the workload, and why each part is there
#
#   * **A wide wave.** The CLI writes one layer per `borg set`, so a round driven by `set` is one
#     invocation wide and schedules nothing. Every batch of writes here goes through **one
#     transaction**, which lands as one layer touching every entity — and that is what gives the round
#     that settles it something to run concurrently.
#   * **A chain**, `headcount → is_investible → tier`, so a round's second hop reads what its own
#     first hop wrote and the order the two run in is genuinely unspecified (§16.5).
#   * **A migration**, in the same rounds as the chain, so a round contains the one producer whose
#     output shares a cell with source data. That composition is what `170` exists for and what broke.
#   * **A backlog.** Two transactions commit before anything settles either, so the round that forks at
#     the earlier layer merges into a trunk that has moved — its guards fail, its consumers cascade,
#     and a later round redoes the work. Which invocations lose is exactly what may vary.
#   * **Two transactions racing one cell**, so the client half of concurrency is in the digest too.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

export BORG_DERIVE_PARALLELISM="${BORG_DERIVE_PARALLELISM:-8}"
RUNS="${BORG_DETERMINISM_RUNS:-5}"
ENTITIES="1 2 3 4"

# Write several cells as one layer, so the round that settles it has a wave to schedule.
# `$1` is a transaction handle; the rest are `cell=value` pairs.
batch() {
    local tx="$1"; shift
    local pair
    for pair in "$@"; do
        borg tx set "${pair%%=*}" "${pair#*=}" --tx "$tx" >/dev/null
    done
    borg tx commit --tx "$tx" >/dev/null
}

# One run, from an empty store to a settled branch. What it prints is the part of the outcome that is
# not state — which transactions committed — and the def-version, which is a layer id and is stable
# because def layers are committed by one process with nothing racing them.
workload() {
    rm -f "$WORK"/borg.*
    borg init >/dev/null
    borg derive pause >/dev/null
    borg repo push "$HERE/repo-v1" >/dev/null
    V1="$(borg def version)"
    echo "def-version: $V1"

    local writes=() i
    for i in $ENTITIES; do
        writes+=("Company#$i.headcount=$((i * i * 3))" "Company#$i.founded=199$i-0$i-01")
    done
    batch "$(borg tx begin)" "${writes[@]}"

    # The schema moves over data that predates it, so the migration owes every existing value (§9.6).
    borg repo push "$HERE/repo-v2" >/dev/null

    # One round for the whole world: `--rebuild` rewinds every watermark and settles the highest
    # source layer, so the chain, the fan-out and both migration directions are in one round's waves.
    # This is the widest thing the CLI can ask for and it is where most of the scheduling happens.
    borg derive --rebuild >/dev/null

    # A backlog: two layers, neither settled, both moving the same cells. The round forked at the
    # first merges into a trunk the second has already moved.
    batch "$(borg tx begin)" 'Company#1.headcount=3' 'Company#2.headcount=4'
    batch "$(borg tx begin)" 'Company#1.headcount=41' 'Company#2.headcount=42'
    borg derive >/dev/null

    # Two transactions against one cell, both forked before either commits.
    local t1 t2
    t1="$(borg tx begin)"
    t2="$(borg tx begin)"
    borg tx get 'Company#3.headcount' --tx "$t1" >/dev/null
    borg tx get 'Company#3.headcount' --tx "$t2" >/dev/null
    borg tx set 'Company#3.headcount' 4 --tx "$t1" >/dev/null
    borg tx commit --tx "$t1" >/dev/null && echo "tx1: committed" || echo "tx1: rejected"
    borg tx set 'Company#3.headcount' 200 --tx "$t2" >/dev/null
    borg tx commit --tx "$t2" >/dev/null 2>&1 && echo "tx2: committed" || echo "tx2: rejected"

    borg derive >/dev/null
}

# The settled state, in a canonical form.
#
# **Layer ids are deliberately not in it.** An id is assigned when a layer opens (§7.3), and which
# invocation of a wave opens first is precisely what the scheduler decides — so `authored at`,
# `landed at` and `fresh as of` are properties of the schedule, and pinning them would pin the thing
# this scenario exists to let vary. What is kept is everything a *client* is promised: the value, the
# interned identity behind it, whether it was written or computed, whether it is current, and which
# producer said so.
digest() {
    local i field
    for i in $ENTITIES; do
        for field in headcount founded is_investible tier; do
            printf '%s.%s\n' "$i" "$field"
            borg get "Company#$i.$field" 2>&1 |
                grep -E '^[[:space:]]+(value|interned|origin|state|produced by):' || true
        done
        printf '%s.founded@v1  %s\n' "$i" \
            "$(borg get "Company#$i.founded" --value --client-version "$V1")"
    done
    printf 'outstanding  %s\n' "$(borg derive --quiet)"
}

# --- run it -----------------------------------------------------------------------------------------

first=""
for run in $(seq 1 "$RUNS"); do
    outcome="$(workload)"
    V1="$(printf '%s\n' "$outcome" | sed -n 's/^def-version: //p')"
    settled="$(digest)"
    this="$outcome
$settled"

    if [ -n "$first" ]; then
        if [ "$this" != "$first" ]; then
            fail "run $run settled differently from run 1
$(diff <(printf '%s\n' "$first") <(printf '%s\n' "$this") || true)"
        fi
        continue
    fi
    first="$this"

    # A digest identical every run because it is empty every run would pass this scenario and mean
    # nothing. So the reference run is checked for content before anything is compared against it.
    assert_contains "$first" "def-version: L" "a def-version is a layer id, and is stable"
    assert_contains "$first" "tx1: committed" "the first of the racing transactions commits"
    assert_contains "$first" "tx2: rejected" "and the second is rejected by its own guard"
    assert_contains "$first" "1991" "the migration produced the new view of the oldest data"
    assert_contains "$first" "1991-01-01" "with what its author wrote still underneath it"
    assert_contains "$first" "core" "the head of the chain produced a value"
    assert_contains "$first" "watch" "and so did the entity on the other side of its threshold"
    assert_contains "$first" "outstanding  0" "and the branch settled"
    if [ "$(printf '%s\n' "$first" | grep -c 'state: *current')" -lt 8 ]; then
        fail "the reference run has almost nothing current in it, so comparing it proves little
$first"
    fi
    pass "the reference run is a settled branch with derived data all through it"
done

pass "$RUNS runs at parallelism $BORG_DERIVE_PARALLELISM settled byte-identically"
if [ "$RUNS" -lt 50 ]; then
    echo "    (BORG_DETERMINISM_RUNS=50 is the number that counts as evidence — see the header)"
fi
