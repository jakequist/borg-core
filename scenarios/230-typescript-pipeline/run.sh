#!/usr/bin/env bash
# The TypeScript twin of `030-shell-pipeline`, and the socket's reason for existing.
#
# 030 proved the protocol has no hidden client-library complexity by writing a worker in bash. This
# proves the other half: that a real client library — where a stray `console.log` is a certainty and
# not a risk — is equally at home, because the engine offers it a socket and leaves stdout alone.
#
# Everything 030 asserts is asserted here against the same store, plus the three things only a real
# SDK can be asked: that printing to stdout mid-invocation does no harm, that a `derived()` field no
# pipeline claims is a push-time error, and that a repo id stated twice is checked rather than
# ignored.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../ts-lib.sh"

need_node "230 needs node and pnpm; the socket transport itself is covered by" \
          "crates/borg-exec-process/tests/transport.rs, which needs neither."
build_sdk

source "$HERE/../lib.sh"
setup

# The repo is copied into the scratch store's directory so that `node_modules` can be linked beside
# it — which is exactly what a real repo would have, and is how `import { borg } from "borg-sdk"`
# resolves from a pipeline file.
cp -r "$HERE/repo" "$HERE/repo-unclaimed" "$WORK/"
link_sdk "$WORK/repo"
link_sdk "$WORK/repo-unclaimed"

# Counting invocations means being the one who asks, so the automation is paused and the rounds are
# stepped by hand — exactly as 030 does.
borg derive pause >/dev/null

# ── The repo will not push until it is coherent ────────────────────────────────────────────────────

# A field declared `derived()` that no pipeline writes is a cell nothing could ever fill: clients may
# not write it (§8) and no producer claims it. The SDK refuses to describe such a repo at all, so the
# push fails at `describe` — before a single def event is emitted.
assert_rejected "no pipeline in this repo writes it" \
    "a derived() field no pipeline claims is a push-time error, not a puzzle later" \
    -- borg repo push "$WORK/repo-unclaimed"

# The repo id is written in two places — `borg.toml`, which is authoritative because a repo is a
# directory, and the DSL, where the author states it in code. Two copies of one fact get checked.
sed -i 's/^id = 1$/id = 9/' "$WORK/repo/borg.toml"
assert_rejected "describes itself as repo 1" \
    "a repo id stated twice is cross-checked rather than quietly ignored" \
    -- borg repo push "$WORK/repo"
sed -i 's/^id = 9$/id = 1/' "$WORK/repo/borg.toml"

# ── The same story as 030, in TypeScript ───────────────────────────────────────────────────────────

borg repo push "$WORK/repo"
assert_contains "$(borg producer list)" "invest" \
    "the TypeScript module described itself, and the server recorded a producer definition"

assert_contains "$(cat "$WORK/borg.producers.json")" '"transport": "socket"' \
    "the transport it asked for is remembered beside the command that implements it"

# Its definitions came from the same `describe`, in the same def layer.
schema="$(borg def show Company)"
assert_contains "$schema" "website" "the DSL's struct definitions landed with its pipeline"
assert_contains "$schema" "derived by P" \
    "and the derived field names the producer that owns it, resolved from the name in describe"
assert_contains "$schema" "Employee[]" \
    "a list field's declared type survives the round trip through describe"

# The other side of declared ownership: a client cannot write a field a producer owns.
assert_rejected "may not write" "a client may not write a derived field" \
    -- borg set 'Company#1.isInvestible' true

# The spec's own motivating example: `company.website.ends_with('.ai')`, plus a headcount threshold.
borg set 'Company#1.website' acme.ai
borg set 'Company#1.headcount' 40
borg set 'Company#2.website' example.com
borg set 'Company#2.headcount' 40
borg derive

assert_eq "$(borg get 'Company#1.isInvestible' --value)" "true" \
    "the TypeScript pipeline derived a field over a unix socket"
assert_eq "$(borg get 'Company#2.isInvestible' --value)" "false" \
    "and it ran per entity, not once globally"

assert_field "$(borg get 'Company#1.isInvestible')" "origin" "derived" \
    "the output is marked derived, and attributed to its producer"

# Dependency capture is automatic and lives entirely server-side: the SDK declared nothing and
# recorded nothing, the server watched what crossed the socket.
website="$(borg get 'Company#1.website' | head -1)"
assert_contains "$(borg explain 'Company#1.isInvestible')" "$website" \
    "lineage shows what the module actually read, without the SDK tracking anything"

# ── Field-granular invalidation, exactly as 030 asserts it ─────────────────────────────────────────

borg set 'Company#2.website' rival.ai
assert_derives 1 "changing one input re-runs exactly one invocation"
assert_eq "$(borg get 'Company#2.isInvestible' --value)" "true" "and the output follows"

borg set 'Company#2.headcount' 3
assert_derives 1 "the number it read is a tracked dependency too"
assert_eq "$(borg get 'Company#2.isInvestible' --value)" "false" "and it flips the answer back"

borg set 'Company#1.foundedYear' 2014
assert_derives 0 \
    "writing a field the pipeline never read runs nothing at all — the read-set is per field"

# ── stdout belongs to the author ───────────────────────────────────────────────────────────────────

# Every `assert_derives` above already carries this claim: the invocation count is read off `borg
# derive --quiet`'s stdout, and the pipeline printed two lines per invocation while it ran. Over
# stdio those lines would have corrupted the message stream; through the socket they cannot reach it,
# and they cannot reach the CLI's own stdout either.
borg set 'Company#1.headcount' 41
chatter="$(borg derive 2>&1 >/dev/null)"
assert_contains "$chatter" "[invest] scoring Company:" \
    "what the pipeline printed reached a human, on stderr, rather than being swallowed"
assert_eq "$(borg get 'Company#1.isInvestible' --value)" "true" \
    "and the invocation that printed it produced the right answer"
