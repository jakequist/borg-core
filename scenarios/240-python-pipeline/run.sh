#!/usr/bin/env bash
# The neutrality gate, run rather than argued.
#
# 030 wrote a worker in bash to prove the protocol has no hidden client-library complexity. 230 wrote
# one in TypeScript to prove a real client library is equally at home. This one writes the *same*
# pipeline a third time, in a language whose model is the opposite of TypeScript's — synchronous,
# blocking, no promises anywhere — and asks whether the contract noticed.
#
# It asserts three things nothing else can:
#
#   * everything 030 and 230 assert, in Python, against the same store;
#   * that the Python and TypeScript SDKs describe the same repo **byte for byte**;
#   * that one repo directory holding one TypeScript pipeline and one Python pipeline pushes once and
#     derives in one round, with a chain running across the language boundary.
HERE="$(cd "$(dirname "$0")" && pwd)"
SDK_PY="$(cd "$HERE/../../packages/borg-sdk-py" && pwd)"
SDK_TS="$(cd "$HERE/../../packages/borg-sdk" && pwd)"

# `check.sh` runs everywhere, and not everywhere has a Python. Skipping loudly beats failing: this
# scenario's subject is an optional toolchain, and the engine's half of it is covered by
# `borg-exec-process`'s transport tests, which need nothing but cargo.
skip() {
    echo "  ⚠ SKIPPED: $1" >&2
    echo "    240 needs python3 3.11+; the socket transport itself is covered by" >&2
    echo "    crates/borg-exec-process/tests/transport.rs, which needs neither." >&2
    exit 0
}

command -v python3 >/dev/null 2>&1 || skip "python3 is not installed"
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' \
    || skip "python3 $(python3 -V 2>&1 | awk '{print $2}') is older than 3.11"

# A checkout, not an install. The SDK has no runtime dependencies and no build step, so a repo
# reaches it the way any Python program reaches a library that is not installed. A real repo would
# `pip install borg-sdk` into a virtualenv; that is the same import, found differently.
export PYTHONPATH="$SDK_PY/src"
# Keeps `__pycache__` out of the repo directory `borg repo push` walks. It would be filtered out
# anyway — it is a directory and the walk takes files — but a scenario should not leave litter.
export PYTHONDONTWRITEBYTECODE=1

source "$HERE/../lib.sh"
setup

cp -r "$HERE/repo" "$HERE/repo-unclaimed" "$HERE/repo-mixed" "$WORK/"

# Counting invocations means being the one who asks, so the automation is paused and the rounds are
# stepped by hand — exactly as 030 and 230 do.
borg derive pause >/dev/null

# ── The repo will not push until it is coherent ────────────────────────────────────────────────────

# The same two refusals 230 asserts, from the same describe-assembly, in the other language. Both are
# static facts about a repo, so both fail at push time rather than mid-round.
assert_rejected "no pipeline in this repo writes it" \
    "a derived() field no pipeline claims is a push-time error in Python too" \
    -- borg repo push "$WORK/repo-unclaimed"

sed -i 's/^id = 1$/id = 9/' "$WORK/repo/borg.toml"
assert_rejected "describes itself as repo 1" \
    "a repo id stated twice is cross-checked rather than quietly ignored" \
    -- borg repo push "$WORK/repo"
sed -i 's/^id = 9$/id = 1/' "$WORK/repo/borg.toml"

# ── The same story as 030 and 230, in Python ───────────────────────────────────────────────────────

borg repo push "$WORK/repo"
assert_contains "$(borg producer list)" "invest" \
    "the Python module described itself, and the server recorded a producer definition"

assert_contains "$(cat "$WORK/borg.producers.json")" '"transport": "socket"' \
    "the transport it asked for is remembered beside the command that implements it"

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
    "the Python pipeline derived a field over a unix socket"
assert_eq "$(borg get 'Company#2.isInvestible' --value)" "false" \
    "and it ran per entity, not once globally"

assert_field "$(borg get 'Company#1.isInvestible')" "origin" "derived" \
    "the output is marked derived, and attributed to its producer"

# Dependency capture is automatic and lives entirely server-side: the SDK declared nothing and
# recorded nothing, the server watched what crossed the socket. A synchronous `get` is the same
# message an awaited one is.
website="$(borg get 'Company#1.website' | head -1)"
assert_contains "$(borg explain 'Company#1.isInvestible')" "$website" \
    "lineage shows what the module actually read, without the SDK tracking anything"

# ── Field-granular invalidation, exactly as 030 and 230 assert it ──────────────────────────────────

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
# derive --quiet`'s stdout, and the pipeline called `print()` twice per invocation while it ran. Over
# stdio those lines would have corrupted the message stream; through the socket they cannot reach it,
# and they cannot reach the CLI's own stdout either.
#
# This is the assertion that the stderr duplication built for 230 is a property of the *transport*
# and not of the TypeScript SDK. Nothing in `borg-exec-process` was changed to run a Python worker.
borg set 'Company#1.headcount' 41
chatter="$(borg derive 2>&1 >/dev/null)"
assert_contains "$chatter" "[invest] scoring Company:" \
    "what a Python print() wrote reached a human, on stderr, rather than being swallowed"
assert_eq "$(borg get 'Company#1.isInvestible' --value)" "true" \
    "and the invocation that printed it produced the right answer"

# ── Two SDKs, one contract ─────────────────────────────────────────────────────────────────────────

# `repo/pipelines/is_investible.py` is a line-for-line mirror of 230's `is_investible.ts`: same
# structs, same field names in the same order, same producer, same repo id. If the contract has
# nothing TypeScript-shaped in it, the two `describe` payloads are the same bytes.
ts_ready=1
command -v node >/dev/null 2>&1 || ts_ready=0
command -v pnpm >/dev/null 2>&1 || ts_ready=0
if [ "$ts_ready" = 1 ]; then
    node -e 'const [a,b]=process.versions.node.split(".").map(Number); process.exit(a>22||(a===22&&b>=18)?0:1)' \
        || ts_ready=0
fi
if [ "$ts_ready" = 1 ] \
    && { [ ! -f "$SDK_TS/dist/index.js" ] \
        || [ -n "$(find "$SDK_TS/src" -newer "$SDK_TS/dist/index.js" -print -quit)" ]; }; then
    echo "  … building borg-sdk" >&2
    (cd "$SDK_TS" && pnpm install --silent && pnpm exec tsc -p tsconfig.build.json) || ts_ready=0
fi

if [ "$ts_ready" = 0 ]; then
    echo "  ⚠ node/pnpm unavailable: skipping the byte-for-byte and mixed-repo halves" >&2
    echo "    (everything above is the whole of the Python story and did run)" >&2
    exit 0
fi

# The TypeScript twin needs `borg-sdk` importable from beside it, exactly as 230 arranges.
cp -r "$HERE/../230-typescript-pipeline/repo" "$WORK/repo-ts"
mkdir -p "$WORK/repo-ts/node_modules" "$WORK/repo-mixed/node_modules"
ln -s "$SDK_TS" "$WORK/repo-ts/node_modules/borg-sdk"
ln -s "$SDK_TS" "$WORK/repo-mixed/node_modules/borg-sdk"

assert_eq "$("$WORK/repo/pipelines/is_investible.py" describe)" \
    "$("$WORK/repo-ts/pipelines/is_investible.ts" describe)" \
    "the Python and TypeScript SDKs describe the same repo byte for byte"

# ── One repo, two languages, one round ─────────────────────────────────────────────────────────────

# `repo-mixed/pipelines/` holds `score.ts` and `summarise.py`. One `borg.toml`, one push, one def
# layer. The engine walks the directory and asks each file to describe itself; nothing anywhere
# records what language answered.
borg repo push "$WORK/repo-mixed"

producers="$(borg producer list)"
assert_contains "$producers" "score" "the TypeScript half of the repo registered"
assert_contains "$producers" "summarise" "and the Python half registered from the same push"
assert_contains "$producers" "score.ts" "the only per-language fact in the store is a file path"
assert_contains "$producers" "summarise.py" "…for each of them"

schema="$(borg def show Startup)"
assert_contains "$schema" "promising" "one struct, declared across two files in two languages"
assert_contains "$schema" "headline" "and both derived fields landed in the same def layer"

borg set 'Startup#1.domain' vector.ai
borg set 'Startup#1.staff' 40

# One round, both languages. `summarise` reads what `score` wrote, so the round also contains a chain
# whose two hops are in different runtimes — and the scheduler, which knows only producers and cells,
# has no way to tell.
mixed="$(borg derive --quiet)"
[ "$mixed" -ge 2 ] || fail "expected at least two invocations in the mixed round, got $mixed"
pass "one round ran invocations of both pipelines ($mixed of them)"

assert_eq "$(borg get 'Startup#1.promising' --value)" "true" \
    "the TypeScript pipeline derived its field"
assert_eq "$(borg get 'Startup#1.headline' --value)" "vector.ai: invest" \
    "and the Python pipeline read that value and derived from it, in the same round"

assert_derives 0 "the branch is a fixpoint: nothing is left over from crossing the boundary"

# And the chain re-runs across the boundary when its input moves, which is the same invalidation
# machinery as everywhere else — it has never had a language in it.
borg set 'Startup#1.staff' 2
borg derive >/dev/null
assert_eq "$(borg get 'Startup#1.promising' --value)" "false" \
    "a write invalidates the TypeScript hop"
assert_eq "$(borg get 'Startup#1.headline' --value)" "vector.ai: pass" \
    "and the Python hop follows it, because it read the cell the first one wrote"
