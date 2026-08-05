#!/usr/bin/env bash
# **Scenario 080's promise, arriving in the SDK.** SPEC.md §5.4, §9.3; SDK-DRAFT.md §4.4.
#
# 080 made the claim on a command line: a client authored against an old def-version keeps reading
# and writing after the schema moves, because `--client-version` pins it and `down` migrations carry
# newer values back. That is the single most important thing in Act 2 to prove for real clients,
# because on a command line the "old client" is a flag somebody typed — and a flag can be quietly
# dropped. Here it is a *generated file*: the version is baked in by `borg generate`, sent on every
# connection, and the only way to change it is to regenerate.
#
# The story, end to end:
#
#   * generate at v1, where `Company.founded` is a `String` holding an ISO date, and write one;
#   * stop the server (the recorded gap: `def push` reads a filesystem and is not on the socket, so
#     pushing a schema to a served store means stopping it — SDK-DRAFT §4.3);
#   * push the migration, so `founded` is now an `Int` holding the year;
#   * regenerate, with `--watch` noticing the def layer land;
#   * and then run **both** clients against one server: the v2 one sees the migrated shape, the v1
#     one — unchanged, unrecompiled — still reads dates, including for a value the v2 client wrote.
#
# The compile step is half the assertion. `founded` is annotated `string` in one client and `number`
# in the other, and `crossed.ts` swaps them and must fail: two generated modules that had *not*
# changed would compile all three.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../ts-lib.sh"

need_node "270 needs node and pnpm to compile and run two generated clients." \
          "The engine-level claim it rests on is scenario 080, which needs only bash."
build_sdk

source "$HERE/../lib.sh"
setup

SOCK="$WORK/borg.sock"
PROGRAM="$WORK/program"

start_serve() {
    "$BORG_BIN" --store "$WORK/borg.db" serve --socket "$SOCK" >>"$WORK/serve.log" 2>&1 &
    SERVE_PID=$!
    for _ in $(seq 100); do
        if python3 -c '
import socket, sys
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
try: s.connect(sys.argv[1])
except OSError: sys.exit(1)
' "$SOCK" 2>/dev/null; then return 0; fi
        sleep 0.1
    done
    echo "server never came up:" >&2; cat "$WORK/serve.log" >&2; exit 1
}

stop_serve() {
    kill "$SERVE_PID" 2>/dev/null || true
    wait "$SERVE_PID" 2>/dev/null || true
}

out() { sed -n "s/^$2=//p" <<<"$1" | head -1; }
client() { (cd "$PROGRAM" && node "$@"); }

cp -r "$HERE/program" "$WORK/"
link_sdk "$PROGRAM"

# ── v1: a String holding an ISO date ──────────────────────────────────────────────────────────────

# 080's own repos, unmodified. What is under test here is the SDK, and reusing the schema that
# scenario already proves the engine behaviour on is what makes the two comparable.
borg repo push "$HERE/../080-migration/repo-v1" >/dev/null
v1="$(borg def version)"

borg generate --lang ts -o "$PROGRAM/gen/v1" >/dev/null 2>&1
v1_module="$(cat "$PROGRAM/gen/v1/borg.generated.ts")"
assert_contains "$v1_module" "export const CLIENT_VERSION = \"$v1\";" \
    "the v1 module is stamped with the def-version it was generated at"
assert_contains "$v1_module" "founded: string | null;" \
    "and at that version founded is a string, because the branch declared String"

start_serve

wrote="$(client client-v1.ts "$SOCK" write 1999-06-01)"
assert_eq "$(out "$wrote" client_version)" "$v1" \
    "the generated client connects as v1 — nobody typed a flag, the module carries it"
assert_contains "$(out "$wrote" landed)" "L" "and its write landed"

# ── The schema moves, and the server has to be stopped for it ────────────────────────────────────

# *Failing means the recorded gap is not where it was recorded.*
#
# `def push` and `repo push` read from a **filesystem** — a JSON file, a directory of scripts — and
# `repo push` writes the table saying where that code lives (§9.2). A client naming paths on the
# server's disk is a deployment operation wearing a client's clothes, so neither is on the socket;
# and the advisory lock means the CLI cannot do it either while the store is served. Pushing a
# schema to a served store therefore means stopping the server, and this asserts it rather than
# leaving it as a comment somebody discovers (SDK-DRAFT §4.3).
assert_rejected "$SOCK" "a schema cannot be pushed to a served store — the server must stop first" \
    -- borg repo push "$HERE/../080-migration/repo-v2"

stop_serve

# `--watch` is running while the push lands, which is what it is for: a developer regenerating on
# every schema change should not have to remember to. It polls the *def view* rather than the branch
# head, because head moves on every `borg set` and a generated module changes only when a def layer
# does (§5.3) — there is no server push in §17.5 to be notified by, and adding one would be a change
# of shape rather than a feature.
borg generate --lang ts -o "$PROGRAM/gen/v2" --watch >"$WORK/watch.log" 2>&1 &
WATCH_PID=$!
for _ in $(seq 100); do
    [ -f "$PROGRAM/gen/v2/borg.generated.ts" ] && break
    sleep 0.1
done
assert_contains "$(cat "$PROGRAM/gen/v2/borg.generated.ts")" "founded: string | null;" \
    "--watch generates once on the way in, at whatever the schema is now"

borg repo push "$HERE/../080-migration/repo-v2" >/dev/null
v2="$(borg def version)"
if [ "$v2" = "$v1" ]; then
    fail "pushing a def mutation must move the branch's def-version"
fi
pass "pushing the migration moved the branch's def-version"

# *Failing means a developer regenerates by hand, or does not, and finds out later.*
for _ in $(seq 100); do
    grep -q "founded: number | null;" "$PROGRAM/gen/v2/borg.generated.ts" && break
    sleep 0.1
done
kill "$WATCH_PID" 2>/dev/null || true
wait "$WATCH_PID" 2>/dev/null || true

v2_module="$(cat "$PROGRAM/gen/v2/borg.generated.ts")"
assert_contains "$v2_module" "founded: number | null;" \
    "--watch noticed the def layer land and rewrote the module: founded is now a number"
assert_contains "$v2_module" "export const CLIENT_VERSION = \"$v2\";" \
    "stamped at the new def-version, which is what makes it a *different* client"

# The migration has to have run for the old value to be readable at the new version.
borg derive >/dev/null

# ── Both clients compile, and neither compiles as the other ──────────────────────────────────────

# *Failing means the two generated modules are the same file with a different stamp.*
tsc_check "$PROGRAM/tsconfig.json"
pass "both clients compile: one annotates founded as a string, the other as a number"

if crossed="$(tsc_check "$PROGRAM/tsconfig.crossed.json" 2>&1)"; then
    fail "the crossed program compiled, so regeneration did not change the types"
fi
assert_contains "$crossed" "Type 'string | null' is not assignable to type 'number | null'" \
    "reading the v1 module's founded as a number does not compile…"
assert_contains "$crossed" "'string' is not assignable to parameter of type 'number'" \
    "…and writing the v1 shape through the v2 module does not either"

# ── One server, two clients, two versions ────────────────────────────────────────────────────────

start_serve

# The v2 client sees the migrated shape. The value it is reading was written by the v1 client above
# and has not been rewritten — `up` produced a second version of it, and this is that version (§5.3).
new="$(client client-v2.ts "$SOCK" read)"
assert_eq "$(out "$new" client_version)" "$v2" "the regenerated client connects as v2"
assert_eq "$(out "$new" one.value)" "1999" \
    "and reads the v1 client's date through the new lens: it is a year now"
assert_eq "$(out "$new" one.origin)" "derived" \
    "which it is told plainly was computed rather than written"

# A write from the new world, in the new shape.
assert_contains "$(out "$(client client-v2.ts "$SOCK" write 2020)" landed)" "L" \
    "the v2 client writes an Int, validated against its own def-view"

# *Failing means shipping a schema change breaks every client that has not been rebuilt.*
#
# **This is the assertion.** Nothing about `client-v1.ts` changed: not the source, not the generated
# module it imports, not a flag. It connects, states the version it was generated at, and the system
# meets it where it is — its own write comes back as the date it wrote, and a value written after the
# schema moved comes back through `down`.
old="$(client client-v1.ts "$SOCK" read)"
assert_eq "$(out "$old" client_version)" "$v1" \
    "the v1 client is still the v1 client, long after the schema moved"
assert_eq "$(out "$old" one.value)" "1999-06-01" \
    "and what it wrote is still there, in the shape it wrote it — a migration adds a version, never \
rewrites one"
assert_eq "$(out "$old" one.origin)" "source" "as ground truth, at its own version"

assert_eq "$(out "$old" two.value)" "2020-01-01" \
    "while a value written by the *new* client reaches it through the down migration"
assert_eq "$(out "$old" two.origin)" "derived" "labelled as the computed view it is…"
assert_eq "$(out "$old" two.state)" "current" "…and current, not a promise of a catch-up"
assert_eq "$(out "$old" two.produced_by)" "P" "attributed to the migration that produced it"

stop_serve

# `up` and `down` are two projections of one value, not a cycle: neither is triggered by the other's
# output, so the world settles rather than ping-ponging until the cycle detector fires (§9.3, §16.6).
assert_derives 0 "everything has settled — the two directions do not chase each other"
