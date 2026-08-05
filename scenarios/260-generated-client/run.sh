#!/usr/bin/env bash
# Generated types, compiled, and then run against a real `borg serve`. SDK-DRAFT.md §3, §4.4.
#
# 250 proved the client protocol needs no client library. This proves what a client library is
# *for*: the same store, reached through a module `borg generate` emitted, where the schema is a
# compile-time fact rather than a string you hope you spelled right.
#
# The claims:
#
#   * generation reads the definitions **from the socket when the store is served** and from the
#     store when it is not — and produces the identical module either way, because a served store
#     refuses CLI opens and "stop your server to regenerate" is not a workflow;
#   * the emitted module is ordinary TypeScript that an ordinary project compiles — and a wrong
#     field name, a wrong value type, or a write to a derived field **fails that compile**;
#   * S2's conflict arrives as a `ConflictError` **naming the cell that moved**, which is what a
#     client needs in order to decide about retrying;
#   * a read that is behind comes back with the §10.4 envelope saying so, never as a bare value.
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../ts-lib.sh"

need_node "260 needs node and pnpm to compile and run a generated client." \
          "\`borg generate\` itself is covered by crates/borg-cli/src/generate.rs's" \
          "golden test, which needs only cargo."
build_sdk

source "$HERE/../lib.sh"
setup

SOCK="$WORK/borg.sock"
PROGRAM="$WORK/program"

# Start the server and wait until it actually answers — a socket file exists a moment before
# anything is listening on it (see 250 for the full reasoning, including why `$!` needs the binary
# rather than the `borg` helper function).
start_serve() {
    "$BORG_BIN" --store "$WORK/borg.db" serve --socket "$SOCK" >"$WORK/serve.log" 2>&1 &
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

# One `key=value` line out of the client program's output.
out() { sed -n "s/^$2=//p" <<<"$1" | head -1; }

# ── A schema, some data, and a producer ───────────────────────────────────────────────────────────

# 030's repo: a bash pipeline, so nothing in *this* scenario's subject depends on the TypeScript
# pipeline SDK. What is being generated from is the branch's definitions, and they do not remember
# what language put them there.
borg repo push "$HERE/../030-shell-pipeline/repo" >/dev/null

# Paused so that the client's own commit leaves derived data behind, which is what makes the stale
# envelope below a real observation rather than a stubbed one.
borg derive pause >/dev/null

borg set 'Company#1.website' acme.ai >/dev/null
borg set 'Company#1.headcount' 40 >/dev/null
borg derive >/dev/null
assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "the pipeline has run, so there is derived data to go stale later"

version="$(borg def version)"

# ── Generate, from the store ──────────────────────────────────────────────────────────────────────

cp -r "$HERE/program" "$WORK/"
link_sdk "$PROGRAM"

direct="$(borg generate --lang ts -o "$PROGRAM/gen" 2>&1)"
assert_contains "$direct" "directly" \
    "with nothing serving the store, generation opens it — and says which mode it is in"

module="$(cat "$PROGRAM/gen/borg.generated.ts")"

# *Failing means a generated client lies about which schema it was built from.*
#
# The stamp is the entire reason codegen is not merely a convenience: §5.4 says an actor's def-view
# is the one its code was authored against, and generated code is the only actor that can honestly
# state one. borg itself cannot — it has no generated code, so every invocation is authored now.
assert_contains "$module" "export const CLIENT_VERSION = \"$version\";" \
    "the module bakes in the def-version it was generated at, as its ClientVersion"
assert_contains "$module" "clientVersion: CLIENT_VERSION" \
    "and its createBorgContext sends it, rather than merely holding it"

assert_contains "$module" "headcount: number | null;" \
    "an Int field is a number, and nullable — absence is not a state a declaration can rule out"
assert_contains "$module" "website: string | null;" "a String field is a string"

# Ownership is static now that it is declared (§8), so it is marked rather than left to a runtime
# rejection. SPEC.md §15 deferred this "with the SDKs themselves"; this is the SDKs themselves.
assert_contains "$module" "readonly is_investible: boolean | null;" \
    "a field a producer owns is emitted readonly, which is what makes a client write to it a \
compile error"
assert_contains "$module" "derived by P" \
    "and the producer that owns it is named, by the id the log holds (§9.2)"

# Field names are used verbatim — `is_investible` in the schema, `is_investible` here and
# `Company#1.is_investible` at the CLI. A case conversion would be a mapping somebody has to
# reverse-engineer from an error message.
if grep -q "isInvestible" "$PROGRAM/gen/borg.generated.ts"; then
    fail "codegen renamed a field; names are verbatim in both directions"
fi
pass "field names are used verbatim, with no case conversion in either direction"

# ── It compiles, and the wrong program does not ───────────────────────────────────────────────────

# *Failing means the generated module is not the TypeScript it claims to be.*
tsc_check "$PROGRAM/tsconfig.json"
pass "a client written against the generated types compiles"

# *Failing means the types are decorative.*
#
# Deliberately shown failing. A compile-time assertion that has never been observed to fail is a
# compile-time assertion that might not be checking anything — and each of the three mistakes in
# `bad.ts` would otherwise surface far from its cause: a typo'd field name reads as a cell nobody
# has written, which is a legitimate answer to a question you did not mean to ask.
if refusal="$(tsc_check "$PROGRAM/tsconfig.bad.json" 2>&1)"; then
    fail "the deliberately wrong client compiled, so the generated types check nothing"
fi
pass "and one with a wrong field name does not"

# The compiler's own words, because *what* it refused is the part that has to be right. The first
# error lists the fields that do exist, the second names the declared type, and the third leaves
# `is_investible` out of the writable set entirely — which is the `readonly` marking working.
assert_contains "$refusal" "'\"headcont\"' is not assignable" \
    "the compiler names the field that does not exist…"
assert_contains "$refusal" "'string' is not assignable to parameter of type 'number'" \
    "…refuses a string where the schema declared an Int…"
assert_contains "$refusal" "'\"is_investible\"' is not assignable" \
    "…and refuses a client write to a field a producer owns"

# ── Generate again, this time through the socket ──────────────────────────────────────────────────

start_serve

# *Failing means you have to stop your server to regenerate.*
#
# A served store turns every other `borg` invocation away by name (§17.5), so a `generate` that only
# knew how to open a file would fail exactly when a developer is most likely to run it. It connects
# instead — which is SDK-DRAFT §2.6's remote-connection future arriving for one read-only command,
# and deliberately not for the write path.
assert_rejected "$SOCK" "a served store still refuses an ordinary command, naming the socket" \
    -- borg get 'Company#1.headcount'

served="$(borg generate --lang ts -o "$WORK/gen-socket" 2>&1)"
assert_contains "$served" "$SOCK" \
    "but generate reads through the socket, and says so"

# *Failing means there are two generators and one of them will drift.*
assert_eq "$(cat "$WORK/gen-socket/borg.generated.ts")" "$module" \
    "and what it emits is byte-identical to what it emitted from the store"

# ── The client, against the running server ────────────────────────────────────────────────────────

result="$(cd "$PROGRAM" && node client.ts "$SOCK")"

assert_eq "$(out "$result" client_version)" "$version" \
    "the generated client connects as the version it was generated at"

assert_eq "$(out "$result" conflict.reason)" "guard" \
    "S2 through the SDK: the second commit is refused as a guard conflict…"
assert_contains "$(out "$result" conflict.cell)" "headcount" \
    "…and the exception carries the cell that moved, which is what makes retrying decidable"
assert_eq "$(out "$result" aborted)" "ok" \
    "the rejected transaction is still open, and still abortable"

# The increment happened once, not twice with one silently lost.
assert_eq "$(out "$result" headcount)" "41" "so the increment happened exactly once"
assert_eq "$(out "$result" website)" "acme.ai" \
    "and a String field arrives as its content, never as the @s-… that is physically stored"

# *Failing means an SDK client is served derived data with no way to know it is behind.*
#
# Invariant 8, through the SDK. The commit above moved `headcount`; auto-derivation is paused, so
# `is_investible` has not been recomputed. The read is served **and labelled**.
assert_eq "$(out "$result" stale.state)" "stale" \
    "a read of derived data that is behind says so — it is never presented as fresh"
assert_eq "$(out "$result" stale.origin)" "derived" "and says it was computed rather than written"
assert_eq "$(out "$result" stale.produced_by)" "P" "naming the producer that produced it"
assert_eq "$(out "$result" stale.value)" "true" \
    "the value is still served, at the last thing that was true — stale is a label, not a refusal"

assert_eq "$(out "$result" website.state)" "current" \
    "while a source cell nothing derives is simply current"
assert_eq "$(out "$result" website.origin)" "source" "and ground truth"
assert_contains "$(out "$result" website.cell)" "Company:o-" \
    "and what comes back is the canonical cell, whatever shorthand went out"

stop_serve

# ── And the store is a normal store again ─────────────────────────────────────────────────────────

assert_eq "$(borg get 'Company#1.headcount' --value)" "41" \
    "the CLI has the store back, with what the generated client wrote"
assert_field "$(borg get 'Company#1.is_investible')" "state" "stale" \
    "and the state the SDK reported is the state borg get prints, because it is one read path"
