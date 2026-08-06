#!/usr/bin/env bash
# Boot the whole CRM: a store, a schema, a server, a generated client, an api and a ui.
#
# Re-runnable. Everything it does is either idempotent or skipped when it has already been done, so
# `./dev.sh` twice in a row is `./dev.sh` once — and `./dev.sh --reset` throws the store away and
# starts from nothing.
#
# ## The order is not arbitrary
#
# **`borg repo push` has to happen before `borg serve` starts**, and this is the single most
# important line in the file. A served store refuses every other `borg` invocation by name
# (SPEC.md §17.5, CLAUDE.md) — `repo push` reads a *directory* off this machine's disk and is not on
# the socket, so there is no way to push a schema into a running server. The sequence is therefore:
# push while nothing is serving, then serve. Changing the schema means stopping the server, which
# for a dev script means re-running this one.
#
# `borg generate`, by contrast, is happy either way: it is the one command that connects to the
# socket rather than being turned away by it, so it runs *after* the server is up and reads the
# definitions through it.
#
#     usage: ./dev.sh [--reset] [--no-ui] [--headless]
#
#       --reset      delete data/ first: new store, new ids, no contacts
#       --no-ui      skip vite (the api is still up on $API_PORT)
#       --headless   --no-ui, and say so — for driving the api with curl from another shell

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

DATA="$HERE/data"
STORE="$DATA/borg.db"
SOCK="$DATA/borg.sock"
BORG="${BORG_BIN:-$ROOT/target/debug/borg}"
SDK="$ROOT/packages/borg-sdk"

API_PORT="${API_PORT:-8787}"
UI_PORT="${UI_PORT:-5173}"

RESET=0
UI=1
HEADLESS=0
for arg in "$@"; do
    case "$arg" in
        --reset) RESET=1 ;;
        --no-ui) UI=0 ;;
        --headless) UI=0; HEADLESS=1 ;;
        *) echo "usage: ./dev.sh [--reset] [--no-ui] [--headless]" >&2; exit 2 ;;
    esac
done

say() { printf '\033[1m▸ %s\033[0m\n' "$*"; }
die() { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

# Every background process this script owns, killed on the way out however we leave.
PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && wait "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT INT TERM

# ── 0. Tools ──────────────────────────────────────────────────────────────────────────────────────

command -v node >/dev/null || die "node is required (22.18+, for running .ts files directly)"
node -e 'const [a,b]=process.versions.node.split(".").map(Number); process.exit(a>22||(a===22&&b>=18)?0:1)' \
    || die "node $(node -v) cannot run a .ts file directly — 22.18+ strips types natively"
command -v pnpm >/dev/null || die "pnpm is required (for the ui's dependencies)"

if [ ! -x "$BORG" ]; then
    say "building borg"
    (cd "$ROOT" && cargo build -p borg-cli) || die "cargo build failed"
fi

if [ ! -f "$SDK/dist/client.js" ] || [ -n "$(find "$SDK/src" -newer "$SDK/dist/client.js" -print -quit)" ]; then
    say "building borg-sdk"
    (cd "$SDK" && pnpm install --silent && pnpm exec tsc -p tsconfig.build.json) || die "borg-sdk would not build"
fi

# `borg-sdk` is not published, so it is linked the way every scenario links it: a `node_modules`
# beside the project, which is exactly what a real one would have.
link_sdk() {
    mkdir -p "$1/node_modules"
    ln -sfn "$SDK" "$1/node_modules/borg-sdk"
    ln -sfn "$SDK/node_modules/@types" "$1/node_modules/@types"
}
link_sdk "$HERE/repo"
link_sdk "$HERE/api"

# ── 1. The store ──────────────────────────────────────────────────────────────────────────────────

if [ "$RESET" = 1 ]; then
    say "--reset: deleting $DATA"
    rm -rf "$DATA"
fi

mkdir -p "$DATA"
if [ ! -f "$STORE" ]; then
    say "creating the store"
    "$BORG" --store "$STORE" init
fi

# ── 2. The schema — while nothing is serving ──────────────────────────────────────────────────────
#
# `borg repo push` is accepted every time it is run and emits a **new def layer every time**, even
# when the definitions are identical. That is correct for an event log and wrong for a dev loop: a
# script that pushed unconditionally would walk the branch's def-version up by one on every boot,
# and regenerate the client on every boot with it. So the push is gated on the repo's contents
# having actually changed. See FRICTION.md — `repo push` has no "only if it moved" of its own.
repo_stamp() {
    find "$HERE/repo" -type f \( -name '*.ts' -o -name '*.toml' \) -exec cksum {} + | sort | cksum
}
STAMP="$DATA/.repo-pushed"
if [ ! -f "$STAMP" ] || [ "$(cat "$STAMP")" != "$(repo_stamp)" ]; then
    say "pushing the repo (schema + the display_name pipeline)"
    # The pipeline is invoked as a program, so it has to be executable. A repo checked out without
    # the mode bit fails here with `Permission denied` and no other clue.
    chmod +x "$HERE"/repo/pipelines/*.ts
    "$BORG" --store "$STORE" repo push "$HERE/repo" || die "repo push failed"
    repo_stamp >"$STAMP"
else
    say "schema unchanged, not pushing (def-version $("$BORG" --store "$STORE" def version))"
fi

# ── 3. The server ─────────────────────────────────────────────────────────────────────────────────

rm -f "$SOCK"
say "starting borg serve on $SOCK"
"$BORG" --store "$STORE" serve --socket "$SOCK" >"$DATA/serve.log" 2>&1 &
PIDS+=($!)

# A socket file exists a moment before anything is listening on it, so the wait is for an answer and
# not for a path.
for _ in $(seq 100); do
    if node -e '
const net = require("node:net");
const s = net.connect(process.argv[1]);
s.on("connect", () => { s.end(); process.exit(0); });
s.on("error", () => process.exit(1));
' "$SOCK" 2>/dev/null; then ok=1; break; fi
    sleep 0.1
done
[ "${ok:-0}" = 1 ] || { cat "$DATA/serve.log" >&2; die "borg serve never came up"; }

# ── 4. The generated client ───────────────────────────────────────────────────────────────────────
#
# Through the socket, because the store is served now. `generate` says which way it read.
say "generating the typed client into api/gen"
"$BORG" --store "$STORE" generate --lang ts -o "$HERE/api/gen" || die "generate failed"

# ── 5. The api ────────────────────────────────────────────────────────────────────────────────────

say "starting the api on http://localhost:$API_PORT"
BORG_SOCKET="$SOCK" PORT="$API_PORT" node "$HERE/api/server.ts" &
PIDS+=($!)

for _ in $(seq 100); do
    if node -e '
fetch(`http://localhost:${process.argv[1]}/api/health`).then(r => process.exit(r.ok ? 0 : 1), () => process.exit(1));
' "$API_PORT" 2>/dev/null; then api=1; break; fi
    sleep 0.1
done
[ "${api:-0}" = 1 ] || die "the api never came up"

# ── 6. The ui ─────────────────────────────────────────────────────────────────────────────────────

if [ "$UI" = 1 ]; then
    if [ ! -d "$HERE/ui/node_modules" ]; then
        say "installing the ui's dependencies"
        (cd "$HERE/ui" && pnpm install) || die "pnpm install failed in ui/"
    fi
    say "starting vite on http://localhost:$UI_PORT"
    (cd "$HERE/ui" && VITE_API="http://localhost:$API_PORT" pnpm exec vite --port "$UI_PORT" --strictPort) &
    PIDS+=($!)
fi

printf '\n\033[1;32mup\033[0m  api http://localhost:%s' "$API_PORT"
[ "$UI" = 1 ] && printf '   ui http://localhost:%s' "$UI_PORT"
printf '   store %s\n' "$STORE"

if [ "$HEADLESS" = 1 ]; then
    # Used by the smoke test: everything is serving, the caller drives it with curl. The trap still
    # tears it all down, so the caller keeps the script running and kills it when done.
    printf 'headless: ctrl-c to stop\n'
fi

wait
