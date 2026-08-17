#!/usr/bin/env bash
# Boot the whole CRM: a server, a registry, a schema, a generated client, an api and a ui.
#
# Re-runnable. Everything it does is either idempotent or skipped when it has already been done, so
# `./dev.sh` twice in a row is `./dev.sh` once.
#
# ## This script does not own the server
#
# `borg-server` is a process that stays up, and this is a script you `^C`. It **ensures** one is
# running — `status || start` — and leaves it running when you stop the script; the api and the ui
# are the only things the trap below kills. That is the right split for the same reason `borg-server
# start` backgrounds by default: a server is a thing you operate, not a thing a dev loop owns.
#
#     ./dev.sh --stop     when you do want it gone
#
# **The schema is pushed into the running server**, which used to be impossible. `repo push` is a
# protocol message now and the *server* executes it against a path on its own disk (SPEC.md §17.6),
# so there is no push-before-serve ordering left to get right and no restart when you edit the
# schema — the earlier version of this file was built around that constraint and said so at length.
#
#     usage: ./dev.sh [--reset] [--no-ui] [--headless] [--stop]
#
#       --reset      recreate the registry's store: new ids, no contacts (see below)
#       --no-ui      skip vite (the api is still up on $API_PORT)
#       --headless   --no-ui, and say so — for driving the api with curl from another shell
#       --stop       stop the dev server and exit, doing nothing else
#
#       CRM_DATA=…   the data directory to host (default ./data). bench.sh uses its own.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

# A **data directory of registries**, which is what a server hosts (§17.6), holding one registry.
DATA="${CRM_DATA:-$HERE/data}"
REGISTRY="personal-crm"
SOCK="$DATA/borg.sock"
# The socket is named rather than left to the well-known address on purpose: `borg://localhost` is
# one address per machine, and a demo that took it would fight whatever else the reader is running.
URL="borg+unix://$SOCK/$REGISTRY"

BORG="${BORG_BIN:-$ROOT/target/debug/borg}"
BORG_SERVER="${BORG_SERVER_BIN:-$ROOT/target/debug/borg-server}"
SDK="$ROOT/packages/borg-sdk"

API_PORT="${API_PORT:-8787}"
UI_PORT="${UI_PORT:-5173}"

RESET=0
UI=1
HEADLESS=0
STOP=0
for arg in "$@"; do
    case "$arg" in
        --reset) RESET=1 ;;
        --no-ui) UI=0 ;;
        --headless) UI=0; HEADLESS=1 ;;
        --stop) STOP=1 ;;
        *) echo "usage: ./dev.sh [--reset] [--no-ui] [--headless] [--stop]" >&2; exit 2 ;;
    esac
done

say() { printf '\033[1m▸ %s\033[0m\n' "$*"; }
die() { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

server() { "$BORG_SERVER" --data-dir "$DATA" --socket "$SOCK" "$@"; }
borg() { "$BORG" "$@"; }

# Every background process **this script** owns, killed on the way out however we leave. The server
# is deliberately not one of them: see the header.
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

if [ "$STOP" = 1 ]; then
    [ -x "$BORG_SERVER" ] || die "no borg-server at $BORG_SERVER"
    server stop
    exit 0
fi

# ── 0. Tools ──────────────────────────────────────────────────────────────────────────────────────

command -v node >/dev/null || die "node is required (22.18+, for running .ts files directly)"
node -e 'const [a,b]=process.versions.node.split(".").map(Number); process.exit(a>22||(a===22&&b>=18)?0:1)' \
    || die "node $(node -v) cannot run a .ts file directly — 22.18+ strips types natively"
command -v pnpm >/dev/null || die "pnpm is required (for the ui's dependencies)"

if [ ! -x "$BORG" ] || [ ! -x "$BORG_SERVER" ]; then
    say "building borg and borg-server"
    (cd "$ROOT" && cargo build -p borg-cli -p borg-server) || die "cargo build failed"
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

# ── 1. The reset, which is the one thing that needs the server *down* ──────────────────────────────
#
# **Stop, delete the registry's directory, start again.** The alternative would be a `registry_delete`
# on the protocol, and that is deliberately not built: it is a destructive operation on a wire whose
# `credential` is a static api key rather than an identity (§17.6) — exactly the shape `borg-server
# stop` avoided by being a SIGTERM rather than a message. Throwing a store away is a thing you do
# with filesystem access, on purpose.
#
# Only this registry's directory goes. The server's pidfile and log live beside it in the data dir
# and are worth keeping — the log is where the reason a previous run died is written.

if [ "$RESET" = 1 ]; then
    if server status >/dev/null 2>&1; then
        say "--reset: stopping the server so its store can be thrown away"
        server stop >/dev/null
    fi
    say "--reset: deleting $DATA/$REGISTRY"
    rm -rf "${DATA:?}/$REGISTRY"
fi

# ── 2. The server ─────────────────────────────────────────────────────────────────────────────────
#
# `status` exits non-zero when nothing is answering, which is what makes this one line. `start`
# waits until the server actually answers before it returns, so there is no socket-file race to
# lose here and no retry loop to write — the earlier version of this script had one.

if server status >/dev/null 2>&1; then
    say "borg-server is already up on $SOCK"
else
    say "starting borg-server on $SOCK (data dir $DATA)"
    server start >/dev/null || { server logs -n 40 >&2 2>/dev/null || true; die "borg-server would not start"; }
fi

# ── 3. The registry ───────────────────────────────────────────────────────────────────────────────
#
# Created **through the server**, because a directory appearing under a running server's data dir is
# a store it has not locked, is not hosting and will not route to (§17.6). `create` goes over the
# socket whenever one is up, which is what makes this idempotent-by-checking rather than by luck.

if server status | grep -q "^  $REGISTRY "; then
    say "registry $REGISTRY is hosted"
else
    say "creating registry $REGISTRY"
    server create "$REGISTRY" >/dev/null || die "could not create the registry"
fi

# ── 4. The schema — into the running server ───────────────────────────────────────────────────────
#
# Pushed unconditionally, every boot, and it costs nothing when nothing changed: `repo push` is a
# diff on both halves. Definitions the branch already holds emit nothing, and a producer whose
# implementation fingerprint has not moved emits nothing either (§9.2), so an unchanged repo pushed
# twice lands no layer at all. When the pipeline body *has* changed, the push recomputes every value
# that pipeline owns — which is FRICTION #17, and is also the precondition for pushing into a live
# server rather than a stopped one.

say "pushing the repo into $REGISTRY (schema + the display_name pipeline)"
# The pipeline is invoked as a program, so it has to be executable. A repo checked out without the
# mode bit fails here with `Permission denied` and no other clue.
chmod +x "$HERE"/repo/pipelines/*.ts
borg --url "$URL" repo push "$HERE/repo" || die "repo push failed"

# ── 5. The generated client ───────────────────────────────────────────────────────────────────────
#
# Through the same url, so the schema this reads is the schema the push just landed — one address,
# named once, for every client in this file.

say "generating the typed client into api/gen"
borg --url "$URL" generate --lang ts -o "$HERE/api/gen" || die "generate failed"

# ── 6. The api ────────────────────────────────────────────────────────────────────────────────────

say "starting the api on http://localhost:$API_PORT"
BORG_URL="$URL" PORT="$API_PORT" node "$HERE/api/server.ts" &
PIDS+=($!)

for _ in $(seq 100); do
    if node -e '
fetch(`http://localhost:${process.argv[1]}/api/health`).then(r => process.exit(r.ok ? 0 : 1), () => process.exit(1));
' "$API_PORT" 2>/dev/null; then api=1; break; fi
    sleep 0.1
done
[ "${api:-0}" = 1 ] || die "the api never came up"

# ── 7. The ui ─────────────────────────────────────────────────────────────────────────────────────

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
printf '   borg %s\n' "$URL"
printf '    the server stays up when this script stops — `./dev.sh --stop` ends it\n'

if [ "$HEADLESS" = 1 ]; then
    # Used by the smoke test: everything is serving, the caller drives it with curl. The trap tears
    # down the api and the ui, so the caller keeps the script running and kills it when done.
    printf 'headless: ctrl-c to stop\n'
fi

wait
