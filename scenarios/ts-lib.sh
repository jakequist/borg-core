# Shared setup for the scenarios that need a JavaScript toolchain: 230, 260, 270.
#
# `check.sh` runs everywhere and not everywhere has node, so these scenarios skip loudly rather than
# fail — their subject *is* an optional toolchain. Factored out when the third of them appeared,
# because by then the copy had drifted in three places at once: what counts as "the SDK is built" is
# one fact, and a scenario testing a stale `dist/` is a scenario testing nothing.

BORG_SDK="$(cd "$(dirname "${BASH_SOURCE[0]}")/../packages/borg-sdk" && pwd)"

# ts_skip <reason> [note...]
ts_skip() {
    echo "  ⚠ SKIPPED: $1" >&2
    shift
    for note in "$@"; do echo "    $note" >&2; done
    exit 0
}

# need_node [note...] — the notes are printed with any skip, so a scenario can say what covers it
# in the absence of a runtime.
need_node() {
    command -v node >/dev/null 2>&1 || ts_skip "node is not installed" "$@"
    command -v pnpm >/dev/null 2>&1 || ts_skip "pnpm is not installed" "$@"
    # Running a `.ts` file directly needs the type stripping Node enabled by default in 22.18.
    node -e 'const [a,b]=process.versions.node.split(".").map(Number); process.exit(a>22||(a===22&&b>=18)?0:1)' \
        || ts_skip "node $(node -v) cannot run a .ts file directly (needs 22.18+)" "$@"
}

# Build the SDK if nothing has, or if it is older than its sources. `check.sh` normally gets here
# first; running a scenario on its own should still work.
build_sdk() {
    if [ ! -f "$BORG_SDK/dist/client.js" ] \
        || [ -n "$(find "$BORG_SDK/src" -newer "$BORG_SDK/dist/client.js" -print -quit)" ]; then
        echo "  … building borg-sdk" >&2
        (cd "$BORG_SDK" && pnpm install --silent && pnpm exec tsc -p tsconfig.build.json) \
            || ts_skip "borg-sdk would not build"
    fi
}

# link_sdk <dir> — a `node_modules` beside a project, which is exactly what a real one would have and
# is how `import … from "borg-sdk/client"` resolves from a generated module.
link_sdk() {
    mkdir -p "$1/node_modules"
    ln -sfn "$BORG_SDK" "$1/node_modules/borg-sdk"
    # `@types/node` too, because a client program says `process.argv` and `tsc` walks up from the
    # *file* to find types, not from wherever the compiler happens to have been started.
    ln -sfn "$BORG_SDK/node_modules/@types" "$1/node_modules/@types"
}

# tsc_check <tsconfig> — typecheck a project, with the compiler the SDK already depends on.
#
# `tsc` is run from the SDK (which has it) but resolves everything relative to the tsconfig it is
# handed, so the program under test sees only its own `node_modules`.
tsc_check() {
    (cd "$BORG_SDK" && pnpm exec tsc -p "$1")
}
