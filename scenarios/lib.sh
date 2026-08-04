# Shared harness for CLI scenarios.
#
# Every scenario runs the real `borg` binary against a throwaway store. If a scenario passes, the
# devex it describes actually works — no mocks, no in-process shortcuts.

set -euo pipefail

BORG_BIN="${BORG_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/target/debug/borg}"
# Scenarios capture their own directory before `setup` moves to a scratch store.

setup() {
    WORK="$(mktemp -d)"
    trap 'rm -rf "$WORK"' EXIT
    cd "$WORK"
    borg init >/dev/null
}

borg() { "$BORG_BIN" --store "${WORK:-.}/borg.db" "$@"; }

fail() { echo "  ✗ $*" >&2; exit 1; }
pass() { echo "  ✓ $*"; }

# assert_eq <actual> <expected> <description>
assert_eq() {
    if [ "$1" != "$2" ]; then
        fail "$3
      expected: $2
      actual:   $1"
    fi
    pass "$3"
}

# assert_contains <haystack> <needle> <description>
assert_contains() {
    case "$1" in
        *"$2"*) pass "$3" ;;
        *) fail "$3
      expected to contain: $2
      actual:              $1" ;;
    esac
}

# assert_field <output> <field> <expected> <description>
# Matches a `field:   value` line without caring about column alignment.
assert_field() {
    local got
    got="$(printf '%s\n' "$1" | sed -n "s/^[[:space:]]*$2:[[:space:]]*//p" | head -1)"
    if [ "$got" != "$3" ]; then
        fail "$4
      expected $2: $3
      actual   $2: ${got:-<missing>}"
    fi
    pass "$4"
}

# assert_fails <description> -- <command...>
assert_fails() {
    local desc="$1"; shift; shift
    if "$@" >/dev/null 2>&1; then
        fail "$desc (command unexpectedly succeeded)"
    fi
    pass "$desc"
}
