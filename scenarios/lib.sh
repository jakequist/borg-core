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

# field <output> <field>
# One `field:   value` line from a provenance envelope, without caring about column alignment.
field() {
    printf '%s\n' "$1" | sed -n "s/^[[:space:]]*$2:[[:space:]]*//p" | head -1
}

# assert_field <output> <field> <expected> <description>
assert_field() {
    local got
    got="$(field "$1" "$2")"
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

# assert_rejected <needle> <description> -- <command...>
#
# Like assert_fails, but also asserts what the failure *said*. A rejection nobody can act on is
# barely better than a crash, so the message is part of the behaviour under test.
assert_rejected() {
    local needle="$1" desc="$2"; shift 3
    local output status
    output="$("$@" 2>&1)" && status=0 || status=$?
    if [ "$status" -eq 0 ]; then
        fail "$desc (command unexpectedly succeeded)"
    fi
    case "$output" in
        *"$needle"*) pass "$desc" ;;
        *) fail "$desc
      expected the error to mention: $needle
      actual error:                  $output" ;;
    esac
}
