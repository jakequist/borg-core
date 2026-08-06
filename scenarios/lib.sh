# Shared harness for CLI scenarios.
#
# Every scenario runs the real `borg` binary against a throwaway store. If a scenario passes, the
# devex it describes actually works — no mocks, no in-process shortcuts.

set -euo pipefail

BORG_BIN="${BORG_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/target/debug/borg}"
# The server is a separate binary — a process that stays up and a process that exits after one
# command are opposite lifecycles, not two modes of one thing (SPEC.md §17.6). Scenarios that need
# one build it the same way they build the client.
BORG_SERVER_BIN="${BORG_SERVER_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/target/debug/borg-server}"
# Scenarios capture their own directory before `setup` moves to a scratch store.

setup() {
    WORK="$(mktemp -d)"
    trap 'rm -rf "$WORK"' EXIT
    cd "$WORK"
    # Which store `borg` operates on. A variable rather than a constant because a scenario that
    # drives a *server* wants a registry under a data directory instead — see 250 and 300.
    STORE="$WORK/borg.db"
    borg init >/dev/null
}

borg() { "$BORG_BIN" --store "${STORE:-${WORK:-.}/borg.db}" "$@"; }

# Wait until something is answering on a unix socket.
#
# A socket file exists a moment before anything is listening on it, and a scenario that races that
# is a scenario that fails one run in forty. `borg-server start` already waits before it returns;
# this is for the `--foreground` case, where the caller is the supervisor.
wait_for_socket() {
    local socket="$1" attempt
    for attempt in $(seq 200); do
        if "$BORG_SERVER_BIN" --data-dir "${2:-$WORK/data}" --socket "$socket" status >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

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

# assert_derives <count> <description> [borg args...]
#
# Run a round and assert how many invocations it took. `0` is the settled case, and it is a stronger
# claim than `borg derive status --outstanding` makes: that one queries the frontier, this one runs
# the round and finds it had nothing to do — which is what "the branch is a fixpoint" means.
#
# Here rather than spelled out seventeen times because the spelling is the part that keeps changing:
# knowing that the bare count comes from `--quiet` is one fact, and it now lives in one place.
assert_derives() {
    local want="$1" desc="$2"; shift 2
    assert_eq "$(borg derive --quiet "$@")" "$want" "$desc"
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
