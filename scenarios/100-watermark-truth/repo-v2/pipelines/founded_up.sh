#!/usr/bin/env bash
# `up` for `Company.founded`: the ISO date a v1 client wrote becomes the year a v2 client reads.
#
# It matters to this scenario that a migration is a producer like any other (§9.1) — same watermark,
# same dependency capture, same derived layer. Its output is derived data, so §10.1's claim covers it
# too, and `rating` reading that output means one recomputation has to cross a def-version to be
# right.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ migrations: [ { name: "founded_up" } ] }'
    exit 0
fi

say() { printf '%s\n' "$1"; }

IFS= read -r _server_hello
say '{"codec":"json"}'

# The version this migration reads *from*. An ordinary `get` resolves at the version it writes, which
# for the cell it is migrating would be asking for its own output (§9.3).
get_input() {
    say "$(jq -nc --arg c "$1" '{get_input: $c}')"
    IFS= read -r reply
    CELL="$(jq -r '.value // empty' <<<"$reply")"
}

set_cell() {
    say "$(jq -nc --arg c "$1" --arg v "$2" '{set: {cell: $c, value: $v}}')"
    IFS= read -r _ack
}

while IFS= read -r msg; do
    case "$(jq -r 'keys[0]' <<<"$msg")" in
        shutdown) exit 0 ;;
        invoke) ;;
        *) continue ;;
    esac

    company="$(jq -r '.invoke.input' <<<"$msg")"

    get_input "$company.founded"
    [ -n "$CELL" ] || { say '{"done":{}}'; continue; }

    set_cell "$company.founded" "${CELL%%-*}"
    say '{"done":{}}'
done
