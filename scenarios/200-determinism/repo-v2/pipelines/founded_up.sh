#!/usr/bin/env bash
# `up` for `Company.founded`: an ISO date at the old version becomes a year at the new one.
#
# A migration is a producer like any other (§9.1) — same wire protocol, same dependency capture, same
# watermark. The single difference is `get_input`: an ordinary `get` resolves at the producer's own
# ClientVersion, which for `up` is the version it *writes*, so asking for the cell it is migrating
# would recurse into the value it is supposed to be producing. `get_input` asks for the same cell at
# the version this migration reads from (§9.3).
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    # A migration declares only its name. Which field it bridges, and in which direction, comes from
    # the field that names it as `up` — one source of truth, so the two cannot disagree.
    jq -nc '{ migrations: [ { name: "founded_up" } ] }'
    exit 0
fi

say() { printf '%s\n' "$1"; }

IFS= read -r _server_hello
say '{"codec":"json"}'

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

    # A migration whose input is absent writes nothing — and the *negative* read is still recorded,
    # so a later write at the old version brings this invocation back round (§9.4).
    get_input "$company.founded"
    [ -n "$CELL" ] || { say '{"done":{}}'; continue; }

    set_cell "$company.founded" "${CELL%%-*}"
    say '{"done":{}}'
done
