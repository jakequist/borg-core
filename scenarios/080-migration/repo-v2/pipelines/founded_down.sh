#!/usr/bin/env bash
# `down` for `Company.founded`: a year at the new version becomes an ISO date at the old one.
#
# This is what keeps an old client working after the schema moved (§5.4). It is the exact mirror of
# `up` — including `get_input`, which for a `down` migration means "the newer version I read from".
# One verb, whichever way a migration runs.
#
# v1 trusts `down` (§9.3): the January 1st this invents is not checked against anything.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ migrations: [ { name: "founded_down" } ] }'
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

    get_input "$company.founded"
    [ -n "$CELL" ] || { say '{"done":{}}'; continue; }

    set_cell "$company.founded" "$CELL-01-01"
    say '{"done":{}}'
done
