#!/usr/bin/env bash
# `up` for the second step of `founded`'s chain: a year becomes the decade it fell in.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ migrations: [ { name: "decade_up" } ] }'
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
    [ -n "${CELL:-}" ] || { say '{"done":{}}'; continue; }

    set_cell "$company.founded" "${CELL:0:3}0s"
    say '{"done":{}}'
done
