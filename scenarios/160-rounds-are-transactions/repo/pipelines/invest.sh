#!/usr/bin/env bash
# The head of the chain: `is_investible` from `headcount`.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{
      structs: [
        { name: "Company", fields: [
            { name: "headcount",     type: "Int" },
            { name: "is_investible", type: "Bool", derived_by: "invest" }
        ]}
      ],
      producers: [ { name: "invest", source: "Company" } ]
    }'
    exit 0
fi

say() { printf '%s\n' "$1"; }

IFS= read -r _server_hello
say '{"codec":"json"}'

get() {
    say "$(jq -nc --arg c "$1" '{get: $c}')"
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
    get "$company.headcount"
    headcount="${CELL:-0}"
    if [ "$headcount" -ge 10 ]; then investible=true; else investible=false; fi
    set_cell "$company.is_investible" "$investible"
    say '{"done":{}}'
done
