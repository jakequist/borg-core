#!/usr/bin/env bash
# `Company.score` is `Company.arr`, copied. A pipeline that computes nothing is the point: any
# difference between the two cells below is invalidation having failed, and cannot be anything else.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{
      structs: [
        { name: "Company", fields: [
            { name: "arr",   type: "Int" },
            { name: "score", type: "Int", derived_by: "score" }
        ]}
      ],
      producers: [ { name: "score", source: "Company" } ]
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
    get "$company.arr"
    set_cell "$company.score" "${CELL:-0}"
    say '{"done":{}}'
done
