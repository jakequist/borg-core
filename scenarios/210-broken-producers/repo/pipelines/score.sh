#!/usr/bin/env bash
# The working build of `score`: reads a headcount, writes a risk band.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{
      structs: [
        { name: "Company", fields: [
            { name: "headcount", type: "Int" },
            { name: "risk",      type: "String", derived_by: "score" }
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
    get "$company.headcount"
    if [ -n "$CELL" ] && [ "$CELL" -gt 10 ]; then risk=low; else risk=high; fi
    set_cell "$company.risk" "$risk"
    say '{"done":{}}'
done
