#!/usr/bin/env bash
# `decade` from `founded`: the first three digits of the ISO date, then `0s`.
#
# An ordinary pipeline, and that is the point. It was authored against the schema as it stood before
# the fork changed `founded`'s type, so it reads `founded` at *its own* def-version (§5.3) — which is
# still an ISO date, and still exactly the record its dependency was recorded against.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{
      structs: [ { name: "Company", fields: [
          { name: "decade", type: "String", derived_by: "decade" }
      ] } ],
      producers: [ { name: "decade", source: "Company" } ]
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

    get "$company.founded"
    [ -n "${CELL:-}" ] || { say '{"done":{}}'; continue; }

    set_cell "$company.decade" "${CELL:0:3}0s"
    say '{"done":{}}'
done
