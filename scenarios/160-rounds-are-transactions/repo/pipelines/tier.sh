#!/usr/bin/env bash
# The tail of the chain, and the whole point of the scenario: it reads a cell **its own round
# writes**. The round's guards are its producers' read-sets, and if `is_investible` were guarded,
# this hop would be rejected every single time and the field would simply never appear.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{
      structs: [
        { name: "Company", fields: [
            { name: "tier", type: "String", derived_by: "tier" }
        ]}
      ],
      producers: [ { name: "tier", source: "Company" } ]
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

    # `is_investible` may legitimately be absent on the first pass: both hops are dirtied by the
    # same source layer and this one can win the race. The fixpoint fixes that — the read of an
    # absent cell is a recorded dependency, so the upstream's layer brings this back round.
    get "$company.is_investible"
    case "${CELL:-}" in
        true)  tier=core ;;
        false) tier=watch ;;
        *)     tier=pending ;;
    esac

    set_cell "$company.tier" "$tier"
    say '{"done":{}}'
done
