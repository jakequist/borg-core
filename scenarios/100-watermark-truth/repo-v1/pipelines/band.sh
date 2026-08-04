#!/usr/bin/env bash
# The head of the chain: `Company.band` from `Company.employees`.
#
# Deliberately a pure function of one input and nothing else, because it is what `rating` reads. A
# producer whose output another producer consumes is the case where "recompute at layer W" is easy to
# get wrong — the second hop can quietly read the first hop's *inherited* answer instead of the one
# just recomputed — so the chain exists to make that mistake visible.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{
      structs: [
        { name: "Company", fields: [
            { name: "employees", type: "Int" },
            { name: "band",      type: "String", derived_by: "band" }
        ]}
      ],
      producers: [ { name: "band", source: "Company" } ]
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

    get "$company.employees"
    employees="${CELL:-}"

    # An entity with no headcount still gets a band. Every entity therefore has a value at every
    # derived field, which is what lets the scenario sweep a rectangle of cells rather than having to
    # know which ones happen to exist.
    if [ -z "$employees" ]; then
        band=unknown
    elif [ "$employees" -ge 50 ]; then
        band=large
    elif [ "$employees" -ge 10 ]; then
        band=mid
    else
        band=small
    fi

    set_cell "$company.band" "$band"
    say '{"done":{}}'
done
