#!/usr/bin/env bash
# `band`, computing a different function. This is the scenario's control: a value in the store whose
# stated watermark no longer reproduces it.
#
# It is not a bug being simulated — it is the *symptom* of one. An ordering bug, a watermark advanced
# by an inline run, a ceiling that admitted somebody else's layer: each leaves a stored value that
# replaying its own watermark does not produce, and so does swapping the code that produced it. The
# check cannot tell the two apart, which is the point — it means the check is measuring the
# disagreement itself and not any particular cause of one.
#
# Identical to `repo-v1/pipelines/band.sh` in every respect but the words it writes.
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
        band=UNKNOWN
    elif [ "$employees" -ge 50 ]; then
        band=BIG
    elif [ "$employees" -ge 10 ]; then
        band=MID
    else
        band=SMALL
    fi

    set_cell "$company.band" "$band"
    say '{"done":{}}'
done
