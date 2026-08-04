#!/usr/bin/env bash
# The tail of the chain: `Company.rating` from one source field, one pipeline's output, and one field
# that a migration will later own.
#
# Its three inputs are three different kinds of provenance on purpose:
#
#   * `arr`      — source data, written by a client.
#   * `band`     — another producer's output, so this run cannot start from ground truth alone.
#   * `founded`  — after the schema moves, a *migration's* output, so the chain crosses a def-version
#                  as well as a producer.
#
# The output is the three concatenated, which makes a wrong answer say which input was wrong.
#
# Version 2 of the repo: `founded` is now an Int holding the year, and names `founded_up` as the
# migration that gets the existing dates there. Nothing else about this script changes — the type
# moved underneath it, and it still just reads the cell.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{
      structs: [
        { name: "Company", fields: [
            { name: "arr",     type: "Int" },
            { name: "founded", type: "Int", up: "founded_up" },
            { name: "rating",  type: "String", derived_by: "rating" }
        ]}
      ],
      producers: [ { name: "rating", source: "Company" } ]
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

    # `band` may legitimately be absent when this runs: a round schedules both hops from the same
    # source layer, and this one can win the race. The engine's fixpoint is what fixes it — this
    # run's read of an absent cell is recorded as a dependency, so `band`'s layer brings it back
    # round in the next wave (§9.4, §16.5). Writing `-` rather than failing is what makes that a
    # re-run instead of a broken producer.
    get "$company.band"
    band="${CELL:--}"

    get "$company.arr"
    arr="${CELL:-0}"

    # Read at this producer's own ClientVersion, which is the branch's current view — so once the
    # schema moves this is the migrated Int and not the date the client wrote.
    get "$company.founded"
    founded="${CELL:--}"

    set_cell "$company.rating" "$band/$arr/$founded"
    say '{"done":{}}'
done
