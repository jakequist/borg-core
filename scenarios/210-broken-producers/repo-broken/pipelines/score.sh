#!/usr/bin/env bash
# A bad deploy of `score`: it describes itself exactly as the working build does — same schema, same
# producer name, so the same producer id — and then throws on every invocation.
#
# Each invocation is recorded in `./attempts` before it fails. The engine's cwd is the store's
# directory, so a scenario can count how many times this was actually asked to run, which is how
# "a broken producer is skipped" and "--retry-broken really retried" are told apart from the outside.
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

while IFS= read -r msg; do
    case "$(jq -r 'keys[0]' <<<"$msg")" in
        shutdown) exit 0 ;;
        invoke) ;;
        *) continue ;;
    esac

    echo attempt >> ./attempts
    # Reported over the protocol rather than by exiting, because a producer that raises on one
    # entity is not a broken process — the engine keeps the worker and poisons the producer.
    say '{"error":{"message":"score exploded: no risk model configured"}}'
done
