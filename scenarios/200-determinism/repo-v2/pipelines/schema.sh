#!/usr/bin/env bash
# `founded` moves from an ISO date to the year, in both directions.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Company", fields: [
        { name: "founded", type: "Int", up: "founded_up", down: "founded_down" }
    ] } ] }'
    exit 0
fi

exit 0
