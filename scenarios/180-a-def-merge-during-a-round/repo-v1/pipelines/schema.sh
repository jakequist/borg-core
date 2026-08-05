#!/usr/bin/env bash
# `founded` is an ISO date; `decade` is derived from it by the pipeline next door.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Company", fields: [
        { name: "founded", type: "String" },
        { name: "decade",  type: "String", derived_by: "decade" }
    ] } ] }'
    exit 0
fi

exit 0
