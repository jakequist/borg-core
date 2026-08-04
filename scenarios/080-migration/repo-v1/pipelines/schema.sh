#!/usr/bin/env bash
# Version 1 of the schema: `founded` is a String holding an ISO date.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Company", fields: [
        { name: "founded", type: "String" }
    ] } ] }'
    exit 0
fi

# This repo implements no producers, so nothing ever invokes it.
exit 0
