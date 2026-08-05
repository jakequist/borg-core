#!/usr/bin/env bash
# `founded` before it moves. It is a source field with no producer of its own until the second push
# appoints migrations for it, which is what puts a migration into the same rounds as the chain.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Company", fields: [
        { name: "founded", type: "String" }
    ] } ] }'
    exit 0
fi

exit 0
