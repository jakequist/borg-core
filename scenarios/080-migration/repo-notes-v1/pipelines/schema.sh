#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Note", fields: [
        { name: "tag", type: "String" }
    ] } ] }'
    exit 0
fi

exit 0
