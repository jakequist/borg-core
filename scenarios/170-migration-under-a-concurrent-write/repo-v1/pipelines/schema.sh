#!/usr/bin/env bash
# Version 1: `founded` is a String holding an ISO date, and `rating` is an ordinary Int nobody
# migrates. `rating` is here so a transaction has something to *write* that is not the field the
# migration is busy with — a guard has to be shown to fire on what was read, not on what was written.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Company", fields: [
        { name: "founded", type: "String" },
        { name: "rating",  type: "Int" }
    ] } ] }'
    exit 0
fi

exit 0
