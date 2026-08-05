#!/usr/bin/env bash
# Version 2: `founded` is an Int holding the year, bridged in both directions.
#
# A repo emits its whole schema on every push (§5.2); `borg repo push` diffs it against the
# definitions in force and turns the moved field into a `MutateField` naming these two producers.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Company", fields: [
        { name: "founded", type: "Int", up: "founded_up", down: "founded_down" },
        { name: "rating",  type: "Int" }
    ] } ] }'
    exit 0
fi

exit 0
