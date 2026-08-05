#!/usr/bin/env bash
# The schema change this fork carries: `founded` becomes an Int holding the year, bridged both ways.
#
# `decade` is repeated unchanged, because a repo emits its whole schema every push (§5.2) and a
# field whose type has not moved is a repeat rather than a mutation.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Company", fields: [
        { name: "founded", type: "Int", up: "founded_up", down: "founded_down" },
        { name: "decade",  type: "String", derived_by: "decade" }
    ] } ] }'
    exit 0
fi

exit 0
