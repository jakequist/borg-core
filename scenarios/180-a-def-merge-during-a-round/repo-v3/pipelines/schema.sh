#!/usr/bin/env bash
# `founded` moves again: from the year to the decade it fell in.
#
# The pair named here bridges v2 → v3. The pair that bridged v1 → v2 is not repeated: which two
# versions a migration joins is folded from the `MutateField` that appointed it, per branch (§5.3),
# and those events are already in the log.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Company", fields: [
        { name: "founded", type: "String", up: "decade_up", down: "decade_down" },
        { name: "decade",  type: "String", derived_by: "decade" }
    ] } ] }'
    exit 0
fi

exit 0
