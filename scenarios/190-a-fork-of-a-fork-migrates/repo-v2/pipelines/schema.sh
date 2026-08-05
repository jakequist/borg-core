#!/usr/bin/env bash
# Version 2 of the schema: `founded` is now an Int holding the year.
#
# A repo emits its **whole** schema on every push (§5.2), so this is not a diff — it is the shape the
# repo believes in now. `borg repo push` compares it with the definitions in force and turns a field
# whose type has moved into a `MutateField`, which §6.1 says must be accompanied by migrations. That
# is what `up` and `down` name: producers, by name, exactly as `derived_by` does.
#
# `down` is optional and is what keeps clients on the old version working (§5.4, §9.3). Omitting it
# is a decision to break them, not an oversight the system will paper over.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{ structs: [ { name: "Company", fields: [
        { name: "founded", type: "Int", up: "founded_up", down: "founded_down" }
    ] } ] }'
    exit 0
fi

exit 0
