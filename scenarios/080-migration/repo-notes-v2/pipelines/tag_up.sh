#!/usr/bin/env bash
# `Note.tag` moves String → Int with an `up` and **no `down`**.
#
# That is a legitimate thing to push and a consequential one: values written after the change have no
# way back to the old version, so clients still on it are broken rather than merely behind. The
# read envelope says exactly that (§9.3, §10.4) instead of serving something plausible.
set -euo pipefail

if [ "${1:-}" = "describe" ]; then
    jq -nc '{
      structs: [ { name: "Note", fields: [ { name: "tag", type: "Int", up: "tag_up" } ] } ],
      migrations: [ { name: "tag_up" } ]
    }'
    exit 0
fi

say() { printf '%s\n' "$1"; }

IFS= read -r _server_hello
say '{"codec":"json"}'

while IFS= read -r msg; do
    case "$(jq -r 'keys[0]' <<<"$msg")" in
        shutdown) exit 0 ;;
        invoke) ;;
        *) continue ;;
    esac

    note="$(jq -r '.invoke.input' <<<"$msg")"

    say "$(jq -nc --arg c "$note.tag" '{get_input: $c}')"
    IFS= read -r reply
    tag="$(jq -r '.value // empty' <<<"$reply")"

    if [ -n "$tag" ]; then
        say "$(jq -nc --arg c "$note.tag" --arg v "$tag" '{set: {cell: $c, value: $v}}')"
        IFS= read -r _ack
    fi
    say '{"done":{}}'
done
