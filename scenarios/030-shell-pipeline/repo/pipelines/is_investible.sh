#!/usr/bin/env bash
# A Borg pipeline, in bash.
#
# There is no client library here — just `read`, `jq` and `echo`. If this is workable, the protocol
# is genuinely simple and genuinely language-neutral.
set -euo pipefail

# `describe` runs once at push time: the script declares what it is and what it maps over, and the
# server turns that into a PushProducer def event.
if [ "${1:-}" = "describe" ]; then
    jq -nc '{producers: [{name: "invest", source: "Company"}]}'
    exit 0
fi

# Otherwise: read messages until told to stop. The worker holds no state between invocations, so the
# server may spawn, terminate and parallelise these at will.
say() { printf '%s\n' "$1"; }

# Ask for one cell and read back its value.
get() {
    say "$(jq -nc --arg c "$1" '{get: $c}')"
    IFS= read -r reply
    jq -rc '.value // empty' <<<"$reply"
}

while IFS= read -r msg; do
    case "$(jq -r 'keys[0]' <<<"$msg")" in
        shutdown) exit 0 ;;
        invoke) ;;
        *) continue ;;
    esac

    company="$(jq -r '.invoke.input' <<<"$msg")"

    score=0
    website="$(get "$company.website")"
    if [ -n "$website" ] && [ "$website" -gt 3 ]; then
        score=$((score + 6))
    fi

    investible=$([ "$score" -gt 5 ] && echo true || echo false)
    say "$(jq -nc --arg c "$company.is_investible" --argjson v "$investible" '{set: {cell: $c, value: $v}}')"
    IFS= read -r _ack

    say '{"done":true}'
done
