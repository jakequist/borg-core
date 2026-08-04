#!/usr/bin/env bash
# A Borg pipeline, in bash.
#
# There is no client library here — just `read`, `jq` and `echo`. If this is workable, the protocol
# is genuinely simple and genuinely language-neutral.
#
# Note that stdout carries the protocol. Anything printed for a human goes to stderr.
set -euo pipefail

# `describe` runs once at push time: the script declares what it implements and what it maps over,
# and the server turns that into a PushProducer def event. A producer definition therefore cannot
# exist without the code that satisfies it.
if [ "${1:-}" = "describe" ]; then
    jq -nc '{producers: [{name: "invest", source: "Company"}]}'
    exit 0
fi

# Every protocol message is a single-key object, so dispatching is always `keys[0]`.
say() { printf '%s\n' "$1"; }

# Handshake. We speak JSON; the newline framing is what makes `read` sufficient.
IFS= read -r _server_hello
say '{"codec":"json"}'

# Ask for one cell; the answer lands in $CELL.
#
# Note it does *not* echo the result. A request/response protocol on stdout cannot be driven from
# inside `$( )`, because command substitution captures the request instead of sending it. That is a
# property of the shell, not of the protocol — but it is the kind of thing only writing a real
# worker tells you.
get() {
    say "$(jq -nc --arg c "$1" '{get: $c}')"
    IFS= read -r reply
    CELL="$(jq -r '.value // empty' <<<"$reply")"
}

# Write one cell and wait for the acknowledgement.
set_cell() {
    say "$(jq -nc --arg c "$1" --arg v "$2" '{set: {cell: $c, value: $v}}')"
    IFS= read -r _ack
}

while IFS= read -r msg; do
    case "$(jq -r 'keys[0]' <<<"$msg")" in
        shutdown) exit 0 ;;
        invoke) ;;
        *) continue ;;
    esac

    company="$(jq -r '.invoke.input' <<<"$msg")"

    score=0

    # A string arrives as its content — `acme.ai`, not `@s-1a2b3c`. Strings are content-addressed
    # and interned (§3.1), but that is the engine's business: this script never sees a PID, never
    # asks a second time to resolve one, and would work identically if interning did not exist.
    get "$company.website"
    website="$CELL"
    case "$website" in
        *.ai) score=$((score + 6)) ;;
    esac

    # …and a number arrives as a number. One text form, several types behind it.
    get "$company.headcount"
    headcount="$CELL"
    if [ -n "$headcount" ] && [ "$headcount" -gt 10 ]; then
        score=$((score + 2))
    fi

    # Deliberately needs both: either field moving can flip the answer, which is what makes
    # field-granular invalidation worth demonstrating.
    if [ "$score" -ge 7 ]; then investible=true; else investible=false; fi
    set_cell "$company.is_investible" "$investible"

    say '{"done":{}}'
done
