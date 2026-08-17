#!/usr/bin/env bash
# **Static API keys: what it takes to put a server on the internet.** SPEC.md §17.6, §17.7.
#
# 300 gave a server a directory of registries, 310 gave a client one string to be configured with,
# and 330 gave both a second transport. Every one of them assumed anybody who could reach the socket
# was welcome. This is the assumption being retired — deliberately minimally, and shaped so that the
# platform's signed tokens replace the lookup without a wire change (`ROADMAP.md`, *The production
# arc*): the field a credential travels in was reserved two milestones ago and does not move.
#
# The claims:
#
#   * **no keys file means an open server**, and `status` says so. This is the zero-ceremony case a
#     laptop lives in, and it is asserted first because it is the one a regression would hide in;
#   * **the first `keygen` flips the server to enforcing**, without restarting it or telling it
#     anything — `keygen` writes a file and the handshake re-reads it, which is also the only way
#     minting the *first* credential could have worked at all;
#   * **the old url now fails with `credential required`**, and the refusal names no registry: an
#     unauthenticated caller learns nothing about what this server hosts, so a public address is not
#     a tenant enumerator;
#   * **the keyed url works end to end** — a transaction through commit, over the url and over
#     `$BORG_TOKEN`;
#   * **a key scoped to one registry cannot reach the other, and cannot see it either**;
#   * **revoke, and the next connection fails** while `status` keeps answering — because the server
#     administers itself with a token it minted, rather than the unix socket being exempt, which
#     would make the two transports mean different things;
#   * **the key is never anywhere but the one line that printed it**: not in the file, not in
#     `status`, not in `keys list`, not in the server's log, and not in the error a bad url produces.
#
# *Failing means either that a deployed server can be reached without a credential, or that a local
# one now needs configuration to start — and this feature is worth having only if both stay false.*
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

DATA="$WORK/data"
SOCK="$WORK/borg.sock"
server() { "$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" "$@"; }

trap 'server stop >/dev/null 2>&1 || true; rm -rf "$WORK"' EXIT

# Two registries, so that "scoped to one" is a claim about which store answered rather than about
# there being only one.
server create crm >/dev/null
server create analytics >/dev/null
cat >"$WORK/schema.json" <<'JSON'
{"repo": 1, "events": [{"DeclareField": {"struct_name": "Company", "field": "headcount", "ty": "Int"}}]}
JSON
for registry in crm analytics; do
    "$BORG_BIN" --store "$DATA/$registry/borg.db" def push "$WORK/schema.json" >/dev/null
done

CRM_URL="borg+unix://$SOCK/crm"
ANALYTICS_URL="borg+unix://$SOCK/analytics"

# ── An open server, which is what every server was until this scenario ────────────────────────────
#
# *Failing means `borg-server start` on a laptop has grown configuration, which is the cost this
# feature is not allowed to have.*
server start >/dev/null
assert_contains "$(server status)" "auth   open" \
    "a server with no keys file reports itself open…"
assert_contains "$(server status)" "keygen" \
    "…and says what would change that"
"$BORG_BIN" --url "$CRM_URL" generate --lang ts -o "$WORK/gen" >/dev/null
pass "and a client with no credential reaches it, exactly as before"

assert_contains "$(server keys)" "no keys" "keys on an open server says there are none"

# ── The first keygen flips it, with the server still running ──────────────────────────────────────
#
# `keygen` writes a file in the data directory and never speaks to the socket. That is not an
# implementation detail: minting the first credential over a connection that already requires one is
# a circle, and the way out is that the two commands share a filesystem rather than a protocol. The
# running server picks it up because every handshake re-reads the file — which is the same mechanism
# revocation needs.
#
# *Failing means enforcement requires a restart, and every key rotation is an outage.*
KEY="$(server keygen app)"
SCOPED="$(server keygen crm-only --registries crm)"

case "$KEY" in
    borgk_*) pass "a key is issued, prefixed so a secret scanner and a human both notice it" ;;
    *) fail "a key should be prefixed borgk_, got: $KEY" ;;
esac
assert_contains "$(server status)" "auth   api key required" \
    "the server now reports itself as requiring a key, without having been restarted"
assert_contains "$(server status)" "2 keys issued" "…and how many are issued"

# ── The old url fails, and says nothing else ──────────────────────────────────────────────────────
#
# The refusal happens at the handshake, so it lands where the connection was configured. What it must
# *not* contain is a registry name: a caller that could not authenticate has just failed to prove it
# may know anything about what this server hosts.
#
# *Failing means a public borg address enumerates its tenants for anyone who connects.*
assert_rejected 'requires a credential' "the url that worked a moment ago is now refused…" \
    -- "$BORG_BIN" --url "$CRM_URL" generate --lang ts -o "$WORK/gen"

refusal="$("$BORG_BIN" --url "$CRM_URL" generate --lang ts -o "$WORK/gen" 2>&1 || true)"
case "$refusal" in
    *analytics*|*crm*) fail "the refusal named a registry to an unauthenticated caller: $refusal" ;;
    *) pass "…naming no registry, because nothing has been authenticated yet" ;;
esac

assert_rejected 'not valid' "a key nobody issued is refused…" \
    -- "$BORG_BIN" --url "borg+unix://:borgk_nope@$SOCK/crm" generate --lang ts -o "$WORK/gen"
wrong="$("$BORG_BIN" --url "borg+unix://:borgk_nope@$SOCK/crm" generate --lang ts -o "$WORK/gen" 2>&1 || true)"
case "$wrong" in
    *analytics*) fail "a wrong credential learned what is hosted: $wrong" ;;
    *) pass "…and learns nothing about what is hosted either" ;;
esac

# ── The keyed url works, end to end ───────────────────────────────────────────────────────────────
KEYED_CRM="borg+unix://:$KEY@$SOCK/crm"
"$BORG_BIN" --url "$KEYED_CRM" generate --lang ts -o "$WORK/gen" >/dev/null
assert_contains "$(cat "$WORK/gen/borg.generated.ts")" "export interface Company" \
    "the same url with a key in its userinfo reaches the registry it names"

# A transaction through commit, over a hand-written §17.5 client, because that is the shape a
# deployed application has: connect, greet with a credential, write, commit. Eighty lines of python
# and no dependencies (`scenarios/250-serve`'s client.py is the same idea).
if command -v python3 >/dev/null 2>&1; then
    committed="$(python3 "$HERE/client.py" "$SOCK" crm "$KEY" 41)"
    assert_contains "$committed" "landed=L" "a credentialed transaction commits…"
    assert_eq "$(field "$committed" value)" "41" "…and the write is there afterwards"

    # The environment variable, which is how a deployment carries a key that is not in a url.
    committed="$(BORG_TOKEN="$KEY" python3 "$HERE/client.py" "$SOCK" crm "" 42)"
    assert_eq "$(field "$committed" value)" "42" \
        "a key presented from \$BORG_TOKEN gets in exactly as one in a url does"
else
    echo "  ⚠ SKIPPED: python3 is not installed — the commit half needs an eighty-line client" >&2
fi

# ── A scoped key reaches one registry and cannot see the other ────────────────────────────────────
#
# Two properties, and the second is the one that takes thought: refusing the connection is obvious,
# and making an out-of-scope registry *indistinguishable from one that does not exist* is what stops
# a scoped key being a tenant enumerator.
#
# *Failing means a deploy key for staging can enumerate — or reach — production.*
"$BORG_BIN" --url "borg+unix://:$SCOPED@$SOCK/crm" generate --lang ts -o "$WORK/gen" >/dev/null
pass "a key scoped to one registry reaches that registry"

assert_rejected 'analytics' "…and cannot reach the other…" \
    -- "$BORG_BIN" --url "borg+unix://:$SCOPED@$SOCK/analytics" generate --lang ts -o "$WORK/gen"
outside="$("$BORG_BIN" --url "borg+unix://:$SCOPED@$SOCK/analytics" generate --lang ts -o "$WORK/gen" 2>&1 || true)"
case "$outside" in
    *crm*) fail "the refusal named what else the key reaches, or what else exists: $outside" ;;
    *) pass "…and the refusal says nothing about what else there is" ;;
esac

# ── Revoke, and the next connection fails ─────────────────────────────────────────────────────────
#
# Written to the same file, read by the next handshake. Connections already open are deliberately
# *not* torn down — see `borg_host::keys` for why that trade was taken.
server keys revoke crm-only >/dev/null
assert_rejected 'not valid' "a revoked key is refused by the next connection" \
    -- "$BORG_BIN" --url "borg+unix://:$SCOPED@$SOCK/crm" generate --lang ts -o "$WORK/gen"
"$BORG_BIN" --url "$KEYED_CRM" generate --lang ts -o "$WORK/gen" >/dev/null
pass "and the key that was not revoked still works"

listed="$(server keys)"
assert_contains "$listed" "app" "keys list names the live key…"
case "$listed" in
    *crm-only*) fail "a revoked key is still listed: $listed" ;;
    *) pass "…and not the revoked one" ;;
esac

# ── status names the mode throughout, and administers itself ──────────────────────────────────────
#
# `status`, `create`, `export` and `import` are clients of the server they administer. Enforcement
# would lock them out, and the tempting fix — exempting the unix socket — would make unix and
# websocket semantically different. So a running server mints a `*`-scoped token into its data
# directory and its own commands present it like any other credential.
#
# *Failing means either an operator cannot administer an authed server, or the local transport has
# quietly become the one nothing is checked on.*
assert_contains "$(server status)" "auth   api key required" \
    "status keeps answering against an enforcing server"
assert_contains "$(server status)" "crm" "…and still lists what is hosted"
server create later >/dev/null
assert_contains "$(server status)" "later" "…and create still works, through the server"

# **The admin path is a credential, not an exemption.** Take the token away and the same command is
# refused like anybody else — which is what proves the unix socket is not being waved through, and
# is the assertion that would fail if somebody "simplified" this by exempting the local transport.
#
# It also shows what the admin token *is*: the file's contents, not a second kind of authority. A
# token whose file is gone is not a key, so the same command has to fall back to a real one.
mv "$DATA/borg-server.admin" "$WORK/admin.saved"
assert_contains "$(server status)" "would not answer" \
    "without the admin token, status is refused like anybody else — the socket is not exempt"
assert_contains "$(BORG_TOKEN="$KEY" server status)" "registries:" \
    "and an ordinary key in \$BORG_TOKEN is how a command reaches a server it cannot read a token beside"
mv "$WORK/admin.saved" "$DATA/borg-server.admin"

# ── The key is nowhere but the line that printed it ───────────────────────────────────────────────
#
# Grepped rather than argued. Plaintext exists once, in `keygen`'s stdout; everything else holds a
# digest or nothing.
#
# *Failing means a key is recoverable from a backup, a log or a bug report.*
for where in "$DATA/borg-server.keys.json" "$DATA/borg-server.log"; do
    if [ -f "$where" ] && grep -q -- "$KEY" "$where"; then
        fail "the key is in $where"
    fi
done
pass "the key is in neither the keys file nor the server log"

case "$(server status)$(server keys)" in
    *"$KEY"*) fail "the key is in status or keys output" ;;
    *) pass "…nor in anything status or keys prints" ;;
esac

# A url that is malformed *after* its credential, so the refusal is the parser's and quotes
# the url it was given back.
leaky="$("$BORG_BIN" --url "borg://:$KEY@localhost/a/b" generate --lang ts -o "$WORK/gen" 2>&1 || true)"
case "$leaky" in
    *"$KEY"*) fail "a url refusal quoted the key back: $leaky" ;;
    *"***@"*) pass "a url refusal redacts the key it quotes back" ;;
    *) fail "a url refusal should quote the redacted url, said: $leaky" ;;
esac

# ── Deleting the file reopens the server; emptying it does not ────────────────────────────────────
#
# The one asymmetry worth a scenario: revoking the last key leaves a locked door, and deleting the
# file leaves an open one. They are opposite operations and it must be impossible to reach the
# second by doing the first.
server keys revoke app >/dev/null
assert_rejected 'requires a credential' \
    "with every key revoked the server is closed, not open — the file is what says so" \
    -- "$BORG_BIN" --url "$CRM_URL" generate --lang ts -o "$WORK/gen"

rm "$DATA/borg-server.keys.json"
"$BORG_BIN" --url "$CRM_URL" generate --lang ts -o "$WORK/gen" >/dev/null
pass "and removing the file reopens the server, which is the only way back to open"
assert_contains "$(server status)" "auth   open" "…and status says so"

server stop >/dev/null
