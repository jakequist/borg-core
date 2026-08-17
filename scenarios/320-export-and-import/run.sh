#!/usr/bin/env bash
# **Export and import: the data, not the bytes.** SPEC.md §19.
#
# The format policy is a promise to customers, so it has to be a promise the binaries keep and not
# one a module keeps: *every release writes a registry out as a canonical event stream and reads
# streams from earlier releases back in, and an upgrade is export → upgrade → import.* One mechanism
# doing four jobs — backup, restore, format migration, clone.
#
# What has to be true here:
#
#   * a restored registry answers the questions the original answers — values, provenance,
#     definitions and lineage, not just values;
#   * **the counter survives**, so an object created after a restore cannot land on the address of
#     one that existed before it. `borg.allocations.json` is the one sidecar a store cannot recover
#     from, and this stream is its backup story;
#   * a restored registry **runs**: the producer table came across, so the next write derives;
#   * exporting a restore reproduces the stream byte for byte, which is the cheap total check;
#   * a stream that is not one is refused by line number, and one from a format version this binary
#     does not know is refused naming both versions;
#   * a **served** registry exports live, through the server holding it, without stopping it.
#
# *Failing means the format policy is aspirational: on-disk formats would be frozen in practice,
# because there would be no supported way off them.*
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../lib.sh"
setup

copy() { "$BORG_BIN" --store "$WORK/copy.db" "$@"; }

# --- A registry with something in it ---------------------------------------------------------------

borg repo push "$HERE"/../030-shell-pipeline/repo >/dev/null
borg set 'Company#1.website' acme.ai >/dev/null
borg set 'Company#1.headcount' 40 >/dev/null
borg set 'Company#2.website' example.com >/dev/null
borg set 'Company#2.headcount' 40 >/dev/null

# An object the *server allocator* issued, which is what advances the counter. Its pid is what the
# restore must not hand out again.
tx="$(borg tx begin)"
allocated="$(borg tx create Company --tx "$tx")"
borg tx set "Company:$allocated.website" original.ai --tx "$tx" >/dev/null
borg tx commit --tx "$tx" >/dev/null

assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "the fixture has derived data before it is exported"

# --- Export, and restore into a store that does not exist yet ---------------------------------------

report="$(borg export "$WORK/backup.ndjson")"
assert_contains "$report" "the log ends at L" \
    "an export says where the log stood, because that is what it captured"

# One record per line, and the first is the header — the whole of what "line-oriented" buys is that
# these are checkable with `head` and `grep`.
assert_contains "$(head -1 "$WORK/backup.ndjson")" '"header"' \
    "the stream opens with a header carrying its format version and the binary that wrote it"
assert_contains "$(head -1 "$WORK/backup.ndjson")" '"version":1' \
    "…and the format version is what a future release checks before reading a byte of the rest"

copy import "$WORK/backup.ndjson" >/dev/null

# --- The restore answers the same questions ---------------------------------------------------------

# *Failing means the stream carries values and loses provenance, which is the half that cannot be
# reconstructed by writing the data again.*
for cell in 'Company#1.website' 'Company#1.headcount' 'Company#1.is_investible' \
            'Company#2.is_investible' "Company:$allocated.website"; do
    assert_eq "$(copy get "$cell")" "$(borg get "$cell")" \
        "$cell reads identically after a round trip, envelope and all"
done

assert_eq "$(copy explain 'Company#1.is_investible')" "$(borg explain 'Company#1.is_investible')" \
    "lineage is identical too — the read-sets are recorded facts and travel as data"
assert_eq "$(copy def show Company)" "$(borg def show Company)" \
    "and so are the definitions, which travelled the log like everything else"
assert_eq "$(copy branch list)" "$(borg branch list)" \
    "every branch, with the ids it had: they are referenced by layers and cannot be re-minted"

# --- Exporting the restore reproduces the stream ------------------------------------------------------

# *The cheapest total check there is.* It compares everything at once, including whatever nobody
# thought to assert above — and it works because the header carries no timestamp, no registry name
# and no path, so two exports differ only if the registries do.
copy export "$WORK/again.ndjson" >/dev/null
if ! cmp -s "$WORK/backup.ndjson" "$WORK/again.ndjson"; then
    diff "$WORK/backup.ndjson" "$WORK/again.ndjson" | head -20
    fail "a registry and its restore must export byte-identically"
fi
pass "exporting the restore reproduces the stream byte for byte"

# --- The counter survives ------------------------------------------------------------------------------

# *Failing means a restored registry re-issues ids it has already used, and a fresh object silently
# becomes an old one.* This is the reason `borg.allocations.json` is in the stream at all.
tx2="$(copy tx begin)"
fresh="$(copy tx create Company --tx "$tx2")"
copy tx set "Company:$fresh.website" brand.new --tx "$tx2" >/dev/null
copy tx commit --tx "$tx2" >/dev/null

[ "$fresh" = "$allocated" ] && fail "the restore re-issued $allocated, the pid it had already given out"
pass "an object created after the restore gets an id no pre-export object had"

assert_eq "$(copy get "Company:$allocated.website" --value)" "original.ai" \
    "…and the object that existed before the export is still exactly where it was"
assert_eq "$(copy get "Company:$fresh.website" --value)" "brand.new" \
    "…beside the new one, which is a different object"

# --- A restored registry runs ---------------------------------------------------------------------------

# *Failing means a restore is a museum piece.* Producer *definitions* travel the log; *implementations*
# are a sidecar naming a file on a machine (§9.2), so a stream without the table would restore a
# registry holding definitions it could not run.
assert_contains "$(copy producer list)" "invest" \
    "the producer table came across, so the restore knows where its pipeline's code lives"
copy set 'Company#1.headcount' 3 >/dev/null
assert_eq "$(copy get 'Company#1.is_investible' --value)" "false" \
    "and the next write derives: the restored registry runs its pipeline"
assert_eq "$(borg get 'Company#1.is_investible' --value)" "true" \
    "…without touching the registry it was restored from, which is a separate store"

# --- A stream that is not one --------------------------------------------------------------------------

# *Failing means a corrupt backup is discovered as a corrupt store.* A refusal that names the line is
# a place to look; a refusal that names both versions tells you which end to fix.
sed '1s/.*/{"header":{"version":9999,"binary":"borg 99.0.0"}}/' \
    "$WORK/backup.ndjson" >"$WORK/future.ndjson"
assert_rejected "9999" "a stream from a format version this binary cannot read is refused…" \
    -- "$BORG_BIN" --store "$WORK/future.db" import "$WORK/future.ndjson"
assert_rejected "version 1" "…naming both versions, so the reader knows which end to fix" \
    -- "$BORG_BIN" --store "$WORK/future2.db" import "$WORK/future.ndjson"

sed '4s/.*/{"branch":{"id":"not a number"}}/' "$WORK/backup.ndjson" >"$WORK/torn.ndjson"
assert_rejected "line 4" "a mangled line is refused by line number, which is what line-oriented buys" \
    -- "$BORG_BIN" --store "$WORK/torn.db" import "$WORK/torn.ndjson"

# *Failing means a failed restore leaves a file that looks like a store and is not one — and under a
# data directory, one the next `borg-server start` would discover and host.*
[ -e "$WORK/torn.db" ] && fail "a restore that could not finish left its half-written store behind"
[ -e "$WORK/future.db" ] && fail "a stream refused at its header still created a store"
pass "a restore that fails takes the store it created with it"

# *Failing means restore silently merges two id spaces, which would rewrite the lineage the stream
# exists to preserve.*
assert_rejected "already holds a registry" "importing into a registry that holds anything is refused" \
    -- copy import "$WORK/backup.ndjson"

# --- A pipe is a clone --------------------------------------------------------------------------------

# No file, or `-`, means stdout and stdin. That is the whole reason the summary goes to stderr when
# the stream goes to stdout: a report inside the backup would corrupt the artifact describing it.
borg export - >"$WORK/piped.ndjson" 2>/dev/null
cmp -s "$WORK/backup.ndjson" "$WORK/piped.ndjson" || fail "export to stdout must write the same stream"
"$BORG_BIN" --store "$WORK/piped.db" import - <"$WORK/piped.ndjson" >/dev/null
assert_eq "$("$BORG_BIN" --store "$WORK/piped.db" get 'Company#1.website' --value)" "acme.ai" \
    "a clone is one pipe: borg export | borg --store other.db import -"

# --- A served registry exports live -----------------------------------------------------------------------

DATA="$WORK/data"
SOCK="$WORK/borg.sock"
server() { "$BORG_SERVER_BIN" --data-dir "$DATA" --socket "$SOCK" "$@"; }

# Restore is create-then-import, and against a data directory nobody is serving it happens directly —
# the same pair `borg-server create` draws.
assert_contains "$(server import crm "$WORK/backup.ndjson")" "restored registry crm" \
    "a registry is created and filled in one operation, so it is never briefly empty and routable"

server start >/dev/null
assert_contains "$(server status)" "crm" "the server hosts the registry that was restored into it"

# *Failing means backing up a live registry means stopping it.* The export runs under that registry's
# own gate, so nothing commits while it walks — a snapshot of the whole log at one instant, with no
# snapshot machinery anywhere.
live="$(server export crm "$WORK/live.ndjson")"
assert_contains "$live" "through the server" \
    "a served registry exports through the server that is holding it, rather than behind its back"
if ! cmp -s "$WORK/backup.ndjson" "$WORK/live.ndjson"; then
    diff "$WORK/backup.ndjson" "$WORK/live.ndjson" | head -20
    fail "a live export of a restored registry must reproduce the stream it was restored from"
fi
pass "a live export reproduces the stream the registry was restored from"

# The registry name is optional against a one-registry server, exactly as it is for `status` and in a
# connection url — one rule (§17.6), not a second opinion here.
server export "$WORK/unnamed.ndjson" >/dev/null
cmp -s "$WORK/backup.ndjson" "$WORK/unnamed.ndjson" \
    || fail "naming no registry against a one-registry server must export that registry"
pass "a one-registry server needs no name, here as everywhere else"

# And a registry that already exists is refused rather than overwritten, whoever is asked.
assert_rejected "already exists" "importing over a registry that exists is refused by the server too" \
    -- server import crm "$WORK/backup.ndjson"

server stop >/dev/null
