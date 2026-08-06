# personal-crm

The first application built on Borg. Contacts: make one, list them, look at one.

It is deliberately the smallest app that is still an app, because the point is not the CRM. The
point is that a real thing — a server, a client, a schema, a browser, and data somebody minds losing
— had to be assembled out of what Borg actually ships, and **[`FRICTION.md`](FRICTION.md) is the
record of every place that was awkward**. Read that one first if you are here to improve Borg.

## What makes it a Borg app rather than a CRUD app

One field: `displayName`. Nothing in `api/` or `ui/` ever computes it. It is declared
`borg.string().derived()` and owned by the `display_name` pipeline in `repo/`, which means:

- a client write to it is refused by the engine (§8) and **does not compile** against the generated
  types — `displayName` is `readonly` in `api/gen/borg.generated.ts`;
- editing a contact's `lastName` re-runs that one contact's invocation, because the engine recorded
  which cells the pipeline body read — and editing `phone`, which the pipeline never reads, re-runs
  nothing;
- reading it answers a **provenance envelope**, not a value. The detail view prints the envelope: is
  this current, what produced it, and how far behind might it be (§10.4, invariant 8).

That last one is the whole reason the detail page has a box at the bottom that a normal CRM does not.

## Running it

```
./dev.sh              # boots everything, keeps your data
./dev.sh --reset      # …after deleting the store
./dev.sh --no-ui      # api only, on :8787
```

Needs `node` 22.18+ (it runs `.ts` files by stripping types), `pnpm`, and a `cargo build -p
borg-cli` — `dev.sh` does the last one for you if `target/debug/borg` is missing.

Then: <http://localhost:5173>.

### What dev.sh does, and why in that order

1. builds `borg` and `packages/borg-sdk` if either is stale, and links the SDK into `repo/` and
   `api/` (it is not published, so it is linked the way every scenario links it);
2. `borg init`, if there is no store yet;
3. **`borg repo push` — while nothing is serving.** A served store refuses every other `borg`
   invocation by name (§17.5), and `repo push` reads a directory off this machine's disk, so it is
   not on the socket and never can be as it stands. *Pushing a schema means stopping the server.*
   That constraint is why this is step 3 and `serve` is step 4, and it is why changing the schema
   means re-running this script rather than typing one command;
4. `borg serve --socket data/borg.sock`;
5. `borg generate --lang ts -o api/gen` — which reads **through the socket**, because `generate` is
   the one command that connects to a served store instead of being turned away by it;
6. the api, then vite.

It is re-runnable. The repo push is skipped when the repo's files have not changed, because an
unchanged `repo push` still emits a new def layer (FRICTION #2).

## Layout

```
repo/     the Borg repo: the Contact struct and the display_name pipeline, in the TS DSL
api/      node:http over borg-sdk/client. Zero runtime dependencies beyond the SDK
api/gen/  borg generate's output. Not committed — see below
ui/       vite + react. No state library, no CSS framework, three views
data/     the store, the socket, the logs. Not committed
```

### Why `api/gen/` is not in git

A generated module bakes in `CLIENT_VERSION` — the def-layer of the store it was generated from
(§5.4). That is a fact about one machine's store and not about this source tree: on a fresh clone it
is `L1`, on a store that has been re-pushed a few times it is `L4`. Committing it would commit one
person's layer number as though it were part of the program. `dev.sh` writes it before the api
starts.

The cost is real and is logged: a fresh clone cannot typecheck `api/` until `dev.sh` has run once.

### Typechecking the api

```
cd packages/borg-sdk && pnpm exec tsc -p ../../examples/personal-crm/api/tsconfig.json
```

(`tsc` comes from the SDK, which already depends on it; the api itself has no devDependencies.)

## The api

```
GET  /api/health          the branch, its head, and the def-version this client was generated at
GET  /api/contacts        list → and then two reads per contact. The N+1, left visible
POST /api/contacts        one transaction: begin, create, set each field, commit
GET  /api/contacts/:id    every field with its envelope, plus `explain` when one is broken
```

There is no update and no delete in v1. That is worth stating plainly because of what it means for
conflicts: **the only write this app makes reads nothing, so it cannot conflict.** Two people adding
a contact at the same instant are two contacts, because neither client chose an id. The
`ConflictError` handler in `server.ts` is therefore written and mapped to a `409` that the UI
renders as a sentence, but the app's own routes cannot trip it — the first `PATCH /contacts/:id`
will, because read-then-write is where the guard comes from (§12.1). See FRICTION #12.

## Not a scenario

Nothing here is in `check.sh` or the scenario roster, and nothing here should be. Scenarios assert;
this observes. It uses the real binary and the real SDK for the same reason scenarios do.
