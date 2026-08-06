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
./dev.sh --reset      # …after throwing the registry's store away
./dev.sh --no-ui      # api only, on :8787
./dev.sh --stop       # stop the dev server and exit
./smoke.sh            # drive the whole thing headless and check it still works
```

Needs `node` 22.18+ (it runs `.ts` files by stripping types), `pnpm`, and a `cargo build -p borg-cli
-p borg-server` — `dev.sh` does the last one for you if either binary is missing.

Then: <http://localhost:5173>.

### dev.sh does not own the server

`borg-server` is a process that stays up; `dev.sh` is a script you `^C`. So it **ensures** one —
`borg-server status || borg-server start` — and leaves it running when the script stops. Only the
api and vite are the script's to kill. `./dev.sh --stop` is the verb for when you do want it gone.

1. builds `borg`, `borg-server` and `packages/borg-sdk` if any is stale, and links the SDK into
   `repo/` and `api/` (it is not published, so it is linked the way every scenario links it);
2. ensures a server on `data/borg.sock`, hosting `data/` as a **directory of registries** (§17.6);
3. ensures the `personal-crm` registry *through the server*, because a directory appearing under a
   running server's data dir is a store it has not locked and will not route to;
4. `borg --url borg+unix://data/borg.sock/personal-crm repo push repo/` — **into the running
   server**. `repo_push` is a protocol message and the server performs the push against a path on
   its own disk (§17.6), so there is no push-before-serve ordering left and no restart when you edit
   the schema. This file used to be built around the opposite and said so at length;
5. `borg --url … generate --lang ts -o api/gen`, reading the schema the push just landed;
6. the api with `BORG_URL` set, then vite.

**One string configures every client here** (§17.7): `borg+unix://<socket>/personal-crm` names the
socket and the registry together, and the CLI, the generator and the api are all pointed at it by
copying one variable. The socket is named rather than left to the well-known `borg://localhost`
address on purpose — that is one address per machine, and a demo that took it would fight whatever
else you are running.

It is re-runnable, and step 4 pushes unconditionally. `repo push` is a diff over both halves of what
a repo describes — definitions the branch already holds emit nothing, and a producer whose
implementation fingerprint has not moved emits nothing either (§9.2) — so pushing an unchanged repo
lands no def layer and says `unchanged: 7 definitions already in force, nothing pushed`. This script
used to carry its own `cksum` stamp to avoid pushing; both that and the FRICTION entries behind it
(#2, #17) are gone.

The other side of the same change: when you *do* edit `repo/pipelines/display_name.ts`, re-running
this script recomputes every `displayName` in the store under the new code — in the server that is
already running. Before, it recomputed none of them and served both builds' output side by side,
each labelled `current`.

### What `--reset` does, and why it stops the server

It stops the server, deletes `data/personal-crm/`, and starts it again. There is no
`registry_delete` on the protocol and deliberately so: destroying a store over a wire whose
`credential` nothing checks yet is exactly the shape `borg-server stop` avoided by being a `SIGTERM`
rather than a message. Throwing a store away is a thing you do with filesystem access, on purpose.
Only the registry's directory goes — the server's pidfile and log live beside it in `data/`, and the
log is where the reason a previous run died is written.

### The api survives the server restarting

`api/server.ts` connects with `connect: "on-demand"` and the SDK owns the connection: if no server
is running when the api starts, every request answers `503` with

```
no borg server at data/borg.sock — start one with: borg-server start
```

and the moment a server appears the next request works — no restart, and no connection lifecycle in
the application. That is FRICTION #11, fixed in the app that reported it. `smoke.sh` boots the whole
stack headless, exercises create/list/detail, and then does exactly that: stops the server, starts
the api against nothing, asserts the sentence, starts a server underneath it, and asserts it
recovers.

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
