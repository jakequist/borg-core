# borg-sdk

Borg in TypeScript. **Two entry points, deliberately opposite:**

- `borg-sdk` — the **author side**. Declare structs, write pipelines, serve them to the engine.
- `borg-sdk/client` — the **consumer side**. Read and write through transactions over `borg-server`,
  addressed by a connection url: `createBorgContext({ url: "borg://localhost/<registry>" })`, or
  `$BORG_URL`. The context reconnects on its own when the server it is talking to restarts.

Same struct name, opposite directions. The `Company` below is the *source* of a definition; a
client's `Company` is *generated from* one by `borg generate` and carries the def-version it was
generated at. One package because they share what matters — the value conversion table and the
message framing — and a second package would have meant a second copy of both.

```ts
#!/usr/bin/env node
import { borg } from "borg-sdk";

const Company = borg.struct("Company", {
  website: borg.string(),
  headcount: borg.int(),
  isInvestible: borg.bool().derived(),
});

const invest = borg.pipeline(
  "invest",
  Company,
  { writes: ["isInvestible"] },
  async (c) => {
    const website = await c.get("website");
    const headcount = await c.get("headcount");
    await c.set("isInvestible", website?.endsWith(".ai") === true && (headcount ?? 0) > 10);
  },
);

await borg.repo({ id: 1, structs: [Company], pipelines: [invest] }).main();
```

Drop that in `pipelines/` inside a repo directory, make it executable, and `borg repo push` it. The
module is the whole repo: run with `describe` it prints its definitions, run without it serves
invocations.

## The three things worth knowing

**The SDK records nothing.** Every `get` and `set` is a wire message, and the engine records the
read-set server-side. That is what makes invalidation field-granular — write a field the pipeline
never read and nothing re-runs — without a line of tracking code here. There is no cache to
invalidate and no dependency graph to get wrong. A `Proxy`, when property-access sugar lands, will
only *translate* accesses into the same messages.

**Every access is `await`ed, on purpose.** `await c.get("headcount")` is verbose, and the alternative
is worse: preloading the entity would collapse the server-recorded read-set to object granularity and
destroy exactly the invalidation above. Sync-over-async via `worker_thread` + `Atomics.wait` is the
later path and changes none of the semantics.

**Ownership is stated twice and checked both ways.** `.derived()` on the field says a pipeline owns
it; `writes: [...]` on the pipeline says which one. Assembling the description errors if they
disagree in either direction — a `derived()` field nobody writes is a cell nothing can ever fill, and
a `writes` naming a field that is not `derived()` is a write the engine will refuse. Both are static
facts, so both fail at push time rather than mid-round.

## Values cross the wire as text

`42`, `true`, `~`, `acme.ai`, `@o-1234abcd` — the same forms the CLI accepts. The SDK converts:

| Field type    | JS value        | Wire text        | Notes                                            |
| ------------- | --------------- | ---------------- | ------------------------------------------------ |
| `string()`    | `string`        | the text itself  | no reserved spellings, except `~` — see below     |
| `int()`       | `number`        | `42`, `-1`       | refuses anything past 2⁵³; use `bigint()`         |
| `double()`    | `number`        | `1.5`, `1`       | refuses `NaN` and infinities, as the engine does  |
| `bool()`      | `boolean`       | `true` / `false` |                                                   |
| `binary()`    | `Uint8Array`    | `0xdeadbeef`     | whole octets only                                 |
| `bigint()`    | `bigint`        | `-129n`          | reads with or without the suffix, writes with it  |
| `ref(N)`      | `Ref`           | `@o-1234abcd`    | `Ref<"N">`, a branded string, on the client side   |
| `list(T)`     | `Ref`           | `@l-5678wxyz`    | the handle; element access is not in v1           |

**One row has two carriers**, and it is the only one. A pipeline gets a `Ref` object, whose
`.cell(struct, field)` is how it builds the address for a `world` hop; a client gets the PID as a
branded string, because its next move is always `tx.object(Struct, it)` and because a class cannot
carry the target struct in its *type* — there is one `Ref` class for every struct, and the brand is
per struct. The client's conversion is defined in terms of the pipeline's, so the wire text and the
validation are shared: two carriers is a language question, two tables would be a contract one.

Two rules that are easy to get wrong and are therefore enforced:

- **`null` is absence in both directions.** A cell never written and a cell holding a tombstone both
  read as `null`; writing `null` writes a tombstone. The store distinguishes the two and a pipeline
  has nothing different to do with them.
- **`int()` refuses what it cannot hold.** `Int` is an `i64` and a JS number is a double, so
  everything past 2⁵³ is representable and wrong. Returning the rounded number would be data that
  looks almost right, which is the worst available outcome.
- **A `String` field cannot hold `~`.** That is the tombstone form on every declared type, so the
  write would read back as a deletion. `encode` refuses it and points at `null`.

`world.get(cell)` / `world.set(cell, value)` are the random-access hops beyond the input entity, and
are stringly in v1: a cell is its text address, a value is its text form unless you pass a field type
to convert with (`world.get(cell, borg.int())`). Generated types slot into that second argument.

## The client half: `borg-sdk/client`

```ts
import { ConflictError } from "borg-sdk/client";
import { Company, createBorgContext } from "./borg.generated.js";  // borg generate --lang ts -o .

const bc = await createBorgContext({ socket: process.env.BORG_SOCKET! });
const tx = await bc.branch("main").begin();

const c = tx.object(Company, "o-100");        // a handle; no I/O yet
const hc = await c.get("headcount");          // read → recorded server-side → guarded at commit
await c.set("headcount", (hc ?? 0) + 1);

const fresh = await tx.create(Company);       // the server allocates the id; `fresh.id` is it
await fresh.set("website", "acme.ai");

try {
  await tx.commit();
} catch (err) {
  if (err instanceof ConflictError) console.error(`${err.cell} moved under us`);
}
```

**Nothing is cached and nothing is retried.** Every access is a wire message, as on the author side
and for the same reason: the engine records the read-set, and that read-set *is* the transaction's
guard. Preloading the object would make the guard object-granular. `ConflictError` is contract, not
an implementation detail — a rejected commit names the cell that moved, and what to do about that is
the application's decision, so there is no `transact(fn)` retry wrapper.

**`get` is the value; `resolve` is the value and its provenance.** Both are one round trip, because
the protocol never answers a read with a bare value — `get` is discarding an envelope, not saving a
message. Use `resolve` when you need `state` (`current` / `stale` / `broken` / …), `origin`, or which
producer computed it. Reads outside a transaction always answer the envelope, because they buy no
protection at commit and the envelope is the only thing telling you how much to trust them.

The one place the shortcut refuses to shorten: a `broken` cell throws rather than answering `null`.
`broken` means *no value is reachable at your version* — a value written past a schema change with no
`down` migration, or a producer that failed — and answering `null` would turn that into "nothing was
ever written here", which is a different fact.

**Generated code is pinned to a schema.** `createBorgContext` in a generated module sends the
def-layer it was generated at, so a client that has not been regenerated keeps writing the shape it
knows and reads newer values back through `down` migrations. That is the point of generating rather
than hand-writing: an un-generated client has no version to state and is read as "the schema as it
stands", which is honest but is not the same thing.

Reference fields come back as branded strings — `Ref<"Employee">` is the PID at runtime and the
target struct to the compiler, so `tx.object(Company, employeeRef)` will not compile. Fields a
producer owns are emitted `readonly`, so neither will a write to one.

**`tx.create(Struct)` makes an object; `bc.branch(n).list(Struct)` finds them.** `create` allocates
the id server-side, under an allocator of its own, so nothing an application creates can collide with
a `Company#1` somebody wrote by hand — and the id it answers with is branded, so it goes straight
into a reference field with no cast. `list` answers the ids of one struct at head, skipping deleted
objects, and it is **not** part of a transaction: "the set of Companies" is not a cell, so there is
nothing a guard could be asked about it (§9.6). It answers ids and nothing else, so a name per object
is a read per object — the N+1 is visible on purpose, and the query layer that would remove it is out
of scope.

## The socket, and why your stdout is yours

The worker protocol can run over a worker's own stdin and stdout, and the shell pipelines do. That is
not survivable here: one `console.log` — yours, a dependency's, a runtime warning — would corrupt the
stream, and the failure would surface far from its cause.

So a repo written with this SDK declares `"transport": "socket"` in its `describe` output. The engine
reads that *before* it spawns anything, listens on a unix socket, and passes the path in
`BORG_WORKER_SOCKET`. The protocol lives on that descriptor and **stdout is entirely yours**. The
engine points a worker's stdout at its own stderr, so what you print is visible and can never be
mistaken for a message or corrupt the CLI's own output.

The transport is declared and never sniffed. A detector would have to tell "has not connected yet"
from "printed to stdout first", which is precisely the case the socket exists to make harmless.

## Development

```
pnpm install
pnpm run check        # typecheck, unit tests, build
```

Node 20+. **Zero runtime dependencies** — the protocol is newline-delimited JSON over a socket, and
`node:net` plus a twenty-line reader is the whole of it. The dev dependencies are `typescript`,
`vitest` and `@types/node`, and nothing else.

Running a `.ts` pipeline directly (`#!/usr/bin/env node` on a `.ts` file) needs Node 22.18+, which
strips types without a build step. On older Node, compile the pipeline or write it as `.mjs`.

`tsconfig.json` sets `erasableSyntaxOnly`, so nothing in this package uses syntax that needs a
compiler to *emit* something — enums, parameter properties, namespaces. Code an author might copy
from here has to run under type stripping.

**There is no nx here, and no workspace file.** Both arrive with the second TypeScript *package* —
which the client was deliberately not made into. It shares `values.ts` and `lines.ts` with the author
side, and splitting it out would have meant either duplicating the conversion table (two tables for
one contract, which is the thing the Python gate exists to prevent) or standing up a workspace to
share it. One package with `tsc` and `vitest` still does not need a build graph. `pnpm-lock.yaml`
lives in this directory for the same reason.

`test/generated/borg.generated.ts` is `borg generate`'s output for a fixture schema, checked in
unedited: `crates/borg-cli/src/generate.rs` asserts the emitter still produces exactly it, and
`pnpm run typecheck` asserts it is valid TypeScript. Regenerate it with
`BORG_UPDATE_GOLDEN=1 cargo test -p borg-cli`.
