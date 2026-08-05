# borg-sdk

Author a Borg repo in TypeScript: declare structs, write pipelines, serve them to the engine.

This is the **author-side** half of the SDK story. The consumer-side client — transactions, `fork`,
`commitAndMerge` — is a separate artifact and is not here yet. Same name, opposite directions:
the `Company` below is the *source* of a definition, and the client's `Company` will be *generated
from* one.

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
| `ref(N)`      | `Ref`           | `@o-1234abcd`    |                                                   |
| `list(T)`     | `Ref`           | `@l-5678wxyz`    | the handle; element access is not in v1           |

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

**There is no nx here, and no workspace file.** Both arrive with the second TypeScript package, which
is when a build graph starts paying for itself; one package with `tsc` and `vitest` does not need
one. `pnpm-lock.yaml` lives in this directory for the same reason.
